//! Bake-off gateway node (ADR-0014). One process = one node; run two behind HAProxy
//! (`docker compose --profile gateway`).
//!
//! Environment:
//! * `GATEWAY_TOKEN` (required) — the shared bearer token
//! * `GATEWAY_DATABASE_URL` (required) — PostgreSQL; migrations run at startup
//! * `GATEWAY_REDIS_URL` — event stream between nodes (omit for a single node)
//! * `GATEWAY_FIXTURE` — fixture file path or http(s) URL (default `config/gateway-fixture.json`)
//! * `GATEWAY_BIND` (default `0.0.0.0:4000`), `GATEWAY_NODE_ID` (default hostname),
//!   `GATEWAY_DB_POOL` (default 32), `GATEWAY_SESSION_SECRET` (default derived from the token),
//!   `GATEWAY_DRAIN_SECS` (default 3)

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac};
use omni_m01::modules::gateway::application::{EventBus, Gateway};
use omni_m01::modules::gateway::infrastructure::bus::{LocalBus, RedisBus};
use omni_m01::modules::gateway::infrastructure::fixture::FixtureSource;
use omni_m01::modules::gateway::infrastructure::store::Store;
use omni_m01::modules::gateway::web::{router, GwState};
use sha2::Sha256;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let _ = dotenvy::dotenv();
    let json_logs = env("LOG_FORMAT").is_none_or(|f| f != "pretty");
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    if json_logs {
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let token = env("GATEWAY_TOKEN").ok_or_else(|| anyhow::anyhow!("GATEWAY_TOKEN is required"))?;
    if token.len() < 16 {
        anyhow::bail!("GATEWAY_TOKEN must be at least 16 characters");
    }
    let db_url = env("GATEWAY_DATABASE_URL").ok_or_else(|| anyhow::anyhow!("GATEWAY_DATABASE_URL is required"))?;
    let bind = env("GATEWAY_BIND").unwrap_or_else(|| "0.0.0.0:4000".into());
    let node_id = env("GATEWAY_NODE_ID")
        .or_else(|| env("HOSTNAME"))
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok().map(|s| s.trim().to_string()))
        .unwrap_or_else(|| "gateway".into());
    let pool: u32 = env("GATEWAY_DB_POOL").and_then(|v| v.parse().ok()).unwrap_or(32);
    let drain = Duration::from_secs(env("GATEWAY_DRAIN_SECS").and_then(|v| v.parse().ok()).unwrap_or(3));
    let fixture = FixtureSource::parse(&env("GATEWAY_FIXTURE").unwrap_or_else(|| "config/gateway-fixture.json".into()));
    let session_key = match env("GATEWAY_SESSION_SECRET") {
        Some(s) => s.into_bytes(),
        None => {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(token.as_bytes()).expect("any key length");
            mac.update(b"gateway-session-ids");
            mac.finalize().into_bytes().to_vec()
        }
    };

    // Bind first: /healthz answers 503 until the node is ready (contract C50).
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let store = Store::connect(&db_url, pool).await?;
    let bus: Arc<dyn EventBus> = match env("GATEWAY_REDIS_URL") {
        Some(url) => RedisBus::new(&url)?,
        None => Arc::new(LocalBus::default()),
    };
    let gw = Gateway::new(store, bus.clone(), node_id.clone(), fixture, session_key);
    let app = router(GwState { gw: gw.clone(), token: token.into() });

    let server_gw = gw.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(async move {
                shutdown_signal().await;
                // Rolling deploy: stop advertising health, tell every session to reconnect elsewhere
                // (spread over ~2 s), then stop.
                server_gw.ready.store(false, Ordering::Release);
                let (customers, agents) = server_gw.sessions.counts();
                tracing::info!(customers, agents, "shutdown: draining sessions");
                server_gw.sessions.begin_shutdown();
                tokio::time::sleep(drain).await;
            })
            .await
    });

    gw.start().await?;
    gw.ready.store(true, Ordering::Release);
    tracing::info!(node = %node_id, bind = %bind, bus = bus.name(), startup_ms = started.elapsed().as_millis() as u64, "gateway ready");
    server.await??;
    tracing::info!("gateway stopped");
    Ok(())
}

async fn shutdown_signal() {
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
