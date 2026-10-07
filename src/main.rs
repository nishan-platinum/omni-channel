//! Binary entry point: configuration, tracing, state wiring, background workers, HTTP server
//! with graceful shutdown (WebSocket sessions are told to reconnect elsewhere).

use std::sync::Arc;

use omni_m01::app::{build_router, build_state, spawn_background};
use omni_m01::platform::config::AppConfig;
use omni_m01::platform::observability::init_tracing;
use omni_m01::platform::time::SystemClock;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    // .env is a development convenience; real deployments inject environment variables.
    let _ = dotenvy::dotenv();
    let config = AppConfig::from_env()?;
    init_tracing(config.log_format);
    tracing::info!(env = config.app_env.as_str(), bind = %config.bind_addr, "starting omni-m01");

    let state = build_state(config.clone(), Arc::new(SystemClock)).await?;
    spawn_background(&state);
    let sessions = state.sessions.clone();
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(
        url = %config.public_base_url,
        startup_ms = started.elapsed().as_millis() as u64,
        "listening"
    );
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // Rolling deploy: every live WebSocket gets `reconnect` + close 1012, so clients move
            // to another node and resume by sequence number. Give the close frames a moment.
            let (agents, customers) = sessions.counts();
            tracing::info!(agents, customers, "shutdown signal received; draining WebSocket sessions");
            sessions.begin_shutdown();
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        })
        .await?;
    Ok(())
}

/// Ctrl-C locally, SIGTERM from Docker/Kubernetes.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
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
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
