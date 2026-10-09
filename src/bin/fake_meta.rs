//! `fake_meta` — stand-alone imitation of Meta's WhatsApp Cloud API for development and load tests
//! (see `omni_m01::fake_meta`, ADR-0013). Not WhatsApp: nothing leaves this process.
//!
//! Environment: FAKE_META_BIND (default 0.0.0.0:58090), FAKE_META_WEBHOOK_URL,
//! FAKE_META_ACCESS_TOKEN, FAKE_META_APP_SECRET, FAKE_META_RATE_PER_SEC, FAKE_META_LATENCY_MIN_MS,
//! FAKE_META_LATENCY_MAX_MS, FAKE_META_ERROR_RATE, FAKE_META_DUPLICATE_RATE,
//! FAKE_META_DELIVERED_AFTER_MS, FAKE_META_READ_AFTER_MS, FAKE_META_READ_RATIO,
//! FAKE_META_ENFORCE_WINDOW, FAKE_META_WEBHOOK_WORKERS.

use omni_m01::fake_meta::{router, FakeMeta, FakeMetaConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cfg = FakeMetaConfig::from_env();
    let bind = std::env::var("FAKE_META_BIND").unwrap_or_else(|_| "0.0.0.0:58090".into());
    tracing::info!(bind = %bind, webhook = %cfg.webhook_url, rate_per_sec = cfg.rate_per_sec, "fake-meta (NOT WhatsApp) starting");
    let app = router(FakeMeta::start(cfg));
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
