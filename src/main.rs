//! Binary entry point: configuration, tracing, state wiring, background workers, HTTP server.

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
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(
        url = %config.public_base_url,
        startup_ms = started.elapsed().as_millis() as u64,
        "listening"
    );
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        })
        .await?;
    Ok(())
}
