//! Application wiring: state, dependency injection, router and background workers.

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde_json::json;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::services::ServeDir;

use crate::bootstrap_auth::identity_adapter::BootstrapIdentityAdapter;
use crate::bootstrap_auth::service::AuthService;
use crate::modules::m01_tenancy::application::context::{M01Deps, M01Settings};
use crate::modules::m01_tenancy::application::handlers::{DownstreamCreatedConsumer, NotificationConsumer};
use crate::modules::m01_tenancy::application::M01Services;
use crate::modules::m01_tenancy::infrastructure::adapters::*;
use crate::modules::m01_tenancy::infrastructure::gate::M01TenantGate;
use crate::modules::m01_tenancy::infrastructure::persistence::PgStore;
use crate::modules::m01_tenancy::infrastructure::tenant_data::{targets::load_targets, DataPlaneRouter};
use crate::platform::config::AppConfig;
use crate::platform::db::{self, Db};
use crate::platform::events::{EventHandler, OutboxDispatcher};
use crate::platform::middleware::{request_context, security_headers, ObservabilityState, SecurityHeaderConfig};
use crate::platform::observability::TenantMetrics;
use crate::platform::time::Clock;

pub struct AppInner {
    pub config: AppConfig,
    pub db: Db,
    pub auth: Arc<AuthService>,
    pub m01: Arc<M01Services>,
    pub metrics: Arc<TenantMetrics>,
    pub clock: Arc<dyn Clock>,
    pub dispatcher: Arc<OutboxDispatcher>,
}

#[derive(Clone)]
pub struct AppState(pub Arc<AppInner>);

impl Deref for AppState {
    type Target = AppInner;
    fn deref(&self) -> &AppInner {
        &self.0
    }
}

/// Connects, migrates (owner role), seeds the development Super Admin and wires every service.
pub async fn build_state(config: AppConfig, clock: Arc<dyn Clock>) -> anyhow::Result<AppState> {
    let db = db::connect(&config).await?;
    if config.run_migrations {
        db::migrate(&db.owner).await?;
        tracing::info!("control-plane migrations applied");
    }
    let store = Arc::new(PgStore::new(db.app.clone()));
    let gate = Arc::new(M01TenantGate { pool: db.app.clone(), default_idle_minutes: config.default_session_idle_minutes });
    let auth = Arc::new(AuthService::new(
        db.app.clone(),
        gate,
        clock.clone(),
        config.session_absolute_hours,
        config.default_session_idle_minutes,
        config.api_token_ttl_minutes,
    ));
    if let (Some(email), Some(password)) = (&config.bootstrap_superadmin_email, &config.bootstrap_superadmin_password) {
        if auth.seed_super_admin(email, password).await.map_err(|e| anyhow::anyhow!(e.to_string()))? {
            tracing::info!(email = %email, "bootstrap Super Admin created");
        }
    }
    let targets = load_targets(&config.tenant_db_targets_file)?;
    tracing::info!(count = targets.len(), "dedicated tenant DB targets loaded");
    let settings = M01Settings {
        lifecycle: M01Settings::lifecycle_from_hours(config.grace_period_hours, config.retention_hours),
        platform_domain: config.platform_domain.clone(),
        public_base_url: config.public_base_url.clone(),
        ops_alert_email: config.bootstrap_superadmin_email.clone().unwrap_or_else(|| "platform-ops@localhost".into()),
        auto_purge: config.auto_purge,
        development: config.app_env.is_development(),
    };
    let verifier = Arc::new(SimulatedDnsVerifier { verified_suffixes: config.simulated_verified_suffixes.clone() });
    let deps = Arc::new(M01Deps {
        tenants: store.clone(),
        configs: store.clone(),
        quotas: store.clone(),
        branding: store.clone(),
        connections: store.clone(),
        provisioning: store.clone(),
        grants: store.clone(),
        baselines: store.clone(),
        releases: store.clone(),
        keys: store.clone(),
        exports: store.clone(),
        purge: store.clone(),
        analytics: store.clone(),
        audit: store.clone(),
        plans: store.clone(),
        templates: store.clone(),
        identity: Arc::new(BootstrapIdentityAdapter { auth: auth.clone() }),
        notifications: Arc::new(OutboxNotificationAdapter::new(db.app.clone(), config.app_env.is_development())),
        downstream: Arc::new(ReferenceDownstreamProvisioning),
        domain_verifier: verifier.clone(),
        sender_verifier: verifier,
        kms: Arc::new(LocalKms::new(&config.local_kms_master_key).map_err(|e| anyhow::anyhow!(e.to_string()))?),
        objects: Arc::new(LocalObjectStorage::new(config.data_dir.join("objects"))),
        release_port: Arc::new(ReferenceReleaseManager),
        anonymiser: Arc::new(ReferenceAnonymiser),
        export_participants: Vec::new(),
        data_router: Arc::new(DataPlaneRouter::new(db.app.clone(), db.owner.clone(), targets)),
        clock: clock.clone(),
        settings,
    });
    let metrics = Arc::new(TenantMetrics::default());
    let m01 = Arc::new(M01Services::new(deps.clone(), metrics.clone(), db.app.clone()));
    let handlers: Vec<Arc<dyn EventHandler>> = vec![Arc::new(NotificationConsumer { deps }), Arc::new(DownstreamCreatedConsumer)];
    let dispatcher = Arc::new(OutboxDispatcher::new(db.app.clone(), handlers));
    Ok(AppState(Arc::new(AppInner { config, db, auth, m01, metrics, clock, dispatcher })))
}

/// Background workers: outbox dispatch, lifecycle scheduler, release rollouts, API meter flush.
pub fn spawn_background(state: &AppState) {
    state.dispatcher.clone().spawn(Duration::from_secs(2));
    let s = state.clone();
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(10));
        loop {
            t.tick().await;
            s.m01.quotas.flush_api_calls().await;
        }
    });
    if state.config.scheduler_enabled {
        let s = state.clone();
        let every = Duration::from_secs(state.config.scheduler_interval_secs.max(5));
        tokio::spawn(async move {
            let mut t = tokio::time::interval(every);
            loop {
                t.tick().await;
                match s.m01.lifecycle.run_scheduler_tick().await {
                    Ok((term, purged)) if term + purged > 0 => tracing::info!(terminated = term, purged, "lifecycle scheduler tick"),
                    Ok(_) => {}
                    Err(e) => e.log(),
                }
                if let Err(e) = s.m01.releases.process_due().await {
                    e.log();
                }
            }
        });
    }
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// Readiness: the central DB must be reachable. Tenant DBs are reported separately (never fail
/// readiness because one tenant database is down).
async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    match db::ping(&state.db.app).await {
        Ok(()) => (StatusCode::OK, Json(json!({ "status": "ready", "central_db": "ok" }))),
        Err(e) => {
            tracing::warn!(error = %e, "readiness check failed");
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": "not_ready", "central_db": "unreachable" })))
        }
    }
}

pub fn build_router(state: AppState) -> Router {
    let obs = ObservabilityState { metrics: state.metrics.clone(), clock: state.clock.clone(), access_log: state.config.access_log };
    let headers = SecurityHeaderConfig { hsts: state.config.cookie_secure };
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .merge(crate::bootstrap_auth::web::routes())
        .merge(crate::modules::m01_tenancy::web::routes())
        .nest_service("/static", ServeDir::new("static"))
        .fallback(crate::modules::m01_tenancy::web::html::not_found)
        .layer(from_fn_with_state(state.clone(), crate::modules::m01_tenancy::web::host_guard))
        .layer(RequestBodyLimitLayer::new(3 * 1024 * 1024))
        .layer(from_fn_with_state(headers, security_headers))
        .layer(from_fn_with_state(obs, request_context))
        .layer(CatchPanicLayer::new())
        .with_state(state)
}
