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
use crate::modules::m10_hub::application::ports::ProviderSimulator;
use crate::modules::m10_hub::application::ports::{ChannelAdapter, RealtimeBus};
use crate::modules::m10_hub::application::sessions::SessionRegistry;
use crate::modules::m10_hub::application::HubService;
use crate::modules::m10_hub::infrastructure::bus::{LocalBus, RedisBus};
use crate::modules::m10_hub::infrastructure::channels::fake_meta_client::FakeMetaClient;
use crate::modules::m10_hub::infrastructure::channels::sip_sim::SimSipAdapter;
use crate::modules::m10_hub::infrastructure::channels::webchat::WebChatAdapter;
use crate::modules::m10_hub::infrastructure::channels::whatsapp_cloud::{CloudProvider, WhatsAppCloudAdapter, WhatsAppCloudConfig};
use crate::modules::m10_hub::infrastructure::channels::whatsapp_setup::MetaLink;
use crate::modules::m10_hub::infrastructure::persistence::PgHubRepository;
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
    /// M10 hub (gateway slice) and this node's live WebSocket sessions.
    pub hub: Arc<HubService>,
    pub sessions: Arc<SessionRegistry>,
    /// Admission control for customer WebSocket handshakes (token lookup + history replay hit the
    /// database). A reconnect storm — e.g. every client of a crashed node arriving at once — then
    /// queues here instead of starving the pool for message traffic.
    pub ws_admission: Arc<tokio::sync::Semaphore>,
    /// Real Meta only: token/number check + automatic webhook registration, shown on the status box.
    pub meta_link: Option<Arc<MetaLink>>,
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

    // ---- M10 hub (ADR-0011/0012) ----
    let bus: Arc<dyn RealtimeBus> = match &config.redis_url {
        Some(url) => {
            let b = RedisBus::new(url)?;
            if let Err(e) = b.ping().await {
                anyhow::bail!("REDIS_URL is set but Redis is unreachable: {e}");
            }
            Arc::new(b)
        }
        None => {
            tracing::warn!("REDIS_URL not set: hub uses the in-process bus (single node only)");
            Arc::new(LocalBus::default())
        }
    };
    let sessions = Arc::new(SessionRegistry::new());
    bus.start(sessions.clone());
    let hub_repo = Arc::new(PgHubRepository::new(db.app.clone()));
    let wa = &config.whatsapp;
    let provider = CloudProvider::parse(&wa.provider).ok_or_else(|| anyhow::anyhow!("unknown WHATSAPP_PROVIDER"))?;
    let whatsapp = WhatsAppCloudAdapter::new(WhatsAppCloudConfig {
        provider,
        base_url: wa.base_url.clone(),
        api_version: wa.api_version.clone(),
        access_token: wa.access_token.clone(),
        app_secret: wa.app_secret.clone(),
    })
    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let simulator: Option<Arc<dyn ProviderSimulator>> = match &wa.fake_control_url {
        Some(url) => Some(Arc::new(FakeMetaClient::new(url, &wa.access_token).map_err(|e| anyhow::anyhow!(e.to_string()))?)),
        None => None,
    };
    let adapters: Vec<Arc<dyn ChannelAdapter>> =
        vec![Arc::new(whatsapp), Arc::new(SimSipAdapter::new(&config.hub_sim_sip_secret)), Arc::new(WebChatAdapter)];
    let hub = Arc::new(HubService::new(
        hub_repo,
        bus,
        Arc::new(M01TenantGate { pool: db.app.clone(), default_idle_minutes: config.default_session_idle_minutes }),
        adapters,
        config.node_id.clone(),
        simulator,
    ));
    hub.set_send_rate(wa.send_rate_per_sec);
    hub.connect_adapters().await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
    tracing::info!(node = %config.node_id, bus = hub.bus.name(), whatsapp = provider.as_str(), whatsapp_api = %wa.base_url, "M10 hub ready");

    let db_max = config.db_max_connections as usize;
    let meta_link = (provider == CloudProvider::Meta).then(|| MetaLink::new(config.whatsapp.clone()));
    let state = AppState(Arc::new(AppInner {
        config,
        db,
        auth,
        m01,
        metrics,
        clock,
        dispatcher,
        hub,
        sessions,
        ws_admission: Arc::new(tokio::sync::Semaphore::new((db_max / 2).max(4))),
        meta_link,
    }));
    if state.config.hub_demo_seed {
        // Several nodes may start together against an empty database: retry once after a
        // concurrent seeder won the race.
        if let Err(e) = crate::demo_seed::seed(&state).await {
            tracing::warn!(error = %e, "hub demo seed failed; retrying once");
            tokio::time::sleep(Duration::from_secs(3)).await;
            crate::demo_seed::seed(&state).await?;
        }
    }
    Ok(state)
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
    spawn_hub_workers(state);
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

/// Hub workers (every node runs them; row locks make them safe to run concurrently):
/// outbound delivery, routing safety net, presence reaper.
fn spawn_hub_workers(state: &AppState) {
    if let Some(link) = &state.meta_link {
        link.clone().spawn();
    }
    let s = state.clone();
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_millis(250));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            // Keep going while there is a backlog (full batches); otherwise wait for the tick.
            loop {
                match s.hub.delivery_tick().await {
                    Ok(n) if n as i64 >= crate::modules::m10_hub::application::service::DELIVERY_BATCH => continue,
                    Ok(_) => break,
                    Err(e) => {
                        e.log();
                        break;
                    }
                }
            }
        }
    });
    let s = state.clone();
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(2));
        loop {
            t.tick().await;
            if let Err(e) = s.hub.routing_tick().await {
                e.log();
            }
        }
    });
    let s = state.clone();
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(15));
        loop {
            t.tick().await;
            if let Err(e) = s.hub.reaper_tick().await {
                e.log();
            }
        }
    });
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// Readiness: the central DB must be reachable. Tenant DBs are reported separately (never fail
/// readiness because one tenant database is down).
async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let bus_ok = state.hub.bus.healthy().await;
    let (agents, customers) = state.sessions.counts();
    let hub = json!({ "node": state.hub.node_id, "bus": state.hub.bus.name(), "bus_ok": bus_ok, "agent_sockets": agents, "customer_sockets": customers });
    if !bus_ok {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": "not_ready", "hub": hub })));
    }
    match db::ping(&state.db.app).await {
        Ok(()) => (StatusCode::OK, Json(json!({ "status": "ready", "central_db": "ok", "hub": hub }))),
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
        .merge(crate::modules::m10_hub::web::routes())
        .nest_service("/static", ServeDir::new("static"))
        .fallback(crate::modules::m01_tenancy::web::html::not_found)
        .layer(from_fn_with_state(state.clone(), crate::modules::m01_tenancy::web::host_guard))
        .layer(RequestBodyLimitLayer::new(3 * 1024 * 1024))
        .layer(from_fn_with_state(headers, security_headers))
        .layer(from_fn_with_state(obs, request_context))
        .layer(CatchPanicLayer::new())
        .with_state(state)
}
