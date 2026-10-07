//! Host tenant directory with tier, state, health, version and entitlement matrix
//! (OCC-M01-R023), single-tenant reads (`GET /v1/tenants/{id}`), and tenant DB connectivity.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;

use crate::platform::errors::{AppError, AppResult};
use crate::platform::events::pending_counts;
use crate::platform::observability::TenantMetrics;

use super::super::domain::plan::Plan;
use super::super::domain::tenant::TenantStatus;
use super::super::domain::{Tenant, TenantId};
use super::context::{Access, Actor, M01Deps};
use super::ports::{ConnectionProfile, IsolationCheckRecord, Page, ProvisioningRun, TenantFilter, TenantSummary};

#[derive(Debug, Clone, Serialize)]
pub struct HealthInfo {
    pub error_rate_pct: f64,
    pub requests_last_hour: u64,
    pub queue_depth: i64,
    pub max_quota_pct: f64,
    pub db_ok: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirectoryRow {
    pub tenant: TenantSummary,
    pub health: HealthInfo,
}

#[derive(Debug, Clone, Serialize)]
pub struct TenantDetail {
    pub tenant: Tenant,
    pub plan: Plan,
    pub parent: Option<Tenant>,
    pub children: Vec<TenantSummary>,
    pub sandboxes: Vec<TenantSummary>,
    pub sandbox_of: Option<Tenant>,
    pub connection: Option<ConnectionProfile>,
    pub run: Option<ProvisioningRun>,
    pub isolation_checks: Vec<IsolationCheckRecord>,
    pub health: HealthInfo,
    pub next_statuses: Vec<TenantStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EntitlementMatrix {
    pub features: Vec<&'static str>,
    pub rows: Vec<(TenantSummary, Vec<u8>)>,
    pub next_cursor: Option<String>,
}

pub struct DirectoryService {
    deps: Arc<M01Deps>,
    metrics: Arc<TenantMetrics>,
    pool: sqlx::PgPool,
}

impl DirectoryService {
    pub fn new(deps: Arc<M01Deps>, metrics: Arc<TenantMetrics>, pool: sqlx::PgPool) -> Self {
        Self { deps, metrics, pool }
    }

    pub async fn list(&self, actor: &Actor, filter: TenantFilter) -> AppResult<Page<TenantSummary>> {
        let scope = actor.platform_scope()?;
        self.deps.tenants.list(&scope, &filter).await
    }

    /// Directory rows with health signals, without N+1 queries.
    pub async fn directory(&self, actor: &Actor, filter: TenantFilter) -> AppResult<Page<DirectoryRow>> {
        let scope = actor.platform_scope()?;
        let page = self.deps.tenants.list(&scope, &filter).await?;
        let ids: Vec<TenantId> = page.items.iter().map(|t| t.id).collect();
        let now = self.deps.clock.now();
        let util: HashMap<_, _> = self.deps.quotas.max_utilisation(&scope, &ids, now).await?.into_iter().collect();
        let db: HashMap<_, _> = self.deps.connections.health(&scope, &ids).await?.into_iter().collect();
        let queues: HashMap<_, _> = pending_counts(&self.pool).await?.into_iter().collect();
        let items = page
            .items
            .into_iter()
            .map(|t| {
                let s = self.metrics.sample(t.id.0, now);
                let health = HealthInfo {
                    error_rate_pct: (s.error_rate() * 1000.0).round() / 10.0,
                    requests_last_hour: s.requests_last_hour,
                    queue_depth: queues.get(&t.id.0).copied().unwrap_or(0),
                    max_quota_pct: (util.get(&t.id).copied().unwrap_or(0.0) * 1000.0).round() / 10.0,
                    db_ok: db.get(&t.id).copied().flatten(),
                };
                DirectoryRow { tenant: t, health }
            })
            .collect();
        Ok(Page { items, next_cursor: page.next_cursor })
    }

    /// `GET /v1/tenants/{id}` — SA elevated reads are audited; TA only own tenant.
    pub async fn get(&self, actor: &Actor, id: TenantId) -> AppResult<Tenant> {
        let scope = self.deps.authorize(actor, id, Access::Read, true).await?;
        self.deps.load_tenant(&scope, id).await
    }

    pub async fn detail(&self, actor: &Actor, id: TenantId) -> AppResult<TenantDetail> {
        let scope = self.deps.authorize(actor, id, Access::Read, true).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        let plan = self.deps.plans.get(tenant.plan_id).await?.ok_or_else(|| AppError::not_found("Plan does not exist"))?;
        let platform = actor.is_super_admin();
        let parent = match tenant.parent_tenant_id {
            Some(p) => self.deps.tenants.get(&scope, p).await?,
            None => None,
        };
        let sandbox_of = match tenant.sandbox_of_tenant_id {
            Some(p) => self.deps.tenants.get(&scope, p).await?,
            None => None,
        };
        let children = if platform { self.deps.tenants.children(&scope, id).await? } else { Vec::new() };
        let sandboxes = self.deps.tenants.sandboxes_of(&scope, id).await?;
        let connection = self.deps.connections.get(&scope, id).await?;
        let run = self.deps.provisioning.latest_run(&scope, id).await?;
        let isolation_checks = self.deps.tenants.list_isolation_checks(&scope, id, 5).await?;
        let now = self.deps.clock.now();
        let s = self.metrics.sample(id.0, now);
        let util = self.deps.quotas.max_utilisation(&scope, &[id], now).await?;
        let queues: HashMap<_, _> = pending_counts(&self.pool).await?.into_iter().collect();
        let health = HealthInfo {
            error_rate_pct: (s.error_rate() * 1000.0).round() / 10.0,
            requests_last_hour: s.requests_last_hour,
            queue_depth: queues.get(&id.0).copied().unwrap_or(0),
            max_quota_pct: (util.first().map(|u| u.1).unwrap_or(0.0) * 1000.0).round() / 10.0,
            db_ok: connection.as_ref().and_then(|c| c.last_check_ok),
        };
        let next_statuses = tenant.status.next_statuses();
        Ok(TenantDetail { tenant, plan, parent, children, sandboxes, sandbox_of, connection, run, isolation_checks, health, next_statuses })
    }

    pub async fn status_counts(&self, actor: &Actor) -> AppResult<BTreeMap<String, i64>> {
        let scope = actor.platform_scope()?;
        Ok(self.deps.tenants.status_counts(&scope).await?.into_iter().map(|(s, n)| (s.as_str().to_string(), n)).collect())
    }

    pub async fn entitlement_matrix(&self, actor: &Actor, filter: TenantFilter) -> AppResult<EntitlementMatrix> {
        let scope = actor.platform_scope()?;
        let page = self.deps.tenants.list(&scope, &filter).await?;
        let ids: Vec<_> = page.items.iter().map(|t| t.id).collect();
        let enabled = self.deps.configs.enabled_flags_for(&scope, &ids).await?;
        let features: Vec<&'static str> = super::super::domain::features::FEATURES.iter().map(|f| f.key).collect();
        let mut by_tenant: HashMap<TenantId, Vec<String>> = HashMap::new();
        for (t, f) in enabled {
            by_tenant.entry(t).or_default().push(f);
        }
        let mut plans: HashMap<String, Plan> = HashMap::new();
        for p in self.deps.plans.list_active().await? {
            plans.insert(p.code.clone(), p);
        }
        let rows = page
            .items
            .into_iter()
            .map(|t| {
                let on = by_tenant.get(&t.id).cloned().unwrap_or_default();
                let plan = plans.get(&t.plan_code);
                // 2 = enabled, 1 = entitled but off, 0 = not entitled
                let cells = features
                    .iter()
                    .map(|f| {
                        if on.iter().any(|x| x == f) {
                            2
                        } else if plan.is_some_and(|p| p.entitles(f)) {
                            1
                        } else {
                            0
                        }
                    })
                    .collect();
                (t, cells)
            })
            .collect();
        Ok(EntitlementMatrix { features, rows, next_cursor: page.next_cursor })
    }

    /// Tenant DB connectivity test (shown separately from platform readiness).
    pub async fn check_connectivity(&self, actor: &Actor, id: TenantId) -> AppResult<ConnectionProfile> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let profile = self.deps.connections.get(&scope, id).await?.ok_or_else(|| AppError::not_found("No connection profile"))?;
        let started = Instant::now();
        let result = match self.deps.data_router.store_for(&profile).await {
            Ok(store) => store.ping().await.map(|_| ()),
            Err(e) => Err(e),
        };
        let latency = started.elapsed().as_millis() as i32;
        let (ok, msg) = match result {
            Ok(()) => (true, format!("{} reachable", profile.engine.label())),
            Err(e) => (false, format!("unreachable: {}", e.message)),
        };
        self.deps.connections.record_check(&scope, id, ok, &msg, latency, self.deps.clock.now()).await?;
        let mut a =
            actor.tenant_audit(id, "tenant.db_connectivity_checked", None, Some(serde_json::json!({ "ok": ok, "latency_ms": latency })));
        a.entity_type = "tenant_database_connection".into();
        self.deps.audit.record(&scope, vec![a]).await?;
        self.deps.connections.get(&scope, id).await?.ok_or_else(|| AppError::not_found("No connection profile"))
    }

    pub async fn connection(&self, actor: &Actor, id: TenantId) -> AppResult<Option<ConnectionProfile>> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        self.deps.connections.get(&scope, id).await
    }

    pub async fn plans(&self) -> AppResult<Vec<Plan>> {
        self.deps.plans.list_active().await
    }

    pub async fn templates(&self) -> AppResult<Vec<super::ports::ProvisioningTemplate>> {
        self.deps.templates.list().await
    }

    pub fn dedicated_targets(&self) -> Vec<super::ports::DbTargetInfo> {
        self.deps.data_router.dedicated_targets()
    }

    pub async fn failed_provisioning(&self, actor: &Actor) -> AppResult<Vec<(TenantId, String, chrono::DateTime<chrono::Utc>)>> {
        let scope = actor.platform_scope()?;
        self.deps.provisioning.failed_runs(&scope, 10).await
    }
}
