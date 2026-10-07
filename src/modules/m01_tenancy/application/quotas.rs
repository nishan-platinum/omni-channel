//! Quotas, metering and noisy-neighbour guardrails (M01-F05; OCC-M01-R005, R012, R025;
//! BR-M01-004). `QuotaService::check_and_consume` is the reusable entry point for future modules
//! (M03/M04/M05 channels, M02 user creation, M12 campaigns, M24 reports, M09 BPM, M11 AI).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::Serialize;
use serde_json::json;

use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult};
use crate::platform::ratelimit::{RateDecision, TenantRateLimiter};

use super::super::domain::events::TenantEvent;
use super::super::domain::quota::{validate_limit, validate_threshold, QuotaDecision, QuotaLevel, QuotaMetric, QuotaState};
use super::super::domain::{DomainError, TenantId};
use super::context::{Access, Actor, M01Deps};
use super::ports::ChangeSet;

#[derive(Debug, Clone, Serialize)]
pub struct QuotaLine {
    pub metric: QuotaMetric,
    pub label: &'static str,
    pub period: &'static str,
    pub limit: i64,
    pub usage: i64,
    pub soft_threshold: f64,
    pub utilisation_pct: f64,
    pub level: QuotaLevel,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuotaView {
    pub tenant_id: TenantId,
    pub lines: Vec<QuotaLine>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumeOutcome {
    pub metric: QuotaMetric,
    pub usage: i64,
    pub limit: i64,
    pub warning: bool,
}

/// Metering meters (R025) and the quota each feeds (reference M21 ingestion).
pub const METERS: &[(&str, Option<QuotaMetric>)] = &[
    ("volume", Some(QuotaMetric::VolumeMonth)),
    ("emails_sent", Some(QuotaMetric::EmailsMonth)),
    ("ai_tokens", Some(QuotaMetric::AiTokensMonth)),
    ("channel_sessions", None),
    ("storage_bytes", None),
    ("api_calls", None),
    ("active_users", None),
];

pub struct QuotaService {
    deps: Arc<M01Deps>,
    limiter: TenantRateLimiter,
    limit_cache: Mutex<HashMap<TenantId, (u64, DateTime<Utc>)>>,
    api_calls: Mutex<HashMap<TenantId, i64>>,
}

pub fn month_of(now: DateTime<Utc>) -> NaiveDate {
    NaiveDate::from_ymd_opt(now.year(), now.month(), 1).unwrap_or_else(|| now.date_naive())
}

impl QuotaService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps, limiter: TenantRateLimiter::default(), limit_cache: Mutex::new(HashMap::new()), api_calls: Mutex::new(HashMap::new()) }
    }

    /// `GET /v1/tenants/{id}/quota` → limits, usage, thresholds. Users are a live count.
    pub async fn view(&self, actor: &Actor, id: TenantId) -> AppResult<QuotaView> {
        let scope = self.deps.authorize(actor, id, Access::Read, true).await?;
        self.view_scoped(&scope, id).await
    }

    pub async fn view_scoped(&self, scope: &AccessScope, id: TenantId) -> AppResult<QuotaView> {
        let now = self.deps.clock.now();
        let users = self.deps.identity.count_users(id).await?;
        let states = self.deps.quotas.list(scope, id).await?;
        let lines = states
            .into_iter()
            .map(|mut s| {
                if s.metric == QuotaMetric::Users {
                    s.usage = users;
                }
                QuotaLine {
                    metric: s.metric,
                    label: s.metric.label(),
                    period: s.metric.period().as_str(),
                    limit: s.limit,
                    usage: s.usage_at(now),
                    soft_threshold: s.soft_threshold,
                    utilisation_pct: (s.utilisation_at(now) * 1000.0).round() / 10.0,
                    level: s.level_at(now),
                }
            })
            .collect();
        Ok(QuotaView { tenant_id: id, lines })
    }

    /// Quota-bound action: check usage against the limit and record consumption atomically.
    /// Warns at the soft threshold (event + NT-002), blocks above 100% with 429 (BR-M01-004).
    pub async fn check_and_consume(&self, actor: &Actor, id: TenantId, metric: QuotaMetric, amount: i64) -> AppResult<ConsumeOutcome> {
        if !(1..=1_000_000_000).contains(&amount) {
            return Err(AppError::validation("amount", "Amount must be between 1 and 1000000000"));
        }
        if metric == QuotaMetric::ApiRequestsPerMinute {
            return Err(AppError::validation("metric", "API request rate is enforced automatically per request"));
        }
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        // Tenant users act only in an active tenant; the host/metering may also record usage for
        // draft/suspended tenants. Grace (read-only), terminated and purged tenants are frozen.
        use super::super::domain::TenantStatus as S;
        let permitted = match tenant.status {
            S::Active => true,
            S::Draft | S::Suspended => !actor.is_tenant_admin(),
            S::Grace | S::Terminated | S::Purged => false,
        };
        if !permitted {
            return Err(AppError::conflict(format!("Tenant is {}; quota-bound actions are not permitted", tenant.status.as_str())));
        }
        let now = self.deps.clock.now();
        if metric == QuotaMetric::Users {
            // Users is a current-state count: sync with the identity store before checking.
            let users = self.deps.identity.count_users(id).await?;
            self.deps.quotas.set_static_usage(&scope, id, QuotaMetric::Users, users).await?;
        }
        let r = self.deps.quotas.consume(&scope, id, metric, amount, now, actor.correlation_id.clone()).await?;
        match r.decision {
            QuotaDecision::Allowed { new_usage, raise_warning, .. } => {
                if let Some((meter, _)) = METERS.iter().find(|(_, q)| *q == Some(metric)) {
                    self.deps.quotas.add_meter(&scope, id, meter, month_of(now), amount).await?;
                }
                Ok(ConsumeOutcome {
                    metric,
                    usage: new_usage,
                    limit: r.state.limit,
                    warning: raise_warning || (new_usage as f64) >= r.state.soft_threshold * r.state.limit as f64,
                })
            }
            QuotaDecision::Blocked { retry_after_secs, .. } => Err(DomainError::QuotaExceeded {
                metric: metric.as_str().into(),
                message: metric.blocked_message().into(),
                retry_after_secs,
            }
            .into()),
        }
    }

    /// Host-configurable limits per tenant/tier (R012). SA only.
    pub async fn set_limits(&self, actor: &Actor, id: TenantId, changes: Vec<(QuotaMetric, i64, f64)>) -> AppResult<QuotaView> {
        actor.require_super_admin()?;
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let mut validated = Vec::new();
        for (m, limit, thr) in changes {
            validated.push((m, validate_limit(m, limit)?, validate_threshold(thr)?));
        }
        let before = self.deps.quotas.list(&scope, id).await?;
        let before_map: HashMap<_, _> = before.iter().map(|s| (s.metric, (s.limit, s.soft_threshold))).collect();
        let validated: Vec<_> = validated.into_iter().filter(|(m, l, t)| before_map.get(m) != Some(&(*l, *t))).collect();
        if !validated.is_empty() {
            let mut a = actor.audit(Some(id), "tenant_quota", Some(id.to_string()), "tenant.quota_limits_changed");
            a.before = Some(json!(validated.iter().map(|(m, _, _)| (m.as_str(), before_map.get(m))).collect::<HashMap<_, _>>()));
            a.after = Some(json!(validated.iter().map(|(m, l, t)| (m.as_str(), (l, t))).collect::<HashMap<_, _>>()));
            let changes = ChangeSet::new().with_audit(a).with_event(actor.event(
                id,
                &TenantEvent::QuotaLimitsChanged { metrics: validated.iter().map(|(m, _, _)| m.as_str().to_string()).collect() },
            ));
            self.deps.quotas.set_limits(&scope, id, &validated, actor.user_id, changes).await?;
            if let Ok(mut c) = self.limit_cache.lock() {
                c.remove(&id);
            }
        }
        self.view_scoped(&scope, id).await
    }

    /// REFERENCE metering ingestion (stand-in for M21 CDR/metering feeds). SA/system only.
    pub async fn record_metering(&self, actor: &Actor, id: TenantId, meter: &str, amount: i64) -> AppResult<Option<ConsumeOutcome>> {
        actor.require_super_admin()?;
        let (_, quota) = METERS.iter().find(|(m, _)| *m == meter).ok_or_else(|| AppError::validation("meter", "Unknown meter"))?;
        match quota {
            Some(q) => self.check_and_consume(actor, id, *q, amount).await.map(Some),
            None => {
                if amount < 1 {
                    return Err(AppError::validation("amount", "Amount must be positive"));
                }
                let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
                self.deps.quotas.add_meter(&scope, id, meter, month_of(self.deps.clock.now()), amount).await?;
                Ok(None)
            }
        }
    }

    /// Monthly usage statement (R025). Returns (meter, value) rows for the month.
    pub async fn statement(&self, actor: &Actor, id: TenantId, month: NaiveDate) -> AppResult<Vec<(String, i64)>> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        self.flush_api_calls().await;
        let mut rows = self.deps.quotas.meters(&scope, id, month).await?;
        if month == month_of(self.deps.clock.now()) {
            let users = self.deps.identity.count_users(id).await?;
            rows.retain(|(m, _)| m != "active_users");
            rows.push(("active_users".into(), users));
        }
        rows.sort();
        Ok(rows)
    }

    /// Per-request API rate limit for a tenant (noisy-neighbour protection, R012/API-006).
    pub async fn check_api_rate(&self, id: TenantId) -> AppResult<RateDecision> {
        let now = self.deps.clock.now();
        let cached = self.limit_cache.lock().ok().and_then(|c| c.get(&id).copied());
        let limit = match cached {
            Some((l, at)) if (now - at).num_seconds() < 60 => l,
            _ => {
                let states: Vec<QuotaState> = self.deps.quotas.list(&AccessScope::System, id).await?;
                let l = states.iter().find(|s| s.metric == QuotaMetric::ApiRequestsPerMinute).map(|s| s.limit.max(0) as u64).unwrap_or(600);
                if let Ok(mut c) = self.limit_cache.lock() {
                    c.insert(id, (l, now));
                }
                l
            }
        };
        let d = self.limiter.check(id.0, limit, now);
        if d.allowed {
            if let Ok(mut m) = self.api_calls.lock() {
                *m.entry(id).or_insert(0) += 1;
            }
        }
        Ok(d)
    }

    /// Flushes buffered API-call meters (avoids a DB write per request).
    pub async fn flush_api_calls(&self) {
        let drained: Vec<(TenantId, i64)> = match self.api_calls.lock() {
            Ok(mut m) => m.drain().collect(),
            Err(_) => return,
        };
        let month = month_of(self.deps.clock.now());
        for (t, n) in drained {
            if let Err(e) = self.deps.quotas.add_meter(&AccessScope::System, t, "api_calls", month, n).await {
                e.log();
            }
        }
    }
}
