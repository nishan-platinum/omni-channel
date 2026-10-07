//! ConfigRepository and QuotaRepository.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};
use crate::platform::events::EventEnvelope;

use super::super::super::application::ports::*;
use super::super::super::domain::events::TenantEvent;
use super::super::super::domain::quota::{QuotaDecision, QuotaMetric, QuotaState};
use super::super::super::domain::TenantId;
use super::{write_changes, PgStore};

#[async_trait]
impl ConfigRepository for PgStore {
    async fn load(&self, scope: &AccessScope, id: TenantId) -> AppResult<StoredSettings> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let cfg = sqlx::query("SELECT config_key, config_value FROM tenantadm.tenant_configs WHERE tenant_id = $1")
            .bind(id.0)
            .fetch_all(&mut *tx)
            .await?;
        let flags = sqlx::query("SELECT feature_key, enabled FROM tenantadm.tenant_feature_flags WHERE tenant_id = $1")
            .bind(id.0)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        let mut s = StoredSettings::default();
        for r in cfg {
            s.config.insert(r.try_get("config_key")?, r.try_get("config_value")?);
        }
        for r in flags {
            s.flags.insert(r.try_get("feature_key")?, r.try_get("enabled")?);
        }
        Ok(s)
    }

    async fn apply(
        &self,
        scope: &AccessScope,
        id: TenantId,
        config: &BTreeMap<String, Value>,
        flags: &BTreeMap<String, bool>,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        for (k, v) in config {
            sqlx::query(
                "INSERT INTO tenantadm.tenant_configs (id, tenant_id, config_key, config_value, updated_by) VALUES ($1,$2,$3,$4,$5)
                 ON CONFLICT (tenant_id, config_key) DO UPDATE
                   SET config_value = EXCLUDED.config_value, updated_by = EXCLUDED.updated_by, updated_at = now(),
                       version = tenantadm.tenant_configs.version + 1",
            )
            .bind(Uuid::now_v7())
            .bind(id.0)
            .bind(k)
            .bind(v)
            .bind(actor)
            .execute(&mut *tx)
            .await?;
        }
        for (k, on) in flags {
            sqlx::query(
                "INSERT INTO tenantadm.tenant_feature_flags (id, tenant_id, feature_key, enabled, updated_by) VALUES ($1,$2,$3,$4,$5)
                 ON CONFLICT (tenant_id, feature_key) DO UPDATE
                   SET enabled = EXCLUDED.enabled, updated_by = EXCLUDED.updated_by, updated_at = now(),
                       version = tenantadm.tenant_feature_flags.version + 1",
            )
            .bind(Uuid::now_v7())
            .bind(id.0)
            .bind(k)
            .bind(on)
            .bind(actor)
            .execute(&mut *tx)
            .await?;
        }
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn enabled_flags_for(&self, scope: &AccessScope, ids: &[TenantId]) -> AppResult<Vec<(TenantId, String)>> {
        let ids: Vec<Uuid> = ids.iter().map(|t| t.0).collect();
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT tenant_id, feature_key FROM tenantadm.tenant_feature_flags WHERE tenant_id = ANY($1) AND enabled")
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok((TenantId(r.try_get("tenant_id")?), r.try_get("feature_key")?)))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }
}

const QUOTA_COLS: &str =
    "metric, limit_value, usage_value, soft_threshold::float8 AS soft_threshold, cycle_start, warned_cycle_start, exhausted_cycle_start";

fn quota_from_row(r: &PgRow) -> Result<QuotaState, sqlx::Error> {
    let metric: String = r.try_get("metric")?;
    Ok(QuotaState {
        metric: QuotaMetric::parse(&metric).map_err(|e| sqlx::Error::Decode(format!("{e:?}").into()))?,
        limit: r.try_get("limit_value")?,
        usage: r.try_get("usage_value")?,
        soft_threshold: r.try_get("soft_threshold")?,
        cycle_start: r.try_get("cycle_start")?,
        warned_cycle_start: r.try_get("warned_cycle_start")?,
        exhausted_cycle_start: r.try_get("exhausted_cycle_start")?,
    })
}

fn quota_event(tenant: TenantId, ev: TenantEvent, correlation_id: &Option<String>, now: DateTime<Utc>) -> EventEnvelope {
    EventEnvelope {
        id: Uuid::now_v7(),
        event_type: ev.event_type().to_string(),
        tenant_id: Some(tenant.0),
        aggregate_id: tenant.to_string(),
        payload: ev.payload(tenant),
        correlation_id: correlation_id.clone(),
        occurred_at: now,
    }
}

#[async_trait]
impl QuotaRepository for PgStore {
    async fn list(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<QuotaState>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(&format!("SELECT {QUOTA_COLS} FROM tenantadm.tenant_quotas WHERE tenant_id = $1 ORDER BY metric"))
            .bind(id.0)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(quota_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn consume(
        &self,
        scope: &AccessScope,
        id: TenantId,
        metric: QuotaMetric,
        amount: i64,
        now: DateTime<Utc>,
        correlation_id: Option<String>,
    ) -> AppResult<QuotaConsumption> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        // Row lock serialises concurrent consumers of the same counter (no lost updates).
        let row = sqlx::query(&format!("SELECT {QUOTA_COLS} FROM tenantadm.tenant_quotas WHERE tenant_id = $1 AND metric = $2 FOR UPDATE"))
            .bind(id.0)
            .bind(metric.as_str())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| AppError::not_found("Quota is not configured for this tenant"))?;
        let mut state = quota_from_row(&row)?;
        let decision = state.evaluate(amount, now);
        let mut events = Vec::new();
        match &decision {
            QuotaDecision::Allowed { new_usage, cycle_start, reset_cycle, raise_warning, raise_exhausted } => {
                let warned = if *raise_warning {
                    Some(*cycle_start)
                } else if *reset_cycle {
                    None
                } else {
                    state.warned_cycle_start
                };
                let exhausted = if *raise_exhausted {
                    Some(*cycle_start)
                } else if *reset_cycle {
                    None
                } else {
                    state.exhausted_cycle_start
                };
                sqlx::query(
                    "UPDATE tenantadm.tenant_quotas SET usage_value = $3, cycle_start = $4, warned_cycle_start = $5,
                         exhausted_cycle_start = $6, updated_at = now(), version = version + 1
                     WHERE tenant_id = $1 AND metric = $2",
                )
                .bind(id.0)
                .bind(metric.as_str())
                .bind(new_usage)
                .bind(cycle_start)
                .bind(warned)
                .bind(exhausted)
                .execute(&mut *tx)
                .await?;
                state.usage = *new_usage;
                state.cycle_start = *cycle_start;
                state.warned_cycle_start = warned;
                state.exhausted_cycle_start = exhausted;
                if *raise_warning {
                    events.push(quota_event(
                        id,
                        TenantEvent::QuotaWarning { metric: metric.as_str().into(), usage: *new_usage, limit: state.limit },
                        &correlation_id,
                        now,
                    ));
                }
                if *raise_exhausted {
                    events.push(quota_event(
                        id,
                        TenantEvent::QuotaExhausted { metric: metric.as_str().into(), usage: *new_usage, limit: state.limit },
                        &correlation_id,
                        now,
                    ));
                }
            }
            QuotaDecision::Blocked { cycle_start, reset_cycle, raise_exhausted, .. } => {
                if *reset_cycle || *raise_exhausted {
                    let usage = if *reset_cycle { 0 } else { state.usage };
                    let warned = if *reset_cycle { None } else { state.warned_cycle_start };
                    let exhausted = if *raise_exhausted { Some(*cycle_start) } else { None };
                    sqlx::query(
                        "UPDATE tenantadm.tenant_quotas SET usage_value = $3, cycle_start = $4, warned_cycle_start = $5,
                             exhausted_cycle_start = $6, updated_at = now() WHERE tenant_id = $1 AND metric = $2",
                    )
                    .bind(id.0)
                    .bind(metric.as_str())
                    .bind(usage)
                    .bind(cycle_start)
                    .bind(warned)
                    .bind(exhausted)
                    .execute(&mut *tx)
                    .await?;
                }
                if *raise_exhausted {
                    // "101% send -> 429 + alert" (BR-M01-004).
                    events.push(quota_event(
                        id,
                        TenantEvent::QuotaExhausted { metric: metric.as_str().into(), usage: state.usage, limit: state.limit },
                        &correlation_id,
                        now,
                    ));
                }
            }
        }
        write_changes(&mut tx, &ChangeSet { audit: Vec::new(), events }).await?;
        tx.commit().await?;
        Ok(QuotaConsumption { decision, state })
    }

    async fn set_static_usage(&self, scope: &AccessScope, id: TenantId, metric: QuotaMetric, usage: i64) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("UPDATE tenantadm.tenant_quotas SET usage_value = $3, updated_at = now() WHERE tenant_id = $1 AND metric = $2")
            .bind(id.0)
            .bind(metric.as_str())
            .bind(usage.max(0))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_limits(
        &self,
        scope: &AccessScope,
        id: TenantId,
        limits: &[(QuotaMetric, i64, f64)],
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        for (m, limit, thr) in limits {
            sqlx::query(
                "UPDATE tenantadm.tenant_quotas SET limit_value = $3, soft_threshold = $4::numeric, updated_by = $5,
                     updated_at = now(), version = version + 1 WHERE tenant_id = $1 AND metric = $2",
            )
            .bind(id.0)
            .bind(m.as_str())
            .bind(limit)
            .bind(thr)
            .bind(actor)
            .execute(&mut *tx)
            .await?;
        }
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn add_meter(&self, scope: &AccessScope, id: TenantId, meter: &str, month: NaiveDate, delta: i64) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.usage_meters (id, tenant_id, metric, period_month, value) VALUES ($1,$2,$3,$4,$5)
             ON CONFLICT (tenant_id, metric, period_month) DO UPDATE
               SET value = tenantadm.usage_meters.value + EXCLUDED.value, updated_at = now()",
        )
        .bind(Uuid::now_v7())
        .bind(id.0)
        .bind(meter)
        .bind(month)
        .bind(delta)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn meters(&self, scope: &AccessScope, id: TenantId, month: NaiveDate) -> AppResult<Vec<(String, i64)>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows =
            sqlx::query("SELECT metric, value FROM tenantadm.usage_meters WHERE tenant_id = $1 AND period_month = $2 ORDER BY metric")
                .bind(id.0)
                .bind(month)
                .fetch_all(&mut *tx)
                .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok((r.try_get("metric")?, r.try_get("value")?)))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn max_utilisation(&self, scope: &AccessScope, ids: &[TenantId], now: DateTime<Utc>) -> AppResult<Vec<(TenantId, f64)>> {
        let ids_u: Vec<Uuid> = ids.iter().map(|t| t.0).collect();
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(&format!(
            "SELECT tenant_id, {QUOTA_COLS} FROM tenantadm.tenant_quotas WHERE tenant_id = ANY($1) AND period <> 'per_minute'"
        ))
        .bind(&ids_u)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let mut max: BTreeMap<TenantId, f64> = BTreeMap::new();
        for r in rows {
            let t = TenantId(r.try_get("tenant_id")?);
            let q = quota_from_row(&r)?;
            if q.limit > 0 {
                let u = q.utilisation_at(now);
                let e = max.entry(t).or_insert(0.0);
                if u > *e {
                    *e = u;
                }
            }
        }
        Ok(max.into_iter().collect())
    }
}
