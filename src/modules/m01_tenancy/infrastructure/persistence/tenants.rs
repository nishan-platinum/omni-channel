//! TenantRepository (tenantadm.tenants and the atomic provisioning bundle).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::*;
use super::super::super::domain::ids::{TenantCode, TenantId};
use super::super::super::domain::storage::{IsolationMode, Region, StorageStrategy};
use super::super::super::domain::tenant::{IsolationCheckStatus, ProvisioningStatus, Tenant, TenantStatus, TransitionPlan};
use super::{decode_cursor, encode_cursor, like_pattern, map_unique, write_changes, PgStore};

pub(crate) const TENANT_COLS: &str = "t.id, t.tenant_code, t.name, t.legal_name, t.region, t.plan_id, t.status, t.parent_tenant_id, \
    t.primary_admin_email::text AS primary_admin_email, t.storage_strategy, t.isolation_mode, t.is_sandbox, t.sandbox_of_tenant_id, \
    t.template_code, t.inheritance_flags, t.provisioning_status, t.isolation_check_status, t.isolation_checked_at, t.suspended_reason, \
    t.status_reason, t.activated_at, t.grace_until, t.terminated_at, t.purge_after, t.purged_at, t.legal_hold, t.platform_version, \
    t.created_at, t.updated_at, t.version";

fn bad(e: impl std::fmt::Debug) -> sqlx::Error {
    sqlx::Error::Decode(format!("invalid stored value: {e:?}").into())
}

pub(crate) fn tenant_from_row(r: &PgRow) -> Result<Tenant, sqlx::Error> {
    Ok(Tenant {
        id: TenantId(r.try_get("id")?),
        code: TenantCode::parse(r.try_get::<String, _>("tenant_code")?.as_str()).map_err(bad)?,
        name: r.try_get("name")?,
        legal_name: r.try_get("legal_name")?,
        region: Region::parse(r.try_get::<String, _>("region")?.as_str()).map_err(bad)?,
        plan_id: r.try_get("plan_id")?,
        status: TenantStatus::parse(r.try_get::<String, _>("status")?.as_str()).map_err(bad)?,
        parent_tenant_id: r.try_get::<Option<Uuid>, _>("parent_tenant_id")?.map(TenantId),
        primary_admin_email: r.try_get("primary_admin_email")?,
        storage_strategy: StorageStrategy::parse(r.try_get::<String, _>("storage_strategy")?.as_str()).map_err(bad)?,
        isolation_mode: IsolationMode::parse(r.try_get::<String, _>("isolation_mode")?.as_str()).map_err(bad)?,
        is_sandbox: r.try_get("is_sandbox")?,
        sandbox_of_tenant_id: r.try_get::<Option<Uuid>, _>("sandbox_of_tenant_id")?.map(TenantId),
        template_code: r.try_get("template_code")?,
        inheritance_flags: r.try_get("inheritance_flags")?,
        provisioning_status: ProvisioningStatus::parse(r.try_get::<String, _>("provisioning_status")?.as_str()).map_err(bad)?,
        isolation_check_status: IsolationCheckStatus::parse(r.try_get::<String, _>("isolation_check_status")?.as_str()).map_err(bad)?,
        isolation_checked_at: r.try_get("isolation_checked_at")?,
        suspended_reason: r.try_get("suspended_reason")?,
        status_reason: r.try_get("status_reason")?,
        activated_at: r.try_get("activated_at")?,
        grace_until: r.try_get("grace_until")?,
        terminated_at: r.try_get("terminated_at")?,
        purge_after: r.try_get("purge_after")?,
        purged_at: r.try_get("purged_at")?,
        legal_hold: r.try_get("legal_hold")?,
        platform_version: r.try_get("platform_version")?,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
        version: r.try_get("version")?,
    })
}

const SUMMARY_SQL: &str = "SELECT t.id, t.tenant_code, t.name, t.legal_name, t.region, t.status, t.provisioning_status, \
    t.isolation_check_status, p.code AS plan_code, p.tier, t.storage_strategy, t.is_sandbox, t.platform_version, t.created_at \
    FROM tenantadm.tenants t JOIN tenantadm.plans p ON p.id = t.plan_id";

fn summary_from_row(r: &PgRow) -> Result<TenantSummary, sqlx::Error> {
    Ok(TenantSummary {
        id: TenantId(r.try_get("id")?),
        code: r.try_get("tenant_code")?,
        name: r.try_get("name")?,
        legal_name: r.try_get("legal_name")?,
        region: r.try_get("region")?,
        status: TenantStatus::parse(r.try_get::<String, _>("status")?.as_str()).map_err(bad)?,
        provisioning_status: ProvisioningStatus::parse(r.try_get::<String, _>("provisioning_status")?.as_str()).map_err(bad)?,
        isolation_check_status: IsolationCheckStatus::parse(r.try_get::<String, _>("isolation_check_status")?.as_str()).map_err(bad)?,
        plan_code: r.try_get("plan_code")?,
        tier: r.try_get("tier")?,
        storage_strategy: r.try_get("storage_strategy")?,
        is_sandbox: r.try_get("is_sandbox")?,
        platform_version: r.try_get("platform_version")?,
        created_at: r.try_get("created_at")?,
    })
}

#[async_trait]
impl TenantRepository for PgStore {
    async fn code_exists(&self, code: &str) -> AppResult<bool> {
        // Platform-wide uniqueness (BR-M01-001) is checked under system scope.
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM tenantadm.tenants WHERE tenant_code = $1)")
            .bind(code)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(exists)
    }

    async fn insert_provisioned(&self, scope: &AccessScope, b: &NewTenantBundle, changes: ChangeSet) -> AppResult<()> {
        let t = &b.tenant;
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.tenants (id, tenant_code, name, legal_name, region, plan_id, status, parent_tenant_id,
                primary_admin_email, storage_strategy, isolation_mode, is_sandbox, sandbox_of_tenant_id, template_code,
                inheritance_flags, provisioning_status, isolation_check_status, platform_version, created_at, updated_at, created_by, updated_by)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::citext,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$19,$20,$20)",
        )
        .bind(t.id.0)
        .bind(t.code.as_str())
        .bind(&t.name)
        .bind(&t.legal_name)
        .bind(t.region.as_str())
        .bind(t.plan_id)
        .bind(t.status.as_str())
        .bind(t.parent_tenant_id.map(|p| p.0))
        .bind(&t.primary_admin_email)
        .bind(t.storage_strategy.as_str())
        .bind(t.isolation_mode.as_str())
        .bind(t.is_sandbox)
        .bind(t.sandbox_of_tenant_id.map(|p| p.0))
        .bind(&t.template_code)
        .bind(&t.inheritance_flags)
        .bind(t.provisioning_status.as_str())
        .bind(t.isolation_check_status.as_str())
        .bind(&t.platform_version)
        .bind(t.created_at)
        .bind(b.created_by)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_unique(e, "uq_tenants_tenant_code", "Tenant code already in use"))?;

        for (k, v) in &b.config {
            sqlx::query(
                "INSERT INTO tenantadm.tenant_configs (id, tenant_id, config_key, config_value, updated_by) VALUES ($1,$2,$3,$4,$5)",
            )
            .bind(Uuid::now_v7())
            .bind(t.id.0)
            .bind(k)
            .bind(v)
            .bind(b.created_by)
            .execute(&mut *tx)
            .await?;
        }
        for (k, on) in &b.flags {
            sqlx::query(
                "INSERT INTO tenantadm.tenant_feature_flags (id, tenant_id, feature_key, enabled, updated_by) VALUES ($1,$2,$3,$4,$5)",
            )
            .bind(Uuid::now_v7())
            .bind(t.id.0)
            .bind(k)
            .bind(on)
            .bind(b.created_by)
            .execute(&mut *tx)
            .await?;
        }
        for q in &b.quotas {
            sqlx::query(
                "INSERT INTO tenantadm.tenant_quotas (id, tenant_id, metric, limit_value, usage_value, soft_threshold, period, cycle_start, updated_by)
                 VALUES ($1,$2,$3,$4,$5,$6::numeric,$7,$8,$9)",
            )
            .bind(Uuid::now_v7())
            .bind(t.id.0)
            .bind(q.metric.as_str())
            .bind(q.limit)
            .bind(q.usage)
            .bind(q.soft_threshold)
            .bind(q.metric.period().as_str())
            .bind(q.cycle_start)
            .bind(b.created_by)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("INSERT INTO tenantadm.tenant_branding (id, tenant_id) VALUES ($1,$2)")
            .bind(Uuid::now_v7())
            .bind(t.id.0)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO tenantadm.tenant_release_preferences (id, tenant_id) VALUES ($1,$2)")
            .bind(Uuid::now_v7())
            .bind(t.id.0)
            .execute(&mut *tx)
            .await?;
        let c = &b.connection;
        sqlx::query(
            "INSERT INTO tenantadm.tenant_database_connections (id, tenant_id, engine, storage_strategy, target_name, host, port,
                database_name, schema_name, region, secret_ref, status)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending')",
        )
        .bind(Uuid::now_v7())
        .bind(t.id.0)
        .bind(c.engine.as_str())
        .bind(c.storage_strategy.as_str())
        .bind(&c.target_name)
        .bind(&c.host)
        .bind(c.port)
        .bind(&c.database_name)
        .bind(&c.schema_name)
        .bind(c.region.as_str())
        .bind(&c.secret_ref)
        .execute(&mut *tx)
        .await?;
        let k = &b.key;
        sqlx::query(
            "INSERT INTO tenantadm.tenant_keys (id, tenant_id, key_kind, key_ref, key_version, state, wrapped_dek, activated_at, rotate_after, created_by)
             VALUES ($1,$2,$3,$4,$5,$6,$7,now(),$8,$9)",
        )
        .bind(k.id)
        .bind(t.id.0)
        .bind(k.kind.as_str())
        .bind(&k.key_ref)
        .bind(k.key_version)
        .bind(k.state.as_str())
        .bind(&k.wrapped_dek)
        .bind(k.rotate_after)
        .bind(b.created_by)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO tenantadm.provisioning_runs (id, tenant_id, status, template_code, created_by) VALUES ($1,$2,'in_progress',$3,$4)",
        )
        .bind(b.run_id)
        .bind(t.id.0)
        .bind(&t.template_code)
        .bind(b.created_by)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get(&self, scope: &AccessScope, id: TenantId) -> AppResult<Option<Tenant>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query(&format!("SELECT {TENANT_COLS} FROM tenantadm.tenants t WHERE t.id = $1"))
            .bind(id.0)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(row.as_ref().map(tenant_from_row).transpose()?)
    }

    async fn list(&self, scope: &AccessScope, f: &TenantFilter) -> AppResult<Page<TenantSummary>> {
        let limit = f.limit.clamp(1, 200);
        let cursor = f.cursor.as_deref().and_then(decode_cursor);
        let q = f.q.as_deref().map(str::trim).filter(|s| !s.is_empty());
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(&format!(
            "{SUMMARY_SQL}
             WHERE ($1::text IS NULL OR t.tenant_code ILIKE $2 OR t.name ILIKE $2 OR t.legal_name ILIKE $2 OR t.id::text = $1)
               AND ($3::text IS NULL OR t.status = $3)
               AND ($4::text IS NULL OR t.region = $4)
               AND ($5::timestamptz IS NULL OR (t.created_at, t.id) < ($5, $6))
             ORDER BY t.created_at DESC, t.id DESC
             LIMIT $7"
        ))
        .bind(q)
        .bind(q.map(like_pattern))
        .bind(f.status.map(|s| s.as_str()))
        .bind(f.region.map(|r| r.as_str()))
        .bind(cursor.map(|c| c.0))
        .bind(cursor.map(|c| c.1))
        .bind(limit + 1)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let mut items = rows.iter().map(summary_from_row).collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() as i64 > limit {
            items.truncate(limit as usize);
            items.last().map(|t| encode_cursor(t.created_at, t.id.0))
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    async fn list_all_ids(&self, scope: &AccessScope) -> AppResult<Vec<(TenantId, TenantStatus)>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT id, status FROM tenantadm.tenants ORDER BY created_at").fetch_all(&mut *tx).await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok((TenantId(r.try_get("id")?), TenantStatus::parse(r.try_get::<String, _>("status")?.as_str()).map_err(bad)?)))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn ancestry(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<TenantId>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows: Vec<Uuid> = sqlx::query_scalar(
            "WITH RECURSIVE up(id, parent, depth) AS (
                 SELECT id, parent_tenant_id, 0 FROM tenantadm.tenants WHERE id = $1
                 UNION ALL
                 SELECT t.id, t.parent_tenant_id, up.depth + 1 FROM tenantadm.tenants t JOIN up ON t.id = up.parent
                 WHERE up.depth < 10)
             SELECT id FROM up WHERE depth > 0 ORDER BY depth",
        )
        .bind(id.0)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows.into_iter().map(TenantId).collect())
    }

    async fn subtree_height(&self, scope: &AccessScope, id: TenantId) -> AppResult<usize> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let h: i32 = sqlx::query_scalar(
            "WITH RECURSIVE down(id, depth) AS (
                 SELECT id, 1 FROM tenantadm.tenants WHERE id = $1
                 UNION ALL
                 SELECT t.id, down.depth + 1 FROM tenantadm.tenants t JOIN down ON t.parent_tenant_id = down.id
                 WHERE down.depth < 10)
             SELECT coalesce(max(depth), 1) FROM down",
        )
        .bind(id.0)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(h.max(1) as usize)
    }

    async fn children(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<TenantSummary>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(&format!("{SUMMARY_SQL} WHERE t.parent_tenant_id = $1 ORDER BY t.created_at"))
            .bind(id.0)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(summary_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn sandboxes_of(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<TenantSummary>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(&format!("{SUMMARY_SQL} WHERE t.sandbox_of_tenant_id = $1 AND t.status <> 'purged' ORDER BY t.created_at"))
            .bind(id.0)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(summary_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn apply_transition(
        &self,
        scope: &AccessScope,
        id: TenantId,
        expected_version: i32,
        p: &TransitionPlan,
        changes: ChangeSet,
    ) -> AppResult<Tenant> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query(&format!(
            "UPDATE tenantadm.tenants t SET
                 status = $3,
                 activated_at = coalesce(t.activated_at, $4),
                 suspended_reason = $5,
                 status_reason = $6,
                 grace_until = $7,
                 terminated_at = $8,
                 purge_after = $9,
                 purged_at = coalesce($10, t.purged_at),
                 updated_at = now(),
                 version = t.version + 1
             WHERE t.id = $1 AND t.version = $2 AND t.status = $11
             RETURNING {TENANT_COLS}"
        ))
        .bind(id.0)
        .bind(expected_version)
        .bind(p.to.as_str())
        .bind(p.set_activated_at)
        .bind(&p.suspended_reason)
        .bind(&p.reason)
        .bind(p.grace_until)
        .bind(p.terminated_at)
        .bind(p.purge_after)
        .bind(p.purged_at)
        .bind(p.from.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            // STD-003 optimistic concurrency.
            return Err(AppError::conflict("Tenant was modified concurrently; reload and retry"));
        };
        let t = tenant_from_row(&row)?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(t)
    }

    async fn update_details(
        &self,
        scope: &AccessScope,
        id: TenantId,
        expected_version: i32,
        name: &str,
        legal_name: Option<&str>,
        parent: Option<Option<TenantId>>,
        changes: ChangeSet,
    ) -> AppResult<Tenant> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query(&format!(
            "UPDATE tenantadm.tenants t SET name = $3, legal_name = $4,
                 parent_tenant_id = CASE WHEN $5 THEN $6 ELSE t.parent_tenant_id END,
                 updated_at = now(), version = t.version + 1
             WHERE t.id = $1 AND t.version = $2
             RETURNING {TENANT_COLS}"
        ))
        .bind(id.0)
        .bind(expected_version)
        .bind(name)
        .bind(legal_name)
        .bind(parent.is_some())
        .bind(parent.flatten().map(|p| p.0))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Err(AppError::conflict("Tenant was modified concurrently; reload and retry"));
        };
        let t = tenant_from_row(&row)?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(t)
    }

    async fn set_provisioning_status(&self, scope: &AccessScope, id: TenantId, status: ProvisioningStatus) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("UPDATE tenantadm.tenants SET provisioning_status = $2, updated_at = now() WHERE id = $1")
            .bind(id.0)
            .bind(status.as_str())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn record_isolation_check(
        &self,
        scope: &AccessScope,
        id: TenantId,
        passed: bool,
        results: Value,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("INSERT INTO tenantadm.isolation_checks (id, tenant_id, passed, results, actor_id) VALUES ($1,$2,$3,$4,$5)")
            .bind(Uuid::now_v7())
            .bind(id.0)
            .bind(passed)
            .bind(&results)
            .bind(actor)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE tenantadm.tenants SET isolation_check_status = $2, isolation_checked_at = now(), updated_at = now() WHERE id = $1",
        )
        .bind(id.0)
        .bind(if passed { "passed" } else { "failed" })
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_isolation_checks(&self, scope: &AccessScope, id: TenantId, limit: i64) -> AppResult<Vec<IsolationCheckRecord>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(
            "SELECT id, passed, results, created_at FROM tenantadm.isolation_checks WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2",
        )
        .bind(id.0)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| {
                Ok(IsolationCheckRecord {
                    id: r.try_get("id")?,
                    passed: r.try_get("passed")?,
                    results: r.try_get("results")?,
                    created_at: r.try_get("created_at")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn discard_draft(&self, scope: &AccessScope, id: TenantId, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        write_changes(&mut tx, &changes).await?;
        let n = sqlx::query("DELETE FROM tenantadm.tenants WHERE id = $1 AND status = 'draft' AND activated_at IS NULL")
            .bind(id.0)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n == 0 {
            return Err(AppError::conflict("Only never-activated drafts can be discarded"));
        }
        tx.commit().await?;
        Ok(())
    }

    async fn set_legal_hold(&self, scope: &AccessScope, id: TenantId, hold: bool, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("UPDATE tenantadm.tenants SET legal_hold = $2, updated_at = now(), version = version + 1 WHERE id = $1")
            .bind(id.0)
            .bind(hold)
            .execute(&mut *tx)
            .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_platform_version(&self, scope: &AccessScope, id: TenantId, version: &str) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("UPDATE tenantadm.tenants SET platform_version = $2, updated_at = now() WHERE id = $1")
            .bind(id.0)
            .bind(version)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn status_counts(&self, scope: &AccessScope) -> AppResult<Vec<(TenantStatus, i64)>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT status, count(*) AS n FROM tenantadm.tenants GROUP BY status").fetch_all(&mut *tx).await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok((TenantStatus::parse(r.try_get::<String, _>("status")?.as_str()).map_err(bad)?, r.try_get("n")?)))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn control_plane_probe(&self, own: TenantId, foreign: TenantId) -> AppResult<bool> {
        // (1) Reads under a foreign tenant context must see nothing of `own`.
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(foreign.0)).await?;
        let visible: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM tenantadm.tenants WHERE id = $1)
                  + (SELECT count(*) FROM tenantadm.tenant_configs WHERE tenant_id = $1)
                  + (SELECT count(*) FROM tenantadm.tenant_quotas WHERE tenant_id = $1)
                  + (SELECT count(*) FROM tenantadm.tenant_branding WHERE tenant_id = $1)",
        )
        .bind(own.0)
        .fetch_one(&mut *tx)
        .await?;
        tx.rollback().await?;
        // (2) Writes of `own` rows under a foreign context must be rejected by RLS WITH CHECK.
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(foreign.0)).await?;
        let write = sqlx::query(
            "INSERT INTO tenantadm.tenant_configs (id, tenant_id, config_key, config_value) VALUES ($1, $2, 'probe.isolation', 'true')",
        )
        .bind(Uuid::now_v7())
        .bind(own.0)
        .execute(&mut *tx)
        .await;
        let _ = tx.rollback().await;
        // (3) No context at all must fail closed.
        let mut conn = self.pool.acquire().await?;
        let no_ctx: i64 = sqlx::query_scalar("SELECT count(*) FROM tenantadm.tenant_configs WHERE tenant_id = $1")
            .bind(own.0)
            .fetch_one(&mut *conn)
            .await?;
        Ok(visible == 0 && write.is_err() && no_ctx == 0)
    }

    async fn due_lifecycle(&self, now: DateTime<Utc>) -> AppResult<(Vec<TenantId>, Vec<TenantId>)> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let grace: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM tenantadm.tenants WHERE status = 'grace' AND grace_until <= $1")
            .bind(now)
            .fetch_all(&mut *tx)
            .await?;
        let purge: Vec<Uuid> =
            sqlx::query_scalar("SELECT id FROM tenantadm.tenants WHERE status = 'terminated' AND purge_after <= $1 AND NOT legal_hold")
                .bind(now)
                .fetch_all(&mut *tx)
                .await?;
        tx.commit().await?;
        Ok((grace.into_iter().map(TenantId).collect(), purge.into_iter().map(TenantId).collect()))
    }
}
