//! ConnectionRepository, ProvisioningRepository, SupportGrantRepository.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::*;
use super::super::super::domain::storage::{DatabaseEngine, Region, StorageStrategy};
use super::super::super::domain::support::{GrantStatus, SupportGrant};
use super::super::super::domain::TenantId;
use super::{is_unique_violation, write_changes, PgStore};

fn dec(e: impl std::fmt::Debug) -> sqlx::Error {
    sqlx::Error::Decode(format!("{e:?}").into())
}

fn profile_from_row(r: &PgRow) -> Result<ConnectionProfile, sqlx::Error> {
    Ok(ConnectionProfile {
        tenant_id: TenantId(r.try_get("tenant_id")?),
        engine: DatabaseEngine::parse(&r.try_get::<String, _>("engine")?).map_err(dec)?,
        storage_strategy: StorageStrategy::parse(&r.try_get::<String, _>("storage_strategy")?).map_err(dec)?,
        target_name: r.try_get("target_name")?,
        host: r.try_get("host")?,
        port: r.try_get("port")?,
        database_name: r.try_get("database_name")?,
        schema_name: r.try_get("schema_name")?,
        region: Region::parse(&r.try_get::<String, _>("region")?).map_err(dec)?,
        secret_ref: r.try_get("secret_ref")?,
        status: r.try_get("status")?,
        last_checked_at: r.try_get("last_checked_at")?,
        last_check_ok: r.try_get("last_check_ok")?,
        last_check_message: r.try_get("last_check_message")?,
        last_latency_ms: r.try_get("last_latency_ms")?,
    })
}

#[async_trait]
impl ConnectionRepository for PgStore {
    async fn get(&self, scope: &AccessScope, id: TenantId) -> AppResult<Option<ConnectionProfile>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query(
            "SELECT tenant_id, engine, storage_strategy, target_name, host, port, database_name, schema_name, region, secret_ref,
                    status, last_checked_at, last_check_ok, last_check_message, last_latency_ms
               FROM tenantadm.tenant_database_connections WHERE tenant_id = $1",
        )
        .bind(id.0)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(row.as_ref().map(profile_from_row).transpose()?)
    }

    async fn record_check(
        &self,
        scope: &AccessScope,
        id: TenantId,
        ok: bool,
        message: &str,
        latency_ms: i32,
        at: DateTime<Utc>,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "UPDATE tenantadm.tenant_database_connections
                SET last_checked_at = $2, last_check_ok = $3, last_check_message = $4, last_latency_ms = $5, updated_at = now()
              WHERE tenant_id = $1",
        )
        .bind(id.0)
        .bind(at)
        .bind(ok)
        .bind(message)
        .bind(latency_ms)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_status(&self, scope: &AccessScope, id: TenantId, status: &str) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "UPDATE tenantadm.tenant_database_connections SET status = $2, updated_at = now(), version = version + 1 WHERE tenant_id = $1",
        )
        .bind(id.0)
        .bind(status)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn health(&self, scope: &AccessScope, ids: &[TenantId]) -> AppResult<Vec<(TenantId, Option<bool>)>> {
        let ids: Vec<Uuid> = ids.iter().map(|t| t.0).collect();
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT tenant_id, last_check_ok FROM tenantadm.tenant_database_connections WHERE tenant_id = ANY($1)")
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok((TenantId(r.try_get("tenant_id")?), r.try_get("last_check_ok")?)))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }
}

#[async_trait]
impl ProvisioningRepository for PgStore {
    async fn start_run(&self, scope: &AccessScope, tenant: TenantId, template: Option<&str>, actor: Option<Uuid>) -> AppResult<Uuid> {
        let id = Uuid::now_v7();
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.provisioning_runs (id, tenant_id, status, template_code, created_by) VALUES ($1,$2,'in_progress',$3,$4)",
        )
        .bind(id)
        .bind(tenant.0)
        .bind(template)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn add_step(
        &self,
        scope: &AccessScope,
        run: Uuid,
        tenant: TenantId,
        step: &str,
        status: &str,
        detail: Option<&str>,
        started_at: DateTime<Utc>,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.provisioning_steps (id, run_id, tenant_id, step, status, detail, started_at, finished_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,now())",
        )
        .bind(Uuid::now_v7())
        .bind(run)
        .bind(tenant.0)
        .bind(step)
        .bind(status)
        .bind(detail.map(|d| d.chars().take(1000).collect::<String>()))
        .bind(started_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn finish_run(
        &self,
        scope: &AccessScope,
        run: Uuid,
        tenant: TenantId,
        ok: bool,
        duration_ms: i64,
        error: Option<&str>,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "UPDATE tenantadm.provisioning_runs SET status = $3, completed_at = now(), duration_ms = $4, error_summary = $5
              WHERE id = $1 AND tenant_id = $2",
        )
        .bind(run)
        .bind(tenant.0)
        .bind(if ok { "completed" } else { "failed" })
        .bind(duration_ms)
        .bind(error)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn latest_run(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Option<ProvisioningRun>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let run = sqlx::query(
            "SELECT id, status, template_code, started_at, completed_at, duration_ms, error_summary
               FROM tenantadm.provisioning_runs WHERE tenant_id = $1 ORDER BY started_at DESC LIMIT 1",
        )
        .bind(tenant.0)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(r) = run else {
            tx.commit().await?;
            return Ok(None);
        };
        let id: Uuid = r.try_get("id")?;
        let steps = sqlx::query("SELECT step, status, detail, started_at, finished_at FROM tenantadm.provisioning_steps WHERE run_id = $1 ORDER BY started_at, id")
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        let steps = steps
            .iter()
            .map(|s| {
                Ok(ProvisioningStep {
                    step: s.try_get("step")?,
                    status: s.try_get("status")?,
                    detail: s.try_get("detail")?,
                    started_at: s.try_get("started_at")?,
                    finished_at: s.try_get("finished_at")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        Ok(Some(ProvisioningRun {
            id,
            status: r.try_get("status")?,
            template_code: r.try_get("template_code")?,
            started_at: r.try_get("started_at")?,
            completed_at: r.try_get("completed_at")?,
            duration_ms: r.try_get("duration_ms")?,
            error_summary: r.try_get("error_summary")?,
            steps,
        }))
    }

    async fn failed_runs(&self, scope: &AccessScope, limit: i64) -> AppResult<Vec<(TenantId, String, DateTime<Utc>)>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(
            "SELECT t.id, t.tenant_code, t.updated_at FROM tenantadm.tenants t
              WHERE t.status = 'draft' AND t.provisioning_status = 'failed' ORDER BY t.updated_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok((TenantId(r.try_get("id")?), r.try_get("tenant_code")?, r.try_get("updated_at")?)))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }
}

const GRANT_COLS: &str = "id, tenant_id, requested_by, reason, incident_ref, duration_minutes, named_approver_id, status, decided_by, \
    decided_at, decision_note, starts_at, expires_at, revoked_at, use_count, created_at";

fn grant_from_row(r: &PgRow) -> Result<SupportGrant, sqlx::Error> {
    Ok(SupportGrant {
        id: r.try_get("id")?,
        tenant_id: TenantId(r.try_get("tenant_id")?),
        requested_by: r.try_get("requested_by")?,
        reason: r.try_get("reason")?,
        incident_ref: r.try_get("incident_ref")?,
        duration_minutes: r.try_get("duration_minutes")?,
        named_approver_id: r.try_get("named_approver_id")?,
        status: GrantStatus::parse(&r.try_get::<String, _>("status")?).map_err(dec)?,
        decided_by: r.try_get("decided_by")?,
        decided_at: r.try_get("decided_at")?,
        decision_note: r.try_get("decision_note")?,
        starts_at: r.try_get("starts_at")?,
        expires_at: r.try_get("expires_at")?,
        revoked_at: r.try_get("revoked_at")?,
        use_count: r.try_get("use_count")?,
        created_at: r.try_get("created_at")?,
    })
}

#[async_trait]
impl SupportGrantRepository for PgStore {
    async fn insert(&self, scope: &AccessScope, g: &SupportGrant, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.support_grants (id, tenant_id, requested_by, reason, incident_ref, duration_minutes, named_approver_id, status, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        )
        .bind(g.id)
        .bind(g.tenant_id.0)
        .bind(g.requested_by)
        .bind(&g.reason)
        .bind(&g.incident_ref)
        .bind(g.duration_minutes)
        .bind(g.named_approver_id)
        .bind(g.status.as_str())
        .bind(g.created_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| if is_unique_violation(&e) { AppError::conflict("A grant for this incident is already pending or active") } else { AppError::internal(e) })?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get(&self, scope: &AccessScope, id: Uuid) -> AppResult<Option<SupportGrant>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query(&format!("SELECT {GRANT_COLS} FROM tenantadm.support_grants WHERE id = $1"))
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(row.as_ref().map(grant_from_row).transpose()?)
    }

    async fn list(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<SupportGrant>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(&format!(
            "SELECT {GRANT_COLS} FROM tenantadm.support_grants WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 100"
        ))
        .bind(tenant.0)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows.iter().map(grant_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn decide(&self, scope: &AccessScope, g: &SupportGrant, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let n = sqlx::query(
            "UPDATE tenantadm.support_grants SET status = $2, decided_by = coalesce($3, decided_by), decided_at = coalesce($4, decided_at),
                 decision_note = coalesce($5, decision_note), starts_at = $6, expires_at = $7, revoked_at = $8,
                 updated_at = now(), version = version + 1
             WHERE id = $1",
        )
        .bind(g.id)
        .bind(g.status.as_str())
        .bind(g.decided_by)
        .bind(g.decided_at)
        .bind(&g.decision_note)
        .bind(g.starts_at)
        .bind(g.expires_at)
        .bind(g.revoked_at)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(AppError::not_found("Grant does not exist"));
        }
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn record_use(&self, scope: &AccessScope, id: Uuid, at: DateTime<Utc>, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("UPDATE tenantadm.support_grants SET use_count = use_count + 1, last_used_at = $2, updated_at = now() WHERE id = $1")
            .bind(id)
            .bind(at)
            .execute(&mut *tx)
            .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }
}
