//! PostgreSQL tenant data store, used for the shared data plane (Standard: `tenant_data` schema,
//! RLS), schema-per-tenant (Premium: `tn_<hex>` schema, RLS) and dedicated databases (Regulated:
//! own database, own runtime login, RLS as defense in depth).

use std::time::Instant;

use async_trait::async_trait;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::platform::db::{safe_ident, scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::{CanaryRow, ProbeResult, TenantDataStore};
use super::super::super::domain::storage::{DatabaseEngine, StorageStrategy};
use super::super::super::domain::TenantId;

pub struct PgTenantStore {
    pool: PgPool,
    /// Fully qualified, validated table name, e.g. `tenant_data.isolation_canaries`.
    table: String,
    strategy: StorageStrategy,
}

impl PgTenantStore {
    pub fn new(pool: PgPool, schema: &str, strategy: StorageStrategy) -> AppResult<Self> {
        let schema = safe_ident(schema)?;
        Ok(Self { pool, table: format!("{schema}.isolation_canaries"), strategy })
    }

    async fn count_under(&self, scope: Option<AccessScope>, tenant: TenantId) -> AppResult<i64> {
        let sql = format!("SELECT count(*) FROM {} WHERE tenant_id = $1", self.table);
        match scope {
            Some(s) => {
                let mut tx = scoped_tx(&self.pool, &s).await?;
                let n: i64 = sqlx::query_scalar(&sql).bind(tenant.0).fetch_one(&mut *tx).await?;
                tx.rollback().await?;
                Ok(n)
            }
            None => {
                let mut conn = self.pool.acquire().await?;
                Ok(sqlx::query_scalar(&sql).bind(tenant.0).fetch_one(&mut *conn).await?)
            }
        }
    }
}

fn probe(check: &str, passed: bool, detail: impl Into<String>) -> ProbeResult {
    ProbeResult { check: check.into(), passed, detail: detail.into() }
}

#[async_trait]
impl TenantDataStore for PgTenantStore {
    fn engine(&self) -> DatabaseEngine {
        DatabaseEngine::Postgres
    }

    fn strategy(&self) -> StorageStrategy {
        self.strategy
    }

    async fn ping(&self) -> AppResult<u128> {
        let t = Instant::now();
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(t.elapsed().as_millis())
    }

    async fn write_canary(&self, tenant: TenantId, marker: &str) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant.0)).await?;
        sqlx::query(&format!(
            "INSERT INTO {t} (id, tenant_id, marker) SELECT $1, $2, $3
             WHERE NOT EXISTS (SELECT 1 FROM {t} WHERE tenant_id = $2 AND marker = $3)",
            t = self.table
        ))
        .bind(Uuid::now_v7())
        .bind(tenant.0)
        .bind(marker)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_rows(&self, tenant: TenantId) -> AppResult<Vec<CanaryRow>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant.0)).await?;
        let rows =
            sqlx::query(&format!("SELECT id::text AS id, tenant_id::text AS tenant_id, marker FROM {} ORDER BY created_at", self.table))
                .fetch_all(&mut *tx)
                .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok(CanaryRow { id: r.try_get("id")?, tenant_id: r.try_get("tenant_id")?, marker: r.try_get("marker")? }))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn isolation_probe(&self, own: TenantId, foreign: TenantId) -> AppResult<Vec<ProbeResult>> {
        let mut out = Vec::new();
        let own_visible = self.count_under(Some(AccessScope::Tenant(own.0)), own).await?;
        out.push(probe("data_plane.own_scope_reads_own_rows", own_visible > 0, format!("{own_visible} own row(s) visible")));

        let foreign_visible = self.count_under(Some(AccessScope::Tenant(foreign.0)), own).await?;
        out.push(probe(
            "data_plane.foreign_scope_cannot_read",
            foreign_visible == 0,
            format!("{foreign_visible} row(s) visible to a foreign tenant context"),
        ));

        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(foreign.0)).await?;
        let write = sqlx::query(&format!("INSERT INTO {} (id, tenant_id, marker) VALUES ($1, $2, 'cross-tenant-write')", self.table))
            .bind(Uuid::now_v7())
            .bind(own.0)
            .execute(&mut *tx)
            .await;
        let _ = tx.rollback().await;
        out.push(probe(
            "data_plane.foreign_scope_cannot_write",
            write.is_err(),
            if write.is_err() { "rejected by RLS WITH CHECK" } else { "cross-tenant insert was accepted" },
        ));

        let no_ctx = self.count_under(None, own).await?;
        out.push(probe("data_plane.missing_context_fails_closed", no_ctx == 0, format!("{no_ctx} row(s) visible without tenant context")));

        let platform = self.count_under(Some(AccessScope::Platform), own).await?;
        out.push(probe(
            "data_plane.platform_scope_cannot_read",
            platform == 0,
            format!("{platform} row(s) visible to host scope without break-glass"),
        ));

        if self.strategy == StorageStrategy::DedicatedDatabase {
            // A database that another provisioning is creating right now is connectable by PUBLIC
            // between its CREATE DATABASE and REVOKE (Postgres default, milliseconds). Re-check a
            // few times so that transient window does not fail this tenant's probe; a real leak
            // persists and still fails.
            let mut others: i64 = 0;
            for attempt in 0..4 {
                if attempt > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
                let mut conn = self.pool.acquire().await?;
                others = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_database d WHERE d.datname LIKE 'tn\\_%' AND d.datname <> current_database()
                       AND has_database_privilege(d.datname, 'CONNECT')",
                )
                .fetch_one(&mut *conn)
                .await?;
                if others == 0 {
                    break;
                }
            }
            out.push(probe(
                "dedicated_db.runtime_login_cannot_connect_elsewhere",
                others == 0,
                format!("{others} other tenant database(s) connectable"),
            ));
        }
        Ok(out)
    }

    async fn purge_rows(&self, tenant: TenantId) -> AppResult<u64> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant.0)).await?;
        let n = sqlx::query(&format!("DELETE FROM {} WHERE tenant_id = $1", self.table))
            .bind(tenant.0)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(n)
    }
}
