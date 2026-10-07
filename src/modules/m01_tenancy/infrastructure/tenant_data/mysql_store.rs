//! Dedicated MySQL tenant data store (Regulated tier).
//!
//! MySQL has NO row-level security. Isolation here comes from: (1) a dedicated database per
//! tenant, (2) a per-tenant runtime login granted only on that database, (3) server-side routing
//! (the store is bound to exactly one tenant), and (4) repository-level `tenant_id` checks.

use std::time::Instant;

use async_trait::async_trait;
use sqlx::{MySqlPool, Row};
use uuid::Uuid;

use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::{CanaryRow, ProbeResult, TenantDataStore};
use super::super::super::domain::storage::{DatabaseEngine, StorageStrategy};
use super::super::super::domain::TenantId;

pub struct MySqlTenantStore {
    pool: MySqlPool,
    owner: TenantId,
}

impl MySqlTenantStore {
    pub fn new(pool: MySqlPool, owner: TenantId) -> Self {
        Self { pool, owner }
    }

    /// Repository-level guard: this store only ever serves its owning tenant.
    fn guard(&self, tenant: TenantId) -> AppResult<()> {
        if tenant == self.owner {
            Ok(())
        } else {
            Err(AppError::forbidden("Tenant data store is bound to another tenant"))
        }
    }
}

fn probe(check: &str, passed: bool, detail: impl Into<String>) -> ProbeResult {
    ProbeResult { check: check.into(), passed, detail: detail.into() }
}

#[async_trait]
impl TenantDataStore for MySqlTenantStore {
    fn engine(&self) -> DatabaseEngine {
        DatabaseEngine::Mysql
    }

    fn strategy(&self) -> StorageStrategy {
        StorageStrategy::DedicatedDatabase
    }

    async fn ping(&self) -> AppResult<u128> {
        let t = Instant::now();
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(t.elapsed().as_millis())
    }

    async fn write_canary(&self, tenant: TenantId, marker: &str) -> AppResult<()> {
        self.guard(tenant)?;
        sqlx::query(
            "INSERT INTO isolation_canaries (id, tenant_id, marker)
             SELECT ?, ?, ? FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM isolation_canaries WHERE tenant_id = ? AND marker = ?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(tenant.0.to_string())
        .bind(marker)
        .bind(tenant.0.to_string())
        .bind(marker)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_rows(&self, tenant: TenantId) -> AppResult<Vec<CanaryRow>> {
        self.guard(tenant)?;
        let rows = sqlx::query("SELECT id, tenant_id, marker FROM isolation_canaries WHERE tenant_id = ? ORDER BY created_at")
            .bind(tenant.0.to_string())
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|r| Ok(CanaryRow { id: r.try_get("id")?, tenant_id: r.try_get("tenant_id")?, marker: r.try_get("marker")? }))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn isolation_probe(&self, own: TenantId, foreign: TenantId) -> AppResult<Vec<ProbeResult>> {
        let mut out = Vec::new();
        let own_rows = self.list_rows(own).await?.len();
        out.push(probe("dedicated_mysql.own_rows_visible", own_rows > 0, format!("{own_rows} own row(s)")));

        let foreign_read = self.list_rows(foreign).await;
        out.push(probe("dedicated_mysql.foreign_tenant_read_rejected", foreign_read.is_err(), "repository guard bound to owning tenant"));
        let foreign_write = self.write_canary(foreign, "cross-tenant-write").await;
        out.push(probe("dedicated_mysql.foreign_tenant_write_rejected", foreign_write.is_err(), "repository guard bound to owning tenant"));

        let strangers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM isolation_canaries WHERE tenant_id <> ?")
            .bind(own.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        out.push(probe(
            "dedicated_mysql.database_holds_only_own_rows",
            strangers == 0,
            format!("{strangers} foreign row(s) in the dedicated database"),
        ));

        // The per-tenant runtime login must not see any other tenant database.
        let visible_dbs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'tn\\_%' AND SCHEMA_NAME <> DATABASE()",
        )
        .fetch_one(&self.pool)
        .await?;
        out.push(probe(
            "dedicated_mysql.runtime_login_sees_no_other_tenant_db",
            visible_dbs == 0,
            format!("{visible_dbs} other tenant database(s) visible"),
        ));
        out.push(probe(
            "dedicated_mysql.no_rls_disclaimer",
            true,
            "MySQL provides no RLS; isolation is the dedicated database + login boundary",
        ));
        Ok(out)
    }

    async fn purge_rows(&self, tenant: TenantId) -> AppResult<u64> {
        self.guard(tenant)?;
        Ok(sqlx::query("DELETE FROM isolation_canaries WHERE tenant_id = ?")
            .bind(tenant.0.to_string())
            .execute(&self.pool)
            .await?
            .rows_affected())
    }
}
