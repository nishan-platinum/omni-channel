//! Tenant data plane: server-side routing from tenant → data store and provisioning of shared,
//! schema-per-tenant and dedicated (PostgreSQL/MySQL) stores (OCC-M01-R008, ADR-0004).
//! Engine differences live only in this module.

mod mysql_store;
mod pg_store;
pub mod targets;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{MySqlPool, PgPool};
use tokio::sync::RwLock;

use crate::platform::db::safe_ident;
use crate::platform::errors::{AppError, AppResult};

use super::super::application::ports::{ConnectionProfile, DbTargetInfo, TenantDataRouter, TenantDataStore};
use super::super::domain::storage::{DatabaseEngine, Region, StorageStrategy};
use super::super::domain::TenantId;
pub use mysql_store::MySqlTenantStore;
pub use pg_store::PgTenantStore;
use targets::{runtime_username, DbTarget, EnvSecretResolver};

const PG_TENANT_MIGRATION: &str = include_str!("../../../../../migrations/tenant/postgres/0001_isolation_canaries.sql");
const MYSQL_TENANT_MIGRATION: &str = include_str!("../../../../../migrations/tenant/mysql/0001_isolation_canaries.sql");

pub const SHARED_TARGET: &str = "shared";
pub const SCHEMA_TARGET: &str = "central-schema";
const DEDICATED_PG_SCHEMA: &str = "tenant_data";
const CENTRAL_DB_LABEL: &str = "control_plane";

/// Error text without credentials (sqlx connection errors never include passwords).
fn short(e: &sqlx::Error) -> String {
    let s = e.to_string();
    s.chars().take(160).collect()
}

pub struct DataPlaneRouter {
    app_pool: PgPool,
    owner_pool: PgPool,
    targets: Vec<DbTarget>,
    secrets: EnvSecretResolver,
    cache: RwLock<HashMap<TenantId, Arc<dyn TenantDataStore>>>,
}

impl DataPlaneRouter {
    pub fn new(app_pool: PgPool, owner_pool: PgPool, targets: Vec<DbTarget>) -> Self {
        Self { app_pool, owner_pool, targets, secrets: EnvSecretResolver, cache: RwLock::new(HashMap::new()) }
    }

    fn target(&self, name: &str) -> AppResult<&DbTarget> {
        self.targets.iter().find(|t| t.name == name).ok_or_else(|| AppError::validation("db_target", "Unknown dedicated database target"))
    }

    fn render_pg_migration(schema: &str, role: &str) -> AppResult<String> {
        let schema = safe_ident(schema)?;
        let role = safe_ident(role)?;
        Ok(PG_TENANT_MIGRATION.replace("{{schema}}", schema).replace("{{runtime_role}}", role))
    }

    /// Short-lived single-connection admin pool (DDL during provisioning/decommissioning only).
    async fn pg_admin(&self, t: &DbTarget, database: &str) -> AppResult<PgPool> {
        let pw = self.secrets.resolve(&t.admin_secret_ref)?;
        let opts = PgConnectOptions::new()
            .host(&t.host)
            .port(t.port)
            .username(&t.admin_user)
            .password(&pw)
            .database(database)
            .application_name("omni-m01-provisioning");
        PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(opts)
            .await
            .map_err(|e| AppError::conflict(format!("Dedicated target {} unreachable: {}", t.name, short(&e))))
    }

    async fn mysql_admin(&self, t: &DbTarget, database: Option<&str>) -> AppResult<MySqlPool> {
        let pw = self.secrets.resolve(&t.admin_secret_ref)?;
        let mut opts = MySqlConnectOptions::new().host(&t.host).port(t.port).username(&t.admin_user).password(&pw);
        if let Some(db) = database {
            opts = opts.database(db);
        }
        MySqlPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(opts)
            .await
            .map_err(|e| AppError::conflict(format!("Dedicated target {} unreachable: {}", t.name, short(&e))))
    }

    async fn provision_dedicated_pg(&self, t: &DbTarget, p: &ConnectionProfile) -> AppResult<String> {
        let db = safe_ident(&p.database_name)?;
        let user = runtime_username(p.tenant_id);
        let user = safe_ident(&user)?;
        let pw = self.secrets.derive_password(&t.runtime_seed_ref, user)?;
        let admin = self.pg_admin(t, &t.admin_database).await?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)").bind(db).fetch_one(&admin).await?;
        if !exists {
            // Identifiers are server-generated and validated by safe_ident; DDL cannot bind them.
            sqlx::raw_sql(&format!("CREATE DATABASE {db}")).execute(&admin).await?;
        }
        let role_exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)").bind(user).fetch_one(&admin).await?;
        let pw_lit = pw.replace('\'', "''");
        if role_exists {
            sqlx::raw_sql(&format!("ALTER ROLE {user} WITH LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{pw_lit}'")).execute(&admin).await?;
        } else {
            sqlx::raw_sql(&format!("CREATE ROLE {user} WITH LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE PASSWORD '{pw_lit}'"))
                .execute(&admin)
                .await?;
        }
        sqlx::raw_sql(&format!("REVOKE ALL ON DATABASE {db} FROM PUBLIC; GRANT CONNECT ON DATABASE {db} TO {user};"))
            .execute(&admin)
            .await?;
        admin.close().await;
        let conn = self.pg_admin(t, db).await?;
        sqlx::raw_sql(&Self::render_pg_migration(DEDICATED_PG_SCHEMA, user)?).execute(&conn).await?;
        conn.close().await;
        Ok(format!("dedicated PostgreSQL database {db} on {} with runtime login {user} (RLS enabled)", t.name))
    }

    async fn provision_dedicated_mysql(&self, t: &DbTarget, p: &ConnectionProfile) -> AppResult<String> {
        let db = safe_ident(&p.database_name)?;
        let user = runtime_username(p.tenant_id);
        let user = safe_ident(&user)?;
        let pw = self.secrets.derive_password(&t.runtime_seed_ref, user)?;
        let admin = self.mysql_admin(t, None).await?;
        sqlx::raw_sql(&format!("CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"))
            .execute(&admin)
            .await?;
        let pw_lit = pw.replace('\'', "''");
        sqlx::raw_sql(&format!("CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY '{pw_lit}'")).execute(&admin).await?;
        sqlx::raw_sql(&format!("ALTER USER '{user}'@'%' IDENTIFIED BY '{pw_lit}'")).execute(&admin).await?;
        sqlx::raw_sql(&format!("GRANT SELECT, INSERT, DELETE ON `{db}`.* TO '{user}'@'%'")).execute(&admin).await?;
        admin.close().await;
        let conn = self.mysql_admin(t, Some(db)).await?;
        // Strip comment lines first (they may contain ';'), then split into statements.
        let script: String = MYSQL_TENANT_MIGRATION.lines().filter(|l| !l.trim_start().starts_with("--")).collect::<Vec<_>>().join("\n");
        for stmt in script.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::raw_sql(stmt).execute(&conn).await?;
        }
        conn.close().await;
        Ok(format!("dedicated MySQL database {db} on {} with runtime login {user} (no RLS: database + login boundary)", t.name))
    }

    async fn build_store(&self, p: &ConnectionProfile) -> AppResult<Arc<dyn TenantDataStore>> {
        match (p.storage_strategy, p.engine) {
            (StorageStrategy::SharedRowLevel, _) => {
                Ok(Arc::new(PgTenantStore::new(self.app_pool.clone(), "tenant_data", StorageStrategy::SharedRowLevel)?))
            }
            (StorageStrategy::SchemaPerTenant, _) => {
                let schema = p.schema_name.as_deref().ok_or_else(|| AppError::internal(anyhow::anyhow!("schema missing")))?;
                Ok(Arc::new(PgTenantStore::new(self.app_pool.clone(), schema, StorageStrategy::SchemaPerTenant)?))
            }
            (StorageStrategy::DedicatedDatabase, engine) => {
                let t = self.target(&p.target_name)?;
                let user = runtime_username(p.tenant_id);
                let pw = self.secrets.derive_password(&t.runtime_seed_ref, &user)?;
                let host = p.host.clone().unwrap_or_else(|| t.host.clone());
                let port = p.port.map(|x| x as u16).unwrap_or(t.port);
                match engine {
                    DatabaseEngine::Postgres => {
                        let opts = PgConnectOptions::new()
                            .host(&host)
                            .port(port)
                            .username(&user)
                            .password(&pw)
                            .database(&p.database_name)
                            .application_name("omni-m01-tenant");
                        let pool = PgPoolOptions::new().max_connections(3).acquire_timeout(Duration::from_secs(3)).connect_lazy_with(opts);
                        Ok(Arc::new(PgTenantStore::new(pool, DEDICATED_PG_SCHEMA, StorageStrategy::DedicatedDatabase)?))
                    }
                    DatabaseEngine::Mysql => {
                        let opts =
                            MySqlConnectOptions::new().host(&host).port(port).username(&user).password(&pw).database(&p.database_name);
                        let pool =
                            MySqlPoolOptions::new().max_connections(3).acquire_timeout(Duration::from_secs(3)).connect_lazy_with(opts);
                        Ok(Arc::new(MySqlTenantStore::new(pool, p.tenant_id)))
                    }
                }
            }
        }
    }
}

#[async_trait]
impl TenantDataRouter for DataPlaneRouter {
    fn dedicated_targets(&self) -> Vec<DbTargetInfo> {
        self.targets
            .iter()
            .filter_map(|t| {
                Some(DbTargetInfo {
                    name: t.name.clone(),
                    engine: t.engine().ok()?,
                    region: t.region().ok()?,
                    host: t.host.clone(),
                    port: t.port,
                })
            })
            .collect()
    }

    fn plan_profile(
        &self,
        tenant: TenantId,
        strategy: StorageStrategy,
        region: Region,
        target: Option<&str>,
    ) -> AppResult<ConnectionProfile> {
        let base = ConnectionProfile {
            tenant_id: tenant,
            engine: DatabaseEngine::Postgres,
            storage_strategy: strategy,
            target_name: SHARED_TARGET.into(),
            host: None,
            port: None,
            database_name: CENTRAL_DB_LABEL.into(),
            schema_name: Some("tenant_data".into()),
            region,
            secret_ref: None,
            status: "pending".into(),
            last_checked_at: None,
            last_check_ok: None,
            last_check_message: None,
            last_latency_ms: None,
        };
        match strategy {
            StorageStrategy::SharedRowLevel => Ok(base),
            StorageStrategy::SchemaPerTenant => {
                Ok(ConnectionProfile { target_name: SCHEMA_TARGET.into(), schema_name: Some(tenant.storage_ident()), ..base })
            }
            StorageStrategy::DedicatedDatabase => {
                let name = target.ok_or_else(|| AppError::validation("db_target", "Dedicated database target is required"))?;
                let t = self.target(name)?;
                let engine = t.engine()?;
                if t.region()? != region {
                    // Residency pinning (R008, UJ-19 E2).
                    return Err(AppError::validation(
                        "db_target",
                        "Dedicated target must be in the tenant's data region (residency pinning)",
                    ));
                }
                Ok(ConnectionProfile {
                    engine,
                    target_name: t.name.clone(),
                    host: Some(t.host.clone()),
                    port: Some(i32::from(t.port)),
                    database_name: tenant.storage_ident(),
                    schema_name: (engine == DatabaseEngine::Postgres).then(|| DEDICATED_PG_SCHEMA.to_string()),
                    secret_ref: Some(t.runtime_seed_ref.clone()),
                    ..base
                })
            }
        }
    }

    async fn provision(&self, p: &ConnectionProfile) -> AppResult<String> {
        match p.storage_strategy {
            StorageStrategy::SharedRowLevel => Ok("shared PostgreSQL data plane (tenant_id + RLS); no per-tenant DDL".into()),
            StorageStrategy::SchemaPerTenant => {
                let schema = p.schema_name.as_deref().ok_or_else(|| AppError::internal(anyhow::anyhow!("schema missing")))?;
                let sql = Self::render_pg_migration(schema, "crm_app")?;
                sqlx::raw_sql(&sql).execute(&self.owner_pool).await?;
                Ok(format!("schema {schema} created in the central PostgreSQL cluster (RLS enabled)"))
            }
            StorageStrategy::DedicatedDatabase => {
                let t = self.target(&p.target_name)?.clone();
                match p.engine {
                    DatabaseEngine::Postgres => self.provision_dedicated_pg(&t, p).await,
                    DatabaseEngine::Mysql => self.provision_dedicated_mysql(&t, p).await,
                }
            }
        }
    }

    async fn store_for(&self, p: &ConnectionProfile) -> AppResult<Arc<dyn TenantDataStore>> {
        if let Some(s) = self.cache.read().await.get(&p.tenant_id) {
            return Ok(s.clone());
        }
        let store = self.build_store(p).await?;
        self.cache.write().await.insert(p.tenant_id, store.clone());
        Ok(store)
    }

    async fn decommission(&self, p: &ConnectionProfile) -> AppResult<String> {
        self.cache.write().await.remove(&p.tenant_id);
        match p.storage_strategy {
            StorageStrategy::SharedRowLevel => Ok("shared data plane rows removed by tenant-scoped delete".into()),
            StorageStrategy::SchemaPerTenant => {
                let schema = safe_ident(p.schema_name.as_deref().unwrap_or_default())?.to_string();
                sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).execute(&self.owner_pool).await?;
                Ok(format!("schema {schema} dropped"))
            }
            StorageStrategy::DedicatedDatabase => {
                let t = self.target(&p.target_name)?.clone();
                let db = safe_ident(&p.database_name)?.to_string();
                let user = runtime_username(p.tenant_id);
                match p.engine {
                    DatabaseEngine::Postgres => {
                        let admin = self.pg_admin(&t, &t.admin_database).await?;
                        sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).execute(&admin).await?;
                        sqlx::raw_sql(&format!("DROP ROLE IF EXISTS {user}")).execute(&admin).await?;
                        admin.close().await;
                    }
                    DatabaseEngine::Mysql => {
                        let admin = self.mysql_admin(&t, None).await?;
                        sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS `{db}`")).execute(&admin).await?;
                        sqlx::raw_sql(&format!("DROP USER IF EXISTS '{user}'@'%'")).execute(&admin).await?;
                        admin.close().await;
                    }
                }
                Ok(format!("dedicated {} database {db} and login {user} dropped on {}", p.engine.label(), t.name))
            }
        }
    }
}
