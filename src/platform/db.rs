//! Database infrastructure: pools, control-plane migrations, and the **tenant-scoped transaction**
//! primitive that every tenant-scoped query must use (ADR-0005).

use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::config::AppConfig;

/// Access scope of one database transaction. Mapped to transaction-local GUCs `app.scope` and
/// `app.tenant_id`, which the RLS policies read. There is deliberately no "global" tenant
/// variable anywhere in the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessScope {
    /// A tenant principal (or a break-glass session acting inside one tenant).
    Tenant(Uuid),
    /// Super Admin host operations on M01 metadata. Callers must audit platform reads/writes.
    Platform,
    /// Internal jobs that are not tied to a principal (login lookup, scheduler, outbox).
    System,
}

impl AccessScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tenant(_) => "tenant",
            Self::Platform => "platform",
            Self::System => "system",
        }
    }

    pub fn tenant_id(&self) -> Option<Uuid> {
        match self {
            Self::Tenant(t) => Some(*t),
            _ => None,
        }
    }
}

/// Opens a transaction with the given access scope applied via `set_config(..., is_local => true)`.
/// The settings vanish at COMMIT/ROLLBACK, so pooled connections never carry tenant state.
pub async fn scoped_tx<'a>(pool: &'a PgPool, scope: &AccessScope) -> Result<Transaction<'a, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('app.scope', $1, true), set_config('app.tenant_id', $2, true)")
        .bind(scope.as_str())
        .bind(scope.tenant_id().map(|t| t.to_string()).unwrap_or_default())
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

/// Validates a server-generated SQL identifier (schema/database names). User input must never
/// reach this function; it exists as a second safety net for identifiers we build ourselves.
pub fn safe_ident(ident: &str) -> Result<&str, sqlx::Error> {
    let ok = !ident.is_empty()
        && ident.len() <= 63
        && ident.as_bytes()[0].is_ascii_lowercase()
        && ident.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if ok {
        Ok(ident)
    } else {
        Err(sqlx::Error::Protocol(format!("refusing unsafe SQL identifier of length {}", ident.len())))
    }
}

#[derive(Clone)]
pub struct Db {
    /// Runtime pool, role crm_app (RLS enforced).
    pub app: PgPool,
    /// Owner pool: migrations and tenant schema DDL during provisioning/purge only.
    pub owner: PgPool,
}

pub async fn connect(cfg: &AppConfig) -> anyhow::Result<Db> {
    let app_opts: PgConnectOptions = cfg.database_url.parse()?;
    let app = PgPoolOptions::new()
        .max_connections(cfg.db_max_connections)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(300))
        .connect_with(app_opts.application_name("omni-m01"))
        .await?;
    let owner_opts: PgConnectOptions = cfg.migration_database_url.parse()?;
    let owner = PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(owner_opts.application_name("omni-m01-ddl"))
        .await?;
    Ok(Db { app, owner })
}

pub static CONTROL_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/control");

/// Applies control-plane migrations with the owner role. Idempotent; SQLx takes an advisory lock
/// so concurrent instances do not race.
pub async fn migrate(owner: &PgPool) -> anyhow::Result<()> {
    CONTROL_MIGRATOR.run(owner).await?;
    Ok(())
}

/// Readiness probe: central DB reachable with the runtime role.
pub async fn ping(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1").execute(pool).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_ident_rejects_injection() {
        assert!(safe_ident("tn_0192abcd").is_ok());
        assert!(safe_ident("tn; drop table x").is_err());
        assert!(safe_ident("Tn_upper").is_err());
        assert!(safe_ident("").is_err());
        assert!(safe_ident("1abc").is_err());
        assert!(safe_ident(&"a".repeat(64)).is_err());
    }

    #[test]
    fn scope_strings() {
        let t = Uuid::now_v7();
        assert_eq!(AccessScope::Tenant(t).as_str(), "tenant");
        assert_eq!(AccessScope::Tenant(t).tenant_id(), Some(t));
        assert_eq!(AccessScope::Platform.tenant_id(), None);
        assert_eq!(AccessScope::System.as_str(), "system");
    }
}
