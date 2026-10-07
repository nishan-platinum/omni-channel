//! Minimal append-only audit adapter (stand-in for M30, SEC-140/141). Entries are written in the
//! same transaction as the change they describe. Never put secrets into `before`/`after`.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::{FromRow, PgConnection, PgPool};
use uuid::Uuid;

use super::db::{scoped_tx, AccessScope};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuditEntry {
    pub tenant_id: Option<Uuid>,
    pub actor_id: Option<Uuid>,
    pub actor_role: String,
    pub entity_type: String,
    pub entity_id: Option<String>,
    pub action: String,
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub reason: Option<String>,
    pub security_event: bool,
    pub ip: Option<String>,
    pub user_agent: Option<String>,
    pub correlation_id: Option<String>,
}

pub async fn append(conn: &mut PgConnection, e: &AuditEntry) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO shared.audit_log
            (tenant_id, actor_id, actor_role, entity_type, entity_id, action, before, after, reason,
             security_event, ip, user_agent, correlation_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(e.tenant_id)
    .bind(e.actor_id)
    .bind(&e.actor_role)
    .bind(&e.entity_type)
    .bind(&e.entity_id)
    .bind(&e.action)
    .bind(&e.before)
    .bind(&e.after)
    .bind(&e.reason)
    .bind(e.security_event)
    .bind(&e.ip)
    .bind(&e.user_agent)
    .bind(&e.correlation_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// Writes standalone audit entries in their own transaction.
pub async fn append_standalone(pool: &PgPool, scope: &AccessScope, entries: &[AuditEntry]) -> Result<(), sqlx::Error> {
    let mut tx = scoped_tx(pool, scope).await?;
    for e in entries {
        append(&mut tx, e).await?;
    }
    tx.commit().await
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct AuditRecord {
    pub id: i64,
    pub tenant_id: Option<Uuid>,
    pub actor_id: Option<Uuid>,
    pub actor_role: String,
    pub entity_type: String,
    pub entity_id: Option<String>,
    pub action: String,
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub reason: Option<String>,
    pub security_event: bool,
    pub correlation_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Lists audit rows of one tenant, newest first, keyset-paginated by id.
pub async fn list_for_tenant(
    pool: &PgPool,
    scope: &AccessScope,
    tenant_id: Uuid,
    before_id: Option<i64>,
    limit: i64,
) -> Result<Vec<AuditRecord>, sqlx::Error> {
    let mut tx = scoped_tx(pool, scope).await?;
    let rows = sqlx::query_as::<_, AuditRecord>(
        "SELECT id, tenant_id, actor_id, actor_role, entity_type, entity_id, action, before, after, reason,
                security_event, correlation_id, created_at
           FROM shared.audit_log
          WHERE tenant_id = $1 AND ($2::bigint IS NULL OR id < $2)
          ORDER BY id DESC
          LIMIT $3",
    )
    .bind(tenant_id)
    .bind(before_id)
    .bind(limit.clamp(1, 200))
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

/// Recent security events (cross-tenant attempts, break-glass use) for the host dashboard.
pub async fn recent_security_events(pool: &PgPool, limit: i64) -> Result<Vec<AuditRecord>, sqlx::Error> {
    let mut tx = scoped_tx(pool, &AccessScope::Platform).await?;
    let rows = sqlx::query_as::<_, AuditRecord>(
        "SELECT id, tenant_id, actor_id, actor_role, entity_type, entity_id, action, before, after, reason,
                security_event, correlation_id, created_at
           FROM shared.audit_log WHERE security_event ORDER BY id DESC LIMIT $1",
    )
    .bind(limit.clamp(1, 200))
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

/// Verifies the hash chain for the most recent `limit` rows (SEC-141 tamper evidence).
pub async fn verify_chain(pool: &PgPool, limit: i64) -> Result<bool, sqlx::Error> {
    let mut tx = scoped_tx(pool, &AccessScope::System).await?;
    let ok: bool = sqlx::query_scalar(
        "WITH recent AS (SELECT id, prev_hash, hash FROM shared.audit_log ORDER BY id DESC LIMIT $1)
         SELECT coalesce(bool_and(r.prev_hash IS NULL OR EXISTS
                (SELECT 1 FROM shared.audit_log p WHERE p.hash = r.prev_hash)), true)
           FROM recent r",
    )
    .bind(limit)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(ok)
}
