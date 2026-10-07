//! Idempotency-Key support for creating POSTs (API-003): the first response is stored for 24 h
//! and replayed for retries with the same key and payload; a different payload is a conflict.

use chrono::{Duration, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::db::{scoped_tx, AccessScope};
use super::errors::{AppError, AppResult};

pub enum Lookup {
    Miss,
    Replay(u16, Value),
}

pub fn validate_key(k: &str) -> AppResult<&str> {
    let k = k.trim();
    if k.is_empty() || k.len() > 200 || !k.chars().all(|c| c.is_ascii_graphic()) {
        return Err(AppError::validation("Idempotency-Key", "Idempotency-Key must be 1-200 visible ASCII characters"));
    }
    Ok(k)
}

pub async fn lookup(pool: &PgPool, principal: Uuid, key: &str, request_hash: &str) -> AppResult<Lookup> {
    let mut tx = scoped_tx(pool, &AccessScope::System).await?;
    let row = sqlx::query("SELECT request_hash, response_status, response_body FROM shared.idempotency_keys WHERE principal_id = $1 AND idem_key = $2 AND expires_at > now()")
        .bind(principal)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    match row {
        None => Ok(Lookup::Miss),
        Some(r) => {
            let stored: String = r.try_get("request_hash")?;
            if stored != request_hash {
                return Err(AppError::conflict("Idempotency-Key was already used with a different request"));
            }
            let status: i32 = r.try_get("response_status")?;
            Ok(Lookup::Replay(status as u16, r.try_get("response_body")?))
        }
    }
}

pub async fn store(
    pool: &PgPool,
    tenant: Option<Uuid>,
    principal: Uuid,
    key: &str,
    request_hash: &str,
    status: u16,
    body: &Value,
) -> AppResult<()> {
    let mut tx = scoped_tx(pool, &AccessScope::System).await?;
    sqlx::query("DELETE FROM shared.idempotency_keys WHERE expires_at <= now() AND principal_id = $1")
        .bind(principal)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO shared.idempotency_keys (id, tenant_id, principal_id, idem_key, request_hash, response_status, response_body, expires_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (principal_id, idem_key) DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .bind(tenant)
    .bind(principal)
    .bind(key)
    .bind(request_hash)
    .bind(i32::from(status))
    .bind(body)
    .bind(Utc::now() + Duration::hours(24))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}
