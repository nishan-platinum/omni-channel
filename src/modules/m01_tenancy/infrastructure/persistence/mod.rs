//! SQLx/PostgreSQL implementations of the M01 repository ports. Every query runs inside
//! `platform::db::scoped_tx`, so RLS enforces the access scope independently of the code paths.

mod branding;
mod catalogs;
mod lifecycle_data;
mod ops;
mod settings;
mod tenants;

use base64::Engine;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::platform::audit;
use crate::platform::errors::AppError;
use crate::platform::events;

use super::super::application::ports::ChangeSet;

/// The PostgreSQL adapter for all M01 repositories (one pool, many narrow trait impls).
#[derive(Clone)]
pub struct PgStore {
    pub pool: PgPool,
}

impl PgStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Writes audit rows and outbox events inside the caller's transaction.
pub(crate) async fn write_changes(conn: &mut PgConnection, changes: &ChangeSet) -> Result<(), sqlx::Error> {
    for a in &changes.audit {
        audit::append(&mut *conn, a).await?;
    }
    for e in &changes.events {
        events::write_outbox(&mut *conn, e).await?;
    }
    Ok(())
}

/// Maps unique-constraint violations to CONFLICT with a business message; everything else is
/// an internal error (cause logged, never returned).
pub(crate) fn map_unique(e: sqlx::Error, constraint: &str, message: &str) -> AppError {
    if let sqlx::Error::Database(db) = &e {
        if db.code().as_deref() == Some("23505") && db.constraint() == Some(constraint) {
            return AppError::conflict(message);
        }
    }
    AppError::internal(e)
}

pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

pub(crate) fn encode_cursor(at: DateTime<Utc>, id: Uuid) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{}|{}", at.to_rfc3339(), id))
}

pub(crate) fn decode_cursor(c: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(c).ok()?;
    let s = String::from_utf8(raw).ok()?;
    let (a, b) = s.split_once('|')?;
    Some((DateTime::parse_from_rfc3339(a).ok()?.with_timezone(&Utc), Uuid::parse_str(b).ok()?))
}

/// Escapes LIKE wildcards in user search input (the value is still bound as a parameter).
pub(crate) fn like_pattern(q: &str) -> String {
    let escaped = q.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
    format!("%{escaped}%")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip() {
        let now = Utc::now();
        let id = Uuid::now_v7();
        assert_eq!(decode_cursor(&encode_cursor(now, id)), Some((now, id)));
        assert_eq!(decode_cursor("garbage"), None);
    }

    #[test]
    fn like_escapes() {
        assert_eq!(like_pattern("a%b_c"), "%a\\%b\\_c%");
    }
}
