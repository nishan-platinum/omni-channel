//! Domain-event transport: a transactional outbox (`shared.event_outbox`) written in the same
//! transaction as the state change, and an in-process dispatcher delivering to idempotent handlers.
//! A real bus (Kafka/RabbitMQ, Part G Ch 78) can replace the dispatcher without touching producers.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::db::{scoped_tx, AccessScope};

#[derive(Debug, Clone, PartialEq)]
pub struct EventEnvelope {
    pub id: Uuid,
    /// `<entity>.<action>` per spec §7.1 (e.g. `tenant.created`).
    pub event_type: String,
    pub tenant_id: Option<Uuid>,
    pub aggregate_id: String,
    pub payload: Value,
    pub correlation_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

pub async fn write_outbox(conn: &mut PgConnection, ev: &EventEnvelope) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO shared.event_outbox (id, tenant_id, event_type, aggregate_id, payload, correlation_id, occurred_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(ev.id)
    .bind(ev.tenant_id)
    .bind(&ev.event_type)
    .bind(&ev.aggregate_id)
    .bind(&ev.payload)
    .bind(&ev.correlation_id)
    .bind(ev.occurred_at)
    .execute(conn)
    .await?;
    Ok(())
}

#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Stable consumer name used for idempotency bookkeeping.
    fn name(&self) -> &'static str;
    fn handles(&self, event_type: &str) -> bool;
    async fn handle(&self, event: &EventEnvelope) -> anyhow::Result<()>;
}

const MAX_ATTEMPTS: i32 = 5;

pub struct OutboxDispatcher {
    pool: PgPool,
    handlers: Vec<Arc<dyn EventHandler>>,
}

impl OutboxDispatcher {
    pub fn new(pool: PgPool, handlers: Vec<Arc<dyn EventHandler>>) -> Self {
        Self { pool, handlers }
    }

    /// Delivers one batch of pending events. Returns the number of events processed.
    pub async fn run_once(&self) -> anyhow::Result<usize> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, event_type, aggregate_id, payload, correlation_id, occurred_at, attempts
               FROM shared.event_outbox
              WHERE published_at IS NULL
              ORDER BY occurred_at
              LIMIT 50
              FOR UPDATE SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await?;

        let mut processed = 0;
        for row in rows {
            let ev = EventEnvelope {
                id: row.try_get("id")?,
                tenant_id: row.try_get("tenant_id")?,
                event_type: row.try_get("event_type")?,
                aggregate_id: row.try_get("aggregate_id")?,
                payload: row.try_get("payload")?,
                correlation_id: row.try_get("correlation_id")?,
                occurred_at: row.try_get("occurred_at")?,
            };
            let attempts: i32 = row.try_get("attempts")?;
            let mut failure: Option<String> = None;
            for h in self.handlers.iter().filter(|h| h.handles(&ev.event_type)) {
                let already: bool =
                    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM shared.event_consumptions WHERE consumer = $1 AND event_id = $2)")
                        .bind(h.name())
                        .bind(ev.id)
                        .fetch_one(&mut *tx)
                        .await?;
                if already {
                    continue;
                }
                match h.handle(&ev).await {
                    Ok(()) => {
                        sqlx::query(
                            "INSERT INTO shared.event_consumptions (consumer, event_id, tenant_id) VALUES ($1, $2, $3)
                             ON CONFLICT DO NOTHING",
                        )
                        .bind(h.name())
                        .bind(ev.id)
                        .bind(ev.tenant_id)
                        .execute(&mut *tx)
                        .await?;
                    }
                    Err(e) => {
                        tracing::warn!(event_id = %ev.id, event_type = %ev.event_type, consumer = h.name(), error = %e, "event handler failed");
                        failure = Some(format!("{}: {e}", h.name()));
                    }
                }
            }
            match failure {
                None => {
                    sqlx::query("UPDATE shared.event_outbox SET published_at = now(), attempts = attempts + 1 WHERE id = $1")
                        .bind(ev.id)
                        .execute(&mut *tx)
                        .await?;
                }
                Some(err) if attempts + 1 >= MAX_ATTEMPTS => {
                    // Dead-letter: stop retrying, keep the error for operators (ERR-004).
                    sqlx::query(
                        "UPDATE shared.event_outbox SET published_at = now(), attempts = attempts + 1, last_error = $2 WHERE id = $1",
                    )
                    .bind(ev.id)
                    .bind(format!("DLQ: {err}"))
                    .execute(&mut *tx)
                    .await?;
                    tracing::error!(event_id = %ev.id, "event dead-lettered after {MAX_ATTEMPTS} attempts");
                }
                Some(err) => {
                    sqlx::query("UPDATE shared.event_outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1")
                        .bind(ev.id)
                        .bind(err)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            processed += 1;
        }
        tx.commit().await?;
        Ok(processed)
    }

    pub fn spawn(self: Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(e) = self.run_once().await {
                    tracing::warn!(error = %e, "outbox dispatch failed");
                }
            }
        })
    }
}

/// Count of undelivered events per tenant ("queue depth" health signal, R023).
pub async fn pending_counts(pool: &PgPool) -> Result<Vec<(Uuid, i64)>, sqlx::Error> {
    let mut tx = scoped_tx(pool, &AccessScope::System).await?;
    let rows = sqlx::query(
        "SELECT tenant_id, count(*) AS n FROM shared.event_outbox
          WHERE published_at IS NULL AND tenant_id IS NOT NULL GROUP BY tenant_id",
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    rows.into_iter().map(|r| Ok((r.try_get("tenant_id")?, r.try_get("n")?))).collect()
}
