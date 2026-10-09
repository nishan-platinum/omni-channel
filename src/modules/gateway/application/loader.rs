//! Batched subscription lookups for customer sessions. A reconnect storm (rolling deploy, Redis
//! resync) would otherwise cost one database query per customer `hello`; requests that arrive
//! within a few milliseconds share one query (up to `MAX_BATCH` customers), so 100k reconnects
//! become a few hundred queries.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::super::infrastructure::store::Store;
use super::{GwError, GwResult};

const MAX_BATCH: usize = 500;
const LINGER: Duration = Duration::from_millis(3);
/// Pending lookups before callers wait for room (bounded, like every queue here).
const QUEUE: usize = 20_000;

type Reply = oneshot::Sender<GwResult<Vec<(String, i64)>>>;

#[derive(Clone)]
pub struct SubscriptionLoader {
    tx: mpsc::Sender<(String, Reply)>,
}

impl SubscriptionLoader {
    pub fn spawn(store: Store) -> Self {
        let (tx, rx) = mpsc::channel(QUEUE);
        tokio::spawn(run(store, rx));
        Self { tx }
    }

    /// The customer's open conversations with their last `seq`.
    pub async fn customer(&self, customer: &str) -> GwResult<Vec<(String, i64)>> {
        let (reply, rx) = oneshot::channel();
        self.tx.send((customer.to_string(), reply)).await.map_err(|_| GwError::Unavailable("lookup worker stopped".into()))?;
        rx.await.map_err(|_| GwError::Unavailable("lookup worker stopped".into()))?
    }
}

async fn run(store: Store, mut rx: mpsc::Receiver<(String, Reply)>) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + LINGER;
        while batch.len() < MAX_BATCH {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(item)) => batch.push(item),
                _ => break,
            }
        }
        let mut ids: Vec<String> = batch.iter().map(|(c, _)| c.clone()).collect();
        ids.sort();
        ids.dedup();
        match store.customers_conversations(&ids).await {
            Ok(rows) => {
                let mut by: HashMap<String, Vec<(String, i64)>> = HashMap::new();
                for (customer, conv, seq) in rows {
                    by.entry(customer).or_default().push((conv, seq));
                }
                for (customer, reply) in batch {
                    let mut v = by.get(&customer).cloned().unwrap_or_default();
                    v.sort();
                    let _ = reply.send(Ok(v));
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, customers = ids.len(), "batched subscription lookup failed");
                for (_, reply) in batch {
                    let _ = reply.send(Err(GwError::Unavailable("subscription lookup failed".into())));
                }
            }
        }
    }
}
