//! Real-time buses (FR-ARC-003). `RedisBus` fans events out across nodes through Redis pub/sub:
//! every node subscribes to one channel and forwards envelopes to the sessions it holds.
//! `LocalBus` is the single-node fallback used when `REDIS_URL` is not set.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::Mutex;

use super::super::application::ports::{BusEnvelope, BusSink, RealtimeBus};

pub const HUB_CHANNEL: &str = "occ:hub:events";

/// In-process bus: publish delivers straight to this node's sessions.
#[derive(Default)]
pub struct LocalBus {
    sink: OnceLock<Arc<dyn BusSink>>,
}

#[async_trait]
impl RealtimeBus for LocalBus {
    fn name(&self) -> &'static str {
        "local"
    }

    async fn publish(&self, env: BusEnvelope) {
        if let Some(s) = self.sink.get() {
            s.deliver(env);
        }
    }

    fn start(&self, sink: Arc<dyn BusSink>) {
        let _ = self.sink.set(sink);
    }

    async fn healthy(&self) -> bool {
        true
    }
}

pub struct RedisBus {
    client: redis::Client,
    conn: Mutex<Option<redis::aio::MultiplexedConnection>>,
}

impl RedisBus {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        Ok(Self { client: redis::Client::open(url)?, conn: Mutex::new(None) })
    }

    async fn connection(&self) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let c = self.client.get_multiplexed_async_connection().await?;
        *guard = Some(c.clone());
        Ok(c)
    }

    async fn reset(&self) {
        *self.conn.lock().await = None;
    }

    pub async fn ping(&self) -> redis::RedisResult<()> {
        let mut c = self.connection().await?;
        let r = redis::cmd("PING").query_async::<String>(&mut c).await.map(|_| ());
        if r.is_err() {
            // A connection broken by a Redis restart must not keep /ready failing until the next publish.
            self.reset().await;
        }
        r
    }
}

#[async_trait]
impl RealtimeBus for RedisBus {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn publish(&self, env: BusEnvelope) {
        let payload = match serde_json::to_string(&env) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "unserialisable hub event");
                return;
            }
        };
        // One retry with a fresh connection; real-time delivery is best-effort (clients resume
        // by sequence number), durability is the database's job.
        for attempt in 0..2 {
            match self.connection().await {
                Ok(mut c) => match redis::cmd("PUBLISH").arg(HUB_CHANNEL).arg(&payload).query_async::<i64>(&mut c).await {
                    Ok(_) => return,
                    Err(e) => {
                        tracing::warn!(error = %e, attempt, "redis publish failed");
                        self.reset().await;
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, attempt, "redis connection failed");
                    self.reset().await;
                }
            }
        }
    }

    fn start(&self, sink: Arc<dyn BusSink>) {
        let client = self.client.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(200);
            loop {
                match client.get_async_pubsub().await {
                    Ok(mut ps) => match ps.subscribe(HUB_CHANNEL).await {
                        Ok(()) => {
                            tracing::info!(channel = HUB_CHANNEL, "redis bus subscribed");
                            backoff = Duration::from_millis(200);
                            let mut stream = ps.into_on_message();
                            while let Some(msg) = stream.next().await {
                                match serde_json::from_slice::<BusEnvelope>(msg.get_payload_bytes()) {
                                    Ok(env) => sink.deliver(env),
                                    Err(e) => tracing::warn!(error = %e, "malformed hub bus message ignored"),
                                }
                            }
                            tracing::warn!("redis bus subscription ended; reconnecting");
                        }
                        Err(e) => tracing::warn!(error = %e, "redis subscribe failed"),
                    },
                    Err(e) => tracing::warn!(error = %e, "redis pubsub connection failed"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        });
    }

    async fn healthy(&self) -> bool {
        self.ping().await.is_ok()
    }
}
