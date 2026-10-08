//! Event stream between gateway nodes. Redis pub/sub carries every committed message once to all
//! nodes; each node delivers it to the sessions it holds. Pub/sub is not the durability layer —
//! the store is: a session that misses an event (reconnect, Redis blip) gets it from the store
//! through `resume` or the per-conversation gap fill.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::{oneshot, Mutex};

use super::super::application::{EventBus, EventSink, GwEvent};

pub const CHANNEL: &str = "gw:events";

/// Single-node bus (no `GATEWAY_REDIS_URL`): publish delivers in-process.
#[derive(Default)]
pub struct LocalBus {
    sink: StdMutex<Option<EventSink>>,
}

#[async_trait]
impl EventBus for LocalBus {
    fn name(&self) -> &'static str {
        "local"
    }

    async fn publish(&self, event: &GwEvent) {
        let sink = self.sink.lock().ok().and_then(|g| g.clone());
        if let Some(s) = sink {
            s(event.clone());
        }
    }

    fn start(&self, sink: EventSink, subscribed: oneshot::Sender<()>) {
        if let Ok(mut g) = self.sink.lock() {
            *g = Some(sink);
        }
        let _ = subscribed.send(());
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
    pub fn new(url: &str) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self { client: redis::Client::open(url)?, conn: Mutex::new(None) }))
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
}

#[async_trait]
impl EventBus for RedisBus {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn publish(&self, event: &GwEvent) {
        let payload = match serde_json::to_vec(event) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "unserialisable gateway event");
                return;
            }
        };
        for attempt in 0..3 {
            match self.connection().await {
                Ok(mut c) => match redis::cmd("PUBLISH").arg(CHANNEL).arg(&payload).query_async::<i64>(&mut c).await {
                    Ok(_) => return,
                    Err(e) => tracing::warn!(error = %e, attempt, "event publish failed"),
                },
                Err(e) => tracing::warn!(error = %e, attempt, "event stream connection failed"),
            }
            *self.conn.lock().await = None;
            tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
        }
        // Durable already; subscribers catch up via resume / gap fill.
        tracing::error!("event not published after retries; sessions will catch up from the store");
    }

    fn start(&self, sink: EventSink, subscribed: oneshot::Sender<()>) {
        let client = self.client.clone();
        tokio::spawn(async move {
            let mut subscribed = Some(subscribed);
            let mut backoff = Duration::from_millis(200);
            loop {
                match client.get_async_pubsub().await {
                    Ok(mut ps) => match ps.subscribe(CHANNEL).await {
                        Ok(()) => {
                            tracing::info!(channel = CHANNEL, "event stream subscribed");
                            if let Some(tx) = subscribed.take() {
                                let _ = tx.send(());
                            }
                            backoff = Duration::from_millis(200);
                            let mut stream = ps.into_on_message();
                            while let Some(msg) = stream.next().await {
                                match serde_json::from_slice::<GwEvent>(msg.get_payload_bytes()) {
                                    Ok(ev) => sink(ev),
                                    Err(e) => tracing::warn!(error = %e, "malformed gateway event ignored"),
                                }
                            }
                            tracing::warn!("event stream subscription ended; reconnecting");
                        }
                        Err(e) => tracing::warn!(error = %e, "event stream subscribe failed"),
                    },
                    Err(e) => tracing::warn!(error = %e, "event stream connection failed"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        });
    }

    async fn healthy(&self) -> bool {
        match self.connection().await {
            Ok(mut c) => redis::cmd("PING").query_async::<String>(&mut c).await.is_ok(),
            Err(_) => false,
        }
    }
}
