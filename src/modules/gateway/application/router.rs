//! Per-node routing coordinator. At most one queue drain per skill runs on this node at a time;
//! a request that arrives meanwhile marks the skill dirty and the running drain goes round once
//! more, so no request is lost and no request waits. Callers never block on routing: ingest
//! acknowledges as soon as the message is durable.
//!
//! Without this, every ingest of a new conversation waited on the skill's advisory lock while
//! holding a pooled connection; a burst of new conversations exhausted the pool and stalled all
//! other database work (E2 measurement, ADR-0014).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::super::infrastructure::store::Store;
use super::{publish, EventBus};

#[derive(Default)]
struct Slot {
    running: bool,
    dirty: bool,
}

#[derive(Clone)]
pub struct Router {
    store: Store,
    bus: Arc<dyn EventBus>,
    slots: Arc<Mutex<HashMap<String, Slot>>>,
}

impl Router {
    pub fn new(store: Store, bus: Arc<dyn EventBus>) -> Self {
        Self { store, bus, slots: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Serve `skill`'s queue soon (this node). Returns at once.
    pub fn request(&self, skill: String) {
        {
            let Ok(mut slots) = self.slots.lock() else { return };
            let slot = slots.entry(skill.clone()).or_default();
            if slot.running {
                slot.dirty = true;
                return;
            }
            slot.running = true;
        }
        let me = self.clone();
        tokio::spawn(async move { me.drain_loop(skill).await });
    }

    pub fn request_all(&self, skills: impl IntoIterator<Item = String>) {
        for s in skills {
            self.request(s);
        }
    }

    async fn drain_loop(&self, skill: String) {
        loop {
            if let Ok(mut slots) = self.slots.lock() {
                if let Some(s) = slots.get_mut(&skill) {
                    s.dirty = false;
                }
            }
            // Chunk by chunk, publishing each as soon as it is committed.
            let ok = loop {
                match self.store.drain_chunk(&skill).await {
                    Ok((assigned, more)) => {
                        for a in assigned {
                            publish(self.bus.as_ref(), a).await;
                        }
                        if !more {
                            break true;
                        }
                    }
                    Err(e) => {
                        // The reaper's safety pass retries within a second.
                        tracing::warn!(error = %e, skill = %skill, "queue drain failed");
                        break false;
                    }
                }
            };
            let Ok(mut slots) = self.slots.lock() else { return };
            let Some(slot) = slots.get_mut(&skill) else { return };
            if ok && slot.dirty {
                continue;
            }
            slot.running = false;
            slot.dirty = false;
            return;
        }
    }
}
