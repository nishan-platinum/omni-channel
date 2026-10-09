//! Per-node registry of live WebSocket sessions. The bus delivers every envelope to every node;
//! each node forwards it only to the sessions it holds. Memory per idle session is one small
//! bounded channel plus the socket task (no per-session timers besides the heartbeat tick).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};

use super::ports::{BusEnvelope, BusSink, Target};

/// Frames pushed to a session task.
#[derive(Debug, Clone)]
pub enum Push {
    Json(Arc<str>),
}

/// Bounded so a stuck client cannot grow memory; on overflow the session is closed and the client
/// resumes from its last sequence number. Customers receive few events and are the 100k-scale
/// population, so their buffer is small; agents are few but get bursts (coming online to a full
/// queue assigns up to their capacity at once, each with replayed history).
pub const CUSTOMER_BUFFER: usize = 64;
pub const AGENT_BUFFER: usize = 1024;

#[derive(Default)]
struct Slots {
    by_target: HashMap<Target, Vec<(u64, mpsc::Sender<Push>)>>,
}

pub struct SessionRegistry {
    slots: Mutex<Slots>,
    next_id: AtomicU64,
    shutdown_tx: watch::Sender<bool>,
    agents: AtomicU64,
    customers: AtomicU64,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Registration {
    pub id: u64,
    pub target: Target,
    pub rx: mpsc::Receiver<Push>,
    pub shutdown: watch::Receiver<bool>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            slots: Mutex::new(Slots::default()),
            next_id: AtomicU64::new(1),
            shutdown_tx,
            agents: AtomicU64::new(0),
            customers: AtomicU64::new(0),
        }
    }

    pub fn register(&self, target: Target) -> Registration {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(match target {
            Target::Agent { .. } => AGENT_BUFFER,
            Target::Customer { .. } => CUSTOMER_BUFFER,
        });
        match target {
            Target::Agent { .. } => self.agents.fetch_add(1, Ordering::Relaxed),
            Target::Customer { .. } => self.customers.fetch_add(1, Ordering::Relaxed),
        };
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).by_target.entry(target.clone()).or_default().push((id, tx));
        Registration { id, target, rx, shutdown: self.shutdown_tx.subscribe() }
    }

    pub fn unregister(&self, target: &Target, id: u64) {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let mut removed = false;
        if let Some(v) = slots.by_target.get_mut(target) {
            let before = v.len();
            v.retain(|(sid, _)| *sid != id);
            removed = v.len() < before;
            if v.is_empty() {
                slots.by_target.remove(target);
            }
        }
        if removed {
            match target {
                Target::Agent { .. } => self.agents.fetch_sub(1, Ordering::Relaxed),
                Target::Customer { .. } => self.customers.fetch_sub(1, Ordering::Relaxed),
            };
        }
    }

    /// Pushes a JSON frame to every local session of `target`. Sessions whose buffer is full are
    /// dropped from the registry (their task ends and the client resumes by sequence number).
    pub fn push(&self, target: &Target, frame: Arc<str>) -> usize {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let Some(v) = slots.by_target.get_mut(target) else {
            return 0;
        };
        let mut delivered = 0;
        let before = v.len();
        v.retain(|(_, tx)| match tx.try_send(Push::Json(frame.clone())) {
            Ok(()) => {
                delivered += 1;
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("slow WebSocket consumer dropped; it will resume by sequence number");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
        let dropped = (before - v.len()) as u64;
        if v.is_empty() {
            slots.by_target.remove(target);
        }
        if dropped > 0 {
            match target {
                Target::Agent { .. } => self.agents.fetch_sub(dropped, Ordering::Relaxed),
                Target::Customer { .. } => self.customers.fetch_sub(dropped, Ordering::Relaxed),
            };
        }
        delivered
    }

    pub fn has_local(&self, target: &Target) -> bool {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).by_target.contains_key(target)
    }

    /// (agent sessions, customer sessions) on this node — exported on /ready and the admin page
    /// (WebSocket connection counts, FR-ARC-007).
    pub fn counts(&self) -> (u64, u64) {
        (self.agents.load(Ordering::Relaxed), self.customers.load(Ordering::Relaxed))
    }

    /// Graceful shutdown: every session sends `reconnect` and closes with 1012 (service restart).
    pub fn begin_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl BusSink for SessionRegistry {
    fn deliver(&self, env: BusEnvelope) {
        match serde_json::to_string(&env.payload) {
            Ok(s) => {
                self.push(&env.target, Arc::from(s));
            }
            Err(e) => tracing::error!(error = %e, "unserialisable hub event"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn push_reaches_only_the_target() {
        let r = SessionRegistry::new();
        let a = Target::Agent { id: Uuid::from_u128(1) };
        let b = Target::Agent { id: Uuid::from_u128(2) };
        let mut ra = r.register(a.clone());
        let mut rb = r.register(b.clone());
        assert_eq!(r.push(&a, Arc::from("{\"x\":1}")), 1);
        assert!(matches!(ra.rx.recv().await, Some(Push::Json(s)) if &*s == "{\"x\":1}"));
        assert!(rb.rx.try_recv().is_err());
        assert_eq!(r.counts(), (2, 0));
        r.unregister(&a, ra.id);
        assert_eq!(r.counts(), (1, 0));
        assert_eq!(r.push(&a, Arc::from("{}")), 0);
    }

    #[tokio::test]
    async fn overflowing_session_is_dropped() {
        let r = SessionRegistry::new();
        let a = Target::Customer { endpoint: Uuid::from_u128(1), visitor: "v".into() };
        let _reg = r.register(a.clone());
        for _ in 0..CUSTOMER_BUFFER {
            assert_eq!(r.push(&a, Arc::from("{}")), 1);
        }
        assert_eq!(r.push(&a, Arc::from("{}")), 0);
        assert!(!r.has_local(&a));
        assert_eq!(r.counts(), (0, 0));
    }
}
