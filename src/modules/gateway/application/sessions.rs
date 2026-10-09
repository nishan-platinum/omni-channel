//! Sessions held by this node, keyed by customer id and agent id. Events from the stream are
//! handed to each session's bounded queue; a session that cannot keep up is disconnected (close
//! 1013) instead of buffering without limit — the client reconnects and `resume`s from its last
//! `seq`, so nothing durable is lost.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};

use super::super::domain::ActorKind;
use super::GwEvent;

pub type Push = Arc<GwEvent>;

/// Pending pushes per session before it counts as a slow consumer.
pub const SESSION_QUEUE: usize = 1024;

struct Entry {
    id: u64,
    tx: mpsc::Sender<Push>,
}

pub struct SessionRegistry {
    next: AtomicU64,
    customers: Mutex<HashMap<String, Vec<Entry>>>,
    agents: Mutex<HashMap<String, Vec<Entry>>>,
    shutdown: watch::Sender<bool>,
    pub customers_open: AtomicI64,
    pub agents_open: AtomicI64,
    pub slow_consumer_closes: AtomicU64,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            next: AtomicU64::new(1),
            customers: Mutex::new(HashMap::new()),
            agents: Mutex::new(HashMap::new()),
            shutdown: watch::channel(false).0,
            customers_open: AtomicI64::new(0),
            agents_open: AtomicI64::new(0),
            slow_consumer_closes: AtomicU64::new(0),
        }
    }
}

pub struct Registration {
    pub id: u64,
    pub role: ActorKind,
    pub key: String,
    pub rx: mpsc::Receiver<Push>,
    pub shutdown: watch::Receiver<bool>,
}

impl SessionRegistry {
    fn map(&self, role: ActorKind) -> &Mutex<HashMap<String, Vec<Entry>>> {
        match role {
            ActorKind::Agent => &self.agents,
            _ => &self.customers,
        }
    }

    fn gauge(&self, role: ActorKind) -> &AtomicI64 {
        match role {
            ActorKind::Agent => &self.agents_open,
            _ => &self.customers_open,
        }
    }

    pub fn register(&self, role: ActorKind, key: &str) -> Registration {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(SESSION_QUEUE);
        if let Ok(mut m) = self.map(role).lock() {
            m.entry(key.to_string()).or_default().push(Entry { id, tx });
        }
        self.gauge(role).fetch_add(1, Ordering::Relaxed);
        Registration { id, role, key: key.to_string(), rx, shutdown: self.shutdown.subscribe() }
    }

    pub fn unregister(&self, reg: &Registration) {
        if let Ok(mut m) = self.map(reg.role).lock() {
            if let Some(v) = m.get_mut(&reg.key) {
                v.retain(|e| e.id != reg.id);
                if v.is_empty() {
                    m.remove(&reg.key);
                }
            }
        }
        self.gauge(reg.role).fetch_sub(1, Ordering::Relaxed);
    }

    fn push_to(&self, entries: &mut Vec<Entry>, ev: &Push) {
        entries.retain(|e| match e.tx.try_send(ev.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Dropping the sender ends the session's queue: it closes with 1013 and resumes.
                self.slow_consumer_closes.fetch_add(1, Ordering::Relaxed);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
    }

    /// Delivers an event from the stream to the sessions on this node that are subscribed to it.
    pub fn dispatch(&self, ev: GwEvent) {
        let ev: Push = Arc::new(ev);
        match ev.as_ref() {
            GwEvent::Message { customer, agent, .. } => {
                if let Ok(mut m) = self.customers.lock() {
                    if let Some(v) = m.get_mut(customer) {
                        self.push_to(v, &ev);
                    }
                }
                if let Some(agent) = agent {
                    if let Ok(mut m) = self.agents.lock() {
                        if let Some(v) = m.get_mut(agent) {
                            self.push_to(v, &ev);
                        }
                    }
                }
            }
            GwEvent::Presence { .. } => {
                if let Ok(mut m) = self.agents.lock() {
                    for v in m.values_mut() {
                        self.push_to(v, &ev);
                    }
                }
            }
            GwEvent::Resync => {
                for map in [&self.customers, &self.agents] {
                    if let Ok(mut m) = map.lock() {
                        for v in m.values_mut() {
                            self.push_to(v, &ev);
                        }
                    }
                }
            }
            GwEvent::Config { .. } => {}
        }
    }

    /// Graceful shutdown: every session is told to reconnect elsewhere.
    pub fn begin_shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    pub fn counts(&self) -> (i64, i64) {
        (self.customers_open.load(Ordering::Relaxed), self.agents_open.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::domain::{Actor, CanonicalMessage, Channel, Direction, MessageKind};
    use super::*;

    fn msg(customer: &str, agent: Option<&str>) -> GwEvent {
        GwEvent::Message {
            customer: customer.into(),
            agent: agent.map(str::to_string),
            message: CanonicalMessage {
                message_id: "m".into(),
                conversation_id: "c".into(),
                seq: 1,
                channel: Channel::Whatsapp,
                direction: Direction::Inbound,
                actor: Actor { kind: ActorKind::Customer, id: customer.into() },
                kind: MessageKind::Text,
                body: serde_json::json!({}),
                received_at: "2026-10-05T09:00:00.000Z".into(),
                external_id: None,
            },
        }
    }

    #[tokio::test]
    async fn messages_reach_the_customer_and_the_assigned_agent_only() {
        let r = SessionRegistry::default();
        let mut c = r.register(ActorKind::Customer, "cust");
        let mut other = r.register(ActorKind::Customer, "other");
        let mut a = r.register(ActorKind::Agent, "a1");
        let mut b = r.register(ActorKind::Agent, "a2");
        r.dispatch(msg("cust", Some("a1")));
        assert!(c.rx.try_recv().is_ok() && a.rx.try_recv().is_ok());
        assert!(other.rx.try_recv().is_err() && b.rx.try_recv().is_err());
        r.dispatch(GwEvent::Presence { agent_id: "a1".into(), available: true });
        assert!(a.rx.try_recv().is_ok() && b.rx.try_recv().is_ok() && c.rx.try_recv().is_err());
        r.dispatch(GwEvent::Resync);
        assert!(c.rx.try_recv().is_ok() && other.rx.try_recv().is_ok() && a.rx.try_recv().is_ok() && b.rx.try_recv().is_ok());
        r.unregister(&c);
        assert_eq!(r.counts(), (1, 2));
    }

    #[tokio::test]
    async fn slow_consumer_is_cut_off_not_buffered_without_limit() {
        let r = SessionRegistry::default();
        let mut c = r.register(ActorKind::Customer, "cust");
        for _ in 0..=SESSION_QUEUE {
            r.dispatch(msg("cust", None));
        }
        let mut n = 0;
        while c.rx.recv().await.is_some() {
            n += 1;
        }
        assert_eq!(n, SESSION_QUEUE, "queue drains, then reports closed");
        assert_eq!(r.slow_consumer_closes.load(Ordering::Relaxed), 1);
    }
}
