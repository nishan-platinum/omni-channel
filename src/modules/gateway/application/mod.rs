//! Gateway application service (ADR-0014): ingestion → durable append → event stream →
//! routing → session fan-out. Every appended message is published once to the event stream; each
//! node delivers it to the sessions it holds (customer of the conversation, assigned agent).

pub mod metrics;
pub mod router;
pub mod sessions;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;

use super::domain::{ulid, Actor, ActorKind, CanonicalMessage, Channel, Direction, Fixture, Invalid, MessageKind, NewMessage};
use super::infrastructure::fixture::FixtureSource;
use super::infrastructure::store::{CustomerTarget, Store};
use metrics::Metrics;
use router::Router;
use sessions::SessionRegistry;

// ------------------------------------------------------------------------------------------------
// Errors and results
// ------------------------------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum GwError {
    #[error("{0}")]
    Invalid(String),
    #[error("duplicate message")]
    Duplicate,
    #[error("duplicate client_ref")]
    DuplicateClientRef,
    #[error("{0} not found")]
    NotFound(&'static str),
    #[error("conversation is not assigned to this agent")]
    NotAssigned,
    #[error("{0}")]
    Unavailable(String),
    #[error("internal error")]
    Internal(#[source] anyhow::Error),
}

impl GwError {
    pub fn internal(msg: &str) -> Self {
        GwError::Internal(anyhow::anyhow!(msg.to_string()))
    }
    pub fn code(&self) -> &'static str {
        match self {
            GwError::Invalid(_) => "invalid",
            GwError::Duplicate | GwError::DuplicateClientRef => "duplicate",
            GwError::NotFound(_) => "not_found",
            GwError::NotAssigned => "not_assigned",
            GwError::Unavailable(_) => "unavailable",
            GwError::Internal(_) => "internal",
        }
    }
}

impl From<Invalid> for GwError {
    fn from(e: Invalid) -> Self {
        GwError::Invalid(e.0)
    }
}

pub type GwResult<T> = Result<T, GwError>;

/// A committed append and who must see it.
#[derive(Debug, Clone)]
pub struct Appended {
    pub customer: String,
    pub agent: Option<String>,
    /// The conversation has no agent: routing must run for this skill.
    pub routing_skill: Option<String>,
    pub message: CanonicalMessage,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentPresence {
    pub id: String,
    pub skills: Vec<String>,
    /// `available` (connected and status true), `unavailable` (connected, status false), `offline`.
    pub status: &'static str,
    pub connected: bool,
    pub open_conversations: i64,
    pub idle_since: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PresenceView {
    pub agents: Vec<AgentPresence>,
    pub queues: BTreeMap<String, i64>,
}

#[derive(Debug, Default)]
pub struct ReapOutcome {
    pub presence_changed: Vec<String>,
    pub requeued: usize,
    pub assigned: Vec<Appended>,
}

// ------------------------------------------------------------------------------------------------
// Event stream
// ------------------------------------------------------------------------------------------------

/// What travels between nodes. `Message` is the canonical message plus its two audiences.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum GwEvent {
    Message { customer: String, agent: Option<String>, message: CanonicalMessage },
    Presence { agent_id: String, available: bool },
    Config { version: i64 },
}

pub type EventSink = Arc<dyn Fn(GwEvent) + Send + Sync>;

/// Publishes a committed append once to the event stream.
pub async fn publish(bus: &dyn EventBus, a: Appended) {
    bus.publish(&GwEvent::Message { customer: a.customer, agent: a.agent, message: a.message }).await;
}

#[async_trait]
pub trait EventBus: Send + Sync {
    fn name(&self) -> &'static str;
    async fn publish(&self, event: &GwEvent);
    /// Starts delivering events to `sink`; `subscribed` fires once the subscription is live.
    fn start(&self, sink: EventSink, subscribed: tokio::sync::oneshot::Sender<()>);
    async fn healthy(&self) -> bool;
}

// ------------------------------------------------------------------------------------------------
// Service
// ------------------------------------------------------------------------------------------------

pub struct Gateway {
    pub store: Store,
    pub bus: Arc<dyn EventBus>,
    pub sessions: Arc<SessionRegistry>,
    pub metrics: Arc<Metrics>,
    pub router: Router,
    pub node_id: String,
    fixture: RwLock<(Arc<Fixture>, i64)>,
    fixture_source: FixtureSource,
    session_key: Vec<u8>,
    /// Accepting connections (`/healthz` 200).
    pub ready: AtomicBool,
}

type HmacSha256 = Hmac<Sha256>;

impl Gateway {
    pub fn new(store: Store, bus: Arc<dyn EventBus>, node_id: String, fixture_source: FixtureSource, session_key: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            router: Router::new(store.clone(), bus.clone()),
            store,
            bus,
            sessions: Arc::new(SessionRegistry::default()),
            metrics: Arc::new(Metrics::default()),
            node_id,
            fixture: RwLock::new((Arc::new(Fixture { skills: vec![], agents: vec![], channel_to_skill: BTreeMap::new() }), 0)),
            fixture_source,
            session_key,
            ready: AtomicBool::new(false),
        })
    }

    pub fn fixture(&self) -> Arc<Fixture> {
        self.fixture.read().map(|g| g.0.clone()).unwrap_or_else(|p| p.into_inner().0.clone())
    }

    fn set_fixture(&self, f: Fixture, version: i64) {
        let mut g = self.fixture.write().unwrap_or_else(|p| p.into_inner());
        if version >= g.1 {
            *g = (Arc::new(f), version);
        }
    }

    /// Startup: load the fixture from its source into the shared store, register the node, drop
    /// connection rows left by this node's previous life, subscribe to the event stream.
    pub async fn start(self: &Arc<Self>) -> anyhow::Result<()> {
        self.store.heartbeat(&self.node_id).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        self.reload(None).await.map_err(|e| anyhow::anyhow!("fixture: {e}"))?;
        for agent in self.store.node_started(&self.node_id).await.map_err(|e| anyhow::anyhow!("{e}"))? {
            self.bus.publish(&GwEvent::Presence { agent_id: agent, available: false }).await;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let me = Arc::downgrade(self);
        self.bus.start(
            Arc::new(move |ev| {
                if let Some(gw) = me.upgrade() {
                    gw.on_event(ev);
                }
            }),
            tx,
        );
        tokio::time::timeout(Duration::from_secs(30), rx)
            .await
            .map_err(|_| anyhow::anyhow!("event stream subscription timed out"))?
            .map_err(|_| anyhow::anyhow!("event stream subscription failed"))?;
        self.spawn_background();
        Ok(())
    }

    fn on_event(self: &Arc<Self>, ev: GwEvent) {
        if let GwEvent::Config { version } = ev {
            let me = self.clone();
            tokio::spawn(async move {
                match me.store.load_fixture().await {
                    Ok(Some((f, v))) => me.set_fixture(f, v.max(version)),
                    Ok(None) => {}
                    Err(e) => tracing::warn!(error = %e, "could not load reloaded fixture"),
                }
            });
            return;
        }
        self.sessions.dispatch(ev);
    }

    fn spawn_background(self: &Arc<Self>) {
        let me = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(gw) = me.upgrade() else { return };
                if let Err(e) = gw.store.heartbeat(&gw.node_id).await {
                    tracing::warn!(error = %e, "node heartbeat failed");
                }
                if let Err(e) = gw.tick().await {
                    tracing::warn!(error = %e, "reaper pass failed");
                }
            }
        });
    }

    /// One failure-detection / safety routing pass.
    pub async fn tick(&self) -> GwResult<()> {
        let outcome = self.store.reap().await?;
        for agent in outcome.presence_changed {
            self.bus.publish(&GwEvent::Presence { agent_id: agent, available: false }).await;
        }
        for a in outcome.assigned {
            self.publish(a).await;
        }
        Ok(())
    }

    async fn publish(&self, a: Appended) {
        publish(self.bus.as_ref(), a).await;
    }

    /// After a committed append: count it, publish it, and ask for routing if it has no agent
    /// (asynchronously — the caller's acknowledgement does not wait for routing).
    async fn after_append(&self, a: Appended) -> CanonicalMessage {
        self.metrics.message(a.message.channel, a.message.direction);
        let message = a.message.clone();
        let skill = a.routing_skill.clone();
        self.publish(a).await;
        if let Some(skill) = skill {
            self.router.request(skill);
        }
        message
    }

    // --- ingress ---------------------------------------------------------------------------------

    pub async fn ingest(&self, channel: Channel, customer: &str, m: NewMessage) -> GwResult<CanonicalMessage> {
        let fixture = self.fixture();
        let target = CustomerTarget { channel, customer, conversation_id: None, fixture: &fixture };
        match self.store.append_customer(target, &m).await {
            Ok(a) => Ok(self.after_append(a).await),
            Err(GwError::Duplicate) => {
                self.metrics.duplicate();
                Err(GwError::Duplicate)
            }
            Err(e) => Err(e),
        }
    }

    // --- WebSocket actions -----------------------------------------------------------------------

    async fn append_idempotent(
        &self,
        actor: &Actor,
        client_ref: Option<&str>,
        append: impl std::future::Future<Output = GwResult<Appended>>,
    ) -> GwResult<CanonicalMessage> {
        match append.await {
            Ok(a) => Ok(self.after_append(a).await),
            Err(GwError::DuplicateClientRef) => {
                self.metrics.duplicate();
                let r = client_ref.unwrap_or_default();
                self.store.by_client_ref(actor, r).await?.ok_or(GwError::Duplicate)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn customer_send(
        &self,
        customer: &str,
        conversation_id: Option<&str>,
        text: &str,
        client_ref: Option<&str>,
    ) -> GwResult<CanonicalMessage> {
        super::domain::validate_text(text)?;
        let actor = Actor { kind: ActorKind::Customer, id: customer.to_string() };
        let m = NewMessage {
            direction: Direction::Inbound,
            actor: actor.clone(),
            kind: MessageKind::Text,
            body: json!({ "text": text }),
            external_id: None,
            dedup_key: None,
            client_ref: client_ref.map(str::to_string),
        };
        let fixture = self.fixture();
        let target = CustomerTarget { channel: Channel::Whatsapp, customer, conversation_id, fixture: &fixture };
        self.append_idempotent(&actor, client_ref, self.store.append_customer(target, &m)).await
    }

    pub async fn agent_send(&self, agent: &str, conversation_id: &str, text: &str, client_ref: Option<&str>) -> GwResult<CanonicalMessage> {
        super::domain::validate_text(text)?;
        let actor = Actor { kind: ActorKind::Agent, id: agent.to_string() };
        let m = NewMessage {
            direction: Direction::Outbound,
            actor: actor.clone(),
            kind: MessageKind::Text,
            body: json!({ "text": text }),
            external_id: None,
            dedup_key: None,
            client_ref: client_ref.map(str::to_string),
        };
        self.append_idempotent(&actor, client_ref, self.store.append_agent(agent, conversation_id, &m)).await
    }

    pub async fn disposition(
        &self,
        agent: &str,
        conversation_id: &str,
        code: &str,
        client_ref: Option<&str>,
    ) -> GwResult<CanonicalMessage> {
        if code.trim().is_empty() || code.len() > 64 {
            return Err(GwError::Invalid("code must be 1..64 characters".into()));
        }
        let actor = Actor { kind: ActorKind::Agent, id: agent.to_string() };
        let m = NewMessage {
            direction: Direction::Outbound,
            actor: actor.clone(),
            kind: MessageKind::Disposition,
            body: json!({ "code": code }),
            external_id: None,
            dedup_key: None,
            client_ref: client_ref.map(str::to_string),
        };
        self.append_idempotent(&actor, client_ref, self.store.append_agent(agent, conversation_id, &m)).await
    }

    // --- presence --------------------------------------------------------------------------------

    pub async fn agent_connected(
        &self,
        agent: &str,
        hello_skills: &[String],
        connection_id: &str,
        session_id: &str,
        resume: bool,
    ) -> GwResult<()> {
        let available = self.store.agent_connected(agent, hello_skills, connection_id, session_id, &self.node_id, resume).await?;
        // A resumed session may come back available: announce it and serve its queues.
        if resume && available {
            self.bus.publish(&GwEvent::Presence { agent_id: agent.to_string(), available: true }).await;
            self.drain_for(agent).await;
        }
        Ok(())
    }

    pub async fn agent_disconnected(&self, agent: &str, connection_id: &str) {
        match self.store.agent_disconnected(agent, connection_id).await {
            Ok(true) => self.bus.publish(&GwEvent::Presence { agent_id: agent.to_string(), available: false }).await,
            Ok(false) => {}
            // The node heartbeat stops covering this row once the node dies; a live node's stale
            // row is cleaned up when the node restarts (node_started).
            Err(e) => tracing::warn!(error = %e, agent, "could not record agent disconnect"),
        }
    }

    pub async fn set_status(&self, agent: &str, available: bool) -> GwResult<()> {
        let changed = self.store.set_status(agent, available).await?;
        if changed {
            self.bus.publish(&GwEvent::Presence { agent_id: agent.to_string(), available }).await;
        }
        if available {
            self.drain_for(agent).await;
        }
        Ok(())
    }

    /// The agent may have become available: serve the queues of its skills.
    async fn drain_for(&self, agent: &str) {
        match self.store.agent_skills(agent).await {
            Ok(skills) => self.router.request_all(skills),
            Err(e) => tracing::warn!(error = %e, agent, "could not read agent skills; the reaper routes instead"),
        }
    }

    pub async fn presence(&self) -> GwResult<PresenceView> {
        let skills: Vec<String> = self.fixture().queue_skills().into_iter().collect();
        self.store.presence(&skills).await
    }

    // --- configuration ---------------------------------------------------------------------------

    /// Re-reads the platform fixture (or applies `override_` when given), stores it for all nodes,
    /// tells the other nodes, and serves queues that may now have agents.
    pub async fn reload(&self, override_: Option<Fixture>) -> GwResult<i64> {
        let fixture = match override_ {
            Some(f) => f,
            None => self.fixture_source.load().await.map_err(|e| GwError::Unavailable(format!("fixture: {e}")))?,
        };
        fixture.validate()?;
        let version = self.store.save_fixture(&fixture).await?;
        self.set_fixture(fixture.clone(), version);
        self.bus.publish(&GwEvent::Config { version }).await;
        self.router.request_all(fixture.queue_skills());
        tracing::info!(version, agents = fixture.agents.len(), skills = fixture.skills.len(), "fixture loaded");
        Ok(version)
    }

    // --- session ids -----------------------------------------------------------------------------

    /// Opaque, signed session id: any node can resume it without shared session storage.
    pub fn issue_session(&self, role: ActorKind, id: &str) -> String {
        let body = format!("{}.{}.{}", &role.as_str()[..1], ulid(), URL_SAFE_NO_PAD.encode(id));
        format!("{body}.{}", self.sign(&body))
    }

    pub fn verify_session(&self, session: &str) -> Option<(ActorKind, String)> {
        let (body, mac) = session.rsplit_once('.')?;
        let expected = self.sign(body);
        if !bool::from(subtle::ConstantTimeEq::ct_eq(mac.as_bytes(), expected.as_bytes())) {
            return None;
        }
        let mut parts = body.splitn(3, '.');
        let role = match parts.next()? {
            "c" => ActorKind::Customer,
            "a" => ActorKind::Agent,
            _ => return None,
        };
        let _ulid = parts.next()?;
        let id = String::from_utf8(URL_SAFE_NO_PAD.decode(parts.next()?).ok()?).ok()?;
        Some((role, id))
    }

    fn sign(&self, body: &str) -> String {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.session_key).expect("HMAC accepts any key length");
        mac.update(body.as_bytes());
        hex::encode(&mac.finalize().into_bytes()[..16])
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}
