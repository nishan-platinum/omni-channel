//! Hub application service: ingest → durable append → route → real-time fan-out, agent replies,
//! presence, outbound delivery with retries, receipts, and the background ticks.
//!
//! Durability rule (OCC-M10-R015/R033): a message is acknowledged to its sender only after the
//! transaction that stored it has committed. Real-time pushes happen after commit and are
//! best-effort; clients that miss a push catch up by sequence number on (re)connect.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};

use chrono::{Duration, Utc};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::bootstrap_auth::service::TenantGate;
use crate::platform::errors::{AppError, AppResult};
use crate::platform::security::{random_token, sha256};

use super::super::domain::{
    validate_body, CanonicalMessage, CanonicalStatus, Channel, ConversationStatus, Direction, MessageKind, Presence,
};
use super::ports::{
    AgentView, Assignment, BusEnvelope, ChannelAdapter, ChannelHealth, ConversationView, CustomerSession, Endpoint, HubRepository, Inbound,
    MessageView, RealtimeBus, StatusChange, Target,
};

/// Heartbeat freshness after which an agent counts as gone (node crash, closed laptop).
pub const AGENT_STALE_SECS: i64 = 60;
/// Messages replayed per conversation on (re)connect.
pub const REPLAY_LIMIT: i64 = 200;
const CUSTOMER_SESSION_HOURS: i64 = 24;

#[derive(Debug, Clone, Copy)]
pub struct AgentCtx {
    pub tenant_id: Uuid,
    pub user_id: Uuid,
}

/// Endpoints almost never change; inbound traffic looks them up on every message.
const ENDPOINT_CACHE_TTL: StdDuration = StdDuration::from_secs(30);
/// Tenant status re-checked at most every 2 s per tenant on the message hot path, so a
/// suspension stops ingest within 2 s (logins and API calls re-check on every request).
const TENANT_CACHE_TTL: StdDuration = StdDuration::from_secs(2);

/// Tiny TTL cache (per node). Entries are tenant-agnostic lookups keyed by provider address or
/// tenant id; nothing tenant-private is cached across tenants.
struct TtlCache<K, V> {
    ttl: StdDuration,
    map: Mutex<HashMap<K, (V, Instant)>>,
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> TtlCache<K, V> {
    fn new(ttl: StdDuration) -> Self {
        Self { ttl, map: Mutex::new(HashMap::new()) }
    }

    fn get(&self, k: &K) -> Option<V> {
        let m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        m.get(k).filter(|(_, at)| at.elapsed() < self.ttl).map(|(v, _)| v.clone())
    }

    fn put(&self, k: K, v: V) {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if m.len() > 50_000 {
            m.retain(|_, (_, at)| at.elapsed() < self.ttl);
        }
        m.insert(k, (v, Instant::now()));
    }
}

pub struct HubService {
    pub repo: Arc<dyn HubRepository>,
    pub bus: Arc<dyn RealtimeBus>,
    pub gate: Arc<dyn TenantGate>,
    adapters: HashMap<Channel, Arc<dyn ChannelAdapter>>,
    pub node_id: String,
    endpoints: TtlCache<(Channel, String), Endpoint>,
    tenant_ok: TtlCache<Uuid, bool>,
}

/// Outcome of processing one provider request.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct IngestReport {
    pub messages: usize,
    pub duplicates: usize,
    pub statuses: usize,
}

impl HubService {
    pub fn new(
        repo: Arc<dyn HubRepository>,
        bus: Arc<dyn RealtimeBus>,
        gate: Arc<dyn TenantGate>,
        adapters: Vec<Arc<dyn ChannelAdapter>>,
        node_id: String,
    ) -> Self {
        let adapters = adapters.into_iter().map(|a| (a.channel(), a)).collect();
        Self {
            repo,
            bus,
            gate,
            adapters,
            node_id,
            endpoints: TtlCache::new(ENDPOINT_CACHE_TTL),
            tenant_ok: TtlCache::new(TENANT_CACHE_TTL),
        }
    }

    pub fn adapter(&self, channel: Channel) -> AppResult<&Arc<dyn ChannelAdapter>> {
        self.adapters.get(&channel).ok_or_else(|| AppError::not_found(format!("No adapter for channel {channel}")))
    }

    pub async fn connect_adapters(&self) -> AppResult<()> {
        for a in self.adapters.values() {
            a.connect().await?;
            tracing::info!(channel = %a.channel(), simulated = a.simulated(), "channel adapter connected");
        }
        Ok(())
    }

    pub async fn channel_health(&self) -> Vec<ChannelHealth> {
        let mut out = Vec::new();
        for a in self.adapters.values() {
            out.push(a.health().await);
        }
        out.sort_by_key(|h| h.channel.as_str());
        out
    }

    // --------------------------------------------------------------------------------------------
    // Inbound
    // --------------------------------------------------------------------------------------------

    /// Raw provider request (webhook / event feed) → adapter verifies and parses → canonical
    /// messages and receipts are applied.
    pub async fn ingest_raw(&self, channel: Channel, signature: Option<&str>, body: &[u8]) -> AppResult<IngestReport> {
        let events = self.adapter(channel)?.ingest(signature, body)?;
        let mut report = IngestReport::default();
        for ev in events {
            match ev {
                Inbound::Message(m) => match self.ingest_message(&m).await? {
                    (_, false) => report.messages += 1,
                    (_, true) => report.duplicates += 1,
                },
                Inbound::Status(s) => {
                    self.apply_status(&s).await?;
                    report.statuses += 1;
                }
            }
        }
        Ok(report)
    }

    /// Stores one canonical inbound message. Tenant comes ONLY from the endpoint registry.
    /// Returns the stored message and whether it was a duplicate (already stored earlier).
    pub async fn ingest_message(&self, m: &CanonicalMessage) -> AppResult<(MessageView, bool)> {
        let ep = self.endpoint(m.channel, &m.endpoint_address).await?;
        self.require_tenant_active(ep.tenant_id).await?;
        let appended = self.repo.append_inbound(&ep, m).await?;
        let msg = appended.message;
        if appended.duplicate {
            return Ok((msg, true));
        }
        let conv = appended.conversation;
        self.fan_out_message(&conv, &msg).await;
        if conv.status == ConversationStatus::Queued {
            if let Some(a) = self.repo.route(conv.tenant_id, conv.id).await? {
                self.announce_assignment(a).await?;
            }
        }
        Ok((msg, false))
    }

    /// Endpoint registry lookup (cached). Unknown addresses are not cached.
    async fn endpoint(&self, channel: Channel, address: &str) -> AppResult<Endpoint> {
        let key = (channel, address.to_string());
        if let Some(ep) = self.endpoints.get(&key) {
            return Ok(ep);
        }
        let ep = self
            .repo
            .resolve_endpoint(channel, address)
            .await?
            .ok_or_else(|| AppError::not_found("Unknown channel endpoint (no tenant owns this number/address)"))?;
        self.endpoints.put(key, ep.clone());
        Ok(ep)
    }

    async fn require_tenant_active(&self, tenant: Uuid) -> AppResult<()> {
        if self.tenant_ok.get(&tenant) == Some(true) {
            return Ok(());
        }
        match self.gate.access_info(tenant).await? {
            Some(i) if i.allows_access && !i.read_only => {
                self.tenant_ok.put(tenant, true);
                Ok(())
            }
            Some(_) => Err(AppError::tenant_suspended()),
            None => Err(AppError::not_found("Unknown tenant")),
        }
    }

    pub async fn apply_status(&self, s: &CanonicalStatus) -> AppResult<()> {
        if let Some(change) = self.repo.apply_status(s).await? {
            self.fan_out_status(&change).await;
        }
        Ok(())
    }

    // --------------------------------------------------------------------------------------------
    // Agent actions
    // --------------------------------------------------------------------------------------------

    pub async fn agent_profile(&self, a: AgentCtx) -> AppResult<AgentView> {
        self.repo.agent(a.tenant_id, a.user_id).await?.ok_or_else(|| AppError::forbidden("This user is not configured as an agent"))
    }

    /// Conversations assigned to the agent with messages after the client's last seen sequence.
    pub async fn agent_snapshot(&self, a: AgentCtx, resume: &HashMap<Uuid, i64>) -> AppResult<Value> {
        let agent = self.agent_profile(a).await?;
        let mut convs = Vec::new();
        for c in self.repo.agent_conversations(a.tenant_id, a.user_id).await? {
            let after = resume.get(&c.id).copied().unwrap_or(0);
            let msgs = self.replay(&c, after).await?;
            convs.push(json!({ "conversation": c, "messages": msgs }));
        }
        Ok(json!({
            "type": "welcome",
            "node": self.node_id,
            "agent": { "id": agent.user_id, "name": agent.display_name, "presence": agent.presence, "skills": agent.skills, "max_concurrent": agent.max_concurrent },
            "conversations": convs,
        }))
    }

    async fn replay(&self, c: &ConversationView, after: i64) -> AppResult<Vec<MessageView>> {
        // Latest REPLAY_LIMIT messages after `after`, in order.
        let from = after.max(c.last_seq - REPLAY_LIMIT);
        self.repo.messages_after(c.tenant_id, c.id, from, REPLAY_LIMIT).await
    }

    pub async fn agent_send(&self, a: AgentCtx, conversation: Uuid, client_msg_id: &str, body: &str) -> AppResult<MessageView> {
        let body = validate_body(body).map_err(|e| AppError::validation("body", e.0))?;
        let key = client_key("agent", a.user_id, client_msg_id)?;
        // Suspension takes effect immediately, also for sockets opened before it (BR-M01-002).
        self.require_tenant_active(a.tenant_id).await?;
        let (conv, msg, duplicate) = self.repo.append_outbound(a.tenant_id, a.user_id, conversation, &body, &key).await?;
        if !duplicate {
            self.fan_out_message(&conv, &msg).await;
        }
        Ok(msg)
    }

    pub async fn set_presence(&self, a: AgentCtx, p: Presence) -> AppResult<()> {
        self.repo.set_presence(a.tenant_id, a.user_id, p, &self.node_id).await?;
        self.publish(a.tenant_id, Target::Agent { id: a.user_id }, json!({ "type": "presence", "status": p })).await;
        if p == Presence::Available {
            self.drain(a).await?;
        }
        Ok(())
    }

    pub async fn heartbeat(&self, a: AgentCtx) -> AppResult<()> {
        self.repo.heartbeat(a.tenant_id, a.user_id, &self.node_id).await
    }

    pub async fn close_conversation(&self, a: AgentCtx, conversation: Uuid) -> AppResult<ConversationView> {
        let c = self.repo.close_conversation(a.tenant_id, a.user_id, conversation).await?;
        self.publish(a.tenant_id, Target::Agent { id: a.user_id }, json!({ "type": "conversation.updated", "conversation": c })).await;
        self.drain(a).await?;
        Ok(c)
    }

    async fn drain(&self, a: AgentCtx) -> AppResult<()> {
        for asg in self.repo.drain_for_agent(a.tenant_id, a.user_id).await? {
            self.announce_assignment(asg).await?;
        }
        Ok(())
    }

    async fn announce_assignment(&self, a: Assignment) -> AppResult<()> {
        let Some(c) = self.repo.conversation(a.tenant_id, a.conversation_id).await? else {
            return Ok(());
        };
        let msgs = self.replay(&c, 0).await?;
        tracing::info!(tenant_id = %a.tenant_id, conversation_id = %c.id, agent_id = %a.agent_id, "conversation assigned");
        self.publish(
            a.tenant_id,
            Target::Agent { id: a.agent_id },
            json!({ "type": "conversation.assigned", "conversation": c, "messages": msgs }),
        )
        .await;
        Ok(())
    }

    // --------------------------------------------------------------------------------------------
    // Web-chat customers (minimal M08 slice)
    // --------------------------------------------------------------------------------------------

    /// Starts a customer session from a public widget key. Returns (token, session).
    pub async fn create_customer_session(&self, widget_key: &str, name: &str) -> AppResult<(String, CustomerSession)> {
        let ep = self
            .repo
            .resolve_endpoint(Channel::WebChat, widget_key.trim())
            .await?
            .ok_or_else(|| AppError::not_found("Unknown chat widget"))?;
        self.require_tenant_active(ep.tenant_id).await?;
        let name = name.trim();
        let name = if name.is_empty() { "Web visitor".to_string() } else { name.chars().take(80).collect() };
        let token = random_token(24);
        let visitor = format!("visitor-{}", Uuid::now_v7().simple());
        let expires = Utc::now() + Duration::hours(CUSTOMER_SESSION_HOURS);
        self.repo.create_customer_session(&ep, &visitor, &name, &sha256(token.as_bytes()), expires).await?;
        let s = self.customer_session(&token).await?;
        Ok((token, s))
    }

    pub async fn customer_session(&self, token: &str) -> AppResult<CustomerSession> {
        if token.is_empty() || token.len() > 200 {
            return Err(AppError::unauthenticated("Invalid chat session"));
        }
        let s = self
            .repo
            .customer_session(&sha256(token.as_bytes()))
            .await?
            .filter(|s| s.expires_at > Utc::now())
            .ok_or_else(|| AppError::unauthenticated("Chat session expired; start a new chat"))?;
        self.require_tenant_active(s.tenant_id).await?;
        Ok(s)
    }

    pub async fn customer_snapshot(&self, s: &CustomerSession, after_seq: i64) -> AppResult<Value> {
        let conv = self.repo.open_conversation_for(s.tenant_id, s.endpoint_id, &s.visitor_id).await?;
        let msgs = match &conv {
            Some(c) => self.replay(c, after_seq).await?,
            None => Vec::new(),
        };
        Ok(json!({
            "type": "welcome",
            "node": self.node_id,
            "name": s.display_name,
            "conversation_id": conv.as_ref().map(|c| c.id),
            "messages": msgs.iter().map(customer_view).collect::<Vec<_>>(),
        }))
    }

    pub async fn customer_send(&self, s: &CustomerSession, client_msg_id: &str, body: &str) -> AppResult<MessageView> {
        let body = validate_body(body).map_err(|e| AppError::validation("body", e.0))?;
        let key = client_key("webchat", s.id, client_msg_id)?;
        let m = CanonicalMessage {
            channel: Channel::WebChat,
            endpoint_address: s.endpoint_address.clone(),
            customer_address: s.visitor_id.clone(),
            customer_name: Some(s.display_name.clone()),
            kind: MessageKind::Text,
            body,
            provider_message_id: None,
            idempotency_key: key.clone(),
        };
        // A client retry of an already-stored message returns the stored message (same seq).
        Ok(self.ingest_message(&m).await?.0)
    }

    /// Web chat receipts (OCC-M10-R031: socket ACK + read markers).
    pub async fn customer_seen(&self, s: &CustomerSession, seq: i64) -> AppResult<()> {
        let Some(conv) = self.repo.open_conversation_for(s.tenant_id, s.endpoint_id, &s.visitor_id).await? else {
            return Ok(());
        };
        for ch in self.repo.mark_read_up_to(s.tenant_id, conv.id, seq).await? {
            self.fan_out_status(&ch).await;
        }
        Ok(())
    }

    // --------------------------------------------------------------------------------------------
    // Fan-out
    // --------------------------------------------------------------------------------------------

    async fn fan_out_message(&self, conv: &ConversationView, msg: &MessageView) {
        if let Some(agent) = conv.assigned_agent {
            self.publish(conv.tenant_id, Target::Agent { id: agent }, json!({ "type": "message.new", "message": msg })).await;
        }
        if conv.channel == Channel::WebChat && msg.direction != Direction::Event {
            let target = Target::Customer { endpoint: conv.endpoint_id, visitor: conv.customer_address.clone() };
            self.publish(
                conv.tenant_id,
                target,
                json!({ "type": "message.new", "conversation_id": conv.id, "message": customer_view(msg) }),
            )
            .await;
        }
    }

    async fn fan_out_status(&self, ch: &StatusChange) {
        if let Some(agent) = ch.assigned_agent {
            let mut v = serde_json::to_value(ch).unwrap_or(Value::Null);
            if let Value::Object(o) = &mut v {
                o.insert("type".into(), Value::String("message.status".into()));
            }
            self.publish(ch.tenant_id, Target::Agent { id: agent }, v).await;
        }
    }

    pub async fn publish(&self, tenant_id: Uuid, target: Target, payload: Value) {
        self.bus.publish(BusEnvelope { tenant_id, target, payload }).await;
    }

    // --------------------------------------------------------------------------------------------
    // Background ticks
    // --------------------------------------------------------------------------------------------

    /// Outbound delivery (OCC-M10-R032/R033): lease due jobs FIFO per conversation, deliver via
    /// the channel adapter, record the receipt or schedule a retry.
    pub async fn delivery_tick(&self) -> AppResult<usize> {
        let jobs = self.repo.claim_outbound(30, 50).await?;
        let n = jobs.len();
        for job in jobs {
            let adapter = self.adapter(job.channel)?;
            let change = match adapter.deliver(&job).await {
                Ok(provider_id) => {
                    if adapter.simulated() {
                        let payload = json!({ "to": job.customer_address, "from_endpoint": job.endpoint_address, "provider_message_id": provider_id, "text": job.body });
                        let summary = format!("Fake BSP sent message to {}", job.customer_address);
                        self.repo.sim_log(job.tenant_id, job.channel, "outbound", &summary, &payload).await?;
                    }
                    self.repo.outbound_sent(&job, &provider_id).await?
                }
                Err(e) => {
                    tracing::warn!(message_id = %job.message_id, attempt = job.attempts + 1, error = %e.0, "outbound delivery failed");
                    self.repo.outbound_failed(&job, &e.0).await?
                }
            };
            if let Some(ch) = change {
                self.fan_out_status(&ch).await;
            }
        }
        Ok(n)
    }

    /// Processes due SIMULATED provider callbacks (receipts) through the normal ingest path.
    pub async fn sim_callback_tick(&self) -> AppResult<usize> {
        let due = self.repo.claim_due_callbacks(100).await?;
        let n = due.len();
        for cb in due {
            if let Err(e) = self.ingest_raw(cb.channel, Some(&cb.signature), &cb.body).await {
                e.log();
            }
        }
        Ok(n)
    }

    /// Safety net for routing races: assign queued conversations an available agent could take.
    pub async fn routing_tick(&self) -> AppResult<usize> {
        let mut n = 0;
        for (tenant, conv) in self.repo.routable_queued(100).await? {
            if let Some(a) = self.repo.route(tenant, conv).await? {
                self.announce_assignment(a).await?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Agents whose heartbeat stopped go Offline; their conversations are re-queued and re-routed.
    pub async fn reaper_tick(&self) -> AppResult<usize> {
        let reaped = self.repo.reap_stale_agents(AGENT_STALE_SECS).await?;
        let mut n = 0;
        for r in reaped {
            tracing::warn!(tenant_id = %r.tenant_id, agent_id = %r.agent_id, requeued = r.requeued.len(), "agent heartbeat lost; set offline");
            for conv in r.requeued {
                n += 1;
                if let Some(a) = self.repo.route(r.tenant_id, conv).await? {
                    self.announce_assignment(a).await?;
                }
            }
        }
        Ok(n)
    }

    // --------------------------------------------------------------------------------------------
    // Admin
    // --------------------------------------------------------------------------------------------

    /// Creates the SIMULATED WhatsApp number, voice DID and web-chat widget for a tenant.
    pub async fn provision_simulated_channels(
        &self,
        tenant: Uuid,
        whatsapp_skill: &str,
        voice_skill: &str,
        webchat_skill: &str,
    ) -> AppResult<Vec<Endpoint>> {
        let wa = self
            .create_unique_endpoint(tenant, Channel::WhatsApp, whatsapp_skill, |n| {
                (format!("10960{n:010}"), format!("+60 3-{} {} (simulated WhatsApp)", n / 10_000 % 10_000, n % 10_000))
            })
            .await?;
        let voice = self
            .create_unique_endpoint(tenant, Channel::Voice, voice_skill, |n| {
                let d = n % 100_000_000;
                (format!("+603{d:08}"), format!("+60 3-{} {} (simulated DID)", d / 10_000, d % 10_000))
            })
            .await?;
        let chat = self
            .create_unique_endpoint(tenant, Channel::WebChat, webchat_skill, |_| (random_token(24), "Website chat (demo widget)".into()))
            .await?;
        Ok(vec![wa, voice, chat])
    }

    /// Random simulated address; retried on the (unlikely) collision with an existing endpoint.
    async fn create_unique_endpoint(
        &self,
        tenant: Uuid,
        channel: Channel,
        skill: &str,
        make: impl Fn(u64) -> (String, String),
    ) -> AppResult<Endpoint> {
        let mut last = None;
        for _ in 0..5 {
            let (address, label) = make(rand::random::<u64>() % 10_000_000_000);
            match self.repo.create_endpoint(tenant, channel, &address, &label, skill).await {
                Ok(ep) => return Ok(ep),
                Err(e) if e.code == crate::platform::errors::ErrorCode::Conflict => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| AppError::conflict("Could not allocate a free simulated address")))
    }
}

/// What a customer may see of a message (no agent ids).
fn customer_view(m: &MessageView) -> Value {
    json!({
        "id": m.id,
        "seq": m.seq,
        "from": if m.direction == Direction::Inbound { "me" } else { "agent" },
        "body": m.body,
        "created_at": m.created_at,
    })
}

fn client_key(prefix: &str, owner: Uuid, client_msg_id: &str) -> AppResult<String> {
    let id = client_msg_id.trim();
    if id.is_empty() || id.len() > 64 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        return Err(AppError::validation("client_msg_id", "client_msg_id must be 1-64 of A-Z, a-z, 0-9, '-' or '_'"));
    }
    Ok(format!("{prefix}:{owner}:{id}"))
}
