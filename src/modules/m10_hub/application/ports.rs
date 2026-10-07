//! Ports of the M10 hub: persistence, the cross-node real-time bus and channel adapters
//! (OCC-M10-R022 / OCC-M26-R029 adapter contract). Implementations live in `infrastructure/`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::platform::errors::AppResult;

use super::super::domain::{
    CanonicalMessage, CanonicalStatus, Channel, ConversationStatus, DeliveryStatus, Direction, MessageKind, Presence, SenderType,
};

// ------------------------------------------------------------------------------------------------
// Read models
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct Endpoint {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub channel: Channel,
    pub address: String,
    pub label: String,
    pub default_skill: String,
    pub simulated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConversationView {
    pub id: Uuid,
    #[serde(skip)]
    pub tenant_id: Uuid,
    #[serde(skip)]
    pub endpoint_id: Uuid,
    pub channel: Channel,
    pub customer_address: String,
    pub customer_name: Option<String>,
    pub status: ConversationStatus,
    pub required_skill: String,
    pub assigned_agent: Option<Uuid>,
    pub last_seq: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageView {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub seq: i64,
    pub direction: Direction,
    pub kind: MessageKind,
    pub sender_type: SenderType,
    pub sender_id: Option<Uuid>,
    pub body: String,
    pub delivery_status: Option<DeliveryStatus>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentView {
    pub user_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub skills: Vec<String>,
    pub max_concurrent: i64,
    pub presence: Presence,
    pub active: i64,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub node_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CustomerSession {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub endpoint_id: Uuid,
    pub endpoint_address: String,
    pub visitor_id: String,
    pub display_name: String,
    pub expires_at: DateTime<Utc>,
}

/// Result of appending an inbound canonical message.
#[derive(Debug, Clone)]
pub struct Appended {
    pub conversation: ConversationView,
    /// The stored message (the earlier one when this was a duplicate provider delivery).
    pub message: MessageView,
    pub duplicate: bool,
    pub created_conversation: bool,
}

/// A delivery-status change to fan out to the assigned agent.
#[derive(Debug, Clone, Serialize)]
pub struct StatusChange {
    #[serde(skip)]
    pub tenant_id: Uuid,
    pub conversation_id: Uuid,
    pub message_id: Uuid,
    pub seq: i64,
    pub status: DeliveryStatus,
    #[serde(skip)]
    pub assigned_agent: Option<Uuid>,
}

/// An outbound message leased from the queue by the delivery worker.
#[derive(Debug, Clone)]
pub struct OutboundJob {
    pub message_id: Uuid,
    pub tenant_id: Uuid,
    pub conversation_id: Uuid,
    pub seq: i64,
    pub attempts: i32,
    pub channel: Channel,
    pub endpoint_address: String,
    pub customer_address: String,
    pub body: String,
}

/// An assignment made by routing (conversation → agent).
#[derive(Debug, Clone, Copy)]
pub struct Assignment {
    pub tenant_id: Uuid,
    pub conversation_id: Uuid,
    pub agent_id: Uuid,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueDepth {
    pub skill: String,
    pub queued: i64,
    pub oldest_wait_secs: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SimLogEntry {
    pub channel: String,
    pub direction: String,
    pub summary: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

/// A provider webhook scheduled for later (the SIMULATED BSP's receipts; ADR-0012).
#[derive(Debug, Clone)]
pub struct ProviderCallback {
    pub channel: Channel,
    pub signature: String,
    pub body: Vec<u8>,
}

/// Re-queued work for an agent whose heartbeat stopped.
#[derive(Debug, Clone)]
pub struct ReapedAgent {
    pub tenant_id: Uuid,
    pub agent_id: Uuid,
    pub requeued: Vec<Uuid>,
}

// ------------------------------------------------------------------------------------------------
// Persistence port
// ------------------------------------------------------------------------------------------------

#[async_trait]
pub trait HubRepository: Send + Sync {
    // Endpoints (tenant resolution for inbound traffic)
    async fn resolve_endpoint(&self, channel: Channel, address: &str) -> AppResult<Option<Endpoint>>;
    async fn endpoints(&self, tenant: Uuid) -> AppResult<Vec<Endpoint>>;
    async fn create_endpoint(&self, tenant: Uuid, channel: Channel, address: &str, label: &str, default_skill: &str)
        -> AppResult<Endpoint>;

    // Agents & presence
    async fn upsert_agent(&self, tenant: Uuid, user_id: Uuid, skills: &[String], max_concurrent: i64) -> AppResult<()>;
    async fn agents(&self, tenant: Uuid) -> AppResult<Vec<AgentView>>;
    async fn agent(&self, tenant: Uuid, user_id: Uuid) -> AppResult<Option<AgentView>>;
    async fn set_presence(&self, tenant: Uuid, agent: Uuid, presence: Presence, node: &str) -> AppResult<()>;
    async fn heartbeat(&self, tenant: Uuid, agent: Uuid, node: &str) -> AppResult<()>;
    /// Agents not Offline whose heartbeat is older than `stale_secs` → Offline; their assigned
    /// conversations (and orphaned ones) go back to the queue.
    async fn reap_stale_agents(&self, stale_secs: i64) -> AppResult<Vec<ReapedAgent>>;

    // Conversations & messages
    async fn append_inbound(&self, ep: &Endpoint, msg: &CanonicalMessage) -> AppResult<Appended>;
    /// Agent reply. Fails unless the conversation is assigned to `agent`. `Ok((msg, true))` = duplicate.
    async fn append_outbound(
        &self,
        tenant: Uuid,
        agent: Uuid,
        conversation: Uuid,
        body: &str,
        idempotency_key: &str,
    ) -> AppResult<(ConversationView, MessageView, bool)>;
    async fn close_conversation(&self, tenant: Uuid, agent: Uuid, conversation: Uuid) -> AppResult<ConversationView>;
    async fn conversation(&self, tenant: Uuid, id: Uuid) -> AppResult<Option<ConversationView>>;
    async fn agent_conversations(&self, tenant: Uuid, agent: Uuid) -> AppResult<Vec<ConversationView>>;
    async fn recent_conversations(&self, tenant: Uuid, limit: i64) -> AppResult<Vec<ConversationView>>;
    async fn open_conversation_for(&self, tenant: Uuid, endpoint: Uuid, customer_address: &str) -> AppResult<Option<ConversationView>>;
    async fn messages_after(&self, tenant: Uuid, conversation: Uuid, after_seq: i64, limit: i64) -> AppResult<Vec<MessageView>>;
    async fn queue_depths(&self, tenant: Uuid) -> AppResult<Vec<QueueDepth>>;

    // Routing (all row-locked; correct across nodes)
    async fn route(&self, tenant: Uuid, conversation: Uuid) -> AppResult<Option<Assignment>>;
    async fn drain_for_agent(&self, tenant: Uuid, agent: Uuid) -> AppResult<Vec<Assignment>>;
    /// Safety net: queued conversations that an available agent could take now.
    async fn routable_queued(&self, limit: i64) -> AppResult<Vec<(Uuid, Uuid)>>;

    // Outbound delivery & receipts
    async fn claim_outbound(&self, lease_secs: i64, limit: i64) -> AppResult<Vec<OutboundJob>>;
    async fn outbound_sent(&self, job: &OutboundJob, provider_message_id: &str) -> AppResult<Option<StatusChange>>;
    async fn outbound_failed(&self, job: &OutboundJob, error: &str) -> AppResult<Option<StatusChange>>;
    async fn apply_status(&self, status: &CanonicalStatus) -> AppResult<Option<StatusChange>>;
    async fn mark_read_up_to(&self, tenant: Uuid, conversation: Uuid, seq: i64) -> AppResult<Vec<StatusChange>>;

    // Web-chat customer sessions
    async fn create_customer_session(
        &self,
        ep: &Endpoint,
        visitor_id: &str,
        name: &str,
        token_hash: &[u8],
        expires_at: DateTime<Utc>,
    ) -> AppResult<Uuid>;
    async fn customer_session(&self, token_hash: &[u8]) -> AppResult<Option<CustomerSession>>;

    /// Takes simulated provider callbacks that are due (each is handed to exactly one caller).
    async fn claim_due_callbacks(&self, limit: i64) -> AppResult<Vec<ProviderCallback>>;

    // Simulator console log (SIMULATED providers only)
    async fn sim_log(&self, tenant: Uuid, channel: Channel, direction: &str, summary: &str, payload: &Value) -> AppResult<()>;
    async fn sim_logs(&self, tenant: Uuid, limit: i64) -> AppResult<Vec<SimLogEntry>>;
}

// ------------------------------------------------------------------------------------------------
// Real-time bus (cross-node fan-out, FR-ARC-003)
// ------------------------------------------------------------------------------------------------

/// Who a real-time event is for. Sessions are found by these keys on every node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Target {
    Agent { id: Uuid },
    Customer { endpoint: Uuid, visitor: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusEnvelope {
    pub tenant_id: Uuid,
    pub target: Target,
    pub payload: Value,
}

/// Receives envelopes from the bus on this node (the session registry).
pub trait BusSink: Send + Sync {
    fn deliver(&self, env: BusEnvelope);
}

#[async_trait]
pub trait RealtimeBus: Send + Sync {
    fn name(&self) -> &'static str;
    async fn publish(&self, env: BusEnvelope);
    /// Starts delivering envelopes published by ANY node to `sink`.
    fn start(&self, sink: std::sync::Arc<dyn BusSink>);
    async fn healthy(&self) -> bool;
}

// ------------------------------------------------------------------------------------------------
// Channel adapter contract (OCC-M10-R022, OCC-M26-R029)
// ------------------------------------------------------------------------------------------------

/// One parsed provider event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inbound {
    Message(CanonicalMessage),
    Status(CanonicalStatus),
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelHealth {
    pub channel: Channel,
    pub simulated: bool,
    pub healthy: bool,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct DeliveryError(pub String);

#[async_trait]
pub trait ChannelAdapter: Send + Sync {
    fn channel(&self) -> Channel;
    /// `true` for the local simulators of ADR-0012 (never a production integration).
    fn simulated(&self) -> bool;
    /// connect: validate configuration/credentials at startup.
    async fn connect(&self) -> AppResult<()>;
    /// ingest: authenticate (signature) and parse a raw provider request.
    fn ingest(&self, signature: Option<&str>, body: &[u8]) -> AppResult<Vec<Inbound>>;
    /// deliver: send one outbound message; returns the provider message id.
    async fn deliver(&self, job: &OutboundJob) -> Result<String, DeliveryError>;
    async fn health(&self) -> ChannelHealth;
    /// backfill: fetch events missed while disconnected (simulators have none).
    async fn backfill(&self, since: DateTime<Utc>) -> AppResult<Vec<Inbound>>;
}
