//! PostgreSQL implementation of the hub repository. Every statement runs inside
//! `platform::db::scoped_tx`: tenant scope for tenant work (RLS enforces the tenant), system scope
//! only for endpoint resolution, receipts and the background workers.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};

use super::super::application::ports::*;
use super::super::domain::{
    pick_agent, retry_delay_secs, AgentCandidate, CanonicalMessage, CanonicalStatus, Channel, ConversationStatus, DeliveryStatus,
    Direction, MessageKind, Presence, SenderType,
};

pub struct PgHubRepository {
    pool: PgPool,
}

impl PgHubRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn parse<T: std::str::FromStr<Err = super::super::domain::DomainError>>(s: String) -> Result<T, sqlx::Error> {
    s.parse::<T>().map_err(|e| sqlx::Error::Decode(e.0.into()))
}

const ENDPOINT_COLS: &str = "id, tenant_id, channel, address, label, default_skill, simulated";

fn endpoint(r: &PgRow) -> Result<Endpoint, sqlx::Error> {
    Ok(Endpoint {
        id: r.try_get("id")?,
        tenant_id: r.try_get("tenant_id")?,
        channel: parse(r.try_get("channel")?)?,
        address: r.try_get("address")?,
        label: r.try_get("label")?,
        default_skill: r.try_get("default_skill")?,
        simulated: r.try_get("simulated")?,
    })
}

const CONV_COLS: &str = "id, tenant_id, endpoint_id, channel, customer_address, customer_name, status, required_skill, assigned_agent, last_seq, created_at, updated_at";

fn conversation(r: &PgRow) -> Result<ConversationView, sqlx::Error> {
    Ok(ConversationView {
        id: r.try_get("id")?,
        tenant_id: r.try_get("tenant_id")?,
        endpoint_id: r.try_get("endpoint_id")?,
        channel: parse(r.try_get("channel")?)?,
        customer_address: r.try_get("customer_address")?,
        customer_name: r.try_get("customer_name")?,
        status: parse(r.try_get("status")?)?,
        required_skill: r.try_get("required_skill")?,
        assigned_agent: r.try_get("assigned_agent")?,
        last_seq: r.try_get("last_seq")?,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
    })
}

const MSG_COLS: &str = "id, conversation_id, seq, direction, kind, sender_type, sender_id, body, delivery_status, created_at";

fn message(r: &PgRow) -> Result<MessageView, sqlx::Error> {
    let ds: Option<String> = r.try_get("delivery_status")?;
    Ok(MessageView {
        id: r.try_get("id")?,
        conversation_id: r.try_get("conversation_id")?,
        seq: r.try_get("seq")?,
        direction: parse(r.try_get("direction")?)?,
        kind: parse(r.try_get("kind")?)?,
        sender_type: parse(r.try_get("sender_type")?)?,
        sender_id: r.try_get("sender_id")?,
        body: r.try_get("body")?,
        delivery_status: ds.map(parse).transpose()?,
        created_at: r.try_get("created_at")?,
    })
}

async fn load_conversation(c: &mut PgConnection, id: Uuid, lock: bool) -> Result<Option<ConversationView>, sqlx::Error> {
    let sql = format!("SELECT {CONV_COLS} FROM hub.conversations WHERE id = $1{}", if lock { " FOR UPDATE" } else { "" });
    sqlx::query(&sql).bind(id).fetch_optional(&mut *c).await?.as_ref().map(conversation).transpose()
}

/// Allocates the next per-conversation sequence number (row is locked until commit, so the
/// sequence is gap-free and strictly ordered per conversation).
async fn next_seq(c: &mut PgConnection, conversation: Uuid) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("UPDATE hub.conversations SET last_seq = last_seq + 1, updated_at = now() WHERE id = $1 RETURNING last_seq")
        .bind(conversation)
        .fetch_one(&mut *c)
        .await
}

async fn status_event(
    c: &mut PgConnection,
    tenant: Uuid,
    message: Uuid,
    status: DeliveryStatus,
    detail: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO hub.message_status_events (id, tenant_id, message_id, status, detail) VALUES ($1, $2, $3, $4, $5)")
        .bind(Uuid::now_v7())
        .bind(tenant)
        .bind(message)
        .bind(status.as_str())
        .bind(detail)
        .execute(&mut *c)
        .await?;
    Ok(())
}

const CANDIDATE_COLS: &str = "a.user_id, a.skills, a.max_concurrent::bigint AS max_concurrent, p.status,
        (SELECT count(*) FROM hub.conversations c WHERE c.assigned_agent = a.user_id AND c.status = 'assigned') AS active";

fn candidate(r: &PgRow) -> Result<AgentCandidate, sqlx::Error> {
    Ok(AgentCandidate {
        user_id: r.try_get("user_id")?,
        skills: r.try_get("skills")?,
        presence: parse(r.try_get("status")?)?,
        active: r.try_get("active")?,
        max_concurrent: r.try_get("max_concurrent")?,
    })
}

/// Available agents with `skill` and their current load (snapshot, no locks).
async fn candidates(c: &mut PgConnection, tenant: Uuid, skill: &str) -> Result<Vec<AgentCandidate>, sqlx::Error> {
    let rows = sqlx::query(&format!(
        "SELECT {CANDIDATE_COLS} FROM hub.agents a JOIN hub.agent_presence p ON p.user_id = a.user_id
          WHERE a.tenant_id = $1 AND $2 = ANY(a.skills) AND p.status = 'available'"
    ))
    .bind(tenant)
    .bind(skill)
    .fetch_all(&mut *c)
    .await?;
    rows.iter().map(candidate).collect()
}

/// Locks ONE agent's presence row, then re-reads its presence and load in a fresh statement, so
/// the capacity check sees every assignment committed by whoever held the lock before us.
async fn lock_candidate(c: &mut PgConnection, agent: Uuid) -> Result<Option<AgentCandidate>, sqlx::Error> {
    let locked: Option<Uuid> = sqlx::query_scalar("SELECT user_id FROM hub.agent_presence WHERE user_id = $1 FOR UPDATE")
        .bind(agent)
        .fetch_optional(&mut *c)
        .await?;
    if locked.is_none() {
        return Ok(None);
    }
    let r = sqlx::query(&format!(
        "SELECT {CANDIDATE_COLS} FROM hub.agents a JOIN hub.agent_presence p ON p.user_id = a.user_id WHERE a.user_id = $1"
    ))
    .bind(agent)
    .fetch_optional(&mut *c)
    .await?;
    r.as_ref().map(candidate).transpose()
}

async fn assign(c: &mut PgConnection, conversation: Uuid, agent: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE hub.conversations SET status = 'assigned', assigned_agent = $2, assigned_at = now(), updated_at = now() WHERE id = $1",
    )
    .bind(conversation)
    .bind(agent)
    .execute(&mut *c)
    .await?;
    Ok(())
}

async fn assigned_agent_of(c: &mut PgConnection, message: Uuid) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT c.assigned_agent FROM hub.messages m JOIN hub.conversations c ON c.id = m.conversation_id WHERE m.id = $1")
        .bind(message)
        .fetch_optional(&mut *c)
        .await
        .map(Option::flatten)
}

#[async_trait]
impl HubRepository for PgHubRepository {
    async fn resolve_endpoint(&self, channel: Channel, address: &str) -> AppResult<Option<Endpoint>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let r = sqlx::query(&format!("SELECT {ENDPOINT_COLS} FROM hub.channel_endpoints WHERE channel = $1 AND address = $2"))
            .bind(channel.as_str())
            .bind(address)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(r.as_ref().map(endpoint).transpose()?)
    }

    async fn endpoints(&self, tenant: Uuid) -> AppResult<Vec<Endpoint>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows = sqlx::query(&format!("SELECT {ENDPOINT_COLS} FROM hub.channel_endpoints ORDER BY channel, created_at"))
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(endpoint).collect::<Result<_, _>>()?)
    }

    async fn create_endpoint(
        &self,
        tenant: Uuid,
        channel: Channel,
        address: &str,
        label: &str,
        default_skill: &str,
        simulated: bool,
    ) -> AppResult<Endpoint> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let r = sqlx::query(&format!(
            "INSERT INTO hub.channel_endpoints (id, tenant_id, channel, address, label, default_skill, simulated)
             VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {ENDPOINT_COLS}"
        ))
        .bind(Uuid::now_v7())
        .bind(tenant)
        .bind(channel.as_str())
        .bind(address)
        .bind(label)
        .bind(default_skill)
        .bind(simulated)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(d) if d.is_unique_violation() => AppError::conflict("That channel address is already in use"),
            e => e.into(),
        })?;
        tx.commit().await?;
        Ok(endpoint(&r)?)
    }

    async fn upsert_agent(&self, tenant: Uuid, user_id: Uuid, skills: &[String], max_concurrent: i64) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        sqlx::query(
            "INSERT INTO hub.agents (user_id, tenant_id, skills, max_concurrent) VALUES ($1, $2, $3, $4)
             ON CONFLICT (user_id) DO UPDATE SET skills = EXCLUDED.skills, max_concurrent = EXCLUDED.max_concurrent",
        )
        .bind(user_id)
        .bind(tenant)
        .bind(skills)
        .bind(max_concurrent as i32)
        .execute(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO hub.agent_presence (user_id, tenant_id) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING")
            .bind(user_id)
            .bind(tenant)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn agents(&self, tenant: Uuid) -> AppResult<Vec<AgentView>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows = sqlx::query(AGENT_SQL).bind(None::<Uuid>).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(rows.iter().map(agent_view).collect::<Result<_, _>>()?)
    }

    async fn agent(&self, tenant: Uuid, user_id: Uuid) -> AppResult<Option<AgentView>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let r = sqlx::query(AGENT_SQL).bind(Some(user_id)).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok(r.as_ref().map(agent_view).transpose()?)
    }

    async fn set_presence(&self, tenant: Uuid, agent: Uuid, presence: Presence, node: &str) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let n = sqlx::query(
            "UPDATE hub.agent_presence SET status = $2, node_id = $3, heartbeat_at = now(), updated_at = now() WHERE user_id = $1",
        )
        .bind(agent)
        .bind(presence.as_str())
        .bind(node)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        if n == 0 {
            return Err(AppError::forbidden("This user is not configured as an agent"));
        }
        Ok(())
    }

    async fn heartbeat(&self, tenant: Uuid, agent: Uuid, node: &str) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        sqlx::query("UPDATE hub.agent_presence SET heartbeat_at = now(), node_id = $2 WHERE user_id = $1")
            .bind(agent)
            .bind(node)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn reap_stale_agents(&self, stale_secs: i64) -> AppResult<Vec<ReapedAgent>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let stale = sqlx::query(
            "UPDATE hub.agent_presence SET status = 'offline', updated_at = now()
              WHERE status <> 'offline' AND (heartbeat_at IS NULL OR heartbeat_at < now() - make_interval(secs => $1))
              RETURNING user_id, tenant_id",
        )
        .bind(stale_secs as f64)
        .fetch_all(&mut *tx)
        .await?;
        let mut out = Vec::new();
        for r in &stale {
            let agent: Uuid = r.try_get("user_id")?;
            let tenant: Uuid = r.try_get("tenant_id")?;
            let requeued: Vec<Uuid> = sqlx::query_scalar(
                "UPDATE hub.conversations SET status = 'queued', assigned_agent = NULL, queued_at = now(), updated_at = now()
                  WHERE assigned_agent = $1 AND status = 'assigned' RETURNING id",
            )
            .bind(agent)
            .fetch_all(&mut *tx)
            .await?;
            out.push(ReapedAgent { tenant_id: tenant, agent_id: agent, requeued });
        }
        // Orphans: assigned conversations whose agent user was deleted, or whose agent is Offline
        // (signed off with work still assigned) — back to the queue so customers are not stranded.
        let orphans = sqlx::query(
            "UPDATE hub.conversations c SET status = 'queued', assigned_agent = NULL, queued_at = now(), updated_at = now()
              WHERE c.status = 'assigned'
                AND (c.assigned_agent IS NULL
                     OR EXISTS (SELECT 1 FROM hub.agent_presence p WHERE p.user_id = c.assigned_agent AND p.status = 'offline'))
              RETURNING c.id, c.tenant_id",
        )
        .fetch_all(&mut *tx)
        .await?;
        for r in &orphans {
            out.push(ReapedAgent { tenant_id: r.try_get("tenant_id")?, agent_id: Uuid::nil(), requeued: vec![r.try_get("id")?] });
        }
        tx.commit().await?;
        Ok(out)
    }

    async fn append_inbound(&self, ep: &Endpoint, m: &CanonicalMessage) -> AppResult<Appended> {
        // Hot path, one round trip per step: (1) find-or-open the customer's thread and take the
        // next sequence number in a single upsert (row stays locked until commit → gap-free,
        // ordered), (2) insert the message. A duplicate provider delivery hits the idempotency
        // unique key; the transaction is rolled back (no sequence number is consumed) and the
        // stored message is returned instead.
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(ep.tenant_id)).await?;
        let r = sqlx::query(&format!(
            "INSERT INTO hub.conversations (id, tenant_id, endpoint_id, channel, customer_address, customer_name, status, required_skill, queued_at, last_seq)
             VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, now(), 1)
             ON CONFLICT (endpoint_id, customer_address) WHERE status <> 'closed'
             DO UPDATE SET last_seq = hub.conversations.last_seq + 1, updated_at = now(),
                           customer_name = coalesce(hub.conversations.customer_name, EXCLUDED.customer_name)
             RETURNING {CONV_COLS}, (xmax = 0) AS inserted"
        ))
        .bind(Uuid::now_v7())
        .bind(ep.tenant_id)
        .bind(ep.id)
        .bind(ep.channel.as_str())
        .bind(&m.customer_address)
        .bind(&m.customer_name)
        .bind(&ep.default_skill)
        .fetch_one(&mut *tx)
        .await?;
        let conv = conversation(&r)?;
        let created: bool = r.try_get("inserted")?;
        let (direction, sender) = match m.kind {
            MessageKind::Text => (Direction::Inbound, SenderType::Customer),
            MessageKind::CallEvent | MessageKind::System => (Direction::Event, SenderType::System),
        };
        let inserted = sqlx::query(&format!(
            "INSERT INTO hub.messages (id, tenant_id, conversation_id, seq, direction, kind, sender_type, body, provider_message_id, idempotency_key)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING RETURNING {MSG_COLS}"
        ))
        .bind(Uuid::now_v7())
        .bind(ep.tenant_id)
        .bind(conv.id)
        .bind(conv.last_seq)
        .bind(direction.as_str())
        .bind(m.kind.as_str())
        .bind(sender.as_str())
        .bind(&m.body)
        .bind(&m.provider_message_id)
        .bind(&m.idempotency_key)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(r) = inserted {
            let msg = message(&r)?;
            tx.commit().await?;
            return Ok(Appended { conversation: conv, message: msg, duplicate: false, created_conversation: created });
        }
        tx.rollback().await?;
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(ep.tenant_id)).await?;
        let r = sqlx::query(&format!("SELECT {MSG_COLS} FROM hub.messages WHERE tenant_id = $1 AND idempotency_key = $2"))
            .bind(ep.tenant_id)
            .bind(&m.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| AppError::conflict("Provider message id already used by another message"))?;
        let msg = message(&r)?;
        let conv =
            load_conversation(&mut tx, msg.conversation_id, false).await?.ok_or_else(|| AppError::not_found("Conversation missing"))?;
        tx.commit().await?;
        Ok(Appended { conversation: conv, message: msg, duplicate: true, created_conversation: false })
    }

    async fn append_outbound(
        &self,
        tenant: Uuid,
        agent: Uuid,
        conversation_id: Uuid,
        body: &str,
        key: &str,
    ) -> AppResult<(ConversationView, MessageView, bool)> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        // RLS: another tenant's conversation is simply not found.
        let conv = load_conversation(&mut tx, conversation_id, true).await?.ok_or_else(|| AppError::not_found("Conversation not found"))?;
        if conv.status != ConversationStatus::Assigned || conv.assigned_agent != Some(agent) {
            return Err(AppError::forbidden("This conversation is not assigned to you"));
        }
        if let Some(r) = sqlx::query(&format!("SELECT {MSG_COLS} FROM hub.messages WHERE tenant_id = $1 AND idempotency_key = $2"))
            .bind(tenant)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?
        {
            let m = message(&r)?;
            tx.commit().await?;
            return Ok((conv, m, true));
        }
        let initial = match conv.channel {
            Channel::WhatsApp => DeliveryStatus::Queued,
            // Web chat is pushed over the customer's socket directly (receipts via read markers).
            Channel::WebChat => DeliveryStatus::Sent,
            Channel::Voice => return Err(AppError::validation("body", "Voice conversations have no text channel to reply on")),
        };
        let seq = next_seq(&mut tx, conversation_id).await?;
        let id = Uuid::now_v7();
        let r = sqlx::query(&format!(
            "INSERT INTO hub.messages (id, tenant_id, conversation_id, seq, direction, kind, sender_type, sender_id, body, idempotency_key, delivery_status)
             VALUES ($1, $2, $3, $4, 'outbound', 'text', 'agent', $5, $6, $7, $8) RETURNING {MSG_COLS}"
        ))
        .bind(id)
        .bind(tenant)
        .bind(conversation_id)
        .bind(seq)
        .bind(agent)
        .bind(body)
        .bind(key)
        .bind(initial.as_str())
        .fetch_one(&mut *tx)
        .await?;
        status_event(&mut tx, tenant, id, initial, None).await?;
        if initial == DeliveryStatus::Queued {
            sqlx::query("INSERT INTO hub.outbound_queue (message_id, tenant_id, conversation_id, seq) VALUES ($1, $2, $3, $4)")
                .bind(id)
                .bind(tenant)
                .bind(conversation_id)
                .bind(seq)
                .execute(&mut *tx)
                .await?;
        }
        let conv =
            load_conversation(&mut tx, conversation_id, false).await?.ok_or_else(|| AppError::not_found("Conversation not found"))?;
        tx.commit().await?;
        Ok((conv, message(&r)?, false))
    }

    async fn close_conversation(&self, tenant: Uuid, agent: Uuid, conversation_id: Uuid) -> AppResult<ConversationView> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let conv = load_conversation(&mut tx, conversation_id, true).await?.ok_or_else(|| AppError::not_found("Conversation not found"))?;
        if conv.status != ConversationStatus::Assigned || conv.assigned_agent != Some(agent) {
            return Err(AppError::forbidden("This conversation is not assigned to you"));
        }
        let seq = next_seq(&mut tx, conversation_id).await?;
        sqlx::query(
            "INSERT INTO hub.messages (id, tenant_id, conversation_id, seq, direction, kind, sender_type, sender_id, body, idempotency_key)
             VALUES ($1, $2, $3, $4, 'event', 'system', 'agent', $5, 'Conversation closed', $6)",
        )
        .bind(Uuid::now_v7())
        .bind(tenant)
        .bind(conversation_id)
        .bind(seq)
        .bind(agent)
        .bind(format!("close:{conversation_id}"))
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE hub.conversations SET status = 'closed', closed_at = now(), updated_at = now() WHERE id = $1")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        let conv =
            load_conversation(&mut tx, conversation_id, false).await?.ok_or_else(|| AppError::not_found("Conversation not found"))?;
        tx.commit().await?;
        Ok(conv)
    }

    async fn conversation(&self, tenant: Uuid, id: Uuid) -> AppResult<Option<ConversationView>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let c = load_conversation(&mut tx, id, false).await?;
        tx.commit().await?;
        Ok(c)
    }

    async fn agent_conversations(&self, tenant: Uuid, agent: Uuid) -> AppResult<Vec<ConversationView>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows = sqlx::query(&format!(
            "SELECT {CONV_COLS} FROM hub.conversations WHERE assigned_agent = $1 AND status = 'assigned' ORDER BY assigned_at"
        ))
        .bind(agent)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows.iter().map(conversation).collect::<Result<_, _>>()?)
    }

    async fn recent_conversations(&self, tenant: Uuid, limit: i64) -> AppResult<Vec<ConversationView>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows = sqlx::query(&format!("SELECT {CONV_COLS} FROM hub.conversations ORDER BY updated_at DESC LIMIT $1"))
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(conversation).collect::<Result<_, _>>()?)
    }

    async fn open_conversation_for(&self, tenant: Uuid, endpoint_id: Uuid, customer: &str) -> AppResult<Option<ConversationView>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let r = sqlx::query(&format!(
            "SELECT {CONV_COLS} FROM hub.conversations WHERE endpoint_id = $1 AND customer_address = $2 AND status <> 'closed'"
        ))
        .bind(endpoint_id)
        .bind(customer)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(r.as_ref().map(conversation).transpose()?)
    }

    async fn messages_after(&self, tenant: Uuid, conversation_id: Uuid, after_seq: i64, limit: i64) -> AppResult<Vec<MessageView>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows =
            sqlx::query(&format!("SELECT {MSG_COLS} FROM hub.messages WHERE conversation_id = $1 AND seq > $2 ORDER BY seq LIMIT $3"))
                .bind(conversation_id)
                .bind(after_seq)
                .bind(limit)
                .fetch_all(&mut *tx)
                .await?;
        tx.commit().await?;
        Ok(rows.iter().map(message).collect::<Result<_, _>>()?)
    }

    async fn queue_depths(&self, tenant: Uuid) -> AppResult<Vec<QueueDepth>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows = sqlx::query(
            "SELECT required_skill, count(*) AS queued, coalesce(extract(epoch FROM now() - min(queued_at))::bigint, 0) AS oldest
               FROM hub.conversations WHERE status = 'queued' GROUP BY required_skill ORDER BY required_skill",
        )
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows
            .iter()
            .map(|r| {
                Ok(QueueDepth { skill: r.try_get("required_skill")?, queued: r.try_get("queued")?, oldest_wait_secs: r.try_get("oldest")? })
            })
            .collect::<Result<_, sqlx::Error>>()?)
    }

    async fn route(&self, tenant: Uuid, conversation_id: Uuid) -> AppResult<Option<Assignment>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let Some(conv) = load_conversation(&mut tx, conversation_id, true).await? else {
            return Ok(None);
        };
        if conv.status != ConversationStatus::Queued {
            return Ok(None);
        }
        // Optimistic: choose from an unlocked snapshot, then lock only the chosen agent and
        // re-check. Concurrent routings to different agents run in parallel; two routings that
        // chose the same agent serialise on its row and the second re-counts its load.
        let skill = conv.required_skill.clone();
        let mut cands = candidates(&mut tx, tenant, &skill).await?;
        while let Some(agent) = pick_agent(&skill, &cands) {
            match lock_candidate(&mut tx, agent).await? {
                Some(fresh) if fresh.can_take(&skill) => {
                    assign(&mut tx, conversation_id, agent).await?;
                    tx.commit().await?;
                    return Ok(Some(Assignment { tenant_id: tenant, conversation_id, agent_id: agent }));
                }
                _ => cands.retain(|c| c.user_id != agent),
            }
        }
        tx.commit().await?;
        Ok(None)
    }

    async fn drain_for_agent(&self, tenant: Uuid, agent: Uuid) -> AppResult<Vec<Assignment>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let Some(r) = sqlx::query(
            "SELECT p.status, a.skills, a.max_concurrent::bigint AS max_concurrent
               FROM hub.agent_presence p JOIN hub.agents a ON a.user_id = p.user_id WHERE p.user_id = $1 FOR UPDATE OF p",
        )
        .bind(agent)
        .fetch_optional(&mut *tx)
        .await?
        else {
            return Ok(Vec::new());
        };
        let presence: Presence = parse(r.try_get("status")?)?;
        let skills: Vec<String> = r.try_get("skills")?;
        let max: i64 = r.try_get("max_concurrent")?;
        let mut out = Vec::new();
        if presence != Presence::Available {
            tx.commit().await?;
            return Ok(out);
        }
        let mut active: i64 =
            sqlx::query_scalar("SELECT count(*) FROM hub.conversations WHERE assigned_agent = $1 AND status = 'assigned'")
                .bind(agent)
                .fetch_one(&mut *tx)
                .await?;
        while active < max {
            let next: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM hub.conversations WHERE status = 'queued' AND required_skill = ANY($1)
                  ORDER BY queued_at, id LIMIT 1 FOR UPDATE SKIP LOCKED",
            )
            .bind(&skills)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(conv) = next else { break };
            assign(&mut tx, conv, agent).await?;
            out.push(Assignment { tenant_id: tenant, conversation_id: conv, agent_id: agent });
            active += 1;
        }
        tx.commit().await?;
        Ok(out)
    }

    async fn routable_queued(&self, limit: i64) -> AppResult<Vec<(Uuid, Uuid)>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let rows = sqlx::query(
            "SELECT c.tenant_id, c.id FROM hub.conversations c
              WHERE c.status = 'queued' AND EXISTS (
                    SELECT 1 FROM hub.agents a JOIN hub.agent_presence p ON p.user_id = a.user_id
                     WHERE a.tenant_id = c.tenant_id AND c.required_skill = ANY(a.skills) AND p.status = 'available'
                       AND (SELECT count(*) FROM hub.conversations x WHERE x.assigned_agent = a.user_id AND x.status = 'assigned') < a.max_concurrent)
              ORDER BY c.queued_at LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows.iter().map(|r| Ok((r.try_get("tenant_id")?, r.try_get("id")?))).collect::<Result<_, sqlx::Error>>()?)
    }

    async fn claim_outbound(&self, lease_secs: i64, limit: i64, tenant: Option<Uuid>) -> AppResult<Vec<OutboundJob>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        // FIFO per conversation: a row is only due when no earlier row of the same conversation
        // is still queued (leased or not), so message n+1 is never sent before message n.
        let rows = sqlx::query(
            "WITH due AS (
                SELECT q.message_id FROM hub.outbound_queue q
                 WHERE q.next_attempt_at <= now() AND ($3::uuid IS NULL OR q.tenant_id = $3)
                   AND NOT EXISTS (SELECT 1 FROM hub.outbound_queue e WHERE e.conversation_id = q.conversation_id AND e.seq < q.seq)
                 ORDER BY q.created_at LIMIT $2 FOR UPDATE SKIP LOCKED)
             UPDATE hub.outbound_queue q SET next_attempt_at = now() + make_interval(secs => $1)
               FROM due WHERE q.message_id = due.message_id
             RETURNING q.message_id",
        )
        .bind(lease_secs as f64)
        .bind(limit)
        .bind(tenant)
        .fetch_all(&mut *tx)
        .await?;
        let ids: Vec<Uuid> = rows.iter().map(|r| r.try_get("message_id")).collect::<Result<_, _>>()?;
        let jobs = if ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query(
                "SELECT q.message_id, q.tenant_id, q.conversation_id, q.seq, q.attempts, c.channel, e.address AS endpoint_address,
                        c.customer_address, m.body
                   FROM hub.outbound_queue q
                   JOIN hub.messages m ON m.id = q.message_id
                   JOIN hub.conversations c ON c.id = q.conversation_id
                   JOIN hub.channel_endpoints e ON e.id = c.endpoint_id
                  WHERE q.message_id = ANY($1) ORDER BY q.created_at",
            )
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await?
            .iter()
            .map(|r| {
                Ok(OutboundJob {
                    message_id: r.try_get("message_id")?,
                    tenant_id: r.try_get("tenant_id")?,
                    conversation_id: r.try_get("conversation_id")?,
                    seq: r.try_get("seq")?,
                    attempts: r.try_get("attempts")?,
                    channel: parse(r.try_get("channel")?)?,
                    endpoint_address: r.try_get("endpoint_address")?,
                    customer_address: r.try_get("customer_address")?,
                    body: r.try_get("body")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?
        };
        tx.commit().await?;
        Ok(jobs)
    }

    async fn outbound_sent(&self, job: &OutboundJob, provider_message_id: &str) -> AppResult<Option<StatusChange>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(job.tenant_id)).await?;
        sqlx::query("DELETE FROM hub.outbound_queue WHERE message_id = $1").bind(job.message_id).execute(&mut *tx).await?;
        let n = sqlx::query(
            "UPDATE hub.messages SET provider_message_id = $2, delivery_status = 'sent' WHERE id = $1 AND delivery_status = 'queued'",
        )
        .bind(job.message_id)
        .bind(provider_message_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let change = if n > 0 {
            status_event(&mut tx, job.tenant_id, job.message_id, DeliveryStatus::Sent, None).await?;
            Some(StatusChange {
                tenant_id: job.tenant_id,
                conversation_id: job.conversation_id,
                message_id: job.message_id,
                seq: job.seq,
                status: DeliveryStatus::Sent,
                assigned_agent: assigned_agent_of(&mut tx, job.message_id).await?,
            })
        } else {
            None
        };
        tx.commit().await?;
        Ok(change)
    }

    async fn outbound_failed(&self, job: &OutboundJob, error: &str, kind: FailureKind) -> AppResult<Option<StatusChange>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(job.tenant_id)).await?;
        let err: String = error.chars().take(500).collect();
        if kind == FailureKind::Throttled {
            sqlx::query(
                "UPDATE hub.outbound_queue SET last_error = $2, next_attempt_at = now() + interval '1 second' WHERE message_id = $1",
            )
            .bind(job.message_id)
            .bind(&err)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(None);
        }
        let attempts = job.attempts + 1;
        let delay = if kind == FailureKind::Permanent { None } else { retry_delay_secs(attempts) };
        let change = match delay {
            Some(delay) => {
                sqlx::query(
                    "UPDATE hub.outbound_queue SET attempts = $2, last_error = $3, next_attempt_at = now() + make_interval(secs => $4)
                      WHERE message_id = $1",
                )
                .bind(job.message_id)
                .bind(attempts)
                .bind(&err)
                .bind(delay as f64)
                .execute(&mut *tx)
                .await?;
                None
            }
            None => {
                sqlx::query("DELETE FROM hub.outbound_queue WHERE message_id = $1").bind(job.message_id).execute(&mut *tx).await?;
                sqlx::query("UPDATE hub.messages SET delivery_status = 'failed' WHERE id = $1")
                    .bind(job.message_id)
                    .execute(&mut *tx)
                    .await?;
                status_event(&mut tx, job.tenant_id, job.message_id, DeliveryStatus::Failed, Some(&err)).await?;
                Some(StatusChange {
                    tenant_id: job.tenant_id,
                    conversation_id: job.conversation_id,
                    message_id: job.message_id,
                    seq: job.seq,
                    status: DeliveryStatus::Failed,
                    assigned_agent: assigned_agent_of(&mut tx, job.message_id).await?,
                })
            }
        };
        tx.commit().await?;
        Ok(change)
    }

    async fn apply_status(&self, s: &CanonicalStatus) -> AppResult<Option<StatusChange>> {
        // Receipts carry no tenant: the provider message id (unique) identifies the message.
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let Some(r) = sqlx::query(
            "SELECT id, tenant_id, conversation_id, seq, delivery_status FROM hub.messages WHERE provider_message_id = $1 FOR UPDATE",
        )
        .bind(&s.provider_message_id)
        .fetch_optional(&mut *tx)
        .await?
        else {
            tx.commit().await?;
            tracing::info!(provider_message_id = %s.provider_message_id, "receipt for unknown message ignored");
            return Ok(None);
        };
        let id: Uuid = r.try_get("id")?;
        let tenant: Uuid = r.try_get("tenant_id")?;
        let current: Option<String> = r.try_get("delivery_status")?;
        let current: DeliveryStatus = current.map(parse).transpose()?.unwrap_or(DeliveryStatus::Queued);
        if !current.can_advance_to(s.status) {
            tx.commit().await?;
            return Ok(None);
        }
        sqlx::query("UPDATE hub.messages SET delivery_status = $2 WHERE id = $1")
            .bind(id)
            .bind(s.status.as_str())
            .execute(&mut *tx)
            .await?;
        status_event(&mut tx, tenant, id, s.status, s.detail.as_deref()).await?;
        let change = StatusChange {
            tenant_id: tenant,
            conversation_id: r.try_get("conversation_id")?,
            message_id: id,
            seq: r.try_get("seq")?,
            status: s.status,
            assigned_agent: assigned_agent_of(&mut tx, id).await?,
        };
        tx.commit().await?;
        Ok(Some(change))
    }

    async fn mark_read_up_to(&self, tenant: Uuid, conversation_id: Uuid, seq: i64) -> AppResult<Vec<StatusChange>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows = sqlx::query(
            "UPDATE hub.messages SET delivery_status = 'read'
              WHERE conversation_id = $1 AND direction = 'outbound' AND seq <= $2 AND delivery_status IN ('sent', 'delivered')
              RETURNING id, seq",
        )
        .bind(conversation_id)
        .bind(seq)
        .fetch_all(&mut *tx)
        .await?;
        let agent: Option<Uuid> = sqlx::query_scalar("SELECT assigned_agent FROM hub.conversations WHERE id = $1")
            .bind(conversation_id)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
        let mut out = Vec::new();
        for r in &rows {
            let id: Uuid = r.try_get("id")?;
            status_event(&mut tx, tenant, id, DeliveryStatus::Read, Some("web chat read marker")).await?;
            out.push(StatusChange {
                tenant_id: tenant,
                conversation_id,
                message_id: id,
                seq: r.try_get("seq")?,
                status: DeliveryStatus::Read,
                assigned_agent: agent,
            });
        }
        tx.commit().await?;
        Ok(out)
    }

    async fn create_customer_session(
        &self,
        ep: &Endpoint,
        visitor_id: &str,
        name: &str,
        token_hash: &[u8],
        expires_at: DateTime<Utc>,
    ) -> AppResult<Uuid> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(ep.tenant_id)).await?;
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO hub.customer_sessions (id, tenant_id, endpoint_id, visitor_id, display_name, token_hash, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(id)
        .bind(ep.tenant_id)
        .bind(ep.id)
        .bind(visitor_id)
        .bind(name)
        .bind(token_hash)
        .bind(expires_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn customer_session(&self, token_hash: &[u8]) -> AppResult<Option<CustomerSession>> {
        // Token lookup has no tenant yet (like login): system scope, exact hash match only.
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let r = sqlx::query(
            "SELECT s.id, s.tenant_id, s.endpoint_id, e.address, s.visitor_id, s.display_name, s.expires_at
               FROM hub.customer_sessions s JOIN hub.channel_endpoints e ON e.id = s.endpoint_id WHERE s.token_hash = $1",
        )
        .bind(token_hash)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(r.map(|r| {
            Ok::<_, sqlx::Error>(CustomerSession {
                id: r.try_get("id")?,
                tenant_id: r.try_get("tenant_id")?,
                endpoint_id: r.try_get("endpoint_id")?,
                endpoint_address: r.try_get("address")?,
                visitor_id: r.try_get("visitor_id")?,
                display_name: r.try_get("display_name")?,
                expires_at: r.try_get("expires_at")?,
            })
        })
        .transpose()?)
    }

    async fn sim_log(&self, tenant: Uuid, channel: Channel, direction: &str, summary: &str, payload: &Value) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        sqlx::query(
            "INSERT INTO hub.sim_provider_log (id, tenant_id, channel, direction, summary, payload) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(Uuid::now_v7())
        .bind(tenant)
        .bind(channel.as_str())
        .bind(direction)
        .bind(summary)
        .bind(payload)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn sim_logs(&self, tenant: Uuid, limit: i64) -> AppResult<Vec<SimLogEntry>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows = sqlx::query(
            "SELECT channel, direction, summary, payload, created_at FROM hub.sim_provider_log ORDER BY created_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows
            .iter()
            .map(|r| {
                Ok(SimLogEntry {
                    channel: r.try_get("channel")?,
                    direction: r.try_get("direction")?,
                    summary: r.try_get("summary")?,
                    payload: r.try_get("payload")?,
                    created_at: r.try_get("created_at")?,
                })
            })
            .collect::<Result<_, sqlx::Error>>()?)
    }
}

const AGENT_SQL: &str = "SELECT a.user_id, u.email::text AS email, u.display_name, a.skills, a.max_concurrent::bigint AS max_concurrent,
        p.status, p.heartbeat_at, p.node_id,
        (SELECT count(*) FROM hub.conversations c WHERE c.assigned_agent = a.user_id AND c.status = 'assigned') AS active
   FROM hub.agents a JOIN identity.users u ON u.id = a.user_id JOIN hub.agent_presence p ON p.user_id = a.user_id
  WHERE ($1::uuid IS NULL OR a.user_id = $1)
  ORDER BY u.display_name";

fn agent_view(r: &PgRow) -> Result<AgentView, sqlx::Error> {
    Ok(AgentView {
        user_id: r.try_get("user_id")?,
        email: r.try_get("email")?,
        display_name: r.try_get("display_name")?,
        skills: r.try_get("skills")?,
        max_concurrent: r.try_get("max_concurrent")?,
        presence: parse(r.try_get("status")?)?,
        active: r.try_get("active")?,
        heartbeat_at: r.try_get("heartbeat_at")?,
        node_id: r.try_get("node_id")?,
    })
}
