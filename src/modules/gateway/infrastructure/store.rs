//! PostgreSQL store for the gateway (ADR-0014). Durability rule: a message is acknowledged only
//! after the transaction that appended it has committed. `seq` is allocated by bumping
//! `conversations.last_seq` under the row lock in that same transaction, so it is gap-free and
//! contiguous across nodes and restarts (a rolled-back append releases its number).
//!
//! Routing runs under a per-skill transaction advisory lock, so two nodes never assign the same
//! queued conversation twice and queue order (arrival order) is kept.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Postgres, Row, Transaction};

use super::super::application::{AgentPresence, Appended, GwError, GwResult, PresenceView, ReapOutcome};
use super::super::domain::{
    pick_longest_idle, rfc3339, ulid, Actor, ActorKind, CanonicalMessage, Channel, Conversation, ConversationStatus, Direction, Fixture,
    MessageKind, NewMessage, RoutingCandidate, REROUTE_GRACE_SECS,
};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/gateway");

/// A node that has not written its heartbeat for this long is dead.
pub const NODE_DEAD_SECS: i64 = 6;

/// SQL fragment: agent `a` has a connection on a live node (6 s = `NODE_DEAD_SECS`).
const LIVE_SESSION: &str = "EXISTS (SELECT 1 FROM gw.agent_sessions s JOIN gw.nodes n ON n.node_id = s.node_id
                                    WHERE s.agent_id = a.id AND n.heartbeat_at > now() - make_interval(secs => 6))";

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

type Tx = Transaction<'static, Postgres>;

fn db(e: sqlx::Error) -> GwError {
    GwError::Internal(anyhow::anyhow!(e))
}

fn unique_violation(e: &sqlx::Error) -> Option<String> {
    match e {
        sqlx::Error::Database(d) if d.code().as_deref() == Some("23505") => Some(d.constraint().unwrap_or("").to_string()),
        _ => None,
    }
}

fn message_from_row(r: &PgRow) -> GwResult<CanonicalMessage> {
    let get = |c: &str| r.try_get::<String, _>(c).map_err(db);
    let received: DateTime<Utc> = r.try_get("received_at").map_err(db)?;
    Ok(CanonicalMessage {
        message_id: get("message_id")?,
        conversation_id: get("conversation_id")?,
        seq: r.try_get("seq").map_err(db)?,
        channel: Channel::parse(&get("channel")?).ok_or_else(|| GwError::internal("bad channel"))?,
        direction: Direction::parse(&get("direction")?).ok_or_else(|| GwError::internal("bad direction"))?,
        actor: Actor { kind: ActorKind::parse(&get("actor_kind")?).ok_or_else(|| GwError::internal("bad actor"))?, id: get("actor_id")? },
        kind: MessageKind::parse(&get("kind")?).ok_or_else(|| GwError::internal("bad kind"))?,
        body: r.try_get("body").map_err(db)?,
        received_at: rfc3339(received),
        external_id: r.try_get("external_id").map_err(db)?,
    })
}

fn conversation_from_row(r: &PgRow) -> GwResult<Conversation> {
    let get = |c: &str| r.try_get::<String, _>(c).map_err(db);
    Ok(Conversation {
        id: get("id")?,
        channel: Channel::parse(&get("channel")?).ok_or_else(|| GwError::internal("bad channel"))?,
        customer: get("customer")?,
        skill: get("skill")?,
        status: ConversationStatus::parse(&get("status")?).ok_or_else(|| GwError::internal("bad status"))?,
        assigned_agent: r.try_get("assigned_agent").map_err(db)?,
        last_seq: r.try_get("last_seq").map_err(db)?,
    })
}

const MESSAGE_COLS: &str =
    "conversation_id, seq, message_id, channel, direction, actor_kind, actor_id, kind, body, received_at, external_id";
const CONV_COLS: &str = "id, channel, customer, skill, status, assigned_agent, last_seq";

/// Where a customer message goes.
pub struct CustomerTarget<'a> {
    pub channel: Channel,
    pub customer: &'a str,
    /// Conversation named by the client (`send{conversation_id}`); must belong to the customer.
    pub conversation_id: Option<&'a str>,
    /// Routing rules: the skill a newly opened conversation queues under.
    pub fixture: &'a Fixture,
}

impl Store {
    /// Opens the whole pool up front: opening connections (SCRAM authentication) in the middle of
    /// a burst stalled the first second of traffic (E2 measurement).
    pub async fn connect(url: &str, max: u32) -> anyhow::Result<Self> {
        let pool =
            PgPoolOptions::new().max_connections(max).min_connections(max).acquire_timeout(Duration::from_secs(5)).connect(url).await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn begin(&self) -> GwResult<Tx> {
        self.pool.begin().await.map_err(db)
    }

    // --------------------------------------------------------------------------------------------
    // Appending messages
    // --------------------------------------------------------------------------------------------

    /// Opens (or reuses) the customer's open conversation on `channel` and locks it.
    async fn open_conversation(tx: &mut Tx, channel: Channel, customer: &str, skill: &str) -> GwResult<Conversation> {
        for _ in 0..3 {
            sqlx::query(
                "INSERT INTO gw.conversations (id, channel, customer, skill, status) VALUES ($1, $2, $3, $4, 'queued')
                 ON CONFLICT (channel, customer) WHERE status <> 'closed' DO NOTHING",
            )
            .bind(ulid())
            .bind(channel.as_str())
            .bind(customer)
            .bind(skill)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
            let row = sqlx::query(&format!(
                "SELECT {CONV_COLS} FROM gw.conversations WHERE channel = $1 AND customer = $2 AND status <> 'closed' FOR UPDATE"
            ))
            .bind(channel.as_str())
            .bind(customer)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?;
            if let Some(r) = row {
                return conversation_from_row(&r);
            }
            // Closed between our insert and select (disposition on another node): try again.
        }
        Err(GwError::internal("could not open a conversation"))
    }

    /// Bumps the conversation's sequence and inserts the message (caller holds the row lock).
    async fn insert_message(tx: &mut Tx, conv: &Conversation, m: &NewMessage) -> GwResult<CanonicalMessage> {
        let seq: i64 = sqlx::query_scalar("UPDATE gw.conversations SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq")
            .bind(&conv.id)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        Self::insert_at(tx, conv, seq, m).await
    }

    /// Inserts the message at a `seq` the caller allocated in this transaction.
    async fn insert_at(tx: &mut Tx, conv: &Conversation, seq: i64, m: &NewMessage) -> GwResult<CanonicalMessage> {
        let row = sqlx::query(&format!(
            "INSERT INTO gw.messages (conversation_id, seq, message_id, channel, direction, actor_kind, actor_id, kind, body,
                                     received_at, external_id, dedup_key, client_ref)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp(), $10, $11, $12)
             RETURNING {MESSAGE_COLS}"
        ))
        .bind(&conv.id)
        .bind(seq)
        .bind(ulid())
        .bind(conv.channel.as_str())
        .bind(m.direction.as_str())
        .bind(m.actor.kind.as_str())
        .bind(&m.actor.id)
        .bind(m.kind.as_str())
        .bind(&m.body)
        .bind(&m.external_id)
        .bind(&m.dedup_key)
        .bind(&m.client_ref)
        .fetch_one(&mut **tx)
        .await;
        match row {
            Ok(r) => message_from_row(&r),
            Err(e) => Err(match unique_violation(&e).as_deref() {
                Some("messages_dedup") => GwError::Duplicate,
                Some("messages_client_ref") => GwError::DuplicateClientRef,
                _ => db(e),
            }),
        }
    }

    /// The message a repeated `client_ref` refers to (for re-sending the original `ack`).
    pub async fn by_client_ref(&self, actor: &Actor, client_ref: &str) -> GwResult<Option<CanonicalMessage>> {
        let row =
            sqlx::query(&format!("SELECT {MESSAGE_COLS} FROM gw.messages WHERE actor_kind = $1 AND actor_id = $2 AND client_ref = $3"))
                .bind(actor.kind.as_str())
                .bind(&actor.id)
                .bind(client_ref)
                .fetch_optional(&self.pool)
                .await
                .map_err(db)?;
        row.as_ref().map(message_from_row).transpose()
    }

    /// Customer → gateway (webhook ingress or customer WebSocket `send`).
    pub async fn append_customer(&self, target: CustomerTarget<'_>, m: &NewMessage) -> GwResult<Appended> {
        let mut tx = self.begin().await?;
        let conv = match target.conversation_id {
            Some(id) => {
                let row = sqlx::query(&format!("SELECT {CONV_COLS} FROM gw.conversations WHERE id = $1 FOR UPDATE"))
                    .bind(id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
                let c = match row {
                    Some(r) => conversation_from_row(&r)?,
                    None => return Err(GwError::NotFound("conversation")),
                };
                if c.customer != target.customer {
                    return Err(GwError::NotFound("conversation"));
                }
                if c.status == ConversationStatus::Closed {
                    // A message on a closed conversation opens a new one (seq starts at 1 again).
                    Self::open_conversation(&mut tx, c.channel, target.customer, &target.fixture.skill_for(c.channel)).await?
                } else {
                    c
                }
            }
            None => Self::open_conversation(&mut tx, target.channel, target.customer, &target.fixture.skill_for(target.channel)).await?,
        };
        let message = Self::insert_message(&mut tx, &conv, m).await?;
        tx.commit().await.map_err(db)?;
        Ok(Appended {
            customer: conv.customer.clone(),
            agent: conv.assigned_agent.clone(),
            routing_skill: (conv.status == ConversationStatus::Queued).then(|| conv.skill.clone()),
            message,
        })
    }

    /// Agent → conversation (`send` or `disposition`); only on a conversation assigned to them.
    pub async fn append_agent(&self, agent: &str, conversation_id: &str, m: &NewMessage) -> GwResult<Appended> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(&format!("SELECT {CONV_COLS} FROM gw.conversations WHERE id = $1 FOR UPDATE"))
            .bind(conversation_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let conv = match row {
            Some(r) => conversation_from_row(&r)?,
            None => return Err(GwError::NotFound("conversation")),
        };
        if conv.status != ConversationStatus::Assigned || conv.assigned_agent.as_deref() != Some(agent) {
            return Err(GwError::NotAssigned);
        }
        let message = Self::insert_message(&mut tx, &conv, m).await?;
        if m.kind == MessageKind::Disposition {
            sqlx::query("UPDATE gw.conversations SET status = 'closed', closed_at = now() WHERE id = $1")
                .bind(&conv.id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(Appended { customer: conv.customer, agent: Some(agent.to_string()), routing_skill: None, message })
    }

    // --------------------------------------------------------------------------------------------
    // Routing
    // --------------------------------------------------------------------------------------------

    async fn lock_skill(tx: &mut Tx, skill: &str) -> GwResult<()> {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('gw:skill:' || $1))").bind(skill).execute(&mut **tx).await.map_err(db)?;
        Ok(())
    }

    async fn candidates(tx: &mut Tx, skill: &str) -> GwResult<Vec<RoutingCandidate>> {
        let rows = sqlx::query(&format!(
            "SELECT a.id, a.skills, a.idle_since, (a.status_available AND {LIVE_SESSION}) AS available
               FROM gw.agents a WHERE $1 = ANY(a.skills) AND a.status_available"
        ))
        .bind(skill)
        .fetch_all(&mut **tx)
        .await
        .map_err(db)?;
        rows.iter()
            .map(|r| {
                Ok(RoutingCandidate {
                    id: r.try_get("id").map_err(db)?,
                    skills: r.try_get("skills").map_err(db)?,
                    idle_since: r.try_get("idle_since").map_err(db)?,
                    available: r.try_get("available").map_err(db)?,
                })
            })
            .collect()
    }

    /// Skill and status of a conversation (routing requests by conversation id).
    pub async fn queued_skill(&self, conversation_id: &str) -> GwResult<Option<String>> {
        let row: Option<(String, String)> = sqlx::query_as("SELECT skill, status FROM gw.conversations WHERE id = $1")
            .bind(conversation_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        Ok(row.and_then(|(skill, status)| (status == "queued").then_some(skill)))
    }

    /// Routes a conversation if it is still queued. Assignment always goes through the skill's
    /// queue in arrival order, so an older queued conversation is served first.
    pub async fn route(&self, conversation_id: &str) -> GwResult<Vec<Appended>> {
        match self.queued_skill(conversation_id).await? {
            Some(skill) => self.drain_queues(&[skill]).await,
            None => Ok(Vec::new()),
        }
    }

    /// Hands queued conversations to agents, oldest first, per skill: each conversation goes to the
    /// longest-idle available agent with the skill. Used when a conversation opens, when an agent
    /// becomes available, after a re-route or a config reload, and as a periodic safety net.
    ///
    /// Works in chunks of up to `CHUNK` conversations, one short transaction each (skill lock held
    /// only for the chunk): candidates read once, the agent for each conversation chosen in memory
    /// (each assignment moves that agent to the back of the idle order), then set-based writes —
    /// status + `seq`, the `assignment` messages, the agents' idle times. A chunk is committed (and
    /// can be published) before the next starts, so a burst of new conversations is not held back
    /// behind one long transaction.
    pub async fn drain_queues(&self, skills: &[String]) -> GwResult<Vec<Appended>> {
        let mut out = Vec::new();
        let mut skills: Vec<String> = skills.to_vec();
        skills.sort();
        skills.dedup();
        for skill in skills {
            loop {
                let (assigned, more) = self.drain_chunk(&skill).await?;
                out.extend(assigned);
                if !more {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// One chunk of `drain_queues`. Returns the assignments and whether more may be waiting.
    pub async fn drain_chunk(&self, skill: &str) -> GwResult<(Vec<Appended>, bool)> {
        const CHUNK: usize = 100;
        let t0 = std::time::Instant::now();
        let mut tx = self.begin().await?;
        Self::lock_skill(&mut tx, skill).await?;
        let t_lock = t0.elapsed();
        let mut candidates: Vec<RoutingCandidate> = Self::candidates(&mut tx, skill).await?.into_iter().filter(|c| c.available).collect();
        tracing::debug!(skill, candidates = candidates.len(), lock_ms = t_lock.as_millis() as u64, "routing chunk");
        if candidates.is_empty() {
            tx.commit().await.map_err(db)?;
            return Ok((Vec::new(), false));
        }
        let rows = sqlx::query(&format!(
            "SELECT {CONV_COLS} FROM gw.conversations WHERE status = 'queued' AND skill = $1
              ORDER BY queued_at, id LIMIT {CHUNK} FOR UPDATE SKIP LOCKED"
        ))
        .bind(skill)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        if rows.is_empty() {
            tx.commit().await.map_err(db)?;
            return Ok((Vec::new(), false));
        }
        let convs: Vec<Conversation> = rows.iter().map(conversation_from_row).collect::<GwResult<_>>()?;
        let mut clock = Utc::now();
        let mut pairs: Vec<(String, String)> = Vec::with_capacity(convs.len());
        let mut busy: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();
        for c in &convs {
            let Some(agent) = pick_longest_idle(skill, &candidates).map(|a| a.id.clone()) else { break };
            // Strictly increasing, so the in-chunk order survives the write-back.
            clock = Utc::now().max(clock + chrono::Duration::microseconds(1));
            if let Some(a) = candidates.iter_mut().find(|a| a.id == agent) {
                a.idle_since = clock;
            }
            busy.insert(agent.clone(), clock);
            pairs.push((c.id.clone(), agent));
        }
        let (conv_ids, agents): (Vec<String>, Vec<String>) = pairs.iter().cloned().unzip();
        let seqs: Vec<(String, i64)> = sqlx::query_as(
            "UPDATE gw.conversations c SET status = 'assigned', assigned_agent = v.agent, last_seq = c.last_seq + 1
               FROM unnest($1::text[], $2::text[]) AS v(id, agent)
              WHERE c.id = v.id
             RETURNING c.id, c.last_seq",
        )
        .bind(&conv_ids)
        .bind(&agents)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        let seq_of: BTreeMap<String, i64> = seqs.into_iter().collect();
        let mut m_conv = Vec::new();
        let mut m_seq = Vec::new();
        let mut m_id = Vec::new();
        let mut m_channel = Vec::new();
        let mut m_body = Vec::new();
        for (cid, agent) in &pairs {
            let conv = convs.iter().find(|c| &c.id == cid).ok_or_else(|| GwError::internal("conversation vanished"))?;
            m_conv.push(cid.clone());
            m_seq.push(*seq_of.get(cid).ok_or_else(|| GwError::internal("missing seq"))?);
            m_id.push(ulid());
            m_channel.push(conv.channel.as_str().to_string());
            m_body.push(json!({ "agent_id": agent, "skill": conv.skill }));
        }
        let inserted = sqlx::query(&format!(
            "INSERT INTO gw.messages (conversation_id, seq, message_id, channel, direction, actor_kind, actor_id, kind, body, received_at)
             SELECT v.conversation_id, v.seq, v.message_id, v.channel, 'outbound', 'system', 'router', 'assignment', v.body, clock_timestamp()
               FROM unnest($1::text[], $2::bigint[], $3::text[], $4::text[], $5::jsonb[]) AS v(conversation_id, seq, message_id, channel, body)
             RETURNING {MESSAGE_COLS}"
        ))
        .bind(&m_conv)
        .bind(&m_seq)
        .bind(&m_id)
        .bind(&m_channel)
        .bind(&m_body)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        let (ids, times): (Vec<String>, Vec<DateTime<Utc>>) = busy.into_iter().unzip();
        sqlx::query(
            "UPDATE gw.agents a SET idle_since = v.t, updated_at = now()
               FROM unnest($1::text[], $2::timestamptz[]) AS v(id, t) WHERE a.id = v.id",
        )
        .bind(&ids)
        .bind(&times)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let mut by_conv: BTreeMap<String, CanonicalMessage> = BTreeMap::new();
        for r in &inserted {
            let m = message_from_row(r)?;
            by_conv.insert(m.conversation_id.clone(), m);
        }
        // Keep queue (arrival) order in the result.
        let mut assigned = Vec::with_capacity(pairs.len());
        for (cid, agent) in pairs {
            let conv = convs.iter().find(|c| c.id == cid).ok_or_else(|| GwError::internal("conversation vanished"))?;
            let message = by_conv.remove(&cid).ok_or_else(|| GwError::internal("assignment not inserted"))?;
            assigned.push(Appended { customer: conv.customer.clone(), agent: Some(agent), routing_skill: None, message });
        }
        let more = rows.len() == CHUNK;
        if t0.elapsed() > Duration::from_millis(100) {
            tracing::info!(
                skill,
                assigned = assigned.len(),
                lock_ms = t_lock.as_millis() as u64,
                total_ms = t0.elapsed().as_millis() as u64,
                "slow routing chunk"
            );
        }
        Ok((assigned, more))
    }

    /// Skills that currently have a queue and at least one available agent.
    pub async fn routable_skills(&self) -> GwResult<Vec<String>> {
        sqlx::query_scalar(&format!(
            "SELECT DISTINCT c.skill FROM gw.conversations c
              WHERE c.status = 'queued'
                AND EXISTS (SELECT 1 FROM gw.agents a WHERE a.status_available AND c.skill = ANY(a.skills) AND {LIVE_SESSION})"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(db)
    }

    // --------------------------------------------------------------------------------------------
    // Agents and presence
    // --------------------------------------------------------------------------------------------

    /// An agent connection opened (`hello` or `resume`). Fixture agents keep their fixture skills;
    /// agents not in the fixture use the skills they announced. A resumed session within the grace
    /// period gets its previous status back. Returns whether the agent is now available.
    pub async fn agent_connected(
        &self,
        agent: &str,
        hello_skills: &[String],
        connection_id: &str,
        session_id: &str,
        node: &str,
        resume: bool,
    ) -> GwResult<bool> {
        let mut tx = self.begin().await?;
        sqlx::query(
            "INSERT INTO gw.agents (id, skills, in_fixture) VALUES ($1, $2, false)
             ON CONFLICT (id) DO UPDATE SET skills = CASE WHEN gw.agents.in_fixture THEN gw.agents.skills ELSE EXCLUDED.skills END",
        )
        .bind(agent)
        .bind(hello_skills)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query("INSERT INTO gw.agent_sessions (connection_id, session_id, agent_id, node_id) VALUES ($1, $2, $3, $4)")
            .bind(connection_id)
            .bind(session_id)
            .bind(agent)
            .bind(node)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let available: bool = sqlx::query_scalar(
            "UPDATE gw.agents SET
                status_available = CASE WHEN $2 AND disconnected_at IS NOT NULL THEN resume_available ELSE status_available END,
                resume_available = false,
                disconnected_at = NULL,
                updated_at = now()
              WHERE id = $1 RETURNING status_available",
        )
        .bind(agent)
        .bind(resume)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(available)
    }

    /// An agent connection closed. When it was the agent's last live connection the agent goes
    /// unavailable and the 30 s re-route clock starts. Returns `true` when presence changed.
    pub async fn agent_disconnected(&self, agent: &str, connection_id: &str) -> GwResult<bool> {
        let mut tx = self.begin().await?;
        sqlx::query("DELETE FROM gw.agent_sessions WHERE connection_id = $1").bind(connection_id).execute(&mut *tx).await.map_err(db)?;
        let changed = sqlx::query(&format!(
            "UPDATE gw.agents a SET resume_available = status_available, status_available = false,
                    disconnected_at = coalesce(disconnected_at, now()), updated_at = now()
              WHERE a.id = $1 AND NOT {LIVE_SESSION}"
        ))
        .bind(agent)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        tx.commit().await.map_err(db)?;
        Ok(changed)
    }

    /// The agent's `status` frame. Becoming available resets the idle clock. Returns whether the
    /// flag changed.
    pub async fn set_status(&self, agent: &str, available: bool) -> GwResult<bool> {
        let mut tx = self.begin().await?;
        let old: Option<bool> = sqlx::query_scalar("SELECT status_available FROM gw.agents WHERE id = $1 FOR UPDATE")
            .bind(agent)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let Some(old) = old else { return Ok(false) };
        if old != available {
            sqlx::query(
                "UPDATE gw.agents SET status_available = $2,
                        idle_since = CASE WHEN $2 THEN clock_timestamp() ELSE idle_since END, updated_at = now()
                  WHERE id = $1",
            )
            .bind(agent)
            .bind(available)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(old != available)
    }

    pub async fn agent_skills(&self, agent: &str) -> GwResult<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT skills FROM gw.agents WHERE id = $1")
            .bind(agent)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .unwrap_or_default())
    }

    pub async fn presence(&self, queue_skills: &[String]) -> GwResult<PresenceView> {
        let rows = sqlx::query(&format!(
            "SELECT a.id, a.skills, a.status_available, {LIVE_SESSION} AS connected, a.idle_since,
                    (SELECT count(*) FROM gw.conversations c WHERE c.assigned_agent = a.id AND c.status = 'assigned') AS open
               FROM gw.agents a ORDER BY a.id"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        let mut agents = Vec::with_capacity(rows.len());
        for r in &rows {
            let connected: bool = r.try_get("connected").map_err(db)?;
            let flag: bool = r.try_get("status_available").map_err(db)?;
            let idle: DateTime<Utc> = r.try_get("idle_since").map_err(db)?;
            agents.push(AgentPresence {
                id: r.try_get("id").map_err(db)?,
                skills: r.try_get("skills").map_err(db)?,
                status: if connected && flag {
                    "available"
                } else if connected {
                    "unavailable"
                } else {
                    "offline"
                },
                connected,
                open_conversations: r.try_get("open").map_err(db)?,
                idle_since: rfc3339(idle),
            });
        }
        let mut queues: BTreeMap<String, i64> = queue_skills.iter().map(|s| (s.clone(), 0)).collect();
        for (skill, n) in self.queue_depths().await? {
            queues.insert(skill, n);
        }
        Ok(PresenceView { agents, queues })
    }

    pub async fn queue_depths(&self) -> GwResult<Vec<(String, i64)>> {
        sqlx::query_as("SELECT skill, count(*) FROM gw.conversations WHERE status = 'queued' GROUP BY skill")
            .fetch_all(&self.pool)
            .await
            .map_err(db)
    }

    // --------------------------------------------------------------------------------------------
    // Nodes, failure detection, re-routing
    // --------------------------------------------------------------------------------------------

    pub async fn heartbeat(&self, node: &str) -> GwResult<()> {
        sqlx::query(
            "INSERT INTO gw.nodes (node_id, heartbeat_at) VALUES ($1, now())
             ON CONFLICT (node_id) DO UPDATE SET heartbeat_at = now()",
        )
        .bind(node)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }

    /// A node that starts fresh holds no connections yet: rows left by its previous life go.
    pub async fn node_started(&self, node: &str) -> GwResult<Vec<String>> {
        let agents: Vec<String> = sqlx::query_scalar("DELETE FROM gw.agent_sessions WHERE node_id = $1 RETURNING agent_id")
            .bind(node)
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;
        self.mark_disconnected(&agents, None).await
    }

    /// Removes this node's connection rows that no longer belong to a live socket (a disconnect
    /// whose database write failed). Rows younger than 5 s are left alone: their socket may be
    /// registering right now. Returns agents whose presence changed.
    pub async fn reconcile_node(&self, node: &str, live: &[String]) -> GwResult<Vec<String>> {
        let stale: Vec<String> = sqlx::query_scalar(
            "DELETE FROM gw.agent_sessions
              WHERE node_id = $1 AND connected_at < now() - interval '5 seconds' AND NOT (connection_id = ANY($2))
             RETURNING agent_id",
        )
        .bind(node)
        .bind(live)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        if !stale.is_empty() {
            tracing::warn!(node, rows = stale.len(), "removed stale agent connection rows");
        }
        self.mark_disconnected(&stale, None).await
    }

    /// Agents with no live connection left become unavailable (status kept for a resume) with the
    /// re-route clock started at `since` (node death: its last heartbeat). Returns those changed.
    async fn mark_disconnected(&self, agents: &[String], since: Option<DateTime<Utc>>) -> GwResult<Vec<String>> {
        if agents.is_empty() {
            return Ok(Vec::new());
        }
        sqlx::query_scalar(&format!(
            "UPDATE gw.agents a SET resume_available = status_available, status_available = false,
                    disconnected_at = coalesce(disconnected_at, $2, now()), updated_at = now()
              WHERE a.id = ANY($1) AND NOT {LIVE_SESSION} AND (a.status_available OR a.disconnected_at IS NULL)
             RETURNING a.id"
        ))
        .bind(agents)
        .bind(since)
        .fetch_all(&self.pool)
        .await
        .map_err(db)
    }

    /// One failure-detection pass (every node runs it each second; every step is idempotent):
    /// 1. connections on dead nodes are dropped (their agents go unavailable as of the node's last
    ///    heartbeat);
    /// 2. conversations of agents disconnected for 30 s go back to routing (queue order kept);
    /// 3. queued conversations with an available agent are assigned (safety net).
    pub async fn reap(&self) -> GwResult<ReapOutcome> {
        let mut outcome = ReapOutcome::default();
        let dead: Vec<(String, DateTime<Utc>)> = sqlx::query_as(&format!(
            "DELETE FROM gw.agent_sessions s
               USING gw.nodes n
              WHERE n.node_id = s.node_id AND n.heartbeat_at <= now() - make_interval(secs => {NODE_DEAD_SECS})
             RETURNING s.agent_id, n.heartbeat_at"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        let mut by_time: BTreeMap<DateTime<Utc>, Vec<String>> = BTreeMap::new();
        for (agent, at) in dead {
            by_time.entry(at).or_default().push(agent);
        }
        for (at, agents) in by_time {
            outcome.presence_changed.extend(self.mark_disconnected(&agents, Some(at)).await?);
        }

        let expired: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT a.id FROM gw.agents a
              WHERE a.disconnected_at IS NOT NULL
                AND a.disconnected_at <= now() - make_interval(secs => {REROUTE_GRACE_SECS})
                AND NOT {LIVE_SESSION}"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        for agent in expired {
            let mut tx = self.begin().await?;
            let lock: Option<String> =
                sqlx::query_scalar("SELECT id FROM gw.agents WHERE id = $1 AND disconnected_at IS NOT NULL FOR UPDATE SKIP LOCKED")
                    .bind(&agent)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            if lock.is_none() {
                continue;
            }
            let skills: Vec<String> = sqlx::query_scalar(
                "UPDATE gw.conversations SET status = 'queued', assigned_agent = NULL
                  WHERE assigned_agent = $1 AND status = 'assigned' RETURNING skill",
            )
            .bind(&agent)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            // The session is gone for good: a later `resume` is a fresh start.
            sqlx::query("UPDATE gw.agents SET disconnected_at = NULL, resume_available = false, updated_at = now() WHERE id = $1")
                .bind(&agent)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            tx.commit().await.map_err(db)?;
            if !skills.is_empty() {
                tracing::info!(agent = %agent, conversations = skills.len(), "re-routing conversations of a disconnected agent");
            }
            outcome.requeued += skills.len();
            outcome.presence_changed.push(agent);
        }

        let skills = self.routable_skills().await?;
        outcome.assigned = self.drain_queues(&skills).await?;
        Ok(outcome)
    }

    // --------------------------------------------------------------------------------------------
    // Reads
    // --------------------------------------------------------------------------------------------

    pub async fn conversation(&self, id: &str) -> GwResult<Option<(Conversation, Vec<CanonicalMessage>)>> {
        let row = sqlx::query(&format!("SELECT {CONV_COLS} FROM gw.conversations WHERE id = $1"))
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        let Some(row) = row else { return Ok(None) };
        let conv = conversation_from_row(&row)?;
        let messages = self.messages_after(id, 0, i64::MAX).await?;
        Ok(Some((conv, messages)))
    }

    pub async fn conversation_exists(&self, id: &str) -> GwResult<bool> {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM gw.conversations WHERE id = $1)").bind(id).fetch_one(&self.pool).await.map_err(db)
    }

    pub async fn messages_after(&self, conversation_id: &str, after: i64, limit: i64) -> GwResult<Vec<CanonicalMessage>> {
        let rows =
            sqlx::query(&format!("SELECT {MESSAGE_COLS} FROM gw.messages WHERE conversation_id = $1 AND seq > $2 ORDER BY seq LIMIT $3"))
                .bind(conversation_id)
                .bind(after)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(db)?;
        rows.iter().map(message_from_row).collect()
    }

    /// Open conversations of many customers in one query (batched session lookups):
    /// `(customer, conversation_id, last_seq)`.
    pub async fn customers_conversations(&self, customers: &[String]) -> GwResult<Vec<(String, String, i64)>> {
        sqlx::query_as("SELECT customer, id, last_seq FROM gw.conversations WHERE customer = ANY($1) AND status <> 'closed'")
            .bind(customers)
            .fetch_all(&self.pool)
            .await
            .map_err(db)
    }

    /// Open conversations a customer session is subscribed to, with their current last `seq`.
    pub async fn customer_conversations(&self, customer: &str) -> GwResult<Vec<(String, i64)>> {
        sqlx::query_as("SELECT id, last_seq FROM gw.conversations WHERE customer = $1 AND status <> 'closed' ORDER BY id")
            .bind(customer)
            .fetch_all(&self.pool)
            .await
            .map_err(db)
    }

    /// Conversations assigned to an agent, with their current last `seq`.
    pub async fn agent_conversations(&self, agent: &str) -> GwResult<Vec<(String, i64)>> {
        sqlx::query_as("SELECT id, last_seq FROM gw.conversations WHERE assigned_agent = $1 AND status = 'assigned' ORDER BY id")
            .bind(agent)
            .fetch_all(&self.pool)
            .await
            .map_err(db)
    }

    /// Whether `who` may receive messages of this conversation (resume replay check).
    pub async fn subscribed(&self, conversation_id: &str, role: ActorKind, who: &str) -> GwResult<bool> {
        let sql = match role {
            ActorKind::Customer => "SELECT EXISTS (SELECT 1 FROM gw.conversations WHERE id = $1 AND customer = $2)",
            _ => "SELECT EXISTS (SELECT 1 FROM gw.conversations WHERE id = $1 AND assigned_agent = $2 AND status = 'assigned')",
        };
        sqlx::query_scalar(sql).bind(conversation_id).bind(who).fetch_one(&self.pool).await.map_err(db)
    }

    // --------------------------------------------------------------------------------------------
    // Configuration
    // --------------------------------------------------------------------------------------------

    /// Stores the fixture and applies its agents/skills. Returns the new config version.
    pub async fn save_fixture(&self, f: &Fixture) -> GwResult<i64> {
        let mut tx = self.begin().await?;
        let version: i64 = sqlx::query_scalar(
            "INSERT INTO gw.config (id, fixture, version) VALUES (1, $1, 1)
             ON CONFLICT (id) DO UPDATE SET fixture = EXCLUDED.fixture, version = gw.config.version + 1, updated_at = now()
             RETURNING version",
        )
        .bind(serde_json::to_value(f).map_err(|e| GwError::Internal(e.into()))?)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let ids: Vec<String> = f.agents.iter().map(|a| a.id.clone()).collect();
        for a in &f.agents {
            sqlx::query(
                "INSERT INTO gw.agents (id, skills, in_fixture) VALUES ($1, $2, true)
                 ON CONFLICT (id) DO UPDATE SET skills = EXCLUDED.skills, in_fixture = true, updated_at = now()",
            )
            .bind(&a.id)
            .bind(&a.skills)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        // Agents dropped from the fixture keep their assignments but get no new work.
        sqlx::query("UPDATE gw.agents SET skills = '{}', in_fixture = false, updated_at = now() WHERE in_fixture AND NOT (id = ANY($1))")
            .bind(&ids)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(version)
    }

    pub async fn load_fixture(&self) -> GwResult<Option<(Fixture, i64)>> {
        let row: Option<(Value, i64)> =
            sqlx::query_as("SELECT fixture, version FROM gw.config WHERE id = 1").fetch_optional(&self.pool).await.map_err(db)?;
        row.map(|(v, version)| serde_json::from_value(v).map(|f| (f, version)).map_err(|e| GwError::Internal(e.into()))).transpose()
    }

    pub async fn ping(&self) -> bool {
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&self.pool).await.is_ok()
    }
}
