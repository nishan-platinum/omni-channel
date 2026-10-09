//! Gateway domain (bake-off contract, ADR-0014): the canonical message, the platform fixture,
//! ULIDs and the routing rule. No Axum / SQLx / Redis types here.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, SecondsFormat, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ------------------------------------------------------------------------------------------------
// Identifiers and time
// ------------------------------------------------------------------------------------------------

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// A new ULID: 48-bit millisecond timestamp + 80 random bits, Crockford base32 (26 chars).
pub fn ulid() -> String {
    ulid_at(Utc::now().timestamp_millis().max(0) as u64)
}

pub fn ulid_at(millis: u64) -> String {
    let mut rnd = [0u8; 10];
    rand::thread_rng().fill_bytes(&mut rnd);
    let mut v: u128 = (u128::from(millis & 0xFFFF_FFFF_FFFF)) << 80;
    for (i, b) in rnd.iter().enumerate() {
        v |= u128::from(*b) << (8 * (9 - i));
    }
    let mut out = [0u8; 26];
    for slot in out.iter_mut().rev() {
        *slot = CROCKFORD[(v & 0x1F) as usize];
        v >>= 5;
    }
    String::from_utf8(out.to_vec()).unwrap_or_default()
}

pub fn is_ulid(s: &str) -> bool {
    s.len() == 26 && s.bytes().all(|b| CROCKFORD.contains(&b.to_ascii_uppercase())) && s.as_bytes()[0] <= b'7'
}

/// RFC 3339 with millisecond precision and `Z`, as the contract requires.
pub fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

// ------------------------------------------------------------------------------------------------
// Canonical message
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Whatsapp,
    Sip,
}

impl Channel {
    pub fn as_str(self) -> &'static str {
        match self {
            Channel::Whatsapp => "whatsapp",
            Channel::Sip => "sip",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "whatsapp" => Some(Channel::Whatsapp),
            "sip" => Some(Channel::Sip),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Inbound => "inbound",
            Direction::Outbound => "outbound",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "inbound" => Some(Direction::Inbound),
            "outbound" => Some(Direction::Outbound),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Customer,
    Agent,
    System,
}

impl ActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActorKind::Customer => "customer",
            ActorKind::Agent => "agent",
            ActorKind::System => "system",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "customer" => Some(ActorKind::Customer),
            "agent" => Some(ActorKind::Agent),
            "system" => Some(ActorKind::System),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Text,
    CallEvent,
    Assignment,
    Disposition,
}

impl MessageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MessageKind::Text => "text",
            MessageKind::CallEvent => "call_event",
            MessageKind::Assignment => "assignment",
            MessageKind::Disposition => "disposition",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "text" => Some(MessageKind::Text),
            "call_event" => Some(MessageKind::CallEvent),
            "assignment" => Some(MessageKind::Assignment),
            "disposition" => Some(MessageKind::Disposition),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub kind: ActorKind,
    pub id: String,
}

/// The one record both adapters produce and the event stream carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalMessage {
    pub message_id: String,
    pub conversation_id: String,
    pub seq: i64,
    pub channel: Channel,
    pub direction: Direction,
    pub actor: Actor,
    pub kind: MessageKind,
    pub body: Value,
    pub received_at: String,
    pub external_id: Option<String>,
}

/// A message about to be appended (the gateway assigns `message_id`, `seq`, `received_at`).
#[derive(Debug, Clone)]
pub struct NewMessage {
    pub direction: Direction,
    pub actor: Actor,
    pub kind: MessageKind,
    pub body: Value,
    pub external_id: Option<String>,
    /// Duplicate-detection key (`409` on repeat): `wa:{external_id}` or `sip:{call_id}|{event}|{at}`.
    pub dedup_key: Option<String>,
    /// WebSocket `send` idempotency: a repeated `client_ref` from the same actor returns the
    /// original `ack` instead of storing a second message.
    pub client_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationStatus {
    Queued,
    Assigned,
    Closed,
}

impl ConversationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ConversationStatus::Queued => "queued",
            ConversationStatus::Assigned => "assigned",
            ConversationStatus::Closed => "closed",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(ConversationStatus::Queued),
            "assigned" => Some(ConversationStatus::Assigned),
            "closed" => Some(ConversationStatus::Closed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Conversation {
    pub id: String,
    pub channel: Channel,
    pub customer: String,
    pub skill: String,
    pub status: ConversationStatus,
    pub assigned_agent: Option<String>,
    pub last_seq: i64,
}

// ------------------------------------------------------------------------------------------------
// Ingress validation
// ------------------------------------------------------------------------------------------------

pub const MAX_TEXT: usize = 4096;
pub const MAX_ID: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid(pub String);

fn id_field(name: &str, v: &str) -> Result<(), Invalid> {
    let v = v.trim();
    if v.is_empty() || v.len() > MAX_ID || v.chars().any(char::is_control) {
        return Err(Invalid(format!("{name} must be 1..{MAX_ID} printable characters")));
    }
    Ok(())
}

pub fn parse_time(name: &str, v: &str) -> Result<DateTime<Utc>, Invalid> {
    DateTime::parse_from_rfc3339(v).map(|t| t.with_timezone(&Utc)).map_err(|_| Invalid(format!("{name} must be an RFC 3339 timestamp")))
}

pub fn validate_text(text: &str) -> Result<(), Invalid> {
    if text.is_empty() || text.chars().count() > MAX_TEXT {
        return Err(Invalid(format!("text must be 1..{MAX_TEXT} characters")));
    }
    Ok(())
}

/// Simulated WhatsApp webhook body.
#[derive(Debug, Clone, Deserialize)]
pub struct WhatsappIngress {
    pub external_id: String,
    pub from: String,
    pub text: String,
    pub sent_at: String,
}

impl WhatsappIngress {
    pub fn into_message(self) -> Result<(String, NewMessage), Invalid> {
        id_field("external_id", &self.external_id)?;
        id_field("from", &self.from)?;
        validate_text(&self.text)?;
        let sent_at = parse_time("sent_at", &self.sent_at)?;
        let from = self.from.trim().to_string();
        Ok((
            from.clone(),
            NewMessage {
                direction: Direction::Inbound,
                actor: Actor { kind: ActorKind::Customer, id: from },
                kind: MessageKind::Text,
                body: serde_json::json!({ "text": self.text, "sent_at": rfc3339(sent_at) }),
                dedup_key: Some(format!("wa:{}", self.external_id)),
                external_id: Some(self.external_id),
                client_ref: None,
            },
        ))
    }
}

pub const SIP_EVENTS: [&str; 3] = ["invite", "bye", "dtmf"];

/// Simulated SIP event feed body.
#[derive(Debug, Clone, Deserialize)]
pub struct SipIngress {
    pub call_id: String,
    pub from: String,
    pub event: String,
    pub at: String,
    /// Optional extra payload (e.g. the DTMF digits); kept in the body as is.
    #[serde(default)]
    pub digits: Option<String>,
}

impl SipIngress {
    pub fn into_message(self) -> Result<(String, NewMessage), Invalid> {
        id_field("call_id", &self.call_id)?;
        id_field("from", &self.from)?;
        if !SIP_EVENTS.contains(&self.event.as_str()) {
            return Err(Invalid("event must be invite, bye or dtmf".into()));
        }
        let at = parse_time("at", &self.at)?;
        let from = self.from.trim().to_string();
        let mut body = serde_json::json!({ "call_id": self.call_id, "event": self.event, "at": rfc3339(at) });
        if let Some(d) = self.digits.filter(|d| !d.is_empty() && d.len() <= 64) {
            body["digits"] = Value::String(d);
        }
        Ok((
            from.clone(),
            NewMessage {
                direction: Direction::Inbound,
                actor: Actor { kind: ActorKind::Customer, id: from },
                kind: MessageKind::CallEvent,
                body,
                // Duplicate = same (call_id, event, at); `at` normalised so formatting cannot dodge it.
                dedup_key: Some(format!("sip:{}|{}|{}", self.call_id, self.event, rfc3339(at))),
                external_id: Some(self.call_id),
                client_ref: None,
            },
        ))
    }
}

// ------------------------------------------------------------------------------------------------
// Platform fixture (configuration owned by the platform; the gateway only reads it)
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FixtureAgent {
    pub id: String,
    #[serde(default)]
    pub skills: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fixture {
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub agents: Vec<FixtureAgent>,
    #[serde(default)]
    pub channel_to_skill: BTreeMap<String, String>,
}

pub const DEFAULT_SKILL: &str = "default";

impl Fixture {
    /// Checks the fixture is coherent: unique agent ids, known channels, every referenced skill
    /// declared in `skills`.
    pub fn validate(&self) -> Result<(), Invalid> {
        let declared: BTreeSet<&str> = self.skills.iter().map(String::as_str).collect();
        let mut seen = BTreeSet::new();
        for a in &self.agents {
            id_field("agent id", &a.id)?;
            if !seen.insert(a.id.as_str()) {
                return Err(Invalid(format!("agent {} is listed twice", a.id)));
            }
            if let Some(s) = a.skills.iter().find(|s| !declared.contains(s.as_str())) {
                return Err(Invalid(format!("agent {} has undeclared skill {s}", a.id)));
            }
        }
        for (ch, skill) in &self.channel_to_skill {
            if Channel::parse(ch).is_none() {
                return Err(Invalid(format!("unknown channel {ch} in channel_to_skill")));
            }
            if !declared.contains(skill.as_str()) {
                return Err(Invalid(format!("channel {ch} maps to undeclared skill {skill}")));
            }
        }
        Ok(())
    }

    pub fn skill_for(&self, channel: Channel) -> String {
        self.channel_to_skill.get(channel.as_str()).cloned().unwrap_or_else(|| DEFAULT_SKILL.to_string())
    }

    pub fn agent_skills(&self, id: &str) -> Option<&[String]> {
        self.agents.iter().find(|a| a.id == id).map(|a| a.skills.as_slice())
    }

    /// Every skill that has (or can have) a queue: declared skills plus channel targets.
    pub fn queue_skills(&self) -> BTreeSet<String> {
        let mut s: BTreeSet<String> = self.skills.iter().cloned().collect();
        s.extend(self.channel_to_skill.values().cloned());
        s
    }
}

// ------------------------------------------------------------------------------------------------
// Routing rule
// ------------------------------------------------------------------------------------------------

/// An agent as routing sees it.
#[derive(Debug, Clone)]
pub struct RoutingCandidate {
    pub id: String,
    pub skills: Vec<String>,
    /// Connected (a live session on a live node) and status `available: true`.
    pub available: bool,
    /// Last time the agent became available or was given a conversation.
    pub idle_since: DateTime<Utc>,
}

/// Pick the available agent with the skill who has been idle longest (ties: lowest id, so the
/// choice is deterministic). `None` → the conversation waits in the skill's queue.
pub fn pick_longest_idle<'a>(skill: &str, candidates: &'a [RoutingCandidate]) -> Option<&'a RoutingCandidate> {
    candidates
        .iter()
        .filter(|c| c.available && c.skills.iter().any(|s| s == skill))
        .min_by(|a, b| a.idle_since.cmp(&b.idle_since).then_with(|| a.id.cmp(&b.id)))
}

/// Seconds an assigned conversation stays with a disconnected agent before it is re-routed.
pub const REROUTE_GRACE_SECS: i64 = 30;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn ulid_shape_and_time_order() {
        let a = ulid_at(1_700_000_000_000);
        let b = ulid_at(1_700_000_000_001);
        assert!(is_ulid(&a) && is_ulid(&b), "{a} {b}");
        assert!(a[..10] < b[..10]);
        assert_ne!(ulid(), ulid());
    }

    #[test]
    fn rfc3339_has_millis_and_z() {
        let t = DateTime::parse_from_rfc3339("2026-10-05T09:00:00.123456+00:00").unwrap().with_timezone(&Utc);
        assert_eq!(rfc3339(t), "2026-10-05T09:00:00.123Z");
    }

    #[test]
    fn whatsapp_ingress_maps_to_canonical_fields() {
        let (customer, m) = WhatsappIngress {
            external_id: "wamid.1".into(),
            from: "60123".into(),
            text: "hi".into(),
            sent_at: "2026-10-05T09:00:00Z".into(),
        }
        .into_message()
        .unwrap();
        assert_eq!(customer, "60123");
        assert_eq!((m.kind, m.direction, m.actor.kind), (MessageKind::Text, Direction::Inbound, ActorKind::Customer));
        assert_eq!(m.dedup_key.as_deref(), Some("wa:wamid.1"));
        assert_eq!(m.body["text"], "hi");
    }

    #[test]
    fn sip_ingress_rejects_unknown_events_and_normalises_dedup_time() {
        let ev = |event: &str, at: &str| SipIngress {
            call_id: "c1".into(),
            from: "+601".into(),
            event: event.into(),
            at: at.into(),
            digits: None,
        };
        assert!(ev("ringing", "2026-10-05T09:00:00Z").into_message().is_err());
        let (_, a) = ev("invite", "2026-10-05T09:00:00Z").into_message().unwrap();
        let (_, b) = ev("invite", "2026-10-05T17:00:00+08:00").into_message().unwrap();
        assert_eq!(a.dedup_key, b.dedup_key);
        assert_eq!(a.kind, MessageKind::CallEvent);
    }

    #[test]
    fn fixture_validation() {
        let f: Fixture = serde_json::from_value(serde_json::json!({
            "skills": ["chat", "voice"],
            "agents": [{"id": "a1", "skills": ["chat"]}],
            "channel_to_skill": {"whatsapp": "chat", "sip": "voice"}
        }))
        .unwrap();
        assert!(f.validate().is_ok());
        assert_eq!(f.skill_for(Channel::Sip), "voice");
        let mut bad = f.clone();
        bad.channel_to_skill.insert("fax".into(), "chat".into());
        assert!(bad.validate().is_err());
        let mut bad = f.clone();
        bad.agents.push(FixtureAgent { id: "a2".into(), skills: vec!["sales".into()] });
        assert!(bad.validate().is_err());
    }

    #[test]
    fn longest_idle_available_agent_with_skill_wins() {
        let now = Utc::now();
        let c = |id: &str, skill: &str, available: bool, idle_secs: i64| RoutingCandidate {
            id: id.into(),
            skills: vec![skill.into()],
            available,
            idle_since: now - Duration::seconds(idle_secs),
        };
        let agents = vec![c("a", "chat", true, 10), c("b", "chat", true, 50), c("c", "chat", false, 99), c("d", "voice", true, 99)];
        assert_eq!(pick_longest_idle("chat", &agents).map(|a| a.id.as_str()), Some("b"));
        assert_eq!(pick_longest_idle("voice", &agents).map(|a| a.id.as_str()), Some("d"));
        assert!(pick_longest_idle("billing", &agents).is_none());
    }
}
