//! M10 hub domain: conversations, messages, presence, delivery status and the routing rule.
//! Pure Rust — no Axum, SQLx or Askama (CLAUDE.md rule 2).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainError(pub String);

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

macro_rules! text_enum {
    ($(#[$m:meta])* $name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name { $($variant),+ }

        // Serialised exactly as the canonical text (the database and wire values).
        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = String::deserialize(d)?;
                s.parse().map_err(|e: DomainError| serde::de::Error::custom(e.0))
            }
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $text),+ }
            }
        }

        impl FromStr for $name {
            type Err = DomainError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s { $($text => Ok(Self::$variant),)+ other => Err(DomainError(format!("invalid {}: {other}", stringify!($name)))) }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
        }
    };
}

text_enum!(
    /// Channels in the gateway slice. WhatsApp and voice are SIMULATED adapters (ADR-0012).
    Channel { WhatsApp => "whatsapp", Voice => "voice", WebChat => "webchat" }
);

text_enum!(ConversationStatus { Queued => "queued", Assigned => "assigned", Closed => "closed" });

text_enum!(Direction { Inbound => "inbound", Outbound => "outbound", Event => "event" });

text_enum!(MessageKind { Text => "text", CallEvent => "call_event", System => "system" });

text_enum!(SenderType { Customer => "customer", Agent => "agent", System => "system" });

text_enum!(
    /// Agent presence (OCC-M10-R034). Only `Available` agents receive new work.
    Presence { Available => "available", Busy => "busy", Away => "away", WrapUp => "wrap_up", Offline => "offline" }
);

text_enum!(
    /// Outbound delivery status (OCC-M10-R030).
    DeliveryStatus { Queued => "queued", Sent => "sent", Delivered => "delivered", Read => "read", Failed => "failed" }
);

impl DeliveryStatus {
    fn rank(self) -> u8 {
        match self {
            Self::Queued => 0,
            Self::Sent => 1,
            Self::Delivered => 2,
            Self::Read => 3,
            Self::Failed => 4,
        }
    }

    /// Receipts can arrive late or out of order (a provider may send `read` before `delivered`).
    /// Status only moves forward; `failed` is terminal and only reachable before `delivered`.
    pub fn can_advance_to(self, next: DeliveryStatus) -> bool {
        match (self, next) {
            (Self::Failed, _) => false,
            (_, Self::Failed) => matches!(self, Self::Queued | Self::Sent),
            (a, b) => b.rank() > a.rank(),
        }
    }
}

/// Skill names: lowercase `[a-z0-9_-]{1,32}`.
pub fn normalize_skill(raw: &str) -> Result<String, DomainError> {
    let s = raw.trim().to_ascii_lowercase();
    let ok = !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
    if ok {
        Ok(s)
    } else {
        Err(DomainError(format!("invalid skill '{}': use 1-32 of a-z, 0-9, '_' or '-'", raw.trim())))
    }
}

/// Parses a comma-separated skill list; duplicates are removed, order kept.
pub fn parse_skills(raw: &str) -> Result<Vec<String>, DomainError> {
    let mut out: Vec<String> = Vec::new();
    for part in raw.split(',').filter(|p| !p.trim().is_empty()) {
        let s = normalize_skill(part)?;
        if !out.contains(&s) {
            out.push(s);
        }
    }
    if out.is_empty() || out.len() > 20 {
        return Err(DomainError("an agent needs between 1 and 20 skills".into()));
    }
    Ok(out)
}

pub const MAX_BODY_CHARS: usize = 4096;

/// Message text: trimmed, non-empty, at most 4096 characters, no control characters except
/// newline/tab.
pub fn validate_body(raw: &str) -> Result<String, DomainError> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(DomainError("message text is required".into()));
    }
    if s.chars().count() > MAX_BODY_CHARS {
        return Err(DomainError(format!("message text is longer than {MAX_BODY_CHARS} characters")));
    }
    Ok(s.chars().filter(|c| !c.is_control() || *c == '\n' || *c == '\t').collect())
}

/// What every channel adapter produces from an inbound provider event (OCC-M10-R022): one
/// canonical message, independent of the channel's wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalMessage {
    pub channel: Channel,
    /// Provider-side address of OUR endpoint (WhatsApp phone_number_id, dialled DID, widget key).
    pub endpoint_address: String,
    /// The customer's channel identity (wa_id, caller ANI, web visitor id).
    pub customer_address: String,
    pub customer_name: Option<String>,
    pub kind: MessageKind,
    pub body: String,
    pub provider_message_id: Option<String>,
    /// Stable per provider event so duplicate deliveries are ignored.
    pub idempotency_key: String,
}

/// A provider delivery receipt (OCC-M10-R031).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalStatus {
    pub channel: Channel,
    pub provider_message_id: String,
    pub status: DeliveryStatus,
    pub detail: Option<String>,
}

/// Routing candidate: one agent with the requested skill, as seen inside the routing transaction.
#[derive(Debug, Clone)]
pub struct AgentCandidate {
    pub user_id: Uuid,
    pub skills: Vec<String>,
    pub presence: Presence,
    pub active: i64,
    pub max_concurrent: i64,
}

impl AgentCandidate {
    pub fn can_take(&self, skill: &str) -> bool {
        self.presence == Presence::Available && self.active < self.max_concurrent && self.skills.iter().any(|s| s == skill)
    }
}

/// Skill-based routing rule (OCC-M10-R017, R037): among Available agents with the skill and free
/// capacity, pick the least loaded (lowest utilisation, then fewest active, then lowest id for
/// determinism). `None` means the conversation stays queued for that skill.
pub fn pick_agent(skill: &str, candidates: &[AgentCandidate]) -> Option<Uuid> {
    candidates
        .iter()
        .filter(|c| c.can_take(skill))
        .min_by(|a, b| {
            let ua = a.active as f64 / a.max_concurrent.max(1) as f64;
            let ub = b.active as f64 / b.max_concurrent.max(1) as f64;
            ua.total_cmp(&ub).then(a.active.cmp(&b.active)).then(a.user_id.cmp(&b.user_id))
        })
        .map(|c| c.user_id)
}

/// Outbound retry ladder (OCC-M10-R032: 1m / 5m / 30m, then failed). `attempts` is the number of
/// failed attempts so far; `None` means give up.
pub fn retry_delay_secs(attempts: i32) -> Option<i64> {
    match attempts {
        1 => Some(60),
        2 => Some(300),
        3 => Some(1800),
        _ => None,
    }
}

text_enum!(
    /// Normalised voice call events (OCC-M03-R012).
    CallEvent {
    Ringing => "ringing",
    Answered => "answered",
    Held => "held",
    Retrieved => "retrieved",
    Transferred => "transferred",
    Ended => "ended",
    Abandoned => "abandoned",
});

impl CallEvent {
    pub fn describe(self, caller: &str) -> String {
        match self {
            Self::Ringing => format!("Incoming call from {caller}"),
            Self::Answered => "Call answered".into(),
            Self::Held => "Call on hold".into(),
            Self::Retrieved => "Call retrieved from hold".into(),
            Self::Transferred => "Call transferred".into(),
            Self::Ended => "Call ended".into(),
            Self::Abandoned => "Caller hung up before answer (abandoned)".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(id: u128, skills: &[&str], presence: Presence, active: i64, max: i64) -> AgentCandidate {
        AgentCandidate {
            user_id: Uuid::from_u128(id),
            skills: skills.iter().map(|s| s.to_string()).collect(),
            presence,
            active,
            max_concurrent: max,
        }
    }

    #[test]
    fn routes_to_matching_available_agent_with_capacity() {
        let c = vec![
            agent(1, &["sales"], Presence::Available, 0, 3),
            agent(2, &["support"], Presence::Available, 0, 3),
            agent(3, &["support"], Presence::Away, 0, 3),
        ];
        assert_eq!(pick_agent("support", &c), Some(Uuid::from_u128(2)));
        assert_eq!(pick_agent("billing", &c), None);
    }

    #[test]
    fn full_or_unavailable_agents_mean_queue() {
        let c = vec![agent(1, &["support"], Presence::Available, 3, 3), agent(2, &["support"], Presence::Offline, 0, 3)];
        assert_eq!(pick_agent("support", &c), None);
    }

    #[test]
    fn least_loaded_agent_wins() {
        let c = vec![
            agent(1, &["support"], Presence::Available, 2, 3),
            agent(2, &["support"], Presence::Available, 1, 3),
            agent(3, &["support"], Presence::Available, 1, 5),
        ];
        assert_eq!(pick_agent("support", &c), Some(Uuid::from_u128(3)));
    }

    #[test]
    fn delivery_status_only_moves_forward() {
        use DeliveryStatus::*;
        assert!(Queued.can_advance_to(Sent));
        assert!(Sent.can_advance_to(Read));
        assert!(!Read.can_advance_to(Delivered));
        assert!(!Delivered.can_advance_to(Failed));
        assert!(Sent.can_advance_to(Failed));
        assert!(!Failed.can_advance_to(Sent));
        assert!(!Sent.can_advance_to(Sent));
    }

    #[test]
    fn retry_ladder_is_1m_5m_30m_then_fail() {
        assert_eq!(retry_delay_secs(1), Some(60));
        assert_eq!(retry_delay_secs(2), Some(300));
        assert_eq!(retry_delay_secs(3), Some(1800));
        assert_eq!(retry_delay_secs(4), None);
    }

    #[test]
    fn skills_are_validated() {
        assert_eq!(parse_skills(" Sales, support,sales ").unwrap(), vec!["sales", "support"]);
        assert!(parse_skills("").is_err());
        assert!(parse_skills("bad skill").is_err());
        assert!(normalize_skill("x".repeat(33).as_str()).is_err());
    }

    #[test]
    fn body_validation() {
        assert_eq!(validate_body("  hi\u{0007} there \n").unwrap(), "hi there");
        assert!(validate_body("   ").is_err());
        assert!(validate_body(&"a".repeat(MAX_BODY_CHARS + 1)).is_err());
    }

    #[test]
    fn enums_serialise_as_canonical_text() {
        assert_eq!(serde_json::to_string(&Channel::WhatsApp).unwrap(), "\"whatsapp\"");
        assert_eq!(serde_json::to_string(&Channel::WebChat).unwrap(), "\"webchat\"");
        assert_eq!(serde_json::from_str::<Presence>("\"wrap_up\"").unwrap(), Presence::WrapUp);
        assert!(serde_json::from_str::<Presence>("\"lunch\"").is_err());
    }

    #[test]
    fn enums_round_trip() {
        for p in ["available", "busy", "away", "wrap_up", "offline"] {
            assert_eq!(p.parse::<Presence>().unwrap().as_str(), p);
        }
        assert!("nope".parse::<Channel>().is_err());
        assert_eq!(CallEvent::Ringing.describe("+60123"), "Incoming call from +60123");
    }
}
