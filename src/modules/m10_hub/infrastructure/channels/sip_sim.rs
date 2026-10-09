//! **SIMULATED** SIP/SBC call-event feed (OCC-M03-R001/R012; ADR-0012).
//!
//! Stands in for the TM SBC / voice connector: a signed JSON event per call state change
//! (`ringing`, `answered`, `held`, `retrieved`, `transferred`, `ended`, `abandoned`). The dialled
//! number (`to`) is resolved to a tenant through the endpoint registry; an unknown DID is
//! rejected (the equivalent of SIP 404). No media, no real SIP signalling.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::{ChannelAdapter, ChannelHealth, DeliveryError, Inbound, OutboundJob};
use super::super::super::domain::{CallEvent, CanonicalMessage, Channel, MessageKind};
use super::{sign, signature_valid};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipEvent {
    /// Unique per event from the SBC; repeated deliveries of the same id are ignored.
    #[serde(default)]
    pub event_id: Option<String>,
    pub call_id: String,
    pub event: String,
    /// Caller ANI / CLI.
    pub from: String,
    /// Dialled number (DNIS) — must be a provisioned DID.
    pub to: String,
    #[serde(default)]
    pub caller_name: Option<String>,
}

pub struct SimSipAdapter {
    secret: Vec<u8>,
}

impl SimSipAdapter {
    pub fn new(secret: &str) -> Self {
        Self { secret: secret.as_bytes().to_vec() }
    }

    pub fn sign(&self, body: &[u8]) -> String {
        sign(&self.secret, body)
    }
}

fn clean(s: &str, max: usize, field: &str) -> AppResult<String> {
    let s = s.trim();
    if s.is_empty() || s.len() > max || s.chars().any(char::is_control) {
        return Err(AppError::validation(field, format!("{field} is required (max {max} characters)")));
    }
    Ok(s.to_string())
}

pub fn parse_event(body: &[u8]) -> AppResult<Vec<Inbound>> {
    let e: SipEvent = serde_json::from_slice(body).map_err(|e| AppError::validation("body", format!("invalid SIP event JSON: {e}")))?;
    let event: CallEvent = e.event.parse().map_err(|_| AppError::validation("event", "unknown call event"))?;
    let call_id = clean(&e.call_id, 128, "call_id")?;
    let from = clean(&e.from, 32, "from")?;
    let to = clean(&e.to, 32, "to")?;
    let key = match e.event_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(id) => format!("sip:{}", id.chars().take(128).collect::<String>()),
        None => format!("sip:{call_id}:{event}"),
    };
    Ok(vec![Inbound::Message(CanonicalMessage {
        channel: Channel::Voice,
        endpoint_address: to,
        customer_address: from.clone(),
        customer_name: e.caller_name.map(|n| n.trim().chars().take(80).collect()).filter(|n: &String| !n.is_empty()),
        kind: MessageKind::CallEvent,
        body: format!("{} (call {call_id})", event.describe(&from)),
        provider_message_id: None,
        idempotency_key: key,
    })])
}

#[async_trait]
impl ChannelAdapter for SimSipAdapter {
    fn channel(&self) -> Channel {
        Channel::Voice
    }

    fn simulated(&self) -> bool {
        true
    }

    async fn connect(&self) -> AppResult<()> {
        if self.secret.len() < 16 {
            return Err(AppError::validation("HUB_SIM_SIP_SECRET", "must be at least 16 characters"));
        }
        Ok(())
    }

    fn ingest(&self, signature: Option<&str>, body: &[u8]) -> AppResult<Vec<Inbound>> {
        if !signature_valid(&self.secret, body, signature) {
            return Err(AppError::unauthenticated("Invalid X-Sim-Signature"));
        }
        parse_event(body)
    }

    async fn deliver(&self, _job: &OutboundJob) -> Result<String, DeliveryError> {
        Err(DeliveryError::permanent("voice has no outbound text messages".into()))
    }

    async fn health(&self) -> ChannelHealth {
        ChannelHealth {
            channel: Channel::Voice, simulated: true, healthy: true, detail: "SIMULATED SBC event feed — no SIP trunk".into()
        }
    }

    async fn backfill(&self, _since: DateTime<Utc>) -> AppResult<Vec<Inbound>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalises_call_events() {
        let a = SimSipAdapter::new("sip-secret-0123456789");
        let body = json!({"call_id": "c-1", "event": "ringing", "from": "+60123", "to": "+60320001234", "caller_name": "Ravi"}).to_string();
        let ev = a.ingest(Some(&a.sign(body.as_bytes())), body.as_bytes()).unwrap();
        let Inbound::Message(m) = &ev[0] else { panic!() };
        assert_eq!(m.channel, Channel::Voice);
        assert_eq!(m.endpoint_address, "+60320001234");
        assert_eq!(m.kind, MessageKind::CallEvent);
        assert_eq!(m.idempotency_key, "sip:c-1:ringing");
        assert!(m.body.starts_with("Incoming call from +60123"));
    }

    #[test]
    fn rejects_unsigned_and_unknown_events() {
        let a = SimSipAdapter::new("sip-secret-0123456789");
        let body = json!({"call_id": "c", "event": "ringing", "from": "1", "to": "2"}).to_string();
        assert!(a.ingest(None, body.as_bytes()).is_err());
        let bad = json!({"call_id": "c", "event": "exploded", "from": "1", "to": "2"}).to_string();
        assert!(a.ingest(Some(&a.sign(bad.as_bytes())), bad.as_bytes()).is_err());
    }
}
