//! **SIMULATED** WhatsApp Business adapter (OCC-M05-R001 shape; ADR-0012).
//!
//! * Inbound: Meta Cloud API webhook JSON (`whatsapp_business_account` → entry → changes → value
//!   with `metadata.phone_number_id`, `contacts`, `messages`, `statuses`), authenticated with
//!   `X-Hub-Signature-256` over the raw body using the (development) app secret.
//! * Outbound: an in-process fake BSP. It "sends" the message, then schedules signed `delivered`
//!   and `read` status webhooks (`SimCallbackSink`, stored in the database so any node processes
//!   them when due) that go through exactly the same `ingest` path as real webhooks. A body
//!   containing `[fail]` makes the fake BSP return an error (retry ladder demo).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::sync::Arc;

use uuid::Uuid;

use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::{ChannelAdapter, ChannelHealth, DeliveryError, Inbound, OutboundJob};
use super::super::super::domain::{CanonicalMessage, CanonicalStatus, Channel, DeliveryStatus, MessageKind};
use super::{sign, signature_valid};

/// A signed webhook the fake BSP wants delivered back to the hub.
pub use super::super::super::application::ports::ProviderCallback as SimCallback;

/// Where the fake BSP schedules its future webhooks (database-backed in the app, in-memory in
/// unit tests).
#[async_trait]
pub trait SimCallbackSink: Send + Sync {
    async fn schedule(&self, tenant: Uuid, callback: SimCallback, after: Duration) -> AppResult<()>;
}

pub struct SimWhatsAppAdapter {
    app_secret: Vec<u8>,
    callbacks: Arc<dyn SimCallbackSink>,
    delivered_after: Duration,
    read_after: Duration,
}

impl SimWhatsAppAdapter {
    pub fn new(app_secret: &str, callbacks: Arc<dyn SimCallbackSink>) -> Self {
        Self {
            app_secret: app_secret.as_bytes().to_vec(),
            callbacks,
            delivered_after: Duration::from_millis(400),
            read_after: Duration::from_millis(1200),
        }
    }

    pub fn sign(&self, body: &[u8]) -> String {
        sign(&self.app_secret, body)
    }
}

/// Builds a Meta-shaped inbound text-message webhook (used by the simulator console and tests).
pub fn inbound_payload(phone_number_id: &str, from: &str, name: &str, text: &str, wamid: &str) -> Value {
    json!({
        "object": "whatsapp_business_account",
        "entry": [{
            "id": "SIMULATED-WABA",
            "changes": [{
                "field": "messages",
                "value": {
                    "messaging_product": "whatsapp",
                    "metadata": { "display_phone_number": "simulated", "phone_number_id": phone_number_id },
                    "contacts": [{ "profile": { "name": name }, "wa_id": from }],
                    "messages": [{
                        "from": from,
                        "id": wamid,
                        "timestamp": Utc::now().timestamp().to_string(),
                        "type": "text",
                        "text": { "body": text }
                    }]
                }
            }]
        }]
    })
}

fn status_payload(phone_number_id: &str, wamid: &str, recipient: &str, status: &str) -> Value {
    json!({
        "object": "whatsapp_business_account",
        "entry": [{
            "id": "SIMULATED-WABA",
            "changes": [{
                "field": "messages",
                "value": {
                    "messaging_product": "whatsapp",
                    "metadata": { "display_phone_number": "simulated", "phone_number_id": phone_number_id },
                    "statuses": [{ "id": wamid, "status": status, "timestamp": Utc::now().timestamp().to_string(), "recipient_id": recipient }]
                }
            }]
        }]
    })
}

fn str_at<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

/// Parses a Meta webhook body into canonical events (graceful degradation for non-text types,
/// OCC-M05-R011: they become a placeholder text instead of being dropped).
pub fn parse_webhook(body: &[u8]) -> AppResult<Vec<Inbound>> {
    let v: Value = serde_json::from_slice(body).map_err(|e| AppError::validation("body", format!("invalid webhook JSON: {e}")))?;
    if str_at(&v, "object") != Some("whatsapp_business_account") {
        return Err(AppError::validation("object", "not a whatsapp_business_account webhook"));
    }
    let mut out = Vec::new();
    for entry in v.get("entry").and_then(Value::as_array).into_iter().flatten() {
        for change in entry.get("changes").and_then(Value::as_array).into_iter().flatten() {
            let Some(value) = change.get("value") else { continue };
            let Some(phone_number_id) = value.get("metadata").and_then(|m| str_at(m, "phone_number_id")) else {
                return Err(AppError::validation("metadata.phone_number_id", "missing phone_number_id"));
            };
            let names: Vec<(String, String)> = value
                .get("contacts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|c| {
                    Some((str_at(c, "wa_id")?.to_string(), c.get("profile").and_then(|p| str_at(p, "name")).unwrap_or("").to_string()))
                })
                .collect();
            for m in value.get("messages").and_then(Value::as_array).into_iter().flatten() {
                let (Some(from), Some(id)) = (str_at(m, "from"), str_at(m, "id")) else {
                    return Err(AppError::validation("messages", "message without from/id"));
                };
                let kind = str_at(m, "type").unwrap_or("unknown");
                let body = match kind {
                    "text" => m.get("text").and_then(|t| str_at(t, "body")).unwrap_or("").to_string(),
                    other => format!("[{other} message — not supported by this channel view]"),
                };
                let name = names.iter().find(|(w, _)| w == from).map(|(_, n)| n.clone()).filter(|n| !n.is_empty());
                out.push(Inbound::Message(CanonicalMessage {
                    channel: Channel::WhatsApp,
                    endpoint_address: phone_number_id.to_string(),
                    customer_address: from.chars().take(32).collect(),
                    customer_name: name.map(|n| n.chars().take(80).collect()),
                    kind: MessageKind::Text,
                    body: crate::modules::m10_hub::domain::validate_body(&body).map_err(|e| AppError::validation("text.body", e.0))?,
                    provider_message_id: Some(id.chars().take(128).collect()),
                    idempotency_key: format!("wa:{}", id.chars().take(128).collect::<String>()),
                }));
            }
            for s in value.get("statuses").and_then(Value::as_array).into_iter().flatten() {
                let (Some(id), Some(st)) = (str_at(s, "id"), str_at(s, "status")) else { continue };
                let status = match st {
                    "sent" => DeliveryStatus::Sent,
                    "delivered" => DeliveryStatus::Delivered,
                    "read" => DeliveryStatus::Read,
                    "failed" => DeliveryStatus::Failed,
                    _ => continue,
                };
                out.push(Inbound::Status(CanonicalStatus {
                    channel: Channel::WhatsApp,
                    provider_message_id: id.chars().take(128).collect(),
                    status,
                    detail: Some("simulated BSP receipt".into()),
                }));
            }
        }
    }
    Ok(out)
}

#[async_trait]
impl ChannelAdapter for SimWhatsAppAdapter {
    fn channel(&self) -> Channel {
        Channel::WhatsApp
    }

    fn simulated(&self) -> bool {
        true
    }

    async fn connect(&self) -> AppResult<()> {
        if self.app_secret.len() < 16 {
            return Err(AppError::validation("HUB_SIM_WHATSAPP_APP_SECRET", "must be at least 16 characters"));
        }
        Ok(())
    }

    fn ingest(&self, signature: Option<&str>, body: &[u8]) -> AppResult<Vec<Inbound>> {
        if !signature_valid(&self.app_secret, body, signature) {
            return Err(AppError::unauthenticated("Invalid X-Hub-Signature-256"));
        }
        parse_webhook(body)
    }

    async fn deliver(&self, job: &OutboundJob) -> Result<String, DeliveryError> {
        if job.body.contains("[fail]") {
            return Err(DeliveryError("SIMULATED BSP error: 503 Service Unavailable (message contains [fail])".into()));
        }
        let wamid = format!("wamid.SIM.{}", Uuid::now_v7().simple());
        for (after, status) in [(self.delivered_after, "delivered"), (self.read_after, "read")] {
            let body = status_payload(&job.endpoint_address, &wamid, &job.customer_address, status).to_string().into_bytes();
            let signature = sign(&self.app_secret, &body);
            self.callbacks
                .schedule(job.tenant_id, SimCallback { channel: Channel::WhatsApp, signature, body }, after)
                .await
                .map_err(|e| DeliveryError(format!("SIMULATED BSP could not schedule receipts: {}", e.message)))?;
        }
        Ok(wamid)
    }

    async fn health(&self) -> ChannelHealth {
        ChannelHealth {
            channel: Channel::WhatsApp,
            simulated: true,
            healthy: true,
            detail: "SIMULATED BSP — no Meta/WhatsApp connection".into(),
        }
    }

    async fn backfill(&self, _since: DateTime<Utc>) -> AppResult<Vec<Inbound>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MemSink(std::sync::Mutex<Vec<(Duration, SimCallback)>>);

    #[async_trait]
    impl SimCallbackSink for MemSink {
        async fn schedule(&self, _tenant: Uuid, callback: SimCallback, after: Duration) -> AppResult<()> {
            self.0.lock().unwrap().push((after, callback));
            Ok(())
        }
    }

    fn adapter() -> (SimWhatsAppAdapter, Arc<MemSink>) {
        let sink = Arc::new(MemSink::default());
        (SimWhatsAppAdapter::new("test-app-secret-0123456789", sink.clone()), sink)
    }

    #[test]
    fn parses_signed_inbound_text() {
        let (a, _) = adapter();
        let body = inbound_payload("PN1", "60123456789", "Aisyah", "Hello there", "wamid.1").to_string();
        let sig = a.sign(body.as_bytes());
        let ev = a.ingest(Some(&sig), body.as_bytes()).unwrap();
        assert_eq!(ev.len(), 1);
        let Inbound::Message(m) = &ev[0] else { panic!("expected message") };
        assert_eq!(m.endpoint_address, "PN1");
        assert_eq!(m.customer_address, "60123456789");
        assert_eq!(m.customer_name.as_deref(), Some("Aisyah"));
        assert_eq!(m.body, "Hello there");
        assert_eq!(m.idempotency_key, "wa:wamid.1");
    }

    #[test]
    fn rejects_bad_signature_and_wrong_object() {
        let (a, _) = adapter();
        let body = inbound_payload("PN1", "6012", "A", "hi", "w1").to_string();
        assert!(a.ingest(Some("sha256=00"), body.as_bytes()).is_err());
        assert!(a.ingest(None, body.as_bytes()).is_err());
        let other = json!({"object": "page"}).to_string();
        assert!(a.ingest(Some(&a.sign(other.as_bytes())), other.as_bytes()).is_err());
    }

    #[test]
    fn non_text_messages_degrade_gracefully_and_statuses_parse() {
        let body = json!({"object":"whatsapp_business_account","entry":[{"changes":[{"value":{
            "metadata":{"phone_number_id":"PN"},
            "messages":[{"from":"6011","id":"w2","type":"image"}],
            "statuses":[{"id":"wamid.X","status":"read"},{"id":"wamid.Y","status":"weird"}]}}]}]})
        .to_string();
        let ev = parse_webhook(body.as_bytes()).unwrap();
        assert_eq!(ev.len(), 2);
        assert!(matches!(&ev[0], Inbound::Message(m) if m.body.starts_with("[image message")));
        assert!(matches!(&ev[1], Inbound::Status(s) if s.status == DeliveryStatus::Read && s.provider_message_id == "wamid.X"));
    }

    #[tokio::test]
    async fn fake_bsp_emits_signed_receipts_and_fails_on_marker() {
        let (a, sink) = adapter();
        let mut job = OutboundJob {
            message_id: Uuid::now_v7(),
            tenant_id: Uuid::now_v7(),
            conversation_id: Uuid::now_v7(),
            seq: 1,
            attempts: 0,
            channel: Channel::WhatsApp,
            endpoint_address: "PN".into(),
            customer_address: "6011".into(),
            body: "hello".into(),
        };
        let wamid = a.deliver(&job).await.unwrap();
        let scheduled = sink.0.lock().unwrap().clone();
        assert_eq!(scheduled.len(), 2);
        assert!(scheduled[0].0 < scheduled[1].0, "delivered is due before read");
        for ((_, cb), expected) in scheduled.iter().zip([DeliveryStatus::Delivered, DeliveryStatus::Read]) {
            let ev = a.ingest(Some(&cb.signature), &cb.body).unwrap();
            assert!(matches!(&ev[0], Inbound::Status(s) if s.status == expected && s.provider_message_id == wamid));
        }
        job.body = "please [fail]".into();
        assert!(a.deliver(&job).await.is_err());
    }
}
