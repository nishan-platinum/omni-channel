//! WhatsApp Cloud API wire format (Meta Graph API), shared by the real adapter
//! (`whatsapp_cloud.rs`) and the fake-meta load-test server (`crate::fake_meta`).
//!
//! * Webhooks: `{"object":"whatsapp_business_account","entry":[{"changes":[{"field":"messages",
//!   "value":{"metadata":{"phone_number_id":..},"contacts":[..],"messages":[..],"statuses":[..]}}]}]}`,
//!   signed with `X-Hub-Signature-256: sha256=<HMAC-SHA256(app secret, raw body)>`.
//! * Send: `POST {base}/{version}/{phone_number_id}/messages` with a bearer token and
//!   `{"messaging_product":"whatsapp","to":..,"type":"text","text":{"body":..}}` →
//!   `{"messages":[{"id":"wamid..."}]}`; errors `{"error":{"code":..,"message":..}}`.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::Inbound;
use super::super::super::domain::{validate_body, CanonicalMessage, CanonicalStatus, Channel, DeliveryStatus, MessageKind};

/// Body of `POST /{version}/{phone_number_id}/messages` (text message).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendRequest {
    pub messaging_product: String,
    #[serde(default)]
    pub recipient_type: Option<String>,
    pub to: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub text: Option<SendText>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendText {
    #[serde(default)]
    pub preview_url: bool,
    pub body: String,
}

impl SendRequest {
    pub fn text(to: &str, body: &str) -> Self {
        Self {
            messaging_product: "whatsapp".into(),
            recipient_type: Some("individual".into()),
            to: to.to_string(),
            kind: "text".into(),
            text: Some(SendText { preview_url: false, body: body.to_string() }),
        }
    }
}

/// Throughput / rate limits (4, 80007, 130429, 131056 or HTTP 429): slow down and try again soon.
pub fn is_throttled(http_status: u16, code: Option<i64>) -> bool {
    http_status == 429 || matches!(code, Some(4 | 80007 | 130429 | 131056))
}

/// Graph API errors worth retrying (throttling or transient platform errors: 131000 something
/// went wrong, 131016 service unavailable, 133004 temporarily unavailable, any 5xx). Everything
/// else (bad recipient, 24-hour window closed, auth, policy, locked account) fails permanently.
pub fn is_retryable(http_status: u16, code: Option<i64>) -> bool {
    is_throttled(http_status, code) || http_status >= 500 || matches!(code, Some(131000 | 131016 | 133004))
}

/// Removes secrets from provider text before it is logged, stored or shown: Meta echoes a
/// malformed access token back inside its error message.
pub fn redact(text: &str, secrets: &[&str]) -> String {
    let mut out = text.to_string();
    for s in secrets.iter().filter(|s| s.len() >= 6) {
        out = out.replace(s, "[redacted]");
    }
    out
}

/// Meta's longer explanation of an error object (`error_data.details`), e.g. *why* a message
/// counts as a re-engagement message — `title`/`message` alone are often too generic to act on.
/// Returned as ` — <details>`, or empty when Meta sent none (or only repeated the title).
pub fn error_details_suffix(error: &Value, title: &str) -> String {
    match str_at(&error["error_data"], "details").map(str::trim) {
        Some(d) if !d.is_empty() && !title.contains(d) => format!(" — {d}"),
        _ => String::new(),
    }
}

/// Error JSON in the Graph API shape.
pub fn error_body(code: i64, title: &str, details: &str) -> Value {
    json!({ "error": { "message": format!("({code}) {title}"), "type": "OAuthException", "code": code,
                       "error_data": { "messaging_product": "whatsapp", "details": details }, "fbtrace_id": "FAKE" } })
}

fn envelope(phone_number_id: &str, display_number: &str, value: Value) -> Value {
    let mut v = value;
    if let Value::Object(o) = &mut v {
        o.insert("messaging_product".into(), json!("whatsapp"));
        o.insert("metadata".into(), json!({ "display_phone_number": display_number, "phone_number_id": phone_number_id }));
    }
    json!({ "object": "whatsapp_business_account", "entry": [{ "id": "WABA", "changes": [{ "field": "messages", "value": v }] }] })
}

/// Inbound text-message webhook, as Meta sends it when a customer writes to the business number.
pub fn inbound_payload(phone_number_id: &str, from: &str, name: &str, text: &str, wamid: &str) -> Value {
    envelope(
        phone_number_id,
        "15550000000",
        json!({
            "contacts": [{ "profile": { "name": name }, "wa_id": from }],
            "messages": [{ "from": from, "id": wamid, "timestamp": Utc::now().timestamp().to_string(), "type": "text", "text": { "body": text } }]
        }),
    )
}

/// Status webhook (`sent` / `delivered` / `read` / `failed`).
pub fn status_payload(phone_number_id: &str, wamid: &str, recipient: &str, status: &str, error: Option<(i64, &str)>) -> Value {
    let mut s = json!({ "id": wamid, "status": status, "timestamp": Utc::now().timestamp().to_string(), "recipient_id": recipient });
    if let (Some((code, title)), Value::Object(o)) = (error, &mut s) {
        o.insert("errors".into(), json!([{ "code": code, "title": title, "message": title }]));
    }
    envelope(phone_number_id, "15550000000", json!({ "statuses": [s] }))
}

fn str_at<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

/// Parses a Meta webhook body into canonical events. Non-text messages degrade to a placeholder
/// text instead of being dropped (OCC-M05-R011). Fields we do not use (e.g. `user_id`,
/// `pricing`, `conversation`) are ignored.
pub fn parse_webhook(body: &[u8]) -> AppResult<Vec<Inbound>> {
    let v: Value = serde_json::from_slice(body).map_err(|e| AppError::validation("body", format!("invalid webhook JSON: {e}")))?;
    if str_at(&v, "object") != Some("whatsapp_business_account") {
        return Err(AppError::validation("object", "not a whatsapp_business_account webhook"));
    }
    let mut out = Vec::new();
    for entry in v.get("entry").and_then(Value::as_array).into_iter().flatten() {
        for change in entry.get("changes").and_then(Value::as_array).into_iter().flatten() {
            if str_at(change, "field").is_some_and(|f| f != "messages") {
                continue; // account_update, templates, … are not conversation traffic
            }
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
                    body: validate_body(&body).map_err(|e| AppError::validation("text.body", e.0))?,
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
                let detail = s
                    .get("errors")
                    .and_then(Value::as_array)
                    .and_then(|e| e.first())
                    .map(|e| {
                        let title = str_at(e, "title").unwrap_or("");
                        let code = e.get("code").map(Value::to_string).unwrap_or_default();
                        format!("{code} {title}{}", error_details_suffix(e, title))
                    })
                    .or_else(|| Some("provider receipt".into()));
                out.push(Inbound::Status(CanonicalStatus {
                    channel: Channel::WhatsApp,
                    provider_message_id: id.chars().take(128).collect(),
                    status,
                    detail: detail.map(|d| d.trim().chars().take(500).collect()),
                }));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inbound_text() {
        let body = inbound_payload("PN1", "60123456789", "Aisyah", "Hello there", "wamid.1").to_string();
        let ev = parse_webhook(body.as_bytes()).unwrap();
        let Inbound::Message(m) = &ev[0] else { panic!("expected message") };
        assert_eq!((m.endpoint_address.as_str(), m.customer_address.as_str()), ("PN1", "60123456789"));
        assert_eq!(m.customer_name.as_deref(), Some("Aisyah"));
        assert_eq!(m.body, "Hello there");
        assert_eq!(m.idempotency_key, "wa:wamid.1");
    }

    #[test]
    fn parses_real_meta_failed_status_with_error() {
        // Shape of a real Cloud API status webhook (account locked), trimmed.
        let body = json!({"object":"whatsapp_business_account","entry":[{"id":"933506166148717","changes":[{"value":{
            "messaging_product":"whatsapp","metadata":{"display_phone_number":"15556334349","phone_number_id":"1279886618550689"},
            "contacts":[{"wa_id":"94700000000","user_id":"LK.1"}],
            "statuses":[{"id":"wamid.X","status":"failed","timestamp":"1791370298","recipient_id":"94700000000",
                "errors":[{"code":131031,"title":"Business Account locked","message":"Business Account locked"}]}]},"field":"messages"}]}]})
        .to_string();
        let ev = parse_webhook(body.as_bytes()).unwrap();
        assert!(
            matches!(&ev[0], Inbound::Status(s) if s.status == DeliveryStatus::Failed && s.detail.as_deref() == Some("131031 Business Account locked"))
        );
    }

    #[test]
    fn failed_status_keeps_metas_details() {
        let body = json!({"object":"whatsapp_business_account","entry":[{"changes":[{"field":"messages","value":{
            "metadata":{"phone_number_id":"PN"},
            "statuses":[{"id":"wamid.Z","status":"failed","recipient_id":"94700000000",
                "errors":[{"code":131047,"title":"Re-engagement message","message":"Re-engagement message",
                    "error_data":{"details":"Message failed to send because more than 24 hours have passed since the customer last replied to this number."}}]}]}}]}]})
        .to_string();
        let ev = parse_webhook(body.as_bytes()).unwrap();
        let Inbound::Status(s) = &ev[0] else { panic!("expected status") };
        assert_eq!(
            s.detail.as_deref(),
            Some("131047 Re-engagement message — Message failed to send because more than 24 hours have passed since the customer last replied to this number.")
        );
    }

    #[test]
    fn non_text_degrades_and_other_fields_are_skipped() {
        let body = json!({"object":"whatsapp_business_account","entry":[
            {"changes":[{"field":"account_update","value":{"event":"VERIFIED_ACCOUNT"}}]},
            {"changes":[{"field":"messages","value":{"metadata":{"phone_number_id":"PN"},
                "messages":[{"from":"6011","id":"w2","type":"image"}],
                "statuses":[{"id":"wamid.X","status":"read"},{"id":"wamid.Y","status":"weird"}]}}]}]})
        .to_string();
        let ev = parse_webhook(body.as_bytes()).unwrap();
        assert_eq!(ev.len(), 2);
        assert!(matches!(&ev[0], Inbound::Message(m) if m.body.starts_with("[image message")));
        assert!(matches!(&ev[1], Inbound::Status(s) if s.status == DeliveryStatus::Read));
    }

    #[test]
    fn retry_classification() {
        assert!(is_retryable(429, None) && is_throttled(429, None));
        assert!(is_throttled(400, Some(130429)));
        assert!(!is_throttled(500, Some(131000)) && is_retryable(500, Some(131000)));
        assert!(is_retryable(503, None));
        assert!(is_retryable(400, Some(130429)));
        assert!(!is_retryable(400, Some(131047)), "24-hour window closed is permanent");
        assert!(!is_retryable(401, Some(190)), "bad token is permanent");
        assert!(!is_retryable(400, Some(131031)), "account locked is permanent");
    }

    #[test]
    fn secrets_are_redacted() {
        assert_eq!(redact("Malformed access token EAAsecret123", &["EAAsecret123"]), "Malformed access token [redacted]");
        assert_eq!(redact("app 1|topsecret failed", &["1|topsecret", "topsecret"]), "app [redacted] failed");
    }

    #[test]
    fn send_request_shape() {
        let v = serde_json::to_value(SendRequest::text("6011", "hi")).unwrap();
        assert_eq!(v["messaging_product"], "whatsapp");
        assert_eq!(v["type"], "text");
        assert_eq!(v["text"]["body"], "hi");
    }
}
