//! Automatic WhatsApp Cloud API setup for `WHATSAPP_PROVIDER=meta` (ADR-0013), so a developer only
//! edits `.env`:
//!
//! 1. **Number check** — `GET /{version}/{phone_number_id}?fields=display_phone_number,verified_name,quality_rating`
//!    with the access token (shows "connected as …" or Meta's exact error, e.g. 190 expired token).
//! 2. **Webhook registration** — `POST /{version}/{app_id}/subscriptions` with
//!    `object=whatsapp_business_account`, the public callback URL, the verify token and
//!    `fields=messages`, using the app access token `{app_id}|{app_secret}`. Meta immediately calls the
//!    hub's GET verification handshake through the public URL.
//! 3. **WABA subscription** — `POST /{version}/{waba_id}/subscribed_apps` with the access token.
//!
//! The public URL comes from `WHATSAPP_PUBLIC_BASE_URL`, else `https://NGROK_DOMAIN`, else the
//! cloudflared quick tunnel's metrics endpoint (`{TUNNEL_METRICS_URL}/quicktunnel`). Steps 2–3 run
//! again whenever that URL changes (a quick tunnel gets a new address on every restart).
//! Nothing here ever stops the hub; failures are shown on the status box with Meta's message.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::platform::config::WhatsAppSettings;

pub const WEBHOOK_PATH: &str = "/v1/hub/channels/whatsapp/webhook";

/// One step's outcome for the status box.
#[derive(Debug, Clone, Serialize)]
pub struct StepStatus {
    pub ok: bool,
    pub detail: String,
    pub at: DateTime<Utc>,
}

impl StepStatus {
    fn ok(detail: impl Into<String>) -> Self {
        Self { ok: true, detail: detail.into(), at: Utc::now() }
    }

    fn fail(detail: impl Into<String>) -> Self {
        Self { ok: false, detail: detail.into(), at: Utc::now() }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MetaStatus {
    /// Public callback URL Meta should call (None = no tunnel/public URL known yet).
    pub callback_url: Option<String>,
    pub number: Option<StepStatus>,
    pub webhook: Option<StepStatus>,
    pub waba: Option<StepStatus>,
    /// Why automatic registration is not possible (missing settings), if so.
    pub manual_reason: Option<String>,
}

impl MetaStatus {
    pub fn all_ok(&self) -> bool {
        [&self.number, &self.webhook, &self.waba].iter().all(|s| s.as_ref().is_some_and(|s| s.ok))
    }
}

pub struct MetaLink {
    settings: WhatsAppSettings,
    http: reqwest::Client,
    status: RwLock<MetaStatus>,
    progress: tokio::sync::Mutex<Progress>,
}

/// Loop bookkeeping: which callback URL is registered, when to retry, last number check.
struct Progress {
    registered_for: Option<String>,
    next_attempt: std::time::Instant,
    last_number_check: Option<std::time::Instant>,
}

/// Graph API error text: `(code) message` from `{"error":{...}}`, or the HTTP status.
fn graph_error(status: u16, body: &Value) -> String {
    match (body["error"]["code"].as_i64(), body["error"]["message"].as_str()) {
        (Some(c), Some(m)) => format!("Meta error {c}: {m}"),
        _ => format!("Meta answered HTTP {status}"),
    }
}

impl MetaLink {
    pub fn new(settings: WhatsAppSettings) -> Arc<Self> {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("HTTP client");
        let manual_reason = match (&settings.app_id, &settings.business_account_id) {
            (Some(_), Some(_)) => None,
            _ => Some("Add WHATSAPP_APP_ID and WHATSAPP_BUSINESS_ACCOUNT_ID to .env for automatic webhook setup, or set the webhook in Meta manually.".into()),
        };
        Arc::new(Self {
            settings,
            http,
            status: RwLock::new(MetaStatus { manual_reason, ..Default::default() }),
            progress: tokio::sync::Mutex::new(Progress {
                registered_for: None,
                next_attempt: std::time::Instant::now(),
                last_number_check: None,
            }),
        })
    }

    pub fn status(&self) -> MetaStatus {
        self.status.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn update(&self, f: impl FnOnce(&mut MetaStatus)) {
        f(&mut self.status.write().unwrap_or_else(|e| e.into_inner()));
    }

    /// Masks the access token, app secret and app access token in Meta's messages.
    fn scrub(&self, mut s: StepStatus) -> StepStatus {
        let app_token = self.settings.app_id.as_ref().map(|a| format!("{a}|{}", self.settings.app_secret)).unwrap_or_default();
        s.detail = super::whatsapp::redact(&s.detail, &[&app_token, &self.settings.access_token, &self.settings.app_secret]);
        s
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}/{}", self.settings.base_url.trim_end_matches('/'), self.settings.api_version, path.trim_start_matches('/'))
    }

    /// Current public base URL (explicit / ngrok domain / cloudflared quick tunnel).
    async fn public_base(&self) -> Option<String> {
        if let Some(u) = &self.settings.public_base_url {
            return Some(u.trim_end_matches('/').to_string());
        }
        let metrics = self.settings.tunnel_metrics_url.as_deref()?;
        let v: Value = self.http.get(format!("{}/quicktunnel", metrics.trim_end_matches('/'))).send().await.ok()?.json().await.ok()?;
        let host = v["hostname"].as_str().filter(|h| !h.is_empty())?;
        Some(format!("https://{host}"))
    }

    async fn check_number(&self) -> StepStatus {
        let Some(id) = &self.settings.phone_number_id else { return StepStatus::fail("WHATSAPP_PHONE_NUMBER_ID is not set") };
        let r = self
            .http
            .get(self.url(id))
            .query(&[("fields", "display_phone_number,verified_name,quality_rating")])
            .bearer_auth(&self.settings.access_token)
            .send()
            .await;
        match r {
            Err(e) => StepStatus::fail(format!("Meta not reachable: {}", e.without_url())),
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                if (200..300).contains(&status) {
                    StepStatus::ok(format!(
                        "Token OK — number {} ({}), quality {}",
                        body["display_phone_number"].as_str().unwrap_or("?"),
                        body["verified_name"].as_str().unwrap_or("?"),
                        body["quality_rating"].as_str().unwrap_or("?")
                    ))
                } else {
                    StepStatus::fail(graph_error(status, &body))
                }
            }
        }
    }

    async fn register_webhook(&self, callback: &str) -> StepStatus {
        let (Some(app_id), Some(_)) = (&self.settings.app_id, &self.settings.business_account_id) else {
            return StepStatus::fail("automatic setup needs WHATSAPP_APP_ID and WHATSAPP_BUSINESS_ACCOUNT_ID");
        };
        let app_token = format!("{app_id}|{}", self.settings.app_secret);
        let r = self
            .http
            .post(self.url(&format!("{app_id}/subscriptions")))
            .query(&[
                ("object", "whatsapp_business_account"),
                ("callback_url", callback),
                ("verify_token", self.settings.verify_token.as_str()),
                ("fields", "messages"),
                ("access_token", app_token.as_str()),
            ])
            .send()
            .await;
        match r {
            Err(e) => StepStatus::fail(format!("Meta not reachable: {}", e.without_url())),
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                if (200..300).contains(&status) && body["success"].as_bool().unwrap_or(false) {
                    StepStatus::ok(format!("Webhook registered: {callback} (field: messages)"))
                } else {
                    StepStatus::fail(graph_error(status, &body))
                }
            }
        }
    }

    async fn subscribe_waba(&self) -> StepStatus {
        let Some(waba) = &self.settings.business_account_id else { return StepStatus::fail("WHATSAPP_BUSINESS_ACCOUNT_ID is not set") };
        match self.http.post(self.url(&format!("{waba}/subscribed_apps"))).bearer_auth(&self.settings.access_token).send().await {
            Err(e) => StepStatus::fail(format!("Meta not reachable: {}", e.without_url())),
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                if (200..300).contains(&status) {
                    StepStatus::ok("App subscribed to the WhatsApp Business Account")
                } else {
                    StepStatus::fail(graph_error(status, &body))
                }
            }
        }
    }

    /// One pass: number check (every 10 minutes, or when `force`), then webhook + WABA
    /// registration if the public URL appeared or changed (retry 60 s after a failure, or at once
    /// when `force`).
    pub async fn sync(&self, force: bool) {
        let mut p = self.progress.lock().await;
        if force || p.last_number_check.is_none_or(|t| t.elapsed() >= Duration::from_secs(600)) {
            let s = self.scrub(self.check_number().await);
            log_step("number check", &s);
            self.update(|st| st.number = Some(s));
            p.last_number_check = Some(std::time::Instant::now());
        }
        let callback = self.public_base().await.map(|b| format!("{b}{WEBHOOK_PATH}"));
        self.update(|st| st.callback_url = callback.clone());
        let can_register = self.settings.app_id.is_some() && self.settings.business_account_id.is_some();
        let Some(cb) = callback else { return };
        let due = force || (p.registered_for.as_deref() != Some(cb.as_str()) && std::time::Instant::now() >= p.next_attempt);
        if !can_register || !due {
            return;
        }
        let w = self.scrub(self.register_webhook(&cb).await);
        log_step("webhook registration", &w);
        let ok = w.ok;
        self.update(|st| st.webhook = Some(w));
        if ok {
            let a = self.scrub(self.subscribe_waba().await);
            log_step("WABA subscription", &a);
            self.update(|st| st.waba = Some(a));
            p.registered_for = Some(cb);
        } else {
            p.next_attempt = std::time::Instant::now() + Duration::from_secs(60);
        }
    }

    /// Background loop (every 5 s). Starts after a short delay so the hub is already listening
    /// when Meta calls the verification handshake.
    pub fn spawn(self: Arc<Self>) {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            loop {
                self.sync(false).await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }
}

fn log_step(step: &str, s: &StepStatus) {
    if s.ok {
        tracing::info!(step, detail = %s.detail, "WhatsApp (Meta) setup");
    } else {
        tracing::warn!(step, detail = %s.detail, "WhatsApp (Meta) setup failed");
    }
}
