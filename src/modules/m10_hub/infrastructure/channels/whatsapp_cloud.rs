//! WhatsApp Cloud API adapter (OCC-M05-R001; ADR-0013).
//!
//! The same code talks to **real Meta** (`https://graph.facebook.com`, sandbox test number or a
//! production number) or to the **fake-meta** load-test server (`crate::fake_meta`), which
//! implements the same HTTP API and webhooks. Only the configuration differs:
//! `WHATSAPP_GRAPH_BASE_URL`, `WHATSAPP_ACCESS_TOKEN`, `WHATSAPP_APP_SECRET`.
//!
//! * ingest: verifies `X-Hub-Signature-256` with the app secret, parses the webhook.
//! * deliver: `POST {base}/{version}/{phone_number_id}/messages` with the bearer token; the
//!   endpoint address of the conversation IS the phone number id. Throttling and transient
//!   platform errors are retried (ladder), everything else fails permanently (e.g. 131047 —
//!   24-hour customer-service window closed, 131031 — account locked, 190 — bad token).
//! * The access token is never logged or shown.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::{ChannelAdapter, ChannelHealth, DeliveryError, Inbound, OutboundJob};
use super::super::super::domain::Channel;
use super::signature_valid;
use super::whatsapp::{is_retryable, is_throttled, parse_webhook, SendRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudProvider {
    /// graph.facebook.com — real WhatsApp (sandbox test number or production).
    Meta,
    /// The fake-meta server (`src/fake_meta`): same API, for development and load tests.
    FakeMeta,
}

impl CloudProvider {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "meta" | "cloud" | "sandbox" => Some(Self::Meta),
            "fake" | "fake-meta" | "fake_meta" => Some(Self::FakeMeta),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::FakeMeta => "fake-meta",
        }
    }
}

#[derive(Clone)]
pub struct WhatsAppCloudConfig {
    pub provider: CloudProvider,
    pub base_url: String,
    pub api_version: String,
    pub access_token: String,
    pub app_secret: String,
}

pub struct WhatsAppCloudAdapter {
    cfg: WhatsAppCloudConfig,
    http: reqwest::Client,
}

impl WhatsAppCloudAdapter {
    pub fn new(cfg: WhatsAppCloudConfig) -> AppResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(64)
            .user_agent("omni-m01-hub/0.1")
            .build()
            .map_err(|e| AppError::internal(anyhow::anyhow!("HTTP client: {e}")))?;
        Ok(Self { cfg, http })
    }

    fn messages_url(&self, phone_number_id: &str) -> String {
        format!("{}/{}/{}/messages", self.cfg.base_url.trim_end_matches('/'), self.cfg.api_version, phone_number_id)
    }

    pub fn provider(&self) -> CloudProvider {
        self.cfg.provider
    }
}

#[async_trait]
impl ChannelAdapter for WhatsAppCloudAdapter {
    fn channel(&self) -> Channel {
        Channel::WhatsApp
    }

    fn simulated(&self) -> bool {
        self.cfg.provider == CloudProvider::FakeMeta
    }

    async fn connect(&self) -> AppResult<()> {
        url::Url::parse(&self.cfg.base_url).map_err(|_| AppError::validation("WHATSAPP_GRAPH_BASE_URL", "must be an absolute URL"))?;
        if self.cfg.access_token.trim().len() < 8 {
            return Err(AppError::validation("WHATSAPP_ACCESS_TOKEN", "is required"));
        }
        if self.cfg.app_secret.trim().len() < 8 {
            return Err(AppError::validation("WHATSAPP_APP_SECRET", "is required (webhook signature verification)"));
        }
        let local = url::Url::parse(&self.cfg.base_url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h == "localhost" || h == "127.0.0.1" || h == "fake-meta"))
            .unwrap_or(false);
        if self.cfg.provider == CloudProvider::Meta && !self.cfg.base_url.starts_with("https://") && !local {
            return Err(AppError::validation("WHATSAPP_GRAPH_BASE_URL", "real Meta must be reached over https"));
        }
        Ok(())
    }

    fn ingest(&self, signature: Option<&str>, body: &[u8]) -> AppResult<Vec<Inbound>> {
        if !signature_valid(self.cfg.app_secret.as_bytes(), body, signature) {
            return Err(AppError::unauthenticated("Invalid X-Hub-Signature-256"));
        }
        parse_webhook(body)
    }

    async fn deliver(&self, job: &OutboundJob) -> Result<String, DeliveryError> {
        let resp = self
            .http
            .post(self.messages_url(&job.endpoint_address))
            .bearer_auth(&self.cfg.access_token)
            .json(&SendRequest::text(&job.customer_address, &job.body))
            .send()
            .await
            .map_err(|e| DeliveryError::retryable(format!("WhatsApp Cloud API unreachable: {}", e.without_url())))?;
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if (200..300).contains(&status) {
            return body["messages"][0]["id"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| DeliveryError::retryable("WhatsApp Cloud API returned no message id".into()));
        }
        let error = &body["error"];
        let code = error["code"].as_i64();
        let message = error["message"].as_str().unwrap_or("unknown error");
        // Meta's `fbtrace_id` identifies the failed call when asking Meta support about it.
        let trace = error["fbtrace_id"].as_str().map(|t| format!(" (fbtrace_id {t})")).unwrap_or_default();
        let text = super::whatsapp::redact(
            &format!(
                "WhatsApp Cloud API HTTP {status}: ({}) {message}{}{trace}",
                code.unwrap_or(0),
                super::whatsapp::error_details_suffix(error, message)
            ),
            &[&self.cfg.access_token, &self.cfg.app_secret],
        );
        Err(if is_throttled(status, code) {
            DeliveryError::throttled(text)
        } else if is_retryable(status, code) {
            DeliveryError::retryable(text)
        } else {
            DeliveryError::permanent(text)
        })
    }

    async fn health(&self) -> ChannelHealth {
        let detail = match self.cfg.provider {
            CloudProvider::Meta => format!("Meta WhatsApp Cloud API {} ({})", self.cfg.api_version, self.cfg.base_url),
            CloudProvider::FakeMeta => {
                format!("fake-meta server at {} (same API as Meta; for development and load tests)", self.cfg.base_url)
            }
        };
        ChannelHealth { channel: Channel::WhatsApp, simulated: self.simulated(), healthy: true, detail }
    }

    async fn backfill(&self, _since: DateTime<Utc>) -> AppResult<Vec<Inbound>> {
        // The Cloud API has no message history endpoint; Meta re-delivers unacknowledged
        // webhooks for up to 7 days instead.
        Ok(Vec::new())
    }
}
