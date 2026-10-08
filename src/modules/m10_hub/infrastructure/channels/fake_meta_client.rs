//! Client for the fake-meta control API (`/_fake/*`), used by the simulator console and the
//! development tooling. Only wired when `WHATSAPP_PROVIDER=fake` (ADR-0013).

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::ProviderSimulator;

pub struct FakeMetaClient {
    base_url: String,
    token: String,
    http: reqwest::Client,
}

impl FakeMetaClient {
    pub fn new(base_url: &str, token: &str) -> AppResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| AppError::internal(anyhow::anyhow!("HTTP client: {e}")))?;
        Ok(Self { base_url: base_url.trim_end_matches('/').to_string(), token: token.to_string(), http })
    }

    async fn call(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> AppResult<Value> {
        let mut req = self.http.request(method, format!("{}{path}", self.base_url)).bearer_auth(&self.token);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| AppError::internal(anyhow::anyhow!("fake-meta unreachable at {}: {}", self.base_url, e.without_url())))?;
        let status = resp.status();
        let v: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(AppError::validation("fake-meta", format!("fake-meta answered HTTP {status}: {v}")));
        }
        Ok(v)
    }
}

#[async_trait]
impl ProviderSimulator for FakeMetaClient {
    fn label(&self) -> String {
        format!("fake-meta at {}", self.base_url)
    }

    async fn customer_message(&self, phone_number_id: &str, from: &str, name: &str, text: &str) -> AppResult<String> {
        let v = self
            .call(
                reqwest::Method::POST,
                "/_fake/inbound",
                Some(json!({ "phone_number_id": phone_number_id, "from": from, "name": name, "text": text })),
            )
            .await?;
        Ok(v["wamid"].as_str().unwrap_or_default().to_string())
    }

    async fn start_load(&self, phone_number_id: &str, customers: u64, messages_per_customer: u64, rate_per_sec: u64) -> AppResult<Value> {
        self.call(
            reqwest::Method::POST,
            "/_fake/load",
            Some(json!({ "phone_number_id": phone_number_id, "customers": customers, "messages_per_customer": messages_per_customer, "rate_per_sec": rate_per_sec })),
        )
        .await
    }

    async fn stats(&self) -> AppResult<Value> {
        self.call(reqwest::Method::GET, "/_fake/stats", None).await
    }

    async fn reset(&self) -> AppResult<()> {
        self.call(reqwest::Method::POST, "/_fake/reset", None).await.map(|_| ())
    }

    async fn outbox(&self, phone_number_id: Option<&str>, limit: usize) -> AppResult<Value> {
        let q = match phone_number_id {
            Some(p) => format!(
                "/_fake/outbox?limit={limit}&phone_number_id={}",
                url::form_urlencoded::byte_serialize(p.as_bytes()).collect::<String>()
            ),
            None => format!("/_fake/outbox?limit={limit}"),
        };
        self.call(reqwest::Method::GET, &q, None).await
    }
}
