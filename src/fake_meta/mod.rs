//! **fake-meta** — a stand-alone imitation of Meta's WhatsApp Cloud API for development and load
//! testing (ADR-0013). It is NOT WhatsApp: no message ever leaves this process.
//!
//! What it imitates (so the hub runs its *real* `WhatsAppCloudAdapter` against it):
//! * `POST /{version}/{phone_number_id}/messages` — bearer token, Graph-style JSON errors,
//!   network latency, per-number throughput limit (HTTP 429 / code 130429), injected platform
//!   errors (HTTP 500 / 131000), the 24-hour customer-service window (131047), `[fail]` in a body →
//!   131026 undeliverable, `[retry]` → 131000 transient.
//! * Webhooks to the hub (`FAKE_META_WEBHOOK_URL`) signed with `X-Hub-Signature-256`: inbound
//!   customer messages and `sent` / `delivered` / `read` statuses (with optional duplicates and
//!   out-of-order delivery), retried with backoff until the hub answers 2xx — like Meta.
//! * Bulk traffic: `POST /_fake/load` makes N simulated customers send M messages at a given
//!   rate; `GET /_fake/stats` reports webhook latency (hub ack), throttling, errors and the round
//!   trip customer → agent reply.
//!
//! Control endpoints (`/_fake/*`) need the same bearer token as the Graph API.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::modules::m10_hub::infrastructure::channels::sign;
use crate::modules::m10_hub::infrastructure::channels::whatsapp::{error_body, inbound_payload, status_payload, SendRequest};
use crate::platform::security::constant_time_eq;

#[derive(Debug, Clone)]
pub struct FakeMetaConfig {
    pub access_token: String,
    pub app_secret: String,
    /// The hub's webhook, e.g. http://app:3000/v1/hub/channels/whatsapp/webhook.
    pub webhook_url: String,
    /// Simulated Graph API latency, uniformly in [min, max] ms.
    pub latency_ms: (u64, u64),
    /// Messages per second accepted per phone number id (Meta's default tier is 80/s).
    pub rate_per_sec: u32,
    /// Fraction of sends answered with a transient 500 / 131000.
    pub error_rate: f64,
    /// Fraction of status webhooks delivered twice.
    pub duplicate_rate: f64,
    pub delivered_after_ms: u64,
    pub read_after_ms: u64,
    /// Fraction of delivered messages that are also read.
    pub read_ratio: f64,
    /// Enforce the 24-hour customer-service window (free text only after an inbound message).
    pub enforce_window: bool,
    pub webhook_workers: usize,
}

impl Default for FakeMetaConfig {
    fn default() -> Self {
        Self {
            access_token: "fake-meta-dev-token".into(),
            app_secret: "fake-meta-dev-app-secret".into(),
            webhook_url: "http://localhost:3000/v1/hub/channels/whatsapp/webhook".into(),
            latency_ms: (20, 80),
            rate_per_sec: 80,
            error_rate: 0.0,
            duplicate_rate: 0.0,
            delivered_after_ms: 300,
            read_after_ms: 1000,
            read_ratio: 1.0,
            enforce_window: true,
            webhook_workers: 128,
        }
    }
}

impl FakeMetaConfig {
    /// From `FAKE_META_*` environment variables (defaults above).
    pub fn from_env() -> Self {
        let d = Self::default();
        let num = |k: &str, def: f64| std::env::var(k).ok().and_then(|v| v.parse::<f64>().ok()).unwrap_or(def);
        let s = |k: &str, def: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| def.to_string());
        Self {
            access_token: s("FAKE_META_ACCESS_TOKEN", &d.access_token),
            app_secret: s("FAKE_META_APP_SECRET", &d.app_secret),
            webhook_url: s("FAKE_META_WEBHOOK_URL", &d.webhook_url),
            latency_ms: (num("FAKE_META_LATENCY_MIN_MS", 20.0) as u64, num("FAKE_META_LATENCY_MAX_MS", 80.0) as u64),
            rate_per_sec: num("FAKE_META_RATE_PER_SEC", 80.0) as u32,
            error_rate: num("FAKE_META_ERROR_RATE", 0.0),
            duplicate_rate: num("FAKE_META_DUPLICATE_RATE", 0.0),
            delivered_after_ms: num("FAKE_META_DELIVERED_AFTER_MS", 300.0) as u64,
            read_after_ms: num("FAKE_META_READ_AFTER_MS", 1000.0) as u64,
            read_ratio: num("FAKE_META_READ_RATIO", 1.0),
            enforce_window: s("FAKE_META_ENFORCE_WINDOW", "true") != "false",
            webhook_workers: num("FAKE_META_WEBHOOK_WORKERS", 128.0) as usize,
        }
    }
}

/// Latency samples (ms), capped so a long run cannot grow memory without bound.
#[derive(Default)]
struct Samples(Mutex<Vec<f32>>);

impl Samples {
    fn push(&self, ms: f64) {
        let mut v = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if v.len() < 2_000_000 {
            v.push(ms as f32);
        }
    }

    fn summary(&self) -> Value {
        let mut v = self.0.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if v.is_empty() {
            return json!({ "count": 0 });
        }
        v.sort_by(f32::total_cmp);
        let p = |q: f64| v[((q * v.len() as f64) as usize).min(v.len() - 1)];
        json!({ "count": v.len(), "p50_ms": p(0.5), "p95_ms": p(0.95), "p99_ms": p(0.99), "max_ms": v[v.len() - 1] })
    }

    fn clear(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

#[derive(Default)]
struct Stats {
    inbound_generated: AtomicU64,
    webhooks_ok: AtomicU64,
    webhook_retries: AtomicU64,
    webhooks_given_up: AtomicU64,
    statuses_queued: AtomicU64,
    outbound_accepted: AtomicU64,
    outbound_throttled: AtomicU64,
    outbound_injected_errors: AtomicU64,
    outbound_window_closed: AtomicU64,
    outbound_undeliverable: AtomicU64,
    outbound_unauthorized: AtomicU64,
    /// Customer message created → the hub acknowledged its webhook (2xx).
    inbound_ack: Samples,
    /// Customer message created → an agent's reply quoting it reached this server.
    round_trip: Samples,
}

struct Webhook {
    body: Vec<u8>,
    created: Instant,
    /// For inbound messages: record hub-ack latency.
    inbound: bool,
}

#[derive(Clone, serde::Serialize)]
struct SentMessage {
    to: String,
    phone_number_id: String,
    text: String,
    wamid: String,
    at: chrono::DateTime<chrono::Utc>,
}

pub struct FakeMeta {
    cfg: FakeMetaConfig,
    started: Instant,
    stats: Stats,
    /// Last inbound per (phone_number_id, customer) → the 24-hour window.
    windows: Mutex<HashMap<(String, String), Instant>>,
    /// Per phone number id: (second, count) for the throughput limit.
    rate: Mutex<HashMap<String, (u64, u32)>>,
    /// Inbound message creation time by text marker (round trip measurement).
    origins: Mutex<HashMap<String, Instant>>,
    outbox: Mutex<VecDeque<SentMessage>>,
    webhooks: mpsc::Sender<Webhook>,
    runs: Mutex<Vec<Value>>,
    /// Where webhooks go: FAKE_META_WEBHOOK_URL until an app registers its callback URL through
    /// `POST /{version}/{app_id}/subscriptions` (like Meta's App Dashboard / subscriptions API).
    webhook_target: Mutex<String>,
    http: reqwest::Client,
}

impl FakeMeta {
    /// Creates the server state and starts its webhook delivery workers.
    pub fn start(cfg: FakeMetaConfig) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<Webhook>(500_000);
        let cfg_webhook = cfg.webhook_url.clone();
        let me = Arc::new(Self {
            cfg,
            started: Instant::now(),
            stats: Stats::default(),
            windows: Mutex::new(HashMap::new()),
            rate: Mutex::new(HashMap::new()),
            origins: Mutex::new(HashMap::new()),
            outbox: Mutex::new(VecDeque::new()),
            webhooks: tx,
            runs: Mutex::new(Vec::new()),
            webhook_target: Mutex::new(cfg_webhook.clone()),
            http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("HTTP client"),
        });
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let http = reqwest::Client::builder().timeout(Duration::from_secs(10)).pool_max_idle_per_host(256).build().expect("HTTP client");
        for _ in 0..me.cfg.webhook_workers.max(1) {
            let (me, rx, http) = (me.clone(), rx.clone(), http.clone());
            tokio::spawn(async move {
                loop {
                    let next = { rx.lock().await.recv().await };
                    let Some(w) = next else { return };
                    me.deliver_webhook(&http, w).await;
                }
            });
        }
        me
    }

    async fn deliver_webhook(&self, http: &reqwest::Client, w: Webhook) {
        let signature = sign(self.cfg.app_secret.as_bytes(), &w.body);
        // Meta retries failed webhooks with backoff (for days); we retry 6 times.
        for attempt in 0..6u32 {
            if attempt > 0 {
                self.stats.webhook_retries.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(250 * 2u64.pow(attempt))).await;
            }
            let target = self.webhook_target.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let r = http
                .post(&target)
                .header("content-type", "application/json")
                .header("x-hub-signature-256", &signature)
                .body(w.body.clone())
                .send()
                .await;
            if let Ok(resp) = r {
                if resp.status().is_success() {
                    self.stats.webhooks_ok.fetch_add(1, Ordering::Relaxed);
                    if w.inbound {
                        self.stats.inbound_ack.push(w.created.elapsed().as_secs_f64() * 1000.0);
                    }
                    return;
                }
            }
        }
        self.stats.webhooks_given_up.fetch_add(1, Ordering::Relaxed);
    }

    async fn enqueue(&self, body: Value, inbound: bool) {
        let _ = self.webhooks.send(Webhook { body: body.to_string().into_bytes(), created: Instant::now(), inbound }).await;
    }

    /// A customer sends a text message to `phone_number_id`.
    pub async fn customer_message(&self, phone_number_id: &str, from: &str, name: &str, text: &str) -> String {
        let wamid = format!("wamid.FAKEIN.{}", Uuid::now_v7().simple());
        let now = Instant::now();
        self.windows.lock().unwrap_or_else(|e| e.into_inner()).insert((phone_number_id.to_string(), from.to_string()), now);
        if let Some(marker) = marker_of(text) {
            let mut o = self.origins.lock().unwrap_or_else(|e| e.into_inner());
            if o.len() < 2_000_000 {
                o.insert(marker, now);
            }
        }
        self.stats.inbound_generated.fetch_add(1, Ordering::Relaxed);
        self.enqueue(inbound_payload(phone_number_id, from, name, text, &wamid), true).await;
        wamid
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|t| constant_time_eq(t.trim().as_bytes(), self.cfg.access_token.as_bytes()))
    }

    /// Throughput limit per phone number id (per wall-clock second).
    fn take_rate(&self, phone_number_id: &str) -> bool {
        let sec = self.started.elapsed().as_secs();
        let mut m = self.rate.lock().unwrap_or_else(|e| e.into_inner());
        let e = m.entry(phone_number_id.to_string()).or_insert((sec, 0));
        if e.0 != sec {
            *e = (sec, 0);
        }
        e.1 += 1;
        e.1 <= self.cfg.rate_per_sec
    }

    fn window_open(&self, phone_number_id: &str, to: &str) -> bool {
        if !self.cfg.enforce_window {
            return true;
        }
        self.windows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(phone_number_id.to_string(), to.to_string()))
            .is_some_and(|t| t.elapsed() < Duration::from_secs(24 * 3600))
    }

    fn schedule_statuses(self: &Arc<Self>, phone_number_id: String, wamid: String, to: String) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut plan = vec![(50u64, "sent"), (me.cfg.delivered_after_ms, "delivered")];
            if rand::random::<f64>() < me.cfg.read_ratio {
                plan.push((me.cfg.read_after_ms, "read"));
            }
            let mut elapsed = 0;
            for (at, status) in plan {
                tokio::time::sleep(Duration::from_millis(at.saturating_sub(elapsed))).await;
                elapsed = at;
                let body = status_payload(&phone_number_id, &wamid, &to, status, None);
                me.stats.statuses_queued.fetch_add(1, Ordering::Relaxed);
                if rand::random::<f64>() < me.cfg.duplicate_rate {
                    me.enqueue(body.clone(), false).await;
                }
                me.enqueue(body, false).await;
            }
        });
    }
}

/// Load-test markers: an inbound text starting `t=<id> ` is answered by agents with `rt=<id> `.
fn marker_of(text: &str) -> Option<String> {
    let rest = text.strip_prefix("t=")?;
    Some(rest.split_whitespace().next()?.to_string())
}

fn reply_marker(text: &str) -> Option<String> {
    let rest = text.strip_prefix("rt=")?;
    Some(rest.split_whitespace().next()?.to_string())
}

pub fn router(me: Arc<FakeMeta>) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({ "status": "ok", "service": "fake-meta" })) }))
        .route("/{version}/{phone_number_id}/messages", post(send_message))
        .route("/{version}/{id}", get(phone_number_info))
        .route("/{version}/{app_id}/subscriptions", post(app_subscriptions))
        .route("/{version}/{waba_id}/subscribed_apps", post(subscribed_apps))
        .route("/_fake/inbound", post(control_inbound))
        .route("/_fake/load", post(control_load))
        .route("/_fake/stats", get(control_stats))
        .route("/_fake/reset", post(control_reset))
        .route("/_fake/outbox", get(control_outbox))
        .with_state(me)
}

fn graph_error(status: StatusCode, code: i64, title: &str, details: &str) -> Response {
    (status, Json(error_body(code, title, details))).into_response()
}

async fn send_message(
    State(me): State<Arc<FakeMeta>>,
    Path((_version, phone_number_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !me.authorized(&headers) {
        me.stats.outbound_unauthorized.fetch_add(1, Ordering::Relaxed);
        return graph_error(StatusCode::UNAUTHORIZED, 190, "Invalid OAuth access token", "Malformed access token");
    }
    let (lo, hi) = me.cfg.latency_ms;
    if hi > 0 {
        tokio::time::sleep(Duration::from_millis(lo + rand::random::<u64>() % (hi.saturating_sub(lo) + 1))).await;
    }
    let req: SendRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return graph_error(StatusCode::BAD_REQUEST, 100, "Invalid parameter", &e.to_string()),
    };
    let text = match (&req.messaging_product[..], &req.kind[..], &req.text) {
        ("whatsapp", "text", Some(t)) if !t.body.is_empty() => t.body.clone(),
        _ => return graph_error(StatusCode::BAD_REQUEST, 100, "Invalid parameter", "only text messages are supported by fake-meta"),
    };
    if !me.take_rate(&phone_number_id) {
        me.stats.outbound_throttled.fetch_add(1, Ordering::Relaxed);
        return graph_error(StatusCode::TOO_MANY_REQUESTS, 130429, "Rate limit hit", "Cloud API message throughput has been reached.");
    }
    if text.contains("[retry]") || rand::random::<f64>() < me.cfg.error_rate {
        me.stats.outbound_injected_errors.fetch_add(1, Ordering::Relaxed);
        return graph_error(StatusCode::INTERNAL_SERVER_ERROR, 131000, "Something went wrong", "Injected transient error");
    }
    if text.contains("[fail]") {
        me.stats.outbound_undeliverable.fetch_add(1, Ordering::Relaxed);
        return graph_error(StatusCode::BAD_REQUEST, 131026, "Message undeliverable", "Injected permanent failure ([fail])");
    }
    if !me.window_open(&phone_number_id, &req.to) {
        me.stats.outbound_window_closed.fetch_add(1, Ordering::Relaxed);
        return graph_error(
            StatusCode::BAD_REQUEST,
            131047,
            "Re-engagement message",
            "More than 24 hours have passed since the recipient last replied; use a template.",
        );
    }
    let wamid = format!("wamid.FAKE.{}", Uuid::now_v7().simple());
    me.stats.outbound_accepted.fetch_add(1, Ordering::Relaxed);
    if let Some(m) = reply_marker(&text) {
        if let Some(t0) = me.origins.lock().unwrap_or_else(|e| e.into_inner()).remove(&m) {
            me.stats.round_trip.push(t0.elapsed().as_secs_f64() * 1000.0);
        }
    }
    {
        let mut o = me.outbox.lock().unwrap_or_else(|e| e.into_inner());
        o.push_front(SentMessage {
            to: req.to.clone(),
            phone_number_id: phone_number_id.clone(),
            text,
            wamid: wamid.clone(),
            at: chrono::Utc::now(),
        });
        o.truncate(500);
    }
    me.schedule_statuses(phone_number_id, wamid.clone(), req.to.clone());
    Json(json!({ "messaging_product": "whatsapp", "contacts": [{ "input": req.to, "wa_id": req.to }], "messages": [{ "id": wamid }] }))
        .into_response()
}

/// `GET /{version}/{phone_number_id}?fields=…` — the phone number object (token check).
async fn phone_number_info(State(me): State<Arc<FakeMeta>>, Path((_version, id)): Path<(String, String)>, headers: HeaderMap) -> Response {
    if !me.authorized(&headers) {
        return graph_error(StatusCode::UNAUTHORIZED, 190, "Invalid OAuth access token", "Malformed access token");
    }
    Json(
        json!({ "id": id, "display_phone_number": "+1 555-000-0000", "verified_name": "fake-meta test number", "quality_rating": "GREEN" }),
    )
    .into_response()
}

#[derive(Deserialize)]
struct SubscriptionQuery {
    object: Option<String>,
    callback_url: Option<String>,
    verify_token: Option<String>,
    fields: Option<String>,
    access_token: Option<String>,
}

/// `POST /{version}/{app_id}/subscriptions` — registers the app's webhook. Like Meta: the app
/// access token is `{app_id}|{app_secret}`, and the callback must answer the GET verification
/// handshake (`hub.mode=subscribe&hub.verify_token=…&hub.challenge=…`) before it is accepted.
async fn app_subscriptions(
    State(me): State<Arc<FakeMeta>>,
    Path((_version, app_id)): Path<(String, String)>,
    Query(q): Query<SubscriptionQuery>,
) -> Response {
    let expected = format!("{app_id}|{}", me.cfg.app_secret);
    if !q.access_token.as_deref().is_some_and(|t| constant_time_eq(t.as_bytes(), expected.as_bytes())) {
        return graph_error(StatusCode::BAD_REQUEST, 190, "Invalid OAuth access token", "App access token {app_id}|{app_secret} required");
    }
    if q.object.as_deref() != Some("whatsapp_business_account")
        || !q.fields.as_deref().unwrap_or("").split(',').any(|f| f.trim() == "messages")
    {
        return graph_error(
            StatusCode::BAD_REQUEST,
            100,
            "Invalid parameter",
            "object=whatsapp_business_account and fields=messages are required",
        );
    }
    let (Some(callback), Some(token)) = (q.callback_url, q.verify_token) else {
        return graph_error(StatusCode::BAD_REQUEST, 100, "Invalid parameter", "callback_url and verify_token are required");
    };
    let challenge = format!("{}", rand::random::<u32>());
    let ok = match me
        .http
        .get(&callback)
        .query(&[("hub.mode", "subscribe"), ("hub.verify_token", token.as_str()), ("hub.challenge", challenge.as_str())])
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r.text().await.map(|b| b.trim() == challenge).unwrap_or(false),
        _ => false,
    };
    if !ok {
        return graph_error(
            StatusCode::BAD_REQUEST,
            2200,
            "Callback verification failed",
            "The URL couldn't be validated: response did not match the challenge",
        );
    }
    *me.webhook_target.lock().unwrap_or_else(|e| e.into_inner()) = callback;
    Json(json!({ "success": true })).into_response()
}

/// `POST /{version}/{waba_id}/subscribed_apps` — subscribes the app to the WABA's events.
async fn subscribed_apps(State(me): State<Arc<FakeMeta>>, headers: HeaderMap) -> Response {
    if !me.authorized(&headers) {
        return graph_error(StatusCode::UNAUTHORIZED, 190, "Invalid OAuth access token", "Malformed access token");
    }
    Json(json!({ "success": true })).into_response()
}

#[derive(Deserialize)]
struct InboundReq {
    phone_number_id: String,
    from: String,
    #[serde(default)]
    name: String,
    text: String,
}

async fn control_inbound(State(me): State<Arc<FakeMeta>>, headers: HeaderMap, Json(r): Json<InboundReq>) -> Response {
    if !me.authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let wamid = me.customer_message(&r.phone_number_id, &r.from, &r.name, &r.text).await;
    Json(json!({ "wamid": wamid })).into_response()
}

#[derive(Deserialize)]
struct LoadReq {
    phone_number_id: String,
    customers: u64,
    messages_per_customer: u64,
    /// Inbound messages per second across all customers.
    rate_per_sec: u64,
}

/// Starts a bulk run: `customers` × `messages_per_customer` inbound messages at `rate_per_sec`.
/// Each text is `t=<run>-<n> …`; agents that reply with `rt=<run>-<n>` close the round trip.
async fn control_load(State(me): State<Arc<FakeMeta>>, headers: HeaderMap, Json(r): Json<LoadReq>) -> Response {
    if !me.authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let total = r.customers.saturating_mul(r.messages_per_customer);
    if total == 0 || r.rate_per_sec == 0 || total > 5_000_000 {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "customers × messages must be 1..5000000 and rate > 0" }))).into_response();
    }
    let run = format!("{:06x}", rand::random::<u32>() & 0xff_ffff);
    let base: u64 = 60_190_000_000 + (rand::random::<u64>() % 9_000) * 1_000_000;
    let info = json!({ "run": run, "phone_number_id": r.phone_number_id, "customers": r.customers, "messages": total, "rate_per_sec": r.rate_per_sec, "started_at": chrono::Utc::now() });
    me.runs.lock().unwrap_or_else(|e| e.into_inner()).push(info.clone());
    let me2 = me.clone();
    tokio::spawn(async move {
        let start = Instant::now();
        // Message n goes out at n / rate seconds; customers take turns (c = n % customers).
        for n in 0..total {
            let due = Duration::from_secs_f64(n as f64 / r.rate_per_sec as f64);
            if let Some(wait) = due.checked_sub(start.elapsed()) {
                tokio::time::sleep(wait).await;
            }
            let c = n % r.customers;
            let from = format!("{}", base + c);
            let text = format!("t={run}-{n} load message {} from customer {c}", n / r.customers + 1);
            me2.customer_message(&r.phone_number_id, &from, &format!("Load customer {c}"), &text).await;
        }
    });
    Json(info).into_response()
}

async fn control_stats(State(me): State<Arc<FakeMeta>>, headers: HeaderMap) -> Response {
    if !me.authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let s = &me.stats;
    let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
    Json(json!({
        "inbound_generated": g(&s.inbound_generated),
        "webhooks_ok": g(&s.webhooks_ok),
        "webhook_retries": g(&s.webhook_retries),
        "webhooks_given_up": g(&s.webhooks_given_up),
        "webhook_queue": me.webhooks.max_capacity() - me.webhooks.capacity(),
        "statuses_queued": g(&s.statuses_queued),
        "outbound_accepted": g(&s.outbound_accepted),
        "outbound_throttled_429": g(&s.outbound_throttled),
        "outbound_injected_errors": g(&s.outbound_injected_errors),
        "outbound_window_closed": g(&s.outbound_window_closed),
        "outbound_undeliverable": g(&s.outbound_undeliverable),
        "outbound_unauthorized": g(&s.outbound_unauthorized),
        "inbound_ack": s.inbound_ack.summary(),
        "round_trip": s.round_trip.summary(),
        "runs": *me.runs.lock().unwrap_or_else(|e| e.into_inner()),
        "webhook_target": *me.webhook_target.lock().unwrap_or_else(|e| e.into_inner()),
        "config": { "rate_per_sec": me.cfg.rate_per_sec, "latency_ms": [me.cfg.latency_ms.0, me.cfg.latency_ms.1], "error_rate": me.cfg.error_rate,
                    "duplicate_rate": me.cfg.duplicate_rate, "enforce_window": me.cfg.enforce_window },
    }))
    .into_response()
}

async fn control_reset(State(me): State<Arc<FakeMeta>>, headers: HeaderMap) -> Response {
    if !me.authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let s = &me.stats;
    for a in [
        &s.inbound_generated,
        &s.webhooks_ok,
        &s.webhook_retries,
        &s.webhooks_given_up,
        &s.statuses_queued,
        &s.outbound_accepted,
        &s.outbound_throttled,
        &s.outbound_injected_errors,
        &s.outbound_window_closed,
        &s.outbound_undeliverable,
        &s.outbound_unauthorized,
    ] {
        a.store(0, Ordering::Relaxed);
    }
    s.inbound_ack.clear();
    s.round_trip.clear();
    me.origins.lock().unwrap_or_else(|e| e.into_inner()).clear();
    me.runs.lock().unwrap_or_else(|e| e.into_inner()).clear();
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Deserialize)]
struct OutboxQuery {
    limit: Option<usize>,
    phone_number_id: Option<String>,
}

/// What the simulated customers' phones received (latest first).
async fn control_outbox(State(me): State<Arc<FakeMeta>>, headers: HeaderMap, Query(q): Query<OutboxQuery>) -> Response {
    if !me.authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let o = me.outbox.lock().unwrap_or_else(|e| e.into_inner());
    let items: Vec<&SentMessage> = o
        .iter()
        .filter(|m| q.phone_number_id.as_deref().is_none_or(|p| p == m.phone_number_id))
        .take(q.limit.unwrap_or(50).min(500))
        .collect();
    Json(json!(items)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markers() {
        assert_eq!(marker_of("t=ab12-7 load message").as_deref(), Some("ab12-7"));
        assert_eq!(reply_marker("rt=ab12-7 thanks").as_deref(), Some("ab12-7"));
        assert_eq!(marker_of("hello"), None);
    }

    #[tokio::test]
    async fn window_and_rate_limit_rules() {
        let me = FakeMeta::start(FakeMetaConfig { rate_per_sec: 2, webhook_url: "http://127.0.0.1:9/none".into(), ..Default::default() });
        assert!(!me.window_open("PN", "6011"), "no inbound yet → window closed");
        me.windows.lock().unwrap().insert(("PN".into(), "6011".into()), Instant::now());
        assert!(me.window_open("PN", "6011"));
        assert!(me.take_rate("PN") && me.take_rate("PN"));
        assert!(!me.take_rate("PN"), "third send in the same second is throttled");
        assert!(me.take_rate("OTHER"), "limits are per phone number id");
    }
}
