//! M10 hub test helpers: a real HTTP server on an ephemeral port (WebSockets need a socket), an
//! in-process **fake-meta** server (the hub's real WhatsApp Cloud API adapter talks to it over
//! HTTP and it posts signed webhooks back), a tenant with channels and agents, WebSocket clients.
#![allow(dead_code)]

use std::time::Duration;

use axum::http::{Method, StatusCode};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

use omni_m01::fake_meta::{FakeMeta, FakeMetaConfig};
use omni_m01::modules::m10_hub::infrastructure::channels::{sign, whatsapp};

use super::{TestApp, PLAN_STANDARD};

pub const AGENT_PASSWORD: &str = "Agent-Passw0rd-123!";

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct HubTenant {
    pub tenant: Value,
    pub tenant_id: Uuid,
    pub code: String,
    pub ta: String,
    pub whatsapp: String,
    pub did: String,
    pub widget: String,
}

pub struct HubApp {
    pub app: TestApp,
    pub addr: std::net::SocketAddr,
    pub fake_meta: std::net::SocketAddr,
    pub sa: String,
}

pub const FAKE_TOKEN: &str = "test-fake-meta-token";
pub const FAKE_SECRET: &str = "test-fake-meta-app-secret";

impl HubApp {
    pub async fn new() -> Self {
        // Hub listener first (fake-meta needs its webhook URL), then fake-meta, then the app.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let fake_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fake_addr = fake_listener.local_addr().unwrap();
        let fake = FakeMeta::start(FakeMetaConfig {
            access_token: FAKE_TOKEN.into(),
            app_secret: FAKE_SECRET.into(),
            webhook_url: format!("http://{addr}/v1/hub/channels/whatsapp/webhook"),
            latency_ms: (1, 5),
            rate_per_sec: 1000,
            delivered_after_ms: 100,
            read_after_ms: 250,
            webhook_workers: 16,
            ..Default::default()
        });
        tokio::spawn(async move {
            axum::serve(fake_listener, omni_m01::fake_meta::router(fake)).await.unwrap();
        });
        let app = TestApp::with_config(|cfg| {
            let url = format!("http://{fake_addr}");
            cfg.whatsapp.provider = "fake".into();
            cfg.whatsapp.base_url = url.clone();
            cfg.whatsapp.fake_control_url = Some(url);
            cfg.whatsapp.access_token = FAKE_TOKEN.into();
            cfg.whatsapp.app_secret = FAKE_SECRET.into();
            cfg.whatsapp.verify_token = "test-verify-token".into();
        })
        .await;
        let router = app.router.clone();
        tokio::spawn(async move {
            axum::serve(listener, router.into_make_service()).await.unwrap();
        });
        let sa = app.sa_token().await;
        Self { app, addr, fake_meta: fake_addr, sa }
    }

    /// Active Standard tenant + Tenant Admin + simulated channels (WhatsApp/voice → support,
    /// web chat → sales).
    pub async fn tenant(&self) -> HubTenant {
        let (t, ta) = self.app.active_tenant_with_admin(&self.sa, PLAN_STANDARD, json!({})).await;
        let r = self.app.post("/v1/hub/channels/simulated", &ta, json!({})).await;
        assert_eq!(r.status, StatusCode::CREATED, "provision channels: {}", r.text);
        let eps = r.data().as_array().unwrap().clone();
        let addr = |c: &str| eps.iter().find(|e| e["channel"] == c).unwrap()["address"].as_str().unwrap().to_string();
        HubTenant {
            tenant_id: Uuid::parse_str(t["tenant_id"].as_str().unwrap()).unwrap(),
            code: t["tenant_code"].as_str().unwrap().to_string(),
            whatsapp: addr("whatsapp"),
            did: addr("voice"),
            widget: addr("webchat"),
            tenant: t,
            ta,
        }
    }

    /// Creates an agent and returns (user id, API bearer token).
    pub async fn agent(&self, t: &HubTenant, skills: &[&str], capacity: i64) -> (Uuid, String) {
        let email = format!("agent-{}@{}.example", &Uuid::new_v4().simple().to_string()[..8], t.code);
        let r = self
            .app
            .post(
                "/v1/hub/agents",
                &t.ta,
                json!({ "email": email, "display_name": "Test Agent", "password": AGENT_PASSWORD, "skills": skills, "max_concurrent": capacity }),
            )
            .await;
        assert_eq!(r.status, StatusCode::CREATED, "create agent: {}", r.text);
        let id = Uuid::parse_str(r.data()["user_id"].as_str().unwrap()).unwrap();
        let tok = self.app.token(&email, AGENT_PASSWORD, Some(&t.code)).await;
        assert_eq!(tok.status, StatusCode::OK, "agent token: {}", tok.text);
        (id, tok.data()["access_token"].as_str().unwrap().to_string())
    }

    pub async fn agent_ws(&self, token: &str) -> Ws {
        let mut req = format!("ws://{}/v1/hub/ws/agent", self.addr).into_client_request().unwrap();
        req.headers_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.expect("agent websocket");
        ws
    }

    /// Connects, says hello and returns the socket plus the welcome frame.
    pub async fn agent_online(&self, token: &str, available: bool) -> (Ws, Value) {
        let mut ws = self.agent_ws(token).await;
        send(&mut ws, json!({ "type": "hello" })).await;
        let welcome = recv_type(&mut ws, "welcome").await;
        if available {
            send(&mut ws, json!({ "type": "presence.set", "status": "available" })).await;
            recv_type(&mut ws, "presence").await;
        }
        (ws, welcome)
    }

    pub async fn customer_ws(&self, token: &str, last_seq: i64) -> (Ws, Value) {
        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("ws://{}/v1/hub/ws/customer", self.addr)).await.expect("customer websocket");
        send(&mut ws, json!({ "type": "auth", "token": token, "last_seq": last_seq })).await;
        let w = recv_type(&mut ws, "welcome").await;
        (ws, w)
    }

    /// A customer message delivered the way fake-meta (or Meta) does it: a signed webhook over
    /// HTTP, asynchronously. Also opens the 24-hour window in fake-meta, so replies are accepted.
    pub async fn customer_writes(&self, phone_number_id: &str, from: &str, text: &str) {
        let sim = self.app.state.hub.simulator.clone().expect("fake-meta client");
        sim.customer_message(phone_number_id, from, "Customer", text).await.expect("fake-meta inbound");
    }

    /// Signed WhatsApp webhook posted straight to the hub (synchronous; for signature,
    /// duplicate and ordering checks). fake-meta does not know about it (no 24-hour window).
    pub async fn whatsapp_inbound(&self, phone_number_id: &str, from: &str, text: &str, wamid: &str) -> (StatusCode, Value) {
        let body = whatsapp::inbound_payload(phone_number_id, from, "Customer", text, wamid).to_string();
        let sig = sign(FAKE_SECRET.as_bytes(), body.as_bytes());
        self.raw_post("/v1/hub/channels/whatsapp/webhook", &body, &[("x-hub-signature-256", &sig)]).await
    }

    pub async fn sip_event(&self, did: &str, call_id: &str, event: &str, from: &str) -> (StatusCode, Value) {
        let body = json!({ "call_id": call_id, "event": event, "from": from, "to": did }).to_string();
        let sig = sign(self.app.state.config.hub_sim_sip_secret.as_bytes(), body.as_bytes());
        self.raw_post("/v1/hub/channels/sip/events", &body, &[("x-sim-signature", &sig)]).await
    }

    pub async fn raw_post(&self, path: &str, body: &str, headers: &[(&str, &str)]) -> (StatusCode, Value) {
        let mut b = axum::http::Request::builder().method(Method::POST).uri(path).header("content-type", "application/json");
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        let r = self.app.raw(b.body(axum::body::Body::from(body.to_string())).unwrap()).await;
        (r.status, r.body)
    }

    /// Runs this tenant's delivery worker until `done` holds for the conversation's timeline.
    /// Receipts come back from fake-meta as real webhooks.
    pub async fn pump_until<F: Fn(&Value) -> bool>(&self, tenant: Uuid, token: &str, conversation: &str, done: F) -> Value {
        for _ in 0..100 {
            self.app.state.hub.delivery_tick_for_tenant(tenant).await.expect("delivery tick");
            let r = self.app.get(&format!("/v1/hub/conversations/{conversation}/messages"), token).await;
            if done(r.data()) {
                return r.data().clone();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("condition not reached for conversation {conversation}");
    }

    /// What fake-meta's "customer phones" received (latest first).
    pub async fn fake_outbox(&self, phone_number_id: &str) -> Vec<Value> {
        let sim = self.app.state.hub.simulator.clone().expect("fake-meta client");
        sim.outbox(Some(phone_number_id), 50).await.expect("outbox").as_array().cloned().unwrap_or_default()
    }
}

pub async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.expect("ws send");
}

/// Next JSON frame of the given type (other frame types are skipped); 5 s timeout.
pub async fn recv_type(ws: &mut Ws, ty: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let msg = tokio::time::timeout(left, ws.next()).await.unwrap_or_else(|_| panic!("timed out waiting for {ty}"));
        match msg {
            Some(Ok(Message::Text(t))) => {
                let v: Value = serde_json::from_str(t.as_str()).unwrap();
                if v["type"] == ty {
                    return v;
                }
                if v["type"] == "error" && ty != "error" {
                    panic!("error frame while waiting for {ty}: {v}");
                }
            }
            Some(Ok(_)) => continue,
            other => panic!("socket ended while waiting for {ty}: {other:?}"),
        }
    }
}

/// Asserts that no frame of type `ty` arrives within `ms`.
pub async fn assert_no_frame(ws: &mut Ws, ty: &str, ms: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(left, ws.next()).await {
            Err(_) => return,
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Value = serde_json::from_str(t.as_str()).unwrap();
                assert_ne!(v["type"], ty, "unexpected frame: {v}");
            }
            Ok(Some(Ok(_))) => continue,
            Ok(other) => panic!("socket ended: {other:?}"),
        }
    }
}

pub fn wamid() -> String {
    format!("wamid.TEST.{}", Uuid::new_v4().simple())
}
