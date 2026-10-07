//! `hub_load` — end-to-end driver for the M10 hub gateway (no browser, no real credentials).
//!
//! ```text
//! hub_load smoke   [--base URL]                                  # functional check, exit 1 on failure
//! hub_load idle    [--base URL] [--sessions N] [--hold SECS] [--hosts 127.0.0.1,127.0.0.2]
//! hub_load latency [--base URL] [--customers N] [--messages M] [--interval-ms MS]
//! hub_load chaos   [--base URL] [--customers N] [--duration SECS] [--interval-ms MS]
//! ```
//!
//! All scenarios use the development demo tenant (`HUB_DEMO_SEED=true`): the Tenant Admin
//! creates a dedicated agent pool and simulated channels that route to a scenario-specific skill,
//! so measurements never mix with people using the demo by hand. Reads `.env` for the demo
//! password. `chaos` keeps customers and agents chatting through reconnects (node kill, rolling
//! restart) and finally checks that every message the server acknowledged is stored and reached
//! the agent: "zero acknowledged messages lost".

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
type R<T> = Result<T, String>;

const DEMO_CODE: &str = "demo";
const DEMO_ADMIN: &str = "admin@demo.omni.local";

// ------------------------------------------------------------------------------------------------
// Arguments
// ------------------------------------------------------------------------------------------------

struct Args {
    cmd: String,
    opts: HashMap<String, String>,
}

impl Args {
    fn parse() -> Self {
        let mut it = std::env::args().skip(1);
        let cmd = it.next().unwrap_or_else(|| "help".into());
        let mut opts = HashMap::new();
        while let Some(k) = it.next() {
            if let Some(name) = k.strip_prefix("--") {
                opts.insert(name.to_string(), it.next().unwrap_or_default());
            }
        }
        Self { cmd, opts }
    }
    fn s(&self, k: &str, d: &str) -> String {
        self.opts.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    fn n(&self, k: &str, d: u64) -> u64 {
        self.opts.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
}

// ------------------------------------------------------------------------------------------------
// Minimal HTTP/1.1 JSON client (keeps the tool dependency-free)
// ------------------------------------------------------------------------------------------------

#[derive(Clone)]
struct Http {
    host: String,
    port: u16,
}

impl Http {
    fn new(base: &str) -> R<Self> {
        let u = url::Url::parse(base).map_err(|e| format!("bad --base: {e}"))?;
        Ok(Self { host: u.host_str().unwrap_or("localhost").to_string(), port: u.port_or_known_default().unwrap_or(80) })
    }

    async fn call(&self, method: &str, path: &str, token: Option<&str>, body: Option<&Value>) -> R<(u16, Value)> {
        let mut s = TcpStream::connect((self.host.as_str(), self.port)).await.map_err(|e| format!("connect: {e}"))?;
        let payload = body.map(|b| b.to_string()).unwrap_or_default();
        let mut req =
            format!("{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\nAccept: application/json\r\n", self.host, self.port);
        if let Some(t) = token {
            req.push_str(&format!("Authorization: Bearer {t}\r\n"));
        }
        if body.is_some() {
            req.push_str(&format!("Content-Type: application/json\r\nContent-Length: {}\r\n", payload.len()));
        }
        req.push_str("\r\n");
        req.push_str(&payload);
        s.write_all(req.as_bytes()).await.map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&buf);
        let (head, rest) = text.split_once("\r\n\r\n").ok_or("malformed HTTP response")?;
        let status: u16 = head.split_whitespace().nth(1).and_then(|s| s.parse().ok()).ok_or("no status")?;
        let body = if head.to_ascii_lowercase().contains("transfer-encoding: chunked") { dechunk(rest) } else { rest.to_string() };
        Ok((status, serde_json::from_str(&body).unwrap_or(Value::Null)))
    }

    fn ws_url(&self, path: &str) -> String {
        format!("ws://{}:{}{path}", self.host, self.port)
    }
}

fn dechunk(s: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some((len, tail)) = rest.split_once("\r\n") {
        let n = usize::from_str_radix(len.trim(), 16).unwrap_or(0);
        if n == 0 || tail.len() < n {
            break;
        }
        out.push_str(&tail[..n]);
        rest = tail[n..].trim_start_matches("\r\n");
    }
    out
}

// ------------------------------------------------------------------------------------------------
// Fixture: dedicated agents + channels in the demo tenant
// ------------------------------------------------------------------------------------------------

struct Fixture {
    http: Http,
    ta: String,
    agents: Vec<String>,
    widget: String,
    whatsapp: String,
}

fn demo_password() -> String {
    std::env::var("HUB_DEMO_PASSWORD").unwrap_or_else(|_| "Demo-Hub-Passw0rd!".into())
}

async fn login(http: &Http, email: &str) -> R<String> {
    let (st, b) = http
        .call("POST", "/v1/bootstrap/token", None, Some(&json!({ "email": email, "password": demo_password(), "tenant_code": DEMO_CODE })))
        .await?;
    if st != 200 {
        return Err(format!("login {email}: HTTP {st} {b}"));
    }
    Ok(b["data"]["access_token"].as_str().unwrap_or_default().to_string())
}

async fn fixture(http: &Http, skill: &str, agents: usize, capacity: i64) -> R<Fixture> {
    let ta = login(http, DEMO_ADMIN).await.map_err(|e| format!("{e} (is HUB_DEMO_SEED=true and APP_ENV=development?)"))?;
    let (_, ch) = http.call("GET", "/v1/hub/channels", Some(&ta), None).await?;
    let mine = |c: &str| {
        ch["data"]["endpoints"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|e| e["channel"] == c && e["default_skill"] == skill)
            .and_then(|e| e["address"].as_str().map(str::to_string))
    };
    let (widget, whatsapp) = match (mine("webchat"), mine("whatsapp")) {
        (Some(w), Some(wa)) => (w, wa),
        _ => {
            let (st, b) = http
                .call(
                    "POST",
                    "/v1/hub/channels/simulated",
                    Some(&ta),
                    Some(&json!({ "whatsapp_skill": skill, "voice_skill": skill, "webchat_skill": skill })),
                )
                .await?;
            if st != 201 {
                return Err(format!("provision channels: HTTP {st} {b}"));
            }
            let get = |c: &str| {
                b["data"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|e| e["channel"] == c)
                    .and_then(|e| e["address"].as_str().map(str::to_string))
            };
            (get("webchat").ok_or("no widget")?, get("whatsapp").ok_or("no whatsapp")?)
        }
    };
    let mut toks = Vec::new();
    for i in 0..agents {
        let email = format!("{skill}.agent{i}@demo.omni.local");
        let tok = match login(http, &email).await {
            Ok(t) => t,
            Err(_) => {
                let (st, b) = http
                    .call(
                        "POST",
                        "/v1/hub/agents",
                        Some(&ta),
                        Some(&json!({ "email": email, "display_name": format!("{skill} agent {i}"), "password": demo_password(), "skills": [skill], "max_concurrent": capacity })),
                    )
                    .await?;
                if st != 201 {
                    return Err(format!("create agent {email}: HTTP {st} {b}"));
                }
                login(http, &email).await?
            }
        };
        toks.push(tok);
    }
    Ok(Fixture { http: http.clone(), ta, agents: toks, widget, whatsapp })
}

// ------------------------------------------------------------------------------------------------
// WebSocket helpers
// ------------------------------------------------------------------------------------------------

async fn agent_connect(http: &Http, token: &str) -> R<Ws> {
    let mut req = http.ws_url("/v1/hub/ws/agent").into_client_request().map_err(|e| e.to_string())?;
    req.headers_mut().insert("authorization", format!("Bearer {token}").parse().map_err(|_| "bad token")?);
    let (ws, _) = tokio_tungstenite::connect_async(req).await.map_err(|e| format!("agent ws: {e}"))?;
    Ok(ws)
}

async fn send(ws: &mut Ws, v: Value) -> R<()> {
    ws.send(Message::Text(v.to_string().into())).await.map_err(|e| e.to_string())
}

async fn next_json(ws: &mut Ws, timeout: Duration) -> R<Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, ws.next()).await {
            Err(_) => return Err("timeout".into()),
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(t.as_str()).map_err(|e| e.to_string()),
            Ok(Some(Ok(_))) => continue,
            Ok(other) => return Err(format!("socket ended: {other:?}")),
        }
    }
}

async fn wait_type(ws: &mut Ws, ty: &str, timeout: Duration) -> R<Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let v = next_json(ws, deadline.saturating_duration_since(Instant::now())).await.map_err(|e| format!("waiting for {ty}: {e}"))?;
        if v["type"] == ty {
            return Ok(v);
        }
        if v["type"] == "error" {
            return Err(format!("error while waiting for {ty}: {v}"));
        }
    }
}

async fn customer_session(http: &Http, widget: &str, name: &str) -> R<String> {
    let (st, b) = http.call("POST", "/v1/hub/customer/sessions", None, Some(&json!({ "widget_key": widget, "name": name }))).await?;
    if st != 201 {
        return Err(format!("customer session: HTTP {st} {b}"));
    }
    Ok(b["data"]["token"].as_str().unwrap_or_default().to_string())
}

/// Small client buffers too, so one load machine can hold ~100k sockets.
fn client_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default().read_buffer_size(4096).write_buffer_size(4096)
}

async fn customer_connect(url: &str, token: &str, last_seq: i64) -> R<(Ws, Value)> {
    let (mut ws, _) =
        tokio_tungstenite::connect_async_with_config(url, Some(client_config()), false).await.map_err(|e| format!("customer ws: {e}"))?;
    send(&mut ws, json!({ "type": "auth", "token": token, "last_seq": last_seq })).await?;
    // The server may queue the handshake behind its admission control (≤ 20 s) under a storm.
    let w = wait_type(&mut ws, "welcome", Duration::from_secs(30)).await?;
    Ok((ws, w))
}

fn now_us() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((p * sorted.len() as f64) as usize).min(sorted.len() - 1)]
}

// ------------------------------------------------------------------------------------------------
// smoke
// ------------------------------------------------------------------------------------------------

async fn smoke(base: &str) -> R<()> {
    let http = Http::new(base)?;
    let f = fixture(&http, "smoke", 1, 50).await?;
    let mut ok = 0;
    let check = |name: &str, cond: bool, ok: &mut i32| -> R<()> {
        if cond {
            *ok += 1;
            println!("  \x1b[32mPASS\x1b[0m {name}");
            Ok(())
        } else {
            Err(format!("FAIL {name}"))
        }
    };
    let mut agent = agent_connect(&http, &f.agents[0]).await?;
    send(&mut agent, json!({ "type": "hello" })).await?;
    let w = wait_type(&mut agent, "welcome", Duration::from_secs(5)).await?;
    check(&format!("agent socket welcome (node {})", w["node"]), true, &mut ok)?;
    // Close leftovers from earlier runs so capacity is free.
    for c in w["conversations"].as_array().into_iter().flatten() {
        send(&mut agent, json!({ "type": "conversation.close", "conversation_id": c["conversation"]["id"] })).await?;
    }
    send(&mut agent, json!({ "type": "presence.set", "status": "available" })).await?;
    wait_type(&mut agent, "presence", Duration::from_secs(5)).await?;

    // WhatsApp (simulated, signed) → agent
    let secret = std::env::var("HUB_SIM_WHATSAPP_APP_SECRET").unwrap_or_else(|_| "dev-sim-whatsapp-app-secret-change-me".into());
    let from = format!("6019{:07}", now_us() % 10_000_000);
    let body = json!({"object":"whatsapp_business_account","entry":[{"id":"SMOKE","changes":[{"field":"messages","value":{
        "messaging_product":"whatsapp","metadata":{"phone_number_id": f.whatsapp},
        "contacts":[{"profile":{"name":"Smoke Customer"},"wa_id": from}],
        "messages":[{"from": from, "id": format!("wamid.SMOKE.{}", now_us()), "type":"text","text":{"body":"smoke: hello from WhatsApp"}}]}}]}]})
    .to_string();
    let sig = omni_m01::modules::m10_hub::infrastructure::channels::sign(secret.as_bytes(), body.as_bytes());
    let (st, _) = raw_post(&http, "/v1/hub/channels/whatsapp/webhook", &body, &[("X-Hub-Signature-256", &sig)]).await?;
    check("signed WhatsApp webhook accepted", st == 200, &mut ok)?;
    let a = wait_type(&mut agent, "conversation.assigned", Duration::from_secs(10)).await?;
    check("WhatsApp conversation routed to the agent", a["messages"][0]["body"] == "smoke: hello from WhatsApp", &mut ok)?;
    let conv = a["conversation"]["id"].as_str().unwrap_or_default().to_string();
    send(
        &mut agent,
        json!({ "type": "message.send", "conversation_id": conv, "client_msg_id": format!("s{}", now_us()), "body": "smoke reply" }),
    )
    .await?;
    let ack = wait_type(&mut agent, "ack", Duration::from_secs(5)).await?;
    check("agent reply acknowledged after commit", ack["message"]["seq"] == 2, &mut ok)?;
    let mut read = false;
    for _ in 0..50 {
        let (_, m) = http.call("GET", &format!("/v1/hub/conversations/{conv}/messages"), Some(&f.agents[0]), None).await?;
        if m["data"]["messages"][1]["delivery_status"] == "read" {
            read = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    check("simulated BSP receipts: queued → sent → delivered → read", read, &mut ok)?;
    send(&mut agent, json!({ "type": "conversation.close", "conversation_id": conv })).await?;

    // Web chat customer ↔ agent
    let token = customer_session(&http, &f.widget, "Smoke Visitor").await?;
    let (mut cust, _) = customer_connect(&http.ws_url("/v1/hub/ws/customer"), &token, 0).await?;
    send(&mut cust, json!({ "type": "message.send", "client_msg_id": "c1", "body": "smoke: hello from web chat" })).await?;
    wait_type(&mut cust, "ack", Duration::from_secs(5)).await?;
    let a = loop {
        let v = wait_type(&mut agent, "conversation.assigned", Duration::from_secs(10)).await?;
        if v["conversation"]["channel"] == "webchat" {
            break v;
        }
    };
    check("web chat conversation routed to the agent", a["messages"][0]["body"] == "smoke: hello from web chat", &mut ok)?;
    let conv = a["conversation"]["id"].as_str().unwrap_or_default().to_string();
    send(
        &mut agent,
        json!({ "type": "message.send", "conversation_id": conv, "client_msg_id": format!("w{}", now_us()), "body": "smoke: agent here" }),
    )
    .await?;
    let got = loop {
        let v = wait_type(&mut cust, "message.new", Duration::from_secs(5)).await?;
        if v["message"]["from"] == "agent" {
            break v;
        }
    };
    check("customer receives the agent reply live", got["message"]["body"] == "smoke: agent here", &mut ok)?;
    send(&mut agent, json!({ "type": "conversation.close", "conversation_id": conv })).await?;
    send(&mut agent, json!({ "type": "presence.set", "status": "offline" })).await?;
    println!("hub smoke: {ok} checks passed");
    Ok(())
}

async fn raw_post(http: &Http, path: &str, body: &str, headers: &[(&str, &str)]) -> R<(u16, String)> {
    let mut s = TcpStream::connect((http.host.as_str(), http.port)).await.map_err(|e| e.to_string())?;
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        http.host,
        http.port,
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    s.write_all(req.as_bytes()).await.map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf).to_string();
    let status = text.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    Ok((status, text))
}

// ------------------------------------------------------------------------------------------------
// idle: many long-lived customer sessions
// ------------------------------------------------------------------------------------------------

async fn idle(a: &Args) -> R<()> {
    let base = a.s("base", "http://localhost:3000");
    let http = Http::new(&base)?;
    let n = a.n("sessions", 1000) as usize;
    let hold = Duration::from_secs(a.n("hold", 30));
    let hosts: Vec<String> = a.s("hosts", &http.host).split(',').map(str::to_string).collect();
    let f = fixture(&http, "idle", 1, 5).await?;
    println!("creating {n} customer sessions…");
    let started = Instant::now();
    let tokens = Arc::new(Mutex::new(Vec::with_capacity(n)));
    let mut set = tokio::task::JoinSet::new();
    let next = Arc::new(AtomicUsize::new(0));
    for w in 0..64 {
        let mut http = http.clone();
        http.host = hosts[w % hosts.len()].clone();
        let (widget, tokens, next) = (f.widget.clone(), tokens.clone(), next.clone());
        set.spawn(async move {
            while next.fetch_add(1, Ordering::Relaxed) < n {
                if let Ok(t) = customer_session(&http, &widget, "idle").await {
                    tokens.lock().await.push(t);
                }
            }
        });
    }
    while set.join_next().await.is_some() {}
    let tokens = Arc::try_unwrap(tokens).map_err(|_| "tokens still shared")?.into_inner();
    println!("{} sessions created in {:.1}s; opening sockets…", tokens.len(), started.elapsed().as_secs_f64());
    let connected = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let deadline = Instant::now() + hold + Duration::from_secs(120);
    let mut set = tokio::task::JoinSet::new();
    for (i, t) in tokens.into_iter().enumerate() {
        let url = format!("ws://{}:{}/v1/hub/ws/customer", hosts[i % hosts.len()], http.port);
        let (connected, failed, dropped) = (connected.clone(), failed.clone(), dropped.clone());
        set.spawn(async move {
            // Like the browser client: retry with exponential backoff + jitter (server busy 1013,
            // transient errors), give up after 6 attempts.
            let mut attempt = 0u32;
            let conn = loop {
                match customer_connect(&url, &t, 0).await {
                    Ok(c) => break Ok(c),
                    Err(_) if attempt < 5 => {
                        attempt += 1;
                        let backoff = 250u64 * 2u64.pow(attempt) + now_us() % 250;
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                    }
                    Err(e) => break Err(e),
                }
            };
            match conn {
                Ok((mut ws, _)) => {
                    connected.fetch_add(1, Ordering::Relaxed);
                    // Hold the socket, answering pings, until the deadline.
                    loop {
                        let left = deadline.saturating_duration_since(Instant::now());
                        match tokio::time::timeout(left, ws.next()).await {
                            Err(_) => break,
                            Ok(Some(Ok(_))) => continue,
                            Ok(_) => {
                                dropped.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                }
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        if i % 200 == 199 {
            tokio::time::sleep(Duration::from_millis(40)).await; // ramp ≈ 5k connects/s
        }
    }
    // Report while holding.
    let ramp_until = Instant::now() + Duration::from_secs(600);
    while connected.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed) < n && Instant::now() < ramp_until {
        tokio::time::sleep(Duration::from_secs(2)).await;
        println!("  open: {}  failed: {}", connected.load(Ordering::Relaxed), failed.load(Ordering::Relaxed));
    }
    let ramp = started.elapsed().as_secs_f64();
    println!("ramp done in {ramp:.1}s — holding {}s (measure server memory now)…", hold.as_secs());
    tokio::time::sleep(hold).await;
    println!(
        "RESULT idle: requested={n} connected={} failed={} dropped_during_hold={} ramp_s={ramp:.1}",
        connected.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        dropped.load(Ordering::Relaxed)
    );
    set.abort_all();
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// latency: customer → agent delivery latency under load
// ------------------------------------------------------------------------------------------------

/// Empties an agent pool's backlog: each agent goes available and closes everything it is given
/// (assigned + queued for its skill) until nothing new arrives for 1.5 s, then goes offline.
/// Earlier runs can leave queued work behind (the presence reaper re-queues conversations of
/// agents whose sockets went away), which would otherwise skew the next measurement.
async fn reset_agents(http: &Http, tokens: &[String]) -> R<usize> {
    let mut closed = 0;
    for t in tokens {
        let mut ws = agent_connect(http, t).await?;
        send(&mut ws, json!({ "type": "hello" })).await?;
        send(&mut ws, json!({ "type": "presence.set", "status": "available" })).await?;
        loop {
            let v = match next_json(&mut ws, Duration::from_millis(1500)).await {
                Ok(v) => v,
                Err(_) => break,
            };
            let ids: Vec<Value> = match v["type"].as_str() {
                Some("welcome") => v["conversations"].as_array().into_iter().flatten().map(|c| c["conversation"]["id"].clone()).collect(),
                Some("conversation.assigned") => vec![v["conversation"]["id"].clone()],
                _ => vec![],
            };
            for id in ids {
                send(&mut ws, json!({ "type": "conversation.close", "conversation_id": id })).await?;
                closed += 1;
            }
        }
        send(&mut ws, json!({ "type": "presence.set", "status": "offline" })).await?;
        let _ = ws.close(None).await;
    }
    Ok(closed)
}

async fn agents_online(http: &Http, tokens: &[String]) -> R<Vec<Ws>> {
    let mut out = Vec::new();
    for t in tokens {
        let mut ws = agent_connect(http, t).await?;
        send(&mut ws, json!({ "type": "hello" })).await?;
        let w = wait_type(&mut ws, "welcome", Duration::from_secs(10)).await?;
        for c in w["conversations"].as_array().into_iter().flatten() {
            send(&mut ws, json!({ "type": "conversation.close", "conversation_id": c["conversation"]["id"] })).await?;
        }
        send(&mut ws, json!({ "type": "presence.set", "status": "available" })).await?;
        out.push(ws);
    }
    Ok(out)
}

async fn latency(a: &Args) -> R<()> {
    let base = a.s("base", "http://localhost:3000");
    let http = Http::new(&base)?;
    let customers = a.n("customers", 200) as usize;
    let messages = a.n("messages", 20) as usize;
    let interval = Duration::from_millis(a.n("interval-ms", 600));
    let per_agent = 50usize;
    let agent_count = customers.div_ceil(per_agent).max(1);
    let f = fixture(&http, "latency", agent_count, per_agent as i64).await?;
    let stale = reset_agents(&http, &f.agents).await?;
    if stale > 0 {
        println!("closed {stale} conversations left over from earlier runs");
    }
    let agents = agents_online(&http, &f.agents).await?;
    let lat = Arc::new(Mutex::new(Vec::<f64>::new()));
    let acks = Arc::new(Mutex::new(Vec::<f64>::new()));
    let received = Arc::new(AtomicU64::new(0));
    for ws in agents {
        let (lat, received) = (lat.clone(), received.clone());
        tokio::spawn(async move {
            let mut ws = ws;
            while let Some(Ok(m)) = ws.next().await {
                let Message::Text(t) = m else { continue };
                let Ok(v) = serde_json::from_str::<Value>(t.as_str()) else { continue };
                let bodies: Vec<&Value> = match v["type"].as_str() {
                    Some("message.new") => vec![&v["message"]],
                    Some("conversation.assigned") => v["messages"].as_array().map(|a| a.iter().collect()).unwrap_or_default(),
                    _ => vec![],
                };
                for m in bodies {
                    if let Some(sent) = m["body"]
                        .as_str()
                        .and_then(|b| b.strip_prefix("t="))
                        .and_then(|b| b.split(' ').next())
                        .and_then(|b| b.parse::<u64>().ok())
                    {
                        lat.lock().await.push((now_us().saturating_sub(sent)) as f64 / 1000.0);
                        received.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }
    let url = http.ws_url("/v1/hub/ws/customer");
    let mut set = tokio::task::JoinSet::new();
    let started = Instant::now();
    for i in 0..customers {
        let (http, widget, url, acks) = (http.clone(), f.widget.clone(), url.clone(), acks.clone());
        set.spawn(async move {
            let token = customer_session(&http, &widget, &format!("load {i}")).await?;
            let (mut ws, _) = customer_connect(&url, &token, 0).await?;
            tokio::time::sleep(Duration::from_millis((i as u64 * 7) % interval.as_millis().max(1) as u64)).await;
            for k in 0..messages {
                let t0 = Instant::now();
                send(
                    &mut ws,
                    json!({ "type": "message.send", "client_msg_id": format!("m{k}"), "body": format!("t={} msg {k}", now_us()) }),
                )
                .await?;
                wait_type(&mut ws, "ack", Duration::from_secs(10)).await?;
                acks.lock().await.push(t0.elapsed().as_secs_f64() * 1000.0);
                tokio::time::sleep(interval).await;
            }
            Ok::<_, String>(())
        });
    }
    let mut errors = 0;
    while let Some(r) = set.join_next().await {
        if !matches!(r, Ok(Ok(()))) {
            errors += 1;
        }
    }
    let expected = (customers * messages) as u64;
    let until = Instant::now() + Duration::from_secs(15);
    while received.load(Ordering::Relaxed) < expected && Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let mut l = lat.lock().await.clone();
    let mut k = acks.lock().await.clone();
    l.sort_by(f64::total_cmp);
    k.sort_by(f64::total_cmp);
    println!(
        "RESULT latency: customers={customers} messages={} customer_errors={errors} delivered_to_agents={} msg_per_s={:.0}",
        k.len(),
        received.load(Ordering::Relaxed),
        k.len() as f64 / elapsed
    );
    println!(
        "  ack (customer send → server ack after commit) ms: p50={:.1} p95={:.1} p99={:.1}",
        pct(&k, 0.5),
        pct(&k, 0.95),
        pct(&k, 0.99)
    );
    println!(
        "  delivery (customer send → agent receives) ms:  p50={:.1} p95={:.1} p99={:.1} max={:.1}",
        pct(&l, 0.5),
        pct(&l, 0.95),
        pct(&l, 0.99),
        l.last().copied().unwrap_or(0.0)
    );
    reset_agents(&http, &f.agents).await?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// chaos: continuous chat through node kills / rolling restarts; verify zero acknowledged loss
// ------------------------------------------------------------------------------------------------

#[derive(Default)]
struct Ledger {
    /// message id → body, for every message the server acknowledged to a customer.
    acked: BTreeMap<String, String>,
    /// conversation ids seen by customers
    conversations: HashSet<String>,
    reconnects: u64,
    resent: u64,
    rate_limited: u64,
    /// node id → number of customer (re)connections served
    customer_nodes: BTreeMap<String, u64>,
}

async fn chaos(a: &Args) -> R<()> {
    let base = a.s("base", "http://localhost:8080");
    let http = Http::new(&base)?;
    let customers = a.n("customers", 20) as usize;
    let duration = Duration::from_secs(a.n("duration", 60));
    let interval = Duration::from_millis(a.n("interval-ms", 600));
    let f = fixture(&http, "chaos", 2, 50).await?;
    reset_agents(&http, &f.agents).await?;
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let agent_seen = Arc::new(Mutex::new(HashSet::<String>::new()));
    let agent_reconnects = Arc::new(AtomicU64::new(0));
    let agent_nodes = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let stop_at = Instant::now() + duration;

    // Agents: reconnect forever, resume by seq, stay available.
    for tok in f.agents.clone() {
        let (http, seen, rc, nodes) = (http.clone(), agent_seen.clone(), agent_reconnects.clone(), agent_nodes.clone());
        tokio::spawn(async move {
            let mut resume: HashMap<String, i64> = HashMap::new();
            let mut first = true;
            loop {
                if Instant::now() > stop_at + Duration::from_secs(30) {
                    return;
                }
                let Ok(mut ws) = agent_connect(&http, &tok).await else {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                };
                if !first {
                    rc.fetch_add(1, Ordering::Relaxed);
                }
                first = false;
                if send(&mut ws, json!({ "type": "hello", "resume": resume })).await.is_err() {
                    continue;
                }
                let _ = send(&mut ws, json!({ "type": "presence.set", "status": "available" })).await;
                while let Some(Ok(m)) = ws.next().await {
                    let Message::Text(t) = m else { continue };
                    let Ok(v) = serde_json::from_str::<Value>(t.as_str()) else { continue };
                    let mut msgs: Vec<Value> = Vec::new();
                    match v["type"].as_str() {
                        Some("welcome") => {
                            *nodes.lock().await.entry(v["node"].as_str().unwrap_or("?").to_string()).or_default() += 1;
                            for c in v["conversations"].as_array().into_iter().flatten() {
                                msgs.extend(c["messages"].as_array().cloned().unwrap_or_default());
                            }
                        }
                        Some("conversation.assigned") => msgs.extend(v["messages"].as_array().cloned().unwrap_or_default()),
                        Some("message.new") => msgs.push(v["message"].clone()),
                        _ => {}
                    }
                    let mut s = seen.lock().await;
                    for m in msgs {
                        if let (Some(id), Some(c), Some(seq)) = (m["id"].as_str(), m["conversation_id"].as_str(), m["seq"].as_i64()) {
                            s.insert(id.to_string());
                            let e = resume.entry(c.to_string()).or_insert(0);
                            *e = (*e).max(seq);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Customers: send continuously; on any socket loss reconnect, resume, re-send unacked.
    let url = http.ws_url("/v1/hub/ws/customer");
    let mut set = tokio::task::JoinSet::new();
    for i in 0..customers {
        let (http, widget, url, ledger) = (http.clone(), f.widget.clone(), url.clone(), ledger.clone());
        set.spawn(async move {
            let token = loop {
                match customer_session(&http, &widget, &format!("chaos {i}")).await {
                    Ok(t) => break t,
                    Err(_) => tokio::time::sleep(Duration::from_millis(300)).await,
                }
            };
            let mut k = 0u64;
            let mut pending: Option<(String, String)> = None;
            let mut last_seq = 0i64;
            let mut first = true;
            while Instant::now() < stop_at || pending.is_some() {
                if Instant::now() > stop_at + Duration::from_secs(30) {
                    break;
                }
                let Ok((mut ws, w)) = customer_connect(&url, &token, last_seq).await else {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                };
                {
                    let mut l = ledger.lock().await;
                    if !first {
                        l.reconnects += 1;
                    }
                    *l.customer_nodes.entry(w["node"].as_str().unwrap_or("?").to_string()).or_default() += 1;
                }
                first = false;
                for m in w["messages"].as_array().into_iter().flatten() {
                    last_seq = last_seq.max(m["seq"].as_i64().unwrap_or(0));
                }
                loop {
                    let (cid, body) = match &pending {
                        Some(p) => {
                            ledger.lock().await.resent += 1;
                            p.clone()
                        }
                        None => {
                            if Instant::now() >= stop_at {
                                break;
                            }
                            k += 1;
                            (format!("c{i}-{k}"), format!("chaos customer {i} message {k}"))
                        }
                    };
                    pending = Some((cid.clone(), body.clone()));
                    if send(&mut ws, json!({ "type": "message.send", "client_msg_id": cid, "body": body })).await.is_err() {
                        break;
                    }
                    let ack = loop {
                        match next_json(&mut ws, Duration::from_secs(10)).await {
                            Ok(v) if v["type"] == "ack" && v["client_msg_id"] == cid.as_str() => break Some(v),
                            // Per-customer inbound rate limit (OCC-M10-R041): back off, re-send.
                            Ok(v) if v["type"] == "error" && v["code"] == "RATE_LIMITED" => {
                                ledger.lock().await.rate_limited += 1;
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                if send(&mut ws, json!({ "type": "message.send", "client_msg_id": cid, "body": body })).await.is_err() {
                                    break None;
                                }
                            }
                            Ok(_) => continue,
                            Err(_) => break None,
                        }
                    };
                    let Some(ack) = ack else { break };
                    pending = None;
                    let mut l = ledger.lock().await;
                    l.acked.insert(ack["message"]["id"].as_str().unwrap_or_default().to_string(), body);
                    if let Some(c) = ack["conversation_id"].as_str() {
                        l.conversations.insert(c.to_string());
                    }
                    last_seq = last_seq.max(ack["message"]["seq"].as_i64().unwrap_or(0));
                    drop(l);
                    tokio::time::sleep(interval).await;
                }
                if Instant::now() >= stop_at && pending.is_none() {
                    break;
                }
            }
        });
    }
    let mut last_report = Instant::now();
    while !set.is_empty() {
        tokio::select! {
            _ = set.join_next() => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
        if last_report.elapsed() > Duration::from_secs(5) {
            let l = ledger.lock().await;
            println!(
                "  acked={} customer_reconnects={} agent_reconnects={}",
                l.acked.len(),
                l.reconnects,
                agent_reconnects.load(Ordering::Relaxed)
            );
            last_report = Instant::now();
        }
    }
    // Let agents catch up (resume after their own reconnects).
    let l = ledger.lock().await;
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        let seen = agent_seen.lock().await;
        if l.acked.keys().all(|id| seen.contains(id)) || Instant::now() > until {
            break;
        }
        drop(seen);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Verify against the database through the Tenant Admin timeline API.
    let mut stored = HashSet::new();
    for c in &l.conversations {
        let (st, b) = f.http.call("GET", &format!("/v1/hub/conversations/{c}/messages"), Some(&f.ta), None).await?;
        if st != 200 {
            return Err(format!("timeline {c}: HTTP {st}"));
        }
        let mut last = 0;
        for m in b["data"]["messages"].as_array().into_iter().flatten() {
            let seq = m["seq"].as_i64().unwrap_or(0);
            if seq != last + 1 {
                return Err(format!("sequence gap in conversation {c}: {last} → {seq}"));
            }
            last = seq;
            stored.insert(m["id"].as_str().unwrap_or_default().to_string());
        }
    }
    let seen = agent_seen.lock().await;
    let lost_db: Vec<_> = l.acked.keys().filter(|id| !stored.contains(*id)).collect();
    let lost_agent: Vec<_> = l.acked.keys().filter(|id| !seen.contains(*id)).collect();
    println!(
        "RESULT chaos: acked={} stored_missing={} agent_missing={} customer_reconnects={} agent_reconnects={} resent_after_reconnect={} rate_limited={}",
        l.acked.len(),
        lost_db.len(),
        lost_agent.len(),
        l.reconnects,
        agent_reconnects.load(Ordering::Relaxed),
        l.resent,
        l.rate_limited
    );
    println!("  customer connections per node: {:?}", l.customer_nodes);
    println!("  agent connections per node:    {:?}", agent_nodes.lock().await);
    // Close the scenario's conversations so the agent pool has capacity next time.
    for tok in &f.agents {
        if let Ok(mut ws) = agent_connect(&http, tok).await {
            let _ = send(&mut ws, json!({ "type": "hello" })).await;
            if let Ok(w) = wait_type(&mut ws, "welcome", Duration::from_secs(5)).await {
                for c in w["conversations"].as_array().into_iter().flatten() {
                    let _ = send(&mut ws, json!({ "type": "conversation.close", "conversation_id": c["conversation"]["id"] })).await;
                }
            }
            let _ = send(&mut ws, json!({ "type": "presence.set", "status": "offline" })).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }
    if l.acked.is_empty() {
        return Err("no messages were acknowledged".into());
    }
    if !lost_db.is_empty() || !lost_agent.is_empty() {
        return Err(format!("acknowledged messages lost: db={} agent={}", lost_db.len(), lost_agent.len()));
    }
    println!("zero acknowledged messages lost ✔");
    Ok(())
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    let a = Args::parse();
    let r = match a.cmd.as_str() {
        "smoke" => smoke(&a.s("base", "http://localhost:3000")).await,
        "idle" => idle(&a).await,
        "latency" => latency(&a).await,
        "chaos" => chaos(&a).await,
        _ => {
            eprintln!("usage: hub_load smoke|idle|latency|chaos [--base URL] (see source header)");
            std::process::exit(2);
        }
    };
    if let Err(e) = r {
        eprintln!("\x1b[31m{e}\x1b[0m");
        std::process::exit(1);
    }
}
