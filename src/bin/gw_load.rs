//! Load generator and ledger for the bake-off gateway (ADR-0014; eval harness E1–E9, scaled to the
//! machine it runs on). Latency is measured with this process's clock only.
//!
//! gw_load ramp   --ws ws://gw1:4000 [--sessions N] [--rate R] [--hold SECS] [--prefix P] [--reconnect]
//!     E1 / E7: open N idle customer sessions at R/s, ping every 30 s, report pong latency and how
//!     many sessions dropped (with --reconnect: resumed) while holding.
//! gw_load load   --base http://gw-lb:8088 [--agents 200] [--customers 2000] [--rate 500]
//!                [--duration 60] [--reply-every 10] [--json FILE]
//!     E2–E6: agents on WebSockets (status available, resume on disconnect), customers post to
//!     /ingress/whatsapp at a fixed rate; reports inbound→agent delivery p50/p95/p99, error rate,
//!     duplicates, then reconciles the ledger (every 202 / ack must be stored, seq contiguous,
//!     every acknowledged inbound message delivered to an agent).
//!
//! Every request uses `GW_TOKEN` (default dev-gateway-token-change-me).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Args(HashMap<String, String>);

impl Args {
    fn parse() -> (String, Self) {
        let mut it = std::env::args().skip(1);
        let mode = it.next().unwrap_or_default();
        let mut m = HashMap::new();
        let rest: Vec<String> = it.collect();
        let mut i = 0;
        while i < rest.len() {
            if let Some(k) = rest[i].strip_prefix("--") {
                if i + 1 < rest.len() && !rest[i + 1].starts_with("--") {
                    m.insert(k.to_string(), rest[i + 1].clone());
                    i += 2;
                } else {
                    m.insert(k.to_string(), "true".into());
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
        (mode, Args(m))
    }
    fn s(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    fn n(&self, k: &str, d: u64) -> u64 {
        self.0.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
    fn flag(&self, k: &str) -> bool {
        self.0.contains_key(k)
    }
}

fn token() -> String {
    std::env::var("GW_TOKEN").unwrap_or_else(|_| "dev-gateway-token-change-me".into())
}

fn ws_config() -> WebSocketConfig {
    WebSocketConfig::default().read_buffer_size(4096).write_buffer_size(4096)
}

fn ws_base(http: &str) -> String {
    http.replacen("http://", "ws://", 1).replacen("https://", "wss://", 1)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::main]
async fn main() {
    let (mode, args) = Args::parse();
    let r = match mode.as_str() {
        "ramp" => ramp(&args).await,
        "load" => load(&args).await,
        _ => {
            eprintln!("usage: gw_load ramp|load [--options]  (see the file header)");
            std::process::exit(2);
        }
    };
    if let Err(e) = r {
        eprintln!("gw_load: {e}");
        std::process::exit(1);
    }
}

// ------------------------------------------------------------------------------------------------
// E1 / E7: idle sessions
// ------------------------------------------------------------------------------------------------

async fn ramp(a: &Args) -> R<()> {
    let base = a.s("ws", "ws://gw1:4000");
    let n = a.n("sessions", 1000) as usize;
    let rate = a.n("rate", 1000).max(1);
    let hold = Duration::from_secs(a.n("hold", 60));
    let prefix = a.s("prefix", &format!("idle-{}", std::process::id()));
    let reconnect = a.flag("reconnect");
    let url = format!("{base}/ws/customer?token={}", token());

    let connected = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let pongs = Arc::new(Mutex::new(Vec::<f64>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    let gap = Duration::from_secs_f64(1.0 / rate as f64);
    let mut next_at = tokio::time::Instant::now();
    for i in 0..n {
        next_at += gap;
        tokio::time::sleep_until(next_at).await;
        let (url, id) = (url.clone(), format!("{prefix}-{i}"));
        let (c_connected, c_failed, c_dropped, c_resumed, c_pongs, c_stop) =
            (connected.clone(), failed.clone(), dropped.clone(), resumed.clone(), pongs.clone(), stop.clone());
        tasks.spawn(async move {
            let (connected, failed, dropped, resumed, pongs, stop) = (c_connected, c_failed, c_dropped, c_resumed, c_pongs, c_stop);
            let mut session: Option<String> = None;
            let mut first = true;
            loop {
                // Counted at `welcome`: a first open is "connected", later ones "resumed".
                let counter = if first { &connected } else { &resumed };
                let opened = idle_session(&url, &id, session.as_deref(), &pongs, &stop, counter).await;
                match opened {
                    Ok((sid, ended_by_us)) => {
                        first = false;
                        session = Some(sid);
                        if ended_by_us {
                            return;
                        }
                        dropped.fetch_add(1, Ordering::Relaxed);
                        if !reconnect {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(100 + (rand_u64() % 900))).await;
                    }
                    Err(_) if first => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
                }
                if stop.load(Ordering::Relaxed) {
                    return;
                }
            }
        });
        if i % 10_000 == 9_999 {
            println!(
                "  {:>6.1}s opened {} connected {} failed {}",
                started.elapsed().as_secs_f64(),
                i + 1,
                connected.load(Ordering::Relaxed),
                failed.load(Ordering::Relaxed)
            );
        }
    }
    // Wait for the ramp's last handshakes.
    let settle = Instant::now();
    while connected.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed) < n && settle.elapsed() < Duration::from_secs(60) {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let ramp_secs = started.elapsed().as_secs_f64();
    println!(
        "ramp done in {ramp_secs:.1}s: connected {} failed {}; holding {}s",
        connected.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        hold.as_secs()
    );
    let hold_start = Instant::now();
    while hold_start.elapsed() < hold {
        tokio::time::sleep(Duration::from_secs(10).min(hold)).await;
        println!(
            "  hold {:>5.0}s connected {} dropped {} resumed {}",
            hold_start.elapsed().as_secs_f64(),
            connected.load(Ordering::Relaxed),
            dropped.load(Ordering::Relaxed),
            resumed.load(Ordering::Relaxed)
        );
    }
    stop.store(true, Ordering::Relaxed);
    let mut p = pongs.lock().map(|g| g.clone()).unwrap_or_default();
    p.sort_by(f64::total_cmp);
    let report = json!({
        "mode": "ramp", "sessions": n, "connected": connected.load(Ordering::Relaxed),
        "failed": failed.load(Ordering::Relaxed), "dropped": dropped.load(Ordering::Relaxed),
        "resumed": resumed.load(Ordering::Relaxed), "ramp_secs": ramp_secs,
        "pong_ms": { "samples": p.len(), "p50": pct(&p, 50.0), "p99": pct(&p, 99.0), "max": p.last().copied().unwrap_or(f64::NAN) },
    });
    println!("RESULT {report}");
    tasks.abort_all();
    Ok(())
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(Instant::now().elapsed().as_nanos());
    h.finish()
}

/// One idle customer connection: hello (or resume), then ping every 30 s. Returns the session id
/// and whether we ended it (stop flag) rather than the server.
async fn idle_session(
    url: &str,
    id: &str,
    resume: Option<&str>,
    pongs: &Mutex<Vec<f64>>,
    stop: &AtomicBool,
    opened: &AtomicUsize,
) -> R<(String, bool)> {
    let (ws, _) =
        tokio::time::timeout(Duration::from_secs(20), tokio_tungstenite::connect_async_with_config(url, Some(ws_config()), false))
            .await
            .map_err(|_| "connect timeout")??;
    let (mut tx, mut rx) = ws.split();
    let first = match resume {
        Some(s) => json!({"type": "resume", "session_id": s, "last_seq_by_conversation": {}}),
        None => json!({"type": "hello", "role": "customer", "id": id}),
    };
    tx.send(Message::Text(first.to_string().into())).await?;
    let session = loop {
        match tokio::time::timeout(Duration::from_secs(20), rx.next()).await.map_err(|_| "welcome timeout")? {
            Some(Ok(Message::Text(t))) => {
                let v: Value = serde_json::from_str(&t)?;
                if v["type"] == "welcome" {
                    opened.fetch_add(1, Ordering::Relaxed);
                    break v["session_id"].as_str().unwrap_or_default().to_string();
                }
            }
            Some(Ok(_)) => continue,
            _ => return Err("closed before welcome".into()),
        }
    };
    // Spread pings over the 30 s period so 100k sessions do not ping in lockstep.
    let mut tick =
        tokio::time::interval_at(tokio::time::Instant::now() + Duration::from_millis(rand_u64() % 30_000), Duration::from_secs(30));
    let mut sent_at: Option<Instant> = None;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if stop.load(Ordering::Relaxed) {
                    let _ = tx.close().await;
                    return Ok((session, true));
                }
                sent_at = Some(Instant::now());
                if tx.send(Message::Text(r#"{"type":"ping"}"#.into())).await.is_err() {
                    return Ok((session, false));
                }
            }
            m = rx.next() => match m {
                Some(Ok(Message::Text(t))) => {
                    if t.contains("\"pong\"") {
                        if let Some(s) = sent_at.take() {
                            if let Ok(mut p) = pongs.lock() { p.push(s.elapsed().as_secs_f64() * 1000.0); }
                        }
                    }
                }
                Some(Ok(_)) => {}
                _ => return Ok((session, stop.load(Ordering::Relaxed))),
            }
        }
    }
}

// ------------------------------------------------------------------------------------------------
// E2–E6: steady load with ledger
// ------------------------------------------------------------------------------------------------

#[derive(Default)]
struct Ledger {
    /// external_id → instant the POST was sent (until an agent receives it)
    pending: Mutex<HashMap<String, Instant>>,
    latencies_ms: Mutex<Vec<f64>>,
    /// conversation → message ids acknowledged by the gateway (202 or ack)
    acked: Mutex<HashMap<String, Vec<String>>>,
    /// inbound message ids acknowledged (must reach an agent)
    acked_inbound: Mutex<HashSet<String>>,
    /// message ids seen by any agent session (after per-session de-duplication)
    delivered: Mutex<HashSet<String>>,
    duplicates: AtomicU64,
    posts: AtomicU64,
    post_errors: AtomicU64,
    post_failed: AtomicU64,
    stored_unacked: AtomicU64,
    agent_reconnects: AtomicU64,
    agent_drops: AtomicU64,
    replies: AtomicU64,
    closed: AtomicU64,
    close_errors: Mutex<HashMap<String, u64>>,
    /// agent session lost → `welcome` again (resume), ms
    reconnect_ms: Mutex<Vec<f64>>,
}

async fn load(a: &Args) -> R<()> {
    let base = a.s("base", "http://gw-lb:8088");
    let agents = a.n("agents", 200) as usize;
    let customers = a.n("customers", 2000) as usize;
    let rate = a.n("rate", 500).max(1);
    let duration = Duration::from_secs(a.n("duration", 60));
    let reply_every = a.n("reply-every", 10);
    let run = a.s("run", &format!("{:x}", rand_u64() & 0xffffff));
    let ledger = Arc::new(Ledger::default());
    let stop = Arc::new(AtomicBool::new(false));
    let http = reqwest::Client::builder().pool_max_idle_per_host(256).timeout(Duration::from_secs(10)).build()?;

    // Agents first: they must be available before traffic starts.
    let ready = Arc::new(AtomicUsize::new(0));
    let (wrap_tx, wrap_rx) = tokio::sync::watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..agents {
        let id = format!("agent-{:03}", i + 1);
        let (base, ledger, stop, ready, wrap) = (base.clone(), ledger.clone(), stop.clone(), ready.clone(), wrap_rx.clone());
        tasks.spawn(async move { agent_loop(base, id, ledger, stop, wrap, ready, reply_every).await });
    }
    let t0 = Instant::now();
    while ready.load(Ordering::Relaxed) < agents && t0.elapsed() < Duration::from_secs(60) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!(
        "{} of {agents} agents available; sending {rate} msg/s from {customers} customers for {}s",
        ready.load(Ordering::Relaxed),
        duration.as_secs()
    );

    // Inbound traffic at a fixed rate (open loop: a slow gateway does not slow the generator).
    let started = Instant::now();
    let total = rate * duration.as_secs();
    let gap = Duration::from_secs_f64(1.0 / rate as f64);
    let mut next_at = tokio::time::Instant::now();
    let inflight = Arc::new(tokio::sync::Semaphore::new(4096));
    let mut last_report = Instant::now();
    for i in 0..total {
        next_at += gap;
        tokio::time::sleep_until(next_at).await;
        let permit = inflight.clone().acquire_owned().await?;
        let (t_http, t_base, t_ledger) = (http.clone(), base.clone(), ledger.clone());
        let customer = format!("ld-{run}-{}", i as usize % customers);
        let ext = format!("{run}-{i}");
        tokio::spawn(async move {
            post_inbound(&t_http, &t_base, &customer, &ext, &t_ledger).await;
            drop(permit);
        });
        if last_report.elapsed() >= Duration::from_secs(10) {
            last_report = Instant::now();
            let l = ledger.latencies_ms.lock().map(|v| v.len()).unwrap_or(0);
            println!(
                "  {:>5.0}s sent {} delivered {} post_errors {} agent_reconnects {}",
                started.elapsed().as_secs_f64(),
                ledger.posts.load(Ordering::Relaxed),
                l,
                ledger.post_errors.load(Ordering::Relaxed),
                ledger.agent_reconnects.load(Ordering::Relaxed)
            );
        }
    }
    let send_secs = started.elapsed().as_secs_f64();
    // Let in-flight requests finish and deliveries arrive.
    let _ = inflight.acquire_many(4096).await;
    let drain = Instant::now();
    // Without agents (ingest-only runs) nothing is delivered; do not wait for it.
    while agents > 0 && drain.elapsed() < Duration::from_secs(20) {
        let missing = {
            let acked = ledger.acked_inbound.lock().map(|s| s.clone()).unwrap_or_default();
            let delivered = ledger.delivered.lock().map(|s| s.clone()).unwrap_or_default();
            acked.difference(&delivered).count()
        };
        if missing == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    stop.store(true, Ordering::Relaxed);
    // Agents close their conversations (so the next run starts with empty queues), then leave.
    let _ = wrap_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(30), async { while tasks.join_next().await.is_some() {} }).await;

    // Reconcile against the gateway's store.
    let acked = ledger.acked.lock().map(|m| m.clone()).unwrap_or_default();
    let mut lost = 0usize;
    let mut gaps = 0usize;
    for (conv, ids) in &acked {
        let url = format!("{base}/conversations/{conv}/messages?after=0");
        let mut stored: Option<Vec<Value>> = None;
        for _ in 0..5 {
            if let Ok(r) = http.get(&url).bearer_auth(token()).send().await {
                if let Ok(v) = r.json::<Value>().await {
                    stored = v["messages"].as_array().cloned();
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let stored = stored.unwrap_or_default();
        let have: HashSet<&str> = stored.iter().filter_map(|m| m["message_id"].as_str()).collect();
        lost += ids.iter().filter(|id| !have.contains(id.as_str())).count();
        let seqs: Vec<i64> = stored.iter().filter_map(|m| m["seq"].as_i64()).collect();
        if seqs != (1..=seqs.len() as i64).collect::<Vec<_>>() {
            gaps += 1;
        }
    }
    let undelivered = {
        let acked = ledger.acked_inbound.lock().map(|s| s.clone()).unwrap_or_default();
        let delivered = ledger.delivered.lock().map(|s| s.clone()).unwrap_or_default();
        acked.difference(&delivered).count()
    };
    let mut lat = ledger.latencies_ms.lock().map(|v| v.clone()).unwrap_or_default();
    lat.sort_by(f64::total_cmp);
    let posts = ledger.posts.load(Ordering::Relaxed);
    let failed = ledger.post_failed.load(Ordering::Relaxed);
    let reconnect = {
        let mut r = ledger.reconnect_ms.lock().map(|v| v.clone()).unwrap_or_default();
        r.sort_by(f64::total_cmp);
        json!({ "samples": r.len(), "p50": pct(&r, 50.0), "max": r.last().copied().unwrap_or(f64::NAN), "within_30s": r.iter().filter(|x| **x <= 30_000.0).count() })
    };
    let report = json!({
        "mode": "load", "run": run, "rate_target": rate, "rate_achieved": posts as f64 / send_secs,
        "duration_s": send_secs, "agents": agents, "customers": customers,
        "posts": posts, "post_attempt_errors": ledger.post_errors.load(Ordering::Relaxed),
        "post_failed": failed, "error_rate_pct": if posts > 0 { failed as f64 * 100.0 / posts as f64 } else { 0.0 },
        "stored_but_unacked": ledger.stored_unacked.load(Ordering::Relaxed),
        "acked_messages": acked.values().map(Vec::len).sum::<usize>(), "conversations": acked.len(),
        "lost_acked": lost, "conversations_with_seq_gaps": gaps, "undelivered_to_agent": undelivered,
        "duplicates_delivered": ledger.duplicates.load(Ordering::Relaxed),
        "agent_reconnects": ledger.agent_reconnects.load(Ordering::Relaxed),
        "agent_replies": ledger.replies.load(Ordering::Relaxed),
        "conversations_closed_at_end": ledger.closed.load(Ordering::Relaxed),
        "close_errors": ledger.close_errors.lock().map(|e| e.clone()).unwrap_or_default(),
        "agent_reconnect_ms": reconnect,
        "latency_ms": { "samples": lat.len(), "p50": pct(&lat, 50.0), "p95": pct(&lat, 95.0), "p99": pct(&lat, 99.0), "max": lat.last().copied().unwrap_or(f64::NAN) },
    });
    println!("RESULT {report}");
    if let Some(path) = a.0.get("json") {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    tasks.abort_all();
    if lost > 0 || gaps > 0 {
        return Err(format!("{lost} acknowledged messages lost, {gaps} conversations with seq gaps").into());
    }
    Ok(())
}

async fn post_inbound(http: &reqwest::Client, base: &str, customer: &str, ext: &str, ledger: &Ledger) {
    ledger.posts.fetch_add(1, Ordering::Relaxed);
    let body = json!({ "external_id": ext, "from": customer, "text": format!("load {ext}"), "sent_at": chrono::Utc::now().to_rfc3339() });
    if let Ok(mut p) = ledger.pending.lock() {
        p.insert(ext.to_string(), Instant::now());
    }
    for attempt in 0..3 {
        match http.post(format!("{base}/ingress/whatsapp")).bearer_auth(token()).json(&body).send().await {
            Ok(r) if r.status().as_u16() == 202 => {
                if let Ok(v) = r.json::<Value>().await {
                    let (conv, mid) = (v["conversation_id"].as_str().unwrap_or(""), v["message_id"].as_str().unwrap_or(""));
                    if let Ok(mut m) = ledger.acked.lock() {
                        m.entry(conv.to_string()).or_default().push(mid.to_string());
                    }
                    if let Ok(mut s) = ledger.acked_inbound.lock() {
                        s.insert(mid.to_string());
                    }
                }
                return;
            }
            Ok(r) if r.status().as_u16() == 409 => {
                // An earlier attempt was stored but its 202 never reached us: not acknowledged.
                ledger.stored_unacked.fetch_add(1, Ordering::Relaxed);
                return;
            }
            _ => {
                ledger.post_errors.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(200 * (attempt + 1))).await;
            }
        }
    }
    ledger.post_failed.fetch_add(1, Ordering::Relaxed);
}

/// One agent: connect, `status available`, record deliveries; on any disconnect resume with the
/// session id and per-conversation cursors. When the run ends it closes (`disposition`) every
/// conversation assigned to it, so the next run starts with empty queues.
async fn agent_loop(
    base: String,
    id: String,
    ledger: Arc<Ledger>,
    stop: Arc<AtomicBool>,
    mut wrap_up: tokio::sync::watch::Receiver<bool>,
    ready: Arc<AtomicUsize>,
    reply_every: u64,
) {
    let url = format!("{}/ws/agent?token={}", ws_base(&base), token());
    let mut session: Option<String> = None;
    let mut cursors: HashMap<String, i64> = HashMap::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut counted_ready = false;
    let mut received: u64 = 0;
    let mut mine: HashSet<String> = HashSet::new();
    let mut dropped_at: Option<Instant> = None;
    while !stop.load(Ordering::Relaxed) {
        let conn = tokio_tungstenite::connect_async_with_config(&url, Some(ws_config()), false).await;
        let Ok((ws, _)) = conn else {
            tokio::time::sleep(Duration::from_millis(300)).await;
            continue;
        };
        let (mut tx, mut rx) = ws.split();
        let open = match &session {
            Some(s) => json!({"type": "resume", "session_id": s, "last_seq_by_conversation": cursors}),
            None => json!({"type": "hello", "role": "agent", "id": id, "skills": ["chat"]}),
        };
        if tx.send(Message::Text(open.to_string().into())).await.is_err() {
            continue;
        }
        if tx.send(Message::Text(json!({"type": "status", "available": true}).to_string().into())).await.is_err() {
            continue;
        }
        let mut ping = tokio::time::interval(Duration::from_secs(20));
        let ended_by_server = loop {
            tokio::select! {
                _ = wrap_up.changed() => {
                    let convs: Vec<String> = mine.drain().collect();
                    for (i, conv) in convs.iter().enumerate() {
                        let f = json!({"type": "disposition", "conversation_id": conv, "code": "load-done", "client_ref": format!("{id}-done-{i}")});
                        if tx.send(Message::Text(f.to_string().into())).await.is_err() { break; }
                    }
                    // Wait for the answers (ack or error) so the closes are durable before we leave.
                    let mut answered = 0;
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
                    while answered < convs.len() {
                        match tokio::time::timeout_at(deadline, rx.next()).await {
                            Ok(Some(Ok(Message::Text(t)))) => {
                                let Ok(v) = serde_json::from_str::<Value>(&t) else { continue };
                                let is_close = v["client_ref"].as_str().or(v["ref"].as_str()).is_some_and(|r| r.starts_with(&format!("{id}-done-")));
                                if !is_close { continue; }
                                answered += 1;
                                if v["type"] == "error" {
                                    let code = v["code"].as_str().unwrap_or("?").to_string();
                                    if let Ok(mut e) = ledger.close_errors.lock() { *e.entry(code).or_insert(0) += 1; }
                                }
                            }
                            Ok(Some(Ok(_))) => {}
                            _ => break,
                        }
                    }
                    ledger.closed.fetch_add(answered as u64, Ordering::Relaxed);
                    let _ = tx.close().await;
                    break false;
                }
                _ = ping.tick() => {
                    if stop.load(Ordering::Relaxed) { let _ = tx.close().await; break false; }
                    if tx.send(Message::Text(r#"{"type":"ping"}"#.into())).await.is_err() { break true; }
                }
                m = rx.next() => {
                    let Some(Ok(Message::Text(t))) = m else {
                        if matches!(m, Some(Ok(_))) { continue; }
                        break true;
                    };
                    let Ok(v) = serde_json::from_str::<Value>(&t) else { continue };
                    match v["type"].as_str() {
                        Some("welcome") => {
                            session = v["session_id"].as_str().map(str::to_string);
                            if let Some(t) = dropped_at.take() {
                                if let Ok(mut r) = ledger.reconnect_ms.lock() { r.push(t.elapsed().as_secs_f64() * 1000.0); }
                            }
                        }
                        Some("status") if !counted_ready => {
                            counted_ready = true;
                            ready.fetch_add(1, Ordering::Relaxed);
                        }
                        Some("message") => {
                            let m = &v["message"];
                            let mid = m["message_id"].as_str().unwrap_or("").to_string();
                            let conv = m["conversation_id"].as_str().unwrap_or("").to_string();
                            if let Some(seq) = m["seq"].as_i64() {
                                let c = cursors.entry(conv.clone()).or_insert(0);
                                *c = (*c).max(seq);
                            }
                            if !seen.insert(mid.clone()) {
                                ledger.duplicates.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            if m["kind"] == "assignment" {
                                if m["body"]["agent_id"] == id.as_str() { mine.insert(conv.clone()); } else { mine.remove(&conv); }
                            }
                            if m["direction"] == "inbound" {
                                if let Ok(mut d) = ledger.delivered.lock() { d.insert(mid.clone()); }
                                if let Some(ext) = m["external_id"].as_str() {
                                    let sent = ledger.pending.lock().ok().and_then(|mut p| p.remove(ext));
                                    if let Some(s) = sent {
                                        if let Ok(mut l) = ledger.latencies_ms.lock() { l.push(s.elapsed().as_secs_f64() * 1000.0); }
                                    }
                                }
                                received += 1;
                                if reply_every > 0 && received % reply_every == 0 {
                                    let frame = json!({"type": "send", "conversation_id": conv, "text": "reply", "client_ref": format!("{id}-{received}")});
                                    if tx.send(Message::Text(frame.to_string().into())).await.is_err() { break true; }
                                }
                            }
                        }
                        Some("ack") => {
                            ledger.replies.fetch_add(1, Ordering::Relaxed);
                            if let (Some(conv), Some(mid)) = (v["conversation_id"].as_str(), v["message_id"].as_str()) {
                                if let Ok(mut m) = ledger.acked.lock() { m.entry(conv.to_string()).or_default().push(mid.to_string()); }
                            }
                        }
                        _ => {}
                    }
                }
            }
        };
        if !ended_by_server {
            return;
        }
        ledger.agent_drops.fetch_add(1, Ordering::Relaxed);
        ledger.agent_reconnects.fetch_add(1, Ordering::Relaxed);
        dropped_at.get_or_insert_with(Instant::now);
        tokio::time::sleep(Duration::from_millis(100 + rand_u64() % 400)).await;
    }
}
