//! Customer and agent WebSocket sessions (`/ws/customer`, `/ws/agent`, token as `?token=`).
//!
//! Frames are JSON objects with a `type` field (docs/architecture/gateway-protocol.md):
//! * client → server: `hello{role, id, skills[]}`, `resume{session_id, last_seq_by_conversation{}}`,
//!   `ping`, `send{conversation_id, text, client_ref}`, `status{available}` (agent),
//!   `disposition{conversation_id, code}` (agent)
//! * server → client: `welcome{session_id, resume_from{}}`, `pong`, `ack{client_ref, message_id,
//!   seq, conversation_id}`, `message{message}`, `presence{agent_id, available}` (agent),
//!   `status{available}` (agent), `error{code, message, ref}`, `reconnect`
//!
//! Delivery is at-least-once and in order per conversation: each session keeps the last `seq` it
//! delivered per conversation; an event that skips ahead is preceded by the missing messages read
//! from the store (gap fill), an event at or below the cursor is dropped. A bad token gets close
//! 4401 before `welcome`; no `ping` for 60 s gets close 4408; a slow consumer gets 1013; a node
//! shutting down sends `reconnect` and close 1012 (spread over two seconds to avoid a storm).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use chrono::Utc;
use rand::Rng;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::Instant;

use super::super::application::sessions::Registration;
use super::super::application::{Gateway, GwError, GwEvent};
use super::super::domain::{ulid, ActorKind, CanonicalMessage, MAX_ID};
use super::GwState;

const PING_TIMEOUT: Duration = Duration::from_secs(60);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FRAME: usize = 64 * 1024;
/// Small socket buffers: per-connection memory decides how many idle sessions a node holds.
const READ_BUFFER: usize = 4 * 1024;
const WRITE_BUFFER: usize = 4 * 1024;
const MAX_WRITE_BUFFER: usize = 1024 * 1024;
const REPLAY_PAGE: i64 = 500;

pub fn routes() -> Router<GwState> {
    Router::new().route("/ws/customer", get(customer_ws)).route("/ws/agent", get(agent_ws))
}

#[derive(Deserialize)]
struct TokenQuery {
    #[serde(default)]
    token: String,
}

async fn customer_ws(State(st): State<GwState>, Query(q): Query<TokenQuery>, ws: WebSocketUpgrade) -> Response {
    upgrade(st, q, ws, ActorKind::Customer)
}

async fn agent_ws(State(st): State<GwState>, Query(q): Query<TokenQuery>, ws: WebSocketUpgrade) -> Response {
    upgrade(st, q, ws, ActorKind::Agent)
}

fn upgrade(st: GwState, q: TokenQuery, ws: WebSocketUpgrade, role: ActorKind) -> Response {
    if !st.gw.is_ready() {
        return (StatusCode::SERVICE_UNAVAILABLE, "node is not ready").into_response();
    }
    let authed = st.token_ok(&q.token);
    let gw = st.gw.clone();
    ws.max_message_size(MAX_FRAME)
        .max_frame_size(MAX_FRAME)
        .read_buffer_size(READ_BUFFER)
        .write_buffer_size(WRITE_BUFFER)
        .max_write_buffer_size(MAX_WRITE_BUFFER)
        .on_upgrade(move |mut socket| async move {
            if !authed {
                close(&mut socket, 4401, "unauthorized").await;
                return;
            }
            Session::run(gw, socket, role).await;
        })
}

async fn close(socket: &mut WebSocket, code: u16, reason: &'static str) {
    let _ = socket.send(Message::Close(Some(CloseFrame { code, reason: reason.into() }))).await;
}

fn frame_error(code: &str, message: &str, reference: Option<&str>) -> Value {
    json!({ "type": "error", "code": code, "message": message, "ref": reference })
}

fn gw_error(e: &GwError, reference: Option<&str>) -> Value {
    if let GwError::Internal(err) = e {
        tracing::error!(error = %err, "websocket action failed");
    }
    frame_error(e.code(), &e.to_string(), reference)
}

fn valid_id(s: &str) -> bool {
    let s = s.trim();
    !s.is_empty() && s.len() <= MAX_ID && !s.chars().any(char::is_control)
}

/// How the client opened the session.
enum Opening {
    Hello { id: String, skills: Vec<String> },
    Resume { session_id: String, id: String, last_seq: HashMap<String, i64> },
}

struct Session {
    gw: Arc<Gateway>,
    socket: WebSocket,
    role: ActorKind,
    id: String,
    session_id: String,
    connection_id: String,
    /// Last `seq` delivered per conversation.
    cursors: HashMap<String, i64>,
}

enum Flow {
    Continue,
    Close(u16, &'static str),
}

impl Session {
    async fn run(gw: Arc<Gateway>, mut socket: WebSocket, role: ActorKind) {
        let opening = match Self::handshake(&gw, &mut socket, role).await {
            Ok(o) => o,
            Err((code, reason, frame)) => {
                if let Some(f) = frame {
                    let _ = socket.send(Message::Text(f.to_string().into())).await;
                }
                close(&mut socket, code, reason).await;
                return;
            }
        };
        let (id, session_id, resume) = match &opening {
            Opening::Hello { id, .. } => (id.clone(), gw.issue_session(role, id), None),
            Opening::Resume { session_id, id, last_seq } => (id.clone(), session_id.clone(), Some(last_seq.clone())),
        };
        // Register before reading subscriptions so no event published in between is missed.
        let mut reg = gw.sessions.register(role, &id);
        let mut s = Session { gw: gw.clone(), socket, role, id, session_id, connection_id: ulid(), cursors: HashMap::new() };
        let flow = match s.open(&opening, resume).await {
            Ok(()) => s.serve(&mut reg).await,
            Err(e) => {
                let _ = s.send(gw_error(&e, None)).await;
                Flow::Close(1011, "internal error")
            }
        };
        if let Flow::Close(code, reason) = flow {
            close(&mut s.socket, code, reason).await;
        }
        gw.sessions.unregister(&reg);
        if role == ActorKind::Agent {
            gw.agent_disconnected(&s.id, &s.connection_id).await;
        }
    }

    /// Waits for `hello` or `resume` and checks the identity against the endpoint.
    async fn handshake(gw: &Gateway, socket: &mut WebSocket, role: ActorKind) -> Result<Opening, (u16, &'static str, Option<Value>)> {
        let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
        loop {
            let msg = match tokio::time::timeout_at(deadline, socket.recv()).await {
                Err(_) => return Err((4408, "no hello", None)),
                Ok(None) | Ok(Some(Err(_))) => return Err((1000, "closed", None)),
                Ok(Some(Ok(m))) => m,
            };
            let text = match msg {
                Message::Text(t) => t,
                Message::Close(_) => return Err((1000, "closed", None)),
                _ => continue,
            };
            let v: Value = serde_json::from_str(&text)
                .map_err(|_| (4400, "bad frame", Some(frame_error("invalid", "frames are JSON objects", None))))?;
            let bad = |m: &str| (4400, "bad handshake", Some(frame_error("invalid", m, None)));
            match v["type"].as_str() {
                Some("ping") => {
                    let _ = socket.send(Message::Text(json!({"type": "pong"}).to_string().into())).await;
                }
                Some("hello") => {
                    let id = v["id"].as_str().unwrap_or("").trim().to_string();
                    if !valid_id(&id) {
                        return Err(bad("hello.id must be 1..128 printable characters"));
                    }
                    if v["role"].as_str() != Some(role.as_str()) {
                        return Err(bad("hello.role does not match this endpoint"));
                    }
                    let skills = v["skills"]
                        .as_array()
                        .map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_string)).filter(|s| valid_id(s)).collect())
                        .unwrap_or_default();
                    return Ok(Opening::Hello { id, skills });
                }
                Some("resume") => {
                    let session_id = v["session_id"].as_str().unwrap_or("");
                    let Some((r, id)) = gw.verify_session(session_id) else {
                        return Err(bad("unknown session_id"));
                    };
                    if r != role {
                        return Err(bad("session belongs to the other endpoint"));
                    }
                    let last_seq = v["last_seq_by_conversation"]
                        .as_object()
                        .map(|o| o.iter().map(|(k, v)| (k.clone(), v.as_i64().unwrap_or(0).max(0))).collect())
                        .unwrap_or_default();
                    return Ok(Opening::Resume { session_id: session_id.to_string(), id, last_seq });
                }
                _ => return Err(bad("first frame must be hello or resume")),
            }
        }
    }

    /// Records the agent connection, sets delivery cursors, sends `welcome`, replays on resume.
    async fn open(&mut self, opening: &Opening, resume: Option<HashMap<String, i64>>) -> Result<(), GwError> {
        if self.role == ActorKind::Agent {
            let skills = match opening {
                Opening::Hello { skills, .. } => skills.clone(),
                Opening::Resume { .. } => Vec::new(),
            };
            self.gw.agent_connected(&self.id, &skills, &self.connection_id, &self.session_id, resume.is_some()).await?;
        }
        let subs = self.subscriptions().await?;
        let resume_from: serde_json::Map<String, Value> = subs.iter().map(|(c, s)| (c.clone(), json!(s))).collect();
        match &resume {
            None => {
                self.cursors = subs.into_iter().collect();
            }
            Some(client) => {
                for (conv, _) in &subs {
                    self.cursors.insert(conv.clone(), *client.get(conv).unwrap_or(&0));
                }
                // Conversations the client still tracks that closed meanwhile (customer side).
                for (conv, seq) in client {
                    if !self.cursors.contains_key(conv) && self.gw.store.subscribed(conv, self.role, &self.id).await? {
                        self.cursors.insert(conv.clone(), *seq);
                    }
                }
            }
        }
        self.send(json!({ "type": "welcome", "session_id": self.session_id, "resume_from": resume_from, "node": self.gw.node_id }))
            .await
            .map_err(|_| GwError::Unavailable("socket closed".into()))?;
        if resume.is_some() {
            self.replay_all().await?;
        }
        Ok(())
    }

    async fn subscriptions(&mut self) -> Result<Vec<(String, i64)>, GwError> {
        self.gw.subscriptions(self.role, &self.id).await
    }

    /// After a possible event-stream gap: re-read subscriptions and fetch only conversations whose
    /// stored `seq` is ahead of what this session delivered (new conversations from `seq` 1).
    async fn resync(&mut self) -> Result<(), GwError> {
        let subs = self.subscriptions().await?;
        for (conv, last) in subs {
            let cursor = *self.cursors.entry(conv.clone()).or_insert(0);
            if last > cursor {
                self.catch_up(&conv, None).await?;
            }
        }
        Ok(())
    }

    async fn send(&mut self, v: Value) -> Result<(), ()> {
        self.socket.send(Message::Text(v.to_string().into())).await.map_err(|_| ())
    }

    async fn send_message(&mut self, m: &CanonicalMessage) -> Result<(), ()> {
        self.send(json!({ "type": "message", "message": m })).await
    }

    /// Replays every tracked conversation from its cursor to the end, in order.
    async fn replay_all(&mut self) -> Result<(), GwError> {
        let convs: Vec<String> = self.cursors.keys().cloned().collect();
        for conv in convs {
            self.catch_up(&conv, None).await?;
        }
        Ok(())
    }

    /// Sends the messages after the conversation's cursor, up to `until` (inclusive) or the end.
    async fn catch_up(&mut self, conv: &str, until: Option<i64>) -> Result<(), GwError> {
        loop {
            let last = *self.cursors.get(conv).unwrap_or(&0);
            if until.is_some_and(|u| last >= u) {
                return Ok(());
            }
            let page = until.map(|u| (u - last).min(REPLAY_PAGE)).unwrap_or(REPLAY_PAGE);
            let batch = self.gw.store.messages_after(conv, last, page).await?;
            if batch.is_empty() {
                return Ok(());
            }
            for m in &batch {
                self.send_message(m).await.map_err(|_| GwError::Unavailable("socket closed".into()))?;
                self.cursors.insert(conv.to_string(), m.seq);
            }
            if (batch.len() as i64) < page {
                return Ok(());
            }
        }
    }

    /// A live message from the event stream.
    async fn deliver(&mut self, m: &CanonicalMessage) -> Result<(), GwError> {
        let last = *self.cursors.get(&m.conversation_id).unwrap_or(&0);
        if m.seq <= last {
            return Ok(()); // already delivered (replay or gap fill got there first)
        }
        if m.seq > last + 1 {
            // Missed or out-of-order events: everything up to this seq is committed — read it.
            self.catch_up(&m.conversation_id, Some(m.seq)).await?;
        } else {
            self.send_message(m).await.map_err(|_| GwError::Unavailable("socket closed".into()))?;
            self.cursors.insert(m.conversation_id.clone(), m.seq);
        }
        if let Ok(t) = chrono::DateTime::parse_from_rfc3339(&m.received_at) {
            let secs = (Utc::now() - t.with_timezone(&Utc)).num_microseconds().unwrap_or(0) as f64 / 1e6;
            self.gw.metrics.delivered(secs);
        }
        Ok(())
    }

    async fn serve(&mut self, reg: &mut Registration) -> Flow {
        let mut last_ping = Instant::now();
        let mut watchdog = tokio::time::interval(Duration::from_secs(1));
        watchdog.tick().await;
        let mut leave_at: Option<Instant> = None;
        if *reg.shutdown.borrow() {
            leave_at = Some(Instant::now());
        }
        // Resync requests are spread over 3 s so a cluster-wide resync is not a thundering herd.
        let mut resync_at: Option<Instant> = None;
        loop {
            let at = |t: Option<Instant>| async move {
                match t {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            };
            let leave = at(leave_at);
            let resync = at(resync_at);
            tokio::select! {
                frame = self.socket.recv() => {
                    let text = match frame {
                        Some(Ok(Message::Text(t))) => t,
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return Flow::Continue,
                        Some(Ok(_)) => continue,
                    };
                    match self.handle(&text, &mut last_ping).await {
                        Ok(Flow::Continue) => {}
                        Ok(flow) => return flow,
                        Err(()) => return Flow::Continue,
                    }
                }
                push = reg.rx.recv() => {
                    let Some(ev) = push else { return Flow::Close(1013, "slow consumer: reconnect and resume") };
                    let r = match ev.as_ref() {
                        GwEvent::Message { message, .. } => self.deliver(message).await.map_err(|_| ()),
                        GwEvent::Presence { agent_id, available } => {
                            self.send(json!({ "type": "presence", "agent_id": agent_id, "available": available })).await
                        }
                        GwEvent::Resync => {
                            if resync_at.is_none() {
                                resync_at = Some(Instant::now() + Duration::from_millis(rand::thread_rng().gen_range(0..3000)));
                            }
                            Ok(())
                        }
                        GwEvent::Config { .. } => Ok(()),
                    };
                    if r.is_err() {
                        return Flow::Close(1011, "delivery failed: reconnect and resume");
                    }
                }
                changed = reg.shutdown.changed(), if leave_at.is_none() => {
                    if changed.is_err() || *reg.shutdown.borrow() {
                        let spread = rand::thread_rng().gen_range(0..2000);
                        leave_at = Some(Instant::now() + Duration::from_millis(spread));
                    }
                }
                _ = resync => {
                    resync_at = None;
                    if self.resync().await.is_err() {
                        return Flow::Close(1011, "resync failed: reconnect and resume");
                    }
                }
                _ = leave => {
                    let _ = self.send(json!({ "type": "reconnect" })).await;
                    return Flow::Close(1012, "node restarting: reconnect and resume");
                }
                _ = watchdog.tick() => {
                    if last_ping.elapsed() > PING_TIMEOUT {
                        return Flow::Close(4408, "ping timeout");
                    }
                }
            }
        }
    }

    /// One client frame. `Err` = socket gone.
    async fn handle(&mut self, text: &str, last_ping: &mut Instant) -> Result<Flow, ()> {
        let v: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return self.send(frame_error("invalid", "frames are JSON objects", None)).await.map(|_| Flow::Continue),
        };
        let client_ref = v["client_ref"].as_str().map(str::to_string);
        let reply = match v["type"].as_str().unwrap_or("") {
            "ping" => {
                *last_ping = Instant::now();
                json!({ "type": "pong" })
            }
            "send" => {
                let text = v["text"].as_str().unwrap_or("");
                let conv = v["conversation_id"].as_str();
                let r = match self.role {
                    ActorKind::Agent => match conv {
                        Some(c) => self.gw.agent_send(&self.id, c, text, client_ref.as_deref()).await,
                        None => Err(GwError::Invalid("send.conversation_id is required".into())),
                    },
                    _ => self.gw.customer_send(&self.id, conv, text, client_ref.as_deref()).await,
                };
                match r {
                    Ok(m) => ack(&m, client_ref.as_deref()),
                    Err(e) => gw_error(&e, client_ref.as_deref()),
                }
            }
            "status" if self.role == ActorKind::Agent => match v["available"].as_bool() {
                Some(available) => match self.gw.set_status(&self.id, available).await {
                    Ok(()) => json!({ "type": "status", "available": available }),
                    Err(e) => gw_error(&e, None),
                },
                None => frame_error("invalid", "status.available must be true or false", None),
            },
            "disposition" if self.role == ActorKind::Agent => match (v["conversation_id"].as_str(), v["code"].as_str()) {
                (Some(c), Some(code)) => match self.gw.disposition(&self.id, c, code, client_ref.as_deref()).await {
                    Ok(m) => ack(&m, client_ref.as_deref()),
                    Err(e) => gw_error(&e, client_ref.as_deref()),
                },
                _ => frame_error("invalid", "disposition needs conversation_id and code", client_ref.as_deref()),
            },
            "resume" => {
                // Mid-session resume: replay from the given cursors.
                if let Some(o) = v["last_seq_by_conversation"].as_object() {
                    for (conv, seq) in o {
                        if self.gw.store.subscribed(conv, self.role, &self.id).await.unwrap_or(false) {
                            self.cursors.insert(conv.clone(), seq.as_i64().unwrap_or(0).max(0));
                        }
                    }
                }
                if let Err(e) = self.replay_all().await {
                    return self.send(gw_error(&e, None)).await.map(|_| Flow::Continue);
                }
                json!({ "type": "welcome", "session_id": self.session_id, "resume_from": self.cursors, "node": self.gw.node_id })
            }
            "hello" => frame_error("invalid", "session already open", None),
            other => frame_error("unknown_frame", &format!("unsupported frame type {other:?} for this session"), client_ref.as_deref()),
        };
        self.send(reply).await.map(|_| Flow::Continue)
    }
}

fn ack(m: &CanonicalMessage, client_ref: Option<&str>) -> Value {
    json!({ "type": "ack", "client_ref": client_ref, "message_id": m.message_id, "seq": m.seq, "conversation_id": m.conversation_id })
}
