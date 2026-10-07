//! WebSocket sessions (FR-ARC-002 "WebSocket pod"; OCC-M08-R002 live chat; OCC-M10-R028).
//!
//! Protocol (JSON text frames, `type` field):
//! * agent  → `hello{resume:{conversation_id: last_seq}}`, `ping`, `presence.set{status}`,
//!   `message.send{conversation_id, client_msg_id, body}`, `conversation.close{conversation_id}`
//! * customer → first frame `auth{token, last_seq}`, then `message.send{client_msg_id, body}`,
//!   `seen{seq}`, `ping`
//! * server → `welcome`, `ack{client_msg_id, message}`, `message.new`, `message.status`,
//!   `conversation.assigned`, `conversation.updated`, `presence`, `pong`, `error`, `reconnect`
//!
//! Each socket runs a **reader** (client frames, handled in order so acks stay ordered) and a
//! **writer** (responses + real-time pushes + heartbeat) concurrently, so pushes keep flowing
//! while the reader waits on the database. Heartbeat: the server pings every 20 s and closes a
//! socket that sent nothing for 60 s. On graceful shutdown each socket gets `reconnect` and close
//! code 1012; clients reconnect (through the load balancer, possibly to another node) and resume
//! from their last sequence numbers.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::app::AppState;
use crate::bootstrap_auth::service::Principal;
use crate::platform::errors::AppError;

use super::super::application::ports::{CustomerSession, Target};
use super::super::application::sessions::{Push, Registration};
use super::super::application::AgentCtx;
use super::super::domain::Presence;
use super::{agent_ctx, authenticate, credential, Credential};

const PING_EVERY: Duration = Duration::from_secs(20);
const IDLE_CLOSE: Duration = Duration::from_secs(60);
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest a customer handshake waits for an admission permit before getting close 1013.
const ADMISSION_WAIT: Duration = Duration::from_secs(20);
const REVALIDATE_EVERY: Duration = Duration::from_secs(60);
const MAX_FRAME: usize = 64 * 1024;
/// Socket buffers: chat frames are small, and per-connection memory decides how many idle
/// sessions a node holds (tungstenite defaults to 128 KiB read + 128 KiB write per socket).
const READ_BUFFER: usize = 4 * 1024;
const WRITE_BUFFER: usize = 4 * 1024;
/// Upper bound on queued outgoing bytes per socket before writes fail (slow client).
const MAX_WRITE_BUFFER: usize = 1024 * 1024;

/// Applies the hub's socket limits to an upgrade.
fn sized(ws: WebSocketUpgrade) -> WebSocketUpgrade {
    ws.max_message_size(MAX_FRAME)
        .max_frame_size(MAX_FRAME)
        .read_buffer_size(READ_BUFFER)
        .write_buffer_size(WRITE_BUFFER)
        .max_write_buffer_size(MAX_WRITE_BUFFER)
}
/// Inbound rate limit per customer session (OCC-M10-R041): 20 messages per 10 s.
const CUSTOMER_BURST: u32 = 20;
const CUSTOMER_WINDOW: Duration = Duration::from_secs(10);

pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/hub/ws/agent", get(agent_ws)).route("/v1/hub/ws/customer", get(customer_ws))
}

type Sink = SplitSink<WebSocket, Message>;

/// What the reader (or a maintenance task) asks the writer to send.
enum Out {
    Json(Value),
    Close(u16, &'static str),
}

/// Sends a frame to the writer; `false` once the session is over.
async fn out(tx: &mpsc::Sender<Out>, v: Value) -> bool {
    tx.send(Out::Json(v)).await.is_ok()
}

fn error_frame(e: &AppError, reference: Option<&str>) -> Value {
    json!({ "type": "error", "code": e.code.as_str(), "message": e.message, "ref": reference })
}

async fn close(sink: &mut Sink, code: u16, reason: &'static str) {
    let _ = sink.send(Message::Close(Some(CloseFrame { code, reason: reason.into() }))).await;
}

/// Runs one registered socket until either side ends. `on_text` handles one client frame and
/// answers through the `Out` channel; it returns `false` to end the session.
async fn drive<F, Fut>(
    state: &AppState,
    socket: WebSocket,
    reg: Registration,
    first: Vec<Value>,
    mut out_rx: mpsc::Receiver<Out>,
    on_text: F,
) where
    F: Fn(String) -> Fut,
    Fut: Future<Output = bool>,
{
    let (target, sid) = (reg.target.clone(), reg.id);
    let (mut push_rx, mut shutdown) = (reg.rx, reg.shutdown);
    let (mut sink, mut stream) = socket.split();
    let started = Instant::now();
    let last_in = AtomicU64::new(0);
    let writer = async {
        for v in first {
            if sink.send(Message::Text(v.to_string().into())).await.is_err() {
                return;
            }
        }
        let mut tick = tokio::time::interval(PING_EVERY);
        tick.tick().await;
        loop {
            tokio::select! {
                p = push_rx.recv() => match p {
                    Some(Push::Json(s)) => { if sink.send(Message::Text(s.as_ref().into())).await.is_err() { return } }
                    None => return, // dropped as a slow consumer; the client resumes by seq
                },
                o = out_rx.recv() => match o {
                    Some(Out::Json(v)) => { if sink.send(Message::Text(v.to_string().into())).await.is_err() { return } }
                    Some(Out::Close(code, reason)) => { close(&mut sink, code, reason).await; return }
                    None => return,
                },
                _ = tick.tick() => {
                    let idle = started.elapsed().saturating_sub(Duration::from_millis(last_in.load(Ordering::Relaxed)));
                    if idle > IDLE_CLOSE {
                        close(&mut sink, 1001, "idle timeout").await;
                        return;
                    }
                    if sink.send(Message::Ping(Default::default())).await.is_err() { return }
                }
                _ = shutdown.changed() => {
                    let _ = sink.send(Message::Text(json!({ "type": "reconnect", "reason": "server restarting" }).to_string().into())).await;
                    close(&mut sink, 1012, "service restart").await;
                    return;
                }
            }
        }
    };
    let reader = async {
        while let Some(Ok(frame)) = stream.next().await {
            last_in.store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
            match frame {
                Message::Text(t) => {
                    if !on_text(t.to_string()).await {
                        return;
                    }
                }
                Message::Close(_) => return,
                _ => {}
            }
        }
    };
    tokio::select! {
        _ = writer => {}
        _ = reader => {}
    }
    state.sessions.unregister(&target, sid);
}

// ------------------------------------------------------------------------------------------------
// Agent
// ------------------------------------------------------------------------------------------------

async fn agent_ws(State(state): State<AppState>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    let Some(cred) = credential(&headers) else {
        return AppError::unauthenticated("Sign in as an agent").into_response();
    };
    if matches!(cred, Credential::Cookie(_)) && !super::same_origin(&headers) {
        return AppError::forbidden("Cross-origin WebSocket refused").into_response();
    }
    let principal = match authenticate(&state, &cred).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let ctx = match agent_ctx(&principal) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = state.hub.agent_profile(ctx).await {
        return e.into_response();
    }
    sized(ws).on_upgrade(move |socket| agent_session(state, socket, ctx, cred, principal))
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum AgentFrame {
    #[serde(rename = "hello")]
    Hello {
        #[serde(default)]
        resume: HashMap<Uuid, i64>,
    },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "presence.set")]
    Presence { status: String },
    #[serde(rename = "message.send")]
    Send { conversation_id: Uuid, client_msg_id: String, body: String },
    #[serde(rename = "conversation.close")]
    Close { conversation_id: Uuid },
}

async fn agent_session(state: AppState, socket: WebSocket, ctx: AgentCtx, cred: Credential, principal: Principal) {
    let reg = state.sessions.register(Target::Agent { id: ctx.user_id });
    let _ = state.hub.heartbeat(ctx).await;
    tracing::info!(tenant_id = %ctx.tenant_id, agent_id = %ctx.user_id, node = %state.hub.node_id, "agent socket open");
    let (out_tx, out_rx) = mpsc::channel::<Out>(256);
    // Maintenance: presence heartbeat + session re-validation (a revoked session or suspended
    // tenant ends the socket, BR-M01-002).
    let maint = {
        let (state, tx) = (state.clone(), out_tx.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PING_EVERY);
            let mut last_auth = Instant::now();
            loop {
                tick.tick().await;
                if let Err(e) = state.hub.heartbeat(ctx).await {
                    e.log();
                }
                if last_auth.elapsed() >= REVALIDATE_EVERY {
                    match authenticate(&state, &cred).await {
                        Ok(p) if p.user_id == principal.user_id => last_auth = Instant::now(),
                        _ => {
                            let _ = tx.send(Out::Close(4401, "session ended")).await;
                            return;
                        }
                    }
                }
            }
        })
    };
    let welcomed = Arc::new(AtomicBool::new(false));
    let handler = {
        let state = state.clone();
        move |text: String| {
            let (state, tx, welcomed) = (state.clone(), out_tx.clone(), welcomed.clone());
            async move { agent_frame(&state, ctx, &tx, &welcomed, &text).await }
        }
    };
    drive(&state, socket, reg, Vec::new(), out_rx, handler).await;
    maint.abort();
    tracing::info!(agent_id = %ctx.user_id, "agent socket closed");
}

async fn agent_frame(state: &AppState, ctx: AgentCtx, tx: &mpsc::Sender<Out>, welcomed: &AtomicBool, text: &str) -> bool {
    let f: AgentFrame = match serde_json::from_str(text) {
        Ok(f) => f,
        Err(e) => return out(tx, error_frame(&AppError::validation("frame", format!("unrecognised frame: {e}")), None)).await,
    };
    if !matches!(f, AgentFrame::Hello { .. }) && !welcomed.swap(true, Ordering::Relaxed) {
        // Implicit hello without resume info.
        match state.hub.agent_snapshot(ctx, &HashMap::new()).await {
            Ok(w) => {
                if !out(tx, w).await {
                    return false;
                }
            }
            Err(e) => {
                out(tx, error_frame(&e, None)).await;
                return false;
            }
        }
    }
    match f {
        AgentFrame::Hello { resume } => {
            welcomed.store(true, Ordering::Relaxed);
            match state.hub.agent_snapshot(ctx, &resume).await {
                Ok(w) => out(tx, w).await,
                Err(e) => out(tx, error_frame(&e, None)).await,
            }
        }
        AgentFrame::Ping => out(tx, json!({ "type": "pong" })).await,
        AgentFrame::Presence { status } => match status.parse::<Presence>() {
            Ok(p) => match state.hub.set_presence(ctx, p).await {
                Ok(()) => true,
                Err(e) => out(tx, error_frame(&e, None)).await,
            },
            Err(e) => out(tx, error_frame(&AppError::validation("status", e.0), None)).await,
        },
        AgentFrame::Send { conversation_id, client_msg_id, body } => {
            match state.hub.agent_send(ctx, conversation_id, &client_msg_id, &body).await {
                Ok(m) => out(tx, json!({ "type": "ack", "client_msg_id": client_msg_id, "message": m })).await,
                Err(e) => out(tx, error_frame(&e, Some(&client_msg_id))).await,
            }
        }
        AgentFrame::Close { conversation_id } => match state.hub.close_conversation(ctx, conversation_id).await {
            Ok(_) => true,
            Err(e) => out(tx, error_frame(&e, None)).await,
        },
    }
}

// ------------------------------------------------------------------------------------------------
// Customer (web chat)
// ------------------------------------------------------------------------------------------------

async fn customer_ws(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    // Authentication happens in the first frame (token not in the URL → not in access logs).
    sized(ws).on_upgrade(move |socket| customer_session(state, socket))
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum CustomerFrame {
    #[serde(rename = "auth")]
    Auth {
        token: String,
        #[serde(default)]
        last_seq: i64,
    },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "message.send")]
    Send { client_msg_id: String, body: String },
    #[serde(rename = "seen")]
    Seen { seq: i64 },
}

/// First frame must be `auth` within 10 s.
async fn customer_auth(state: &AppState, socket: &mut WebSocket) -> Result<(CustomerSession, i64), (u16, &'static str)> {
    let first = tokio::time::timeout(AUTH_TIMEOUT, socket.recv()).await.map_err(|_| (4401, "authentication timeout"))?;
    let Some(Ok(Message::Text(t))) = first else { return Err((4401, "authentication required")) };
    let Ok(CustomerFrame::Auth { token, last_seq }) = serde_json::from_str::<CustomerFrame>(t.as_str()) else {
        let e = AppError::unauthenticated("First frame must be auth");
        let _ = socket.send(Message::Text(error_frame(&e, None).to_string().into())).await;
        return Err((4401, "authentication required"));
    };
    match state.hub.customer_session(&token).await {
        Ok(s) => Ok((s, last_seq)),
        Err(e) => {
            let _ = socket.send(Message::Text(error_frame(&e, None).to_string().into())).await;
            Err((4401, "authentication failed"))
        }
    }
}

async fn customer_session(state: AppState, mut socket: WebSocket) {
    // Admission control: the handshake's database work waits for a permit (bounded wait); when
    // the node is saturated the client is told to retry later (1013) and backs off with jitter.
    let permit = match tokio::time::timeout(ADMISSION_WAIT, state.ws_admission.clone().acquire_owned()).await {
        Ok(Ok(p)) => p,
        _ => {
            let _ = socket.send(Message::Close(Some(CloseFrame { code: 1013, reason: "server busy, retry later".into() }))).await;
            return;
        }
    };
    let (session, last_seq) = match customer_auth(&state, &mut socket).await {
        Ok(v) => v,
        Err((code, reason)) => {
            let _ = socket.send(Message::Close(Some(CloseFrame { code, reason: reason.into() }))).await;
            return;
        }
    };
    let reg = state.sessions.register(Target::Customer { endpoint: session.endpoint_id, visitor: session.visitor_id.clone() });
    let welcome = match state.hub.customer_snapshot(&session, last_seq).await {
        Ok(w) => w,
        Err(e) => {
            let _ = socket.send(Message::Text(error_frame(&e, None).to_string().into())).await;
            state.sessions.unregister(&reg.target, reg.id);
            return;
        }
    };
    drop(permit);
    let session = Arc::new(session);
    let (out_tx, out_rx) = mpsc::channel::<Out>(64);
    let window = Arc::new(std::sync::Mutex::new((Instant::now(), 0u32)));
    let handler = {
        let state = state.clone();
        move |text: String| {
            let (state, tx, session, window) = (state.clone(), out_tx.clone(), session.clone(), window.clone());
            async move { customer_frame(&state, &session, &tx, &window, &text).await }
        }
    };
    drive(&state, socket, reg, vec![welcome], out_rx, handler).await;
}

async fn customer_frame(
    state: &AppState,
    session: &CustomerSession,
    tx: &mpsc::Sender<Out>,
    window: &std::sync::Mutex<(Instant, u32)>,
    text: &str,
) -> bool {
    match serde_json::from_str::<CustomerFrame>(text) {
        Ok(CustomerFrame::Ping) => out(tx, json!({ "type": "pong" })).await,
        Ok(CustomerFrame::Send { client_msg_id, body }) => {
            let limited = {
                let mut w = window.lock().unwrap_or_else(|e| e.into_inner());
                if w.0.elapsed() > CUSTOMER_WINDOW {
                    *w = (Instant::now(), 0);
                }
                w.1 += 1;
                w.1 > CUSTOMER_BURST
            };
            if limited {
                let e = AppError::rate_limited("Too many messages; slow down", CUSTOMER_WINDOW.as_secs());
                return out(tx, error_frame(&e, Some(&client_msg_id))).await;
            }
            match state.hub.customer_send(session, &client_msg_id, &body).await {
                Ok(m) => {
                    out(
                        tx,
                        json!({
                            "type": "ack", "client_msg_id": client_msg_id, "conversation_id": m.conversation_id,
                            "message": { "id": m.id, "seq": m.seq, "from": "me", "body": m.body, "created_at": m.created_at }
                        }),
                    )
                    .await
                }
                Err(e) => out(tx, error_frame(&e, Some(&client_msg_id))).await,
            }
        }
        Ok(CustomerFrame::Seen { seq }) => match state.hub.customer_seen(session, seq).await {
            Ok(()) => true,
            Err(e) => out(tx, error_frame(&e, None)).await,
        },
        Ok(CustomerFrame::Auth { .. }) => true,
        Err(e) => out(tx, error_frame(&AppError::validation("frame", format!("unrecognised frame: {e}")), None)).await,
    }
}
