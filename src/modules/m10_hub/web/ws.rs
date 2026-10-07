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
//! Heartbeat: the server pings every 20 s and closes a socket that sent nothing for 60 s. On
//! graceful shutdown each socket gets `reconnect` and close code 1012; clients reconnect (through
//! the load balancer, possibly to another node) and resume from their last sequence numbers.

use std::collections::HashMap;
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
use uuid::Uuid;

use crate::app::AppState;
use crate::bootstrap_auth::service::Principal;
use crate::platform::errors::AppError;

use super::super::application::ports::{CustomerSession, Target};
use super::super::application::sessions::Push;
use super::super::application::AgentCtx;
use super::super::domain::Presence;
use super::{agent_ctx, authenticate, credential, Credential};

const PING_EVERY: Duration = Duration::from_secs(20);
const IDLE_CLOSE: Duration = Duration::from_secs(60);
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const REVALIDATE_EVERY: Duration = Duration::from_secs(60);
const MAX_FRAME: usize = 64 * 1024;
/// Inbound rate limit per customer session (OCC-M10-R041): 20 messages per 10 s.
const CUSTOMER_BURST: u32 = 20;
const CUSTOMER_WINDOW: Duration = Duration::from_secs(10);

pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/hub/ws/agent", get(agent_ws)).route("/v1/hub/ws/customer", get(customer_ws))
}

type Sink = SplitSink<WebSocket, Message>;

async fn send_json(sink: &mut Sink, v: &Value) -> bool {
    sink.send(Message::Text(v.to_string().into())).await.is_ok()
}

async fn send_error(sink: &mut Sink, e: &AppError, reference: Option<&str>) -> bool {
    send_json(sink, &json!({ "type": "error", "code": e.code.as_str(), "message": e.message, "ref": reference })).await
}

async fn close(sink: &mut Sink, code: u16, reason: &'static str) {
    let _ = sink.send(Message::Close(Some(CloseFrame { code, reason: reason.into() }))).await;
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
    ws.max_message_size(MAX_FRAME).on_upgrade(move |socket| agent_session(state, socket, ctx, cred, principal))
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
    let (target, sid) = (reg.target.clone(), reg.id);
    let (mut rx, mut shutdown) = (reg.rx, reg.shutdown);
    let (mut sink, mut stream) = socket.split();
    let _ = state.hub.heartbeat(ctx).await;
    tracing::info!(tenant_id = %ctx.tenant_id, agent_id = %ctx.user_id, node = %state.hub.node_id, "agent socket open");
    let mut tick = tokio::time::interval(PING_EVERY);
    tick.tick().await;
    let mut last_in = Instant::now();
    let mut last_auth = Instant::now();
    let mut welcomed = false;
    loop {
        tokio::select! {
            frame = stream.next() => {
                let Some(Ok(frame)) = frame else { break };
                last_in = Instant::now();
                let text = match frame {
                    Message::Text(t) => t,
                    Message::Close(_) => break,
                    _ => continue,
                };
                let parsed: Result<AgentFrame, _> = serde_json::from_str(text.as_str());
                let f = match parsed {
                    Ok(f) => f,
                    Err(e) => {
                        let err = AppError::validation("frame", format!("unrecognised frame: {e}"));
                        if !send_error(&mut sink, &err, None).await { break }
                        continue;
                    }
                };
                if !welcomed && !matches!(f, AgentFrame::Hello { .. }) {
                    // Implicit hello without resume info.
                    match state.hub.agent_snapshot(ctx, &HashMap::new()).await {
                        Ok(w) => { if !send_json(&mut sink, &w).await { break } }
                        Err(e) => { send_error(&mut sink, &e, None).await; break }
                    }
                    welcomed = true;
                }
                let ok = match f {
                    AgentFrame::Hello { resume } => {
                        welcomed = true;
                        match state.hub.agent_snapshot(ctx, &resume).await {
                            Ok(w) => send_json(&mut sink, &w).await,
                            Err(e) => send_error(&mut sink, &e, None).await,
                        }
                    }
                    AgentFrame::Ping => send_json(&mut sink, &json!({ "type": "pong" })).await,
                    AgentFrame::Presence { status } => match status.parse::<Presence>() {
                        Ok(p) => match state.hub.set_presence(ctx, p).await {
                            Ok(()) => true,
                            Err(e) => send_error(&mut sink, &e, None).await,
                        },
                        Err(e) => send_error(&mut sink, &AppError::validation("status", e.0), None).await,
                    },
                    AgentFrame::Send { conversation_id, client_msg_id, body } => {
                        match state.hub.agent_send(ctx, conversation_id, &client_msg_id, &body).await {
                            Ok(m) => send_json(&mut sink, &json!({ "type": "ack", "client_msg_id": client_msg_id, "message": m })).await,
                            Err(e) => send_error(&mut sink, &e, Some(&client_msg_id)).await,
                        }
                    }
                    AgentFrame::Close { conversation_id } => match state.hub.close_conversation(ctx, conversation_id).await {
                        Ok(_) => true,
                        Err(e) => send_error(&mut sink, &e, None).await,
                    },
                };
                if !ok { break }
            }
            push = rx.recv() => match push {
                Some(Push::Json(s)) => { if sink.send(Message::Text(s.as_ref().into())).await.is_err() { break } }
                None => { break }
            },
            _ = tick.tick() => {
                if last_in.elapsed() > IDLE_CLOSE {
                    close(&mut sink, 1001, "idle timeout").await;
                    break;
                }
                if sink.send(Message::Ping(Default::default())).await.is_err() { break }
                if let Err(e) = state.hub.heartbeat(ctx).await { e.log(); }
                if last_auth.elapsed() > REVALIDATE_EVERY {
                    // Session revoked / tenant suspended → the socket ends too (BR-M01-002).
                    match authenticate(&state, &cred).await {
                        Ok(p) if p.user_id == principal.user_id => last_auth = Instant::now(),
                        _ => { close(&mut sink, 4401, "session ended").await; break }
                    }
                }
            }
            _ = shutdown.changed() => {
                let _ = send_json(&mut sink, &json!({ "type": "reconnect", "reason": "server restarting" })).await;
                close(&mut sink, 1012, "service restart").await;
                break;
            }
        }
    }
    state.sessions.unregister(&target, sid);
    tracing::info!(agent_id = %ctx.user_id, "agent socket closed");
}

// ------------------------------------------------------------------------------------------------
// Customer (web chat)
// ------------------------------------------------------------------------------------------------

async fn customer_ws(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    // Authentication happens in the first frame (token not in the URL → not in access logs).
    ws.max_message_size(MAX_FRAME).on_upgrade(move |socket| customer_session(state, socket))
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

async fn customer_auth(
    state: &AppState,
    sink: &mut Sink,
    stream: &mut futures::stream::SplitStream<WebSocket>,
) -> Option<(CustomerSession, i64)> {
    let first = tokio::time::timeout(AUTH_TIMEOUT, stream.next()).await.ok()??.ok()?;
    let Message::Text(t) = first else { return None };
    let Ok(CustomerFrame::Auth { token, last_seq }) = serde_json::from_str::<CustomerFrame>(t.as_str()) else {
        send_error(sink, &AppError::unauthenticated("First frame must be auth"), None).await;
        return None;
    };
    match state.hub.customer_session(&token).await {
        Ok(s) => Some((s, last_seq)),
        Err(e) => {
            send_error(sink, &e, None).await;
            None
        }
    }
}

async fn customer_session(state: AppState, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let Some((session, last_seq)) = customer_auth(&state, &mut sink, &mut stream).await else {
        close(&mut sink, 4401, "authentication required").await;
        return;
    };
    let reg = state.sessions.register(Target::Customer { endpoint: session.endpoint_id, visitor: session.visitor_id.clone() });
    let (target, sid) = (reg.target.clone(), reg.id);
    let (mut rx, mut shutdown) = (reg.rx, reg.shutdown);
    let session = Arc::new(session);
    match state.hub.customer_snapshot(&session, last_seq).await {
        Ok(w) => {
            if !send_json(&mut sink, &w).await {
                state.sessions.unregister(&target, sid);
                return;
            }
        }
        Err(e) => {
            send_error(&mut sink, &e, None).await;
            state.sessions.unregister(&target, sid);
            return;
        }
    }
    let mut tick = tokio::time::interval(PING_EVERY);
    tick.tick().await;
    let mut last_in = Instant::now();
    let (mut window_start, mut window_count) = (Instant::now(), 0u32);
    loop {
        tokio::select! {
            frame = stream.next() => {
                let Some(Ok(frame)) = frame else { break };
                last_in = Instant::now();
                let text = match frame {
                    Message::Text(t) => t,
                    Message::Close(_) => break,
                    _ => continue,
                };
                let ok = match serde_json::from_str::<CustomerFrame>(text.as_str()) {
                    Ok(CustomerFrame::Ping) => send_json(&mut sink, &json!({ "type": "pong" })).await,
                    Ok(CustomerFrame::Send { client_msg_id, body }) => {
                        if window_start.elapsed() > CUSTOMER_WINDOW {
                            window_start = Instant::now();
                            window_count = 0;
                        }
                        window_count += 1;
                        if window_count > CUSTOMER_BURST {
                            send_error(&mut sink, &AppError::rate_limited("Too many messages; slow down", CUSTOMER_WINDOW.as_secs()), Some(&client_msg_id)).await
                        } else {
                            match state.hub.customer_send(&session, &client_msg_id, &body).await {
                                Ok(m) => send_json(&mut sink, &json!({
                                    "type": "ack", "client_msg_id": client_msg_id, "conversation_id": m.conversation_id,
                                    "message": { "id": m.id, "seq": m.seq, "from": "me", "body": m.body, "created_at": m.created_at }
                                })).await,
                                Err(e) => send_error(&mut sink, &e, Some(&client_msg_id)).await,
                            }
                        }
                    }
                    Ok(CustomerFrame::Seen { seq }) => match state.hub.customer_seen(&session, seq).await {
                        Ok(()) => true,
                        Err(e) => send_error(&mut sink, &e, None).await,
                    },
                    Ok(CustomerFrame::Auth { .. }) => true,
                    Err(e) => send_error(&mut sink, &AppError::validation("frame", format!("unrecognised frame: {e}")), None).await,
                };
                if !ok { break }
            }
            push = rx.recv() => match push {
                Some(Push::Json(s)) => { if sink.send(Message::Text(s.as_ref().into())).await.is_err() { break } }
                None => { break }
            },
            _ = tick.tick() => {
                if last_in.elapsed() > IDLE_CLOSE {
                    close(&mut sink, 1001, "idle timeout").await;
                    break;
                }
                if sink.send(Message::Ping(Default::default())).await.is_err() { break }
            }
            _ = shutdown.changed() => {
                let _ = send_json(&mut sink, &json!({ "type": "reconnect", "reason": "server restarting" })).await;
                close(&mut sink, 1012, "service restart").await;
                break;
            }
        }
    }
    state.sessions.unregister(&target, sid);
}
