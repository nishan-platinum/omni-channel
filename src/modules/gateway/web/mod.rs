//! HTTP surface of the bake-off contract (ADR-0014): ingress, conversation reads, presence,
//! config reload, health and metrics. Every request needs `Authorization: Bearer <token>`;
//! WebSockets take the token as `?token=` (see `ws.rs`).

pub mod ws;

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use subtle::ConstantTimeEq;

use super::application::{Gateway, GwError};
use super::domain::{Channel, Fixture, SipIngress, WhatsappIngress};

#[derive(Clone)]
pub struct GwState {
    pub gw: Arc<Gateway>,
    pub token: Arc<str>,
}

impl GwState {
    pub fn token_ok(&self, presented: &str) -> bool {
        !presented.is_empty() && bool::from(presented.as_bytes().ct_eq(self.token.as_bytes()))
    }
}

pub fn router(state: GwState) -> Router {
    let protected = Router::new()
        .route("/ingress/whatsapp", post(ingress_whatsapp))
        .route("/ingress/sip", post(ingress_sip))
        .route("/conversations/{id}", get(conversation))
        .route("/conversations/{id}/messages", get(messages_after))
        .route("/presence", get(presence))
        .route("/config/reload", post(reload))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .layer(middleware::from_fn_with_state(state.clone(), require_bearer));
    Router::new().merge(protected).merge(ws::routes()).layer(DefaultBodyLimit::max(256 * 1024)).with_state(state)
}

pub struct ApiError(StatusCode, &'static str, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1, "message": self.2 }))).into_response()
    }
}

impl From<GwError> for ApiError {
    fn from(e: GwError) -> Self {
        let status = match &e {
            GwError::Invalid(_) => StatusCode::BAD_REQUEST,
            GwError::Duplicate | GwError::DuplicateClientRef => StatusCode::CONFLICT,
            GwError::NotFound(_) => StatusCode::NOT_FOUND,
            GwError::NotAssigned => StatusCode::FORBIDDEN,
            GwError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            GwError::Internal(err) => {
                tracing::error!(error = %err, "gateway request failed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        ApiError(status, e.code(), e.to_string())
    }
}

type ApiResult<T> = Result<T, ApiError>;

async fn require_bearer(State(st): State<GwState>, headers: HeaderMap, req: Request, next: Next) -> Response {
    let presented = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("");
    if !st.token_ok(presented) {
        return ApiError(StatusCode::UNAUTHORIZED, "unauthorized", "missing or wrong bearer token".into()).into_response();
    }
    let path = req.uri().path();
    if !st.gw.is_ready() && path != "/healthz" && path != "/metrics" {
        return ApiError(StatusCode::SERVICE_UNAVAILABLE, "starting", "node is not ready".into()).into_response();
    }
    next.run(req).await
}

fn parse<T: serde::de::DeserializeOwned>(body: &Bytes) -> ApiResult<T> {
    serde_json::from_slice(body).map_err(|e| ApiError(StatusCode::BAD_REQUEST, "invalid", format!("malformed JSON: {e}")))
}

async fn ingress_whatsapp(State(st): State<GwState>, body: Bytes) -> ApiResult<Response> {
    let (customer, m) = parse::<WhatsappIngress>(&body)?.into_message().map_err(GwError::from)?;
    let msg = st.gw.ingest(Channel::Whatsapp, &customer, m).await?;
    Ok((StatusCode::ACCEPTED, Json(json!({ "message_id": msg.message_id, "conversation_id": msg.conversation_id, "seq": msg.seq })))
        .into_response())
}

async fn ingress_sip(State(st): State<GwState>, body: Bytes) -> ApiResult<Response> {
    let (customer, m) = parse::<SipIngress>(&body)?.into_message().map_err(GwError::from)?;
    let msg = st.gw.ingest(Channel::Sip, &customer, m).await?;
    Ok((StatusCode::ACCEPTED, Json(json!({ "message_id": msg.message_id, "conversation_id": msg.conversation_id, "seq": msg.seq })))
        .into_response())
}

async fn conversation(State(st): State<GwState>, Path(id): Path<String>) -> ApiResult<Response> {
    let (c, messages) = st.gw.store.conversation(&id).await?.ok_or(GwError::NotFound("conversation"))?;
    Ok(Json(json!({
        "id": c.id,
        "channel": c.channel,
        "customer": c.customer,
        "skill": c.skill,
        "status": c.status,
        "assigned_agent": c.assigned_agent,
        "last_seq": c.last_seq,
        "messages": messages,
    }))
    .into_response())
}

#[derive(Deserialize)]
struct AfterQuery {
    #[serde(default)]
    after: i64,
    #[serde(default)]
    limit: Option<i64>,
}

async fn messages_after(State(st): State<GwState>, Path(id): Path<String>, Query(q): Query<AfterQuery>) -> ApiResult<Response> {
    if !st.gw.store.conversation_exists(&id).await? {
        return Err(GwError::NotFound("conversation").into());
    }
    let limit = q.limit.unwrap_or(10_000).clamp(1, 10_000);
    let messages = st.gw.store.messages_after(&id, q.after.max(0), limit).await?;
    Ok(Json(json!({ "messages": messages })).into_response())
}

async fn presence(State(st): State<GwState>) -> ApiResult<Response> {
    Ok(Json(st.gw.presence().await?).into_response())
}

/// Re-reads the platform fixture. A JSON body, when present, is applied as the new fixture
/// instead (test convenience; the bake-off flow edits the fixture source and posts no body).
async fn reload(State(st): State<GwState>, body: Bytes) -> ApiResult<Response> {
    let override_ = if body.iter().all(u8::is_ascii_whitespace) { None } else { Some(parse::<Fixture>(&body)?) };
    st.gw.reload(override_).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn healthz(State(st): State<GwState>) -> Response {
    if st.gw.is_ready() {
        (StatusCode::OK, Json(json!({ "status": "ok", "node": st.gw.node_id }))).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": "unavailable", "node": st.gw.node_id }))).into_response()
    }
}

async fn metrics(State(st): State<GwState>) -> Response {
    let mut queues: std::collections::BTreeMap<String, i64> = st.gw.fixture().queue_skills().into_iter().map(|s| (s, 0)).collect();
    if let Ok(depths) = st.gw.store.queue_depths().await {
        queues.extend(depths);
    }
    let (customers, agents) = st.gw.sessions.counts();
    let body = st.gw.metrics.render(&st.gw.node_id, customers, agents, &queues);
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body).into_response()
}
