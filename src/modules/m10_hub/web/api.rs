//! `/v1/hub` JSON API (bearer tokens; envelope per ADR-0006).
//!
//! * `POST /v1/hub/customer/sessions` — public: start a web-chat session from a widget key.
//! * Tenant Admin: `GET|POST /v1/hub/agents`, `GET /v1/hub/channels`,
//!   `POST /v1/hub/channels/simulated`, `GET /v1/hub/queues`.
//! * Agent or Tenant Admin: `GET /v1/hub/conversations`, `GET /v1/hub/conversations/{id}/messages`
//!   (agents only see conversations assigned to them; other tenants' rows are invisible via RLS).

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::app::AppState;
use crate::bootstrap_auth::service::{Principal, Role, SessionKind};
use crate::modules::m01_tenancy::domain::quota::QuotaMetric;
use crate::modules::m01_tenancy::domain::TenantId;
use crate::platform::errors::{AppError, AppResult};
use crate::platform::observability::RequestContext;

use super::super::domain::{normalize_skill, parse_skills};
use super::{agent_ctx, envelope, tenant_admin_tenant};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/hub/customer/sessions", post(create_customer_session))
        .route("/v1/hub/agents", get(list_agents).post(create_agent))
        .route("/v1/hub/channels", get(list_channels))
        .route("/v1/hub/channels/simulated", post(provision_simulated))
        .route("/v1/hub/queues", get(queues))
        .route("/v1/hub/conversations", get(list_conversations))
        .route("/v1/hub/conversations/{id}/messages", get(conversation_messages))
}

fn ok(status: StatusCode, data: Value) -> Response {
    (status, Json(envelope(data))).into_response()
}

fn respond(r: AppResult<Response>) -> Response {
    r.unwrap_or_else(|e| e.into_response())
}

async fn bearer(state: &AppState, headers: &HeaderMap) -> AppResult<Principal> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .ok_or_else(|| AppError::unauthenticated("Missing or invalid credentials"))?;
    state.auth.authenticate(token, SessionKind::Api).await
}

fn json_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> AppResult<T> {
    serde_json::from_slice(body).map_err(|e| AppError::validation("body", format!("Invalid JSON body: {e}")))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CustomerSessionRequest {
    widget_key: String,
    #[serde(default)]
    name: String,
}

async fn create_customer_session(State(state): State<AppState>, body: axum::body::Bytes) -> Response {
    respond(
        async {
            let r: CustomerSessionRequest = json_body(&body)?;
            let (token, s) = state.hub.create_customer_session(&r.widget_key, &r.name).await?;
            Ok(ok(
                StatusCode::CREATED,
                json!({ "token": token, "session_id": s.id, "display_name": s.display_name, "expires_at": s.expires_at }),
            ))
        }
        .await,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateAgentRequest {
    email: String,
    display_name: String,
    password: String,
    skills: Vec<String>,
    #[serde(default = "default_capacity")]
    max_concurrent: i64,
}

fn default_capacity() -> i64 {
    3
}

/// Agent to create (from the API body or the admin form).
pub(crate) struct NewAgent<'a> {
    pub email: &'a str,
    pub display_name: &'a str,
    pub password: &'a str,
    /// Comma-separated skill list.
    pub skills: &'a str,
    pub max_concurrent: i64,
}

/// Shared by the API and the admin form: M01 user quota → identity user → hub agent profile.
pub(crate) async fn create_agent_for(state: &AppState, admin: &Principal, ctx: &RequestContext, a: NewAgent<'_>) -> AppResult<Uuid> {
    let tenant = tenant_admin_tenant(admin)?;
    let skills = parse_skills(a.skills).map_err(|e| AppError::validation("skills", e.0))?;
    if !(1..=50).contains(&a.max_concurrent) {
        return Err(AppError::validation("max_concurrent", "Capacity must be between 1 and 50"));
    }
    // Agents are tenant users: the M01 `users` quota applies (BR quota enforcement).
    let actor = crate::modules::m01_tenancy::web::actor(admin, ctx);
    state.m01.quotas.check_and_consume(&actor, TenantId(tenant), QuotaMetric::Users, 1).await?;
    let user = state.auth.create_agent_user(tenant, a.email, a.display_name, a.password).await?;
    state.hub.repo.upsert_agent(tenant, user, &skills, a.max_concurrent).await?;
    tracing::info!(tenant_id = %tenant, agent_id = %user, "hub agent created");
    Ok(user)
}

async fn create_agent(
    State(state): State<AppState>,
    headers: HeaderMap,
    rctx: axum::Extension<RequestContext>,
    body: axum::body::Bytes,
) -> Response {
    respond(
        async {
            let p = bearer(&state, &headers).await?;
            let r: CreateAgentRequest = json_body(&body)?;
            let skills = r.skills.join(",");
            let new = NewAgent {
                email: &r.email,
                display_name: &r.display_name,
                password: &r.password,
                skills: &skills,
                max_concurrent: r.max_concurrent,
            };
            let id = create_agent_for(&state, &p, &rctx.0, new).await?;
            Ok(ok(StatusCode::CREATED, json!({ "user_id": id })))
        }
        .await,
    )
}

async fn list_agents(State(state): State<AppState>, headers: HeaderMap) -> Response {
    respond(
        async {
            let t = tenant_admin_tenant(&bearer(&state, &headers).await?)?;
            Ok(ok(StatusCode::OK, json!(state.hub.repo.agents(t).await?)))
        }
        .await,
    )
}

async fn list_channels(State(state): State<AppState>, headers: HeaderMap) -> Response {
    respond(
        async {
            let t = tenant_admin_tenant(&bearer(&state, &headers).await?)?;
            Ok(ok(StatusCode::OK, json!({ "endpoints": state.hub.repo.endpoints(t).await?, "health": state.hub.channel_health().await })))
        }
        .await,
    )
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ProvisionRequest {
    whatsapp_skill: Option<String>,
    voice_skill: Option<String>,
    webchat_skill: Option<String>,
}

pub(crate) fn skill_or(raw: Option<&str>, default: &str) -> AppResult<String> {
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => normalize_skill(s).map_err(|e| AppError::validation("skill", e.0)),
        None => Ok(default.to_string()),
    }
}

async fn provision_simulated(State(state): State<AppState>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    respond(
        async {
            let t = tenant_admin_tenant(&bearer(&state, &headers).await?)?;
            let r: ProvisionRequest = if body.is_empty() { ProvisionRequest::default() } else { json_body(&body)? };
            let eps = state
                .hub
                .provision_simulated_channels(
                    t,
                    &skill_or(r.whatsapp_skill.as_deref(), "support")?,
                    &skill_or(r.voice_skill.as_deref(), "support")?,
                    &skill_or(r.webchat_skill.as_deref(), "sales")?,
                )
                .await?;
            Ok(ok(StatusCode::CREATED, json!(eps)))
        }
        .await,
    )
}

async fn queues(State(state): State<AppState>, headers: HeaderMap) -> Response {
    respond(
        async {
            let t = tenant_admin_tenant(&bearer(&state, &headers).await?)?;
            Ok(ok(StatusCode::OK, json!(state.hub.repo.queue_depths(t).await?)))
        }
        .await,
    )
}

async fn list_conversations(State(state): State<AppState>, headers: HeaderMap) -> Response {
    respond(
        async {
            let p = bearer(&state, &headers).await?;
            let convs = match p.role {
                Role::Agent => {
                    let a = agent_ctx(&p)?;
                    state.hub.repo.agent_conversations(a.tenant_id, a.user_id).await?
                }
                _ => state.hub.repo.recent_conversations(tenant_admin_tenant(&p)?, 100).await?,
            };
            Ok(ok(StatusCode::OK, json!(convs)))
        }
        .await,
    )
}

#[derive(Deserialize)]
struct AfterQuery {
    after_seq: Option<i64>,
}

/// Conversation timeline (OCC-M10-R021, permission-trimmed).
async fn conversation_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<AfterQuery>,
) -> Response {
    respond(
        async {
            let p = bearer(&state, &headers).await?;
            let id = Uuid::parse_str(&id).map_err(|_| AppError::not_found("Conversation not found"))?;
            let tenant = match p.role {
                Role::Agent => agent_ctx(&p)?.tenant_id,
                _ => tenant_admin_tenant(&p)?,
            };
            let conv = state.hub.repo.conversation(tenant, id).await?.ok_or_else(|| AppError::not_found("Conversation not found"))?;
            if p.role == Role::Agent && conv.assigned_agent != Some(p.user_id) {
                return Err(AppError::not_found("Conversation not found"));
            }
            let msgs = state.hub.repo.messages_after(tenant, id, q.after_seq.unwrap_or(0).max(0), 500).await?;
            Ok(ok(StatusCode::OK, json!({ "conversation": conv, "messages": msgs })))
        }
        .await,
    )
}
