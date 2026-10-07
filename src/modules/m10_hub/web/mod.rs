//! M10 hub web layer: WebSocket sessions (agent + customer), channel webhooks, the `/v1/hub` JSON
//! API and the server-rendered agent desktop / customer chat / simulator console / hub admin.
//! Handlers translate HTTP ↔ `HubService` only — no SQL here (CLAUDE.md rule 2).

pub mod api;
pub mod ui;
pub mod webhooks;
pub mod ws;

use axum::http::{header, HeaderMap};
use axum::Router;
use serde_json::{json, Value};

use crate::app::AppState;
use crate::bootstrap_auth::service::{Principal, Role, SessionKind};
use crate::platform::errors::{AppError, AppResult};
use crate::platform::observability::current_correlation_id;
use crate::web_support::{cookie_value, SESSION_COOKIE};

use super::application::AgentCtx;

pub fn routes() -> Router<AppState> {
    Router::new().merge(ws::routes()).merge(webhooks::routes()).merge(api::routes()).merge(ui::routes())
}

pub(crate) fn envelope(data: Value) -> Value {
    json!({ "data": data, "meta": { "correlation_id": current_correlation_id() } })
}

/// How a hub caller authenticated: a browser session cookie or an API bearer token.
pub(crate) enum Credential {
    Cookie(String),
    Bearer(String),
}

pub(crate) fn credential(headers: &HeaderMap) -> Option<Credential> {
    if let Some(b) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")) {
        return Some(Credential::Bearer(b.trim().to_string()));
    }
    cookie_value(headers, SESSION_COOKIE).map(Credential::Cookie)
}

pub(crate) async fn authenticate(state: &AppState, c: &Credential) -> AppResult<Principal> {
    match c {
        Credential::Cookie(t) => state.auth.authenticate(t, SessionKind::Browser).await,
        Credential::Bearer(t) => state.auth.authenticate(t, SessionKind::Api).await,
    }
}

/// Agent context from the authenticated principal. Tenant comes from the session — never input.
pub(crate) fn agent_ctx(p: &Principal) -> AppResult<AgentCtx> {
    match (p.role, p.tenant_id) {
        (Role::Agent, Some(t)) if p.has_scope("hub:agent") => Ok(AgentCtx { tenant_id: t, user_id: p.user_id }),
        _ => Err(AppError::forbidden("Agent role required")),
    }
}

pub(crate) fn tenant_admin_tenant(p: &Principal) -> AppResult<uuid::Uuid> {
    match (p.role, p.tenant_id) {
        (Role::TenantAdmin, Some(t)) if p.has_scope("hub:admin") => Ok(t),
        _ => Err(AppError::forbidden("Tenant Admin role required")),
    }
}

/// Cross-Site WebSocket Hijacking guard: a cookie-authenticated upgrade must come from our own
/// origin (browsers always send Origin on WebSocket requests).
pub(crate) fn same_origin(parts_headers: &HeaderMap) -> bool {
    let host = parts_headers.get(header::HOST).and_then(|h| h.to_str().ok());
    let origin = parts_headers.get(header::ORIGIN).and_then(|h| h.to_str().ok());
    match (host, origin) {
        (Some(h), Some(o)) => url::Url::parse(o).ok().is_some_and(|u| {
            let o_host = match u.port() {
                Some(p) => format!("{}:{p}", u.host_str().unwrap_or("")),
                None => u.host_str().unwrap_or("").to_string(),
            };
            o_host.eq_ignore_ascii_case(h)
        }),
        _ => false,
    }
}
