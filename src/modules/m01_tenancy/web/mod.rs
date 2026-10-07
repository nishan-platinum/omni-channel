//! M01 web layer: the `/v1` API (spec §10.6 + documented extensions) and the server-rendered
//! Super Admin / Tenant Admin UI. Handlers translate HTTP ↔ application services only.

pub mod admin;
pub mod api;
pub mod forms;
pub mod shared;
pub mod tenant_ui;
pub mod views;

use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use uuid::Uuid;

use crate::app::AppState;
use crate::bootstrap_auth::extract::{SuperAdmin, WebUser};
use crate::bootstrap_auth::service::{Principal, Role};
use crate::platform::errors::AppError;
use crate::platform::observability::RequestContext;
use crate::web_support::{render, IncomingFlash, PageCtx, PageError, PageResult};

use super::application::{Actor, ActorRole};
use super::domain::TenantId;

pub use crate::web_support as html;

pub fn routes() -> Router<AppState> {
    Router::new()
        .merge(api::routes())
        .merge(admin::routes())
        .merge(tenant_ui::routes())
        .route("/assets/tenants/{id}/logo", get(logo))
        .route("/theme.css", get(theme_css))
        .route("/dev/outbox", get(dev_outbox))
}

/// Maps the authenticated principal to the application-layer actor. The tenant comes from the
/// server-side session — never from the request.
pub fn actor(p: &Principal, ctx: &RequestContext) -> Actor {
    Actor {
        user_id: Some(p.user_id),
        role: match p.role {
            Role::SuperAdmin => ActorRole::SuperAdmin,
            Role::TenantAdmin => ActorRole::TenantAdmin,
            Role::Agent => ActorRole::Agent,
        },
        tenant_id: p.tenant_id.map(TenantId),
        email: Some(p.email.clone()),
        correlation_id: Some(ctx.correlation_id.clone()),
        ip: ctx.ip.clone(),
        user_agent: ctx.user_agent.clone(),
    }
}

pub fn parse_tenant_id(raw: &str) -> Result<TenantId, AppError> {
    Uuid::parse_str(raw).map(TenantId).map_err(|_| AppError::not_found("Tenant does not exist"))
}

/// BR-M01-005: a request addressed to a claimed custom domain is only served once that domain
/// is verified and activated. Unknown hosts (localhost, platform host) pass through.
pub async fn host_guard(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let host = req.headers().get(header::HOST).and_then(|h| h.to_str().ok()).map(|h| h.split(':').next().unwrap_or(h).to_ascii_lowercase());
    let public_host = url::Url::parse(&state.config.public_base_url).ok().and_then(|u| u.host_str().map(str::to_ascii_lowercase));
    if let Some(h) = host {
        let is_platform =
            public_host.as_deref() == Some(h.as_str()) || h == "localhost" || h == "127.0.0.1" || h == "app" || !h.contains('.');
        if !is_platform {
            match state.m01.branding.resolve_host(&h).await {
                Ok(Some((_, true))) | Ok(None) => {}
                Ok(Some((_, false))) => {
                    return (StatusCode::MISDIRECTED_REQUEST, "This domain is not verified for any tenant (BR-M01-005).").into_response();
                }
                Err(e) => return e.into_response(),
            }
        }
    }
    next.run(req).await
}

/// Public logo asset. SVGs are served with a sandboxing CSP so they can never run script even
/// when opened directly; uploads were already content-validated.
async fn logo(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let Ok(tid) = parse_tenant_id(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match state.m01.branding.logo(tid).await {
        Ok(Some((ct, bytes))) => {
            let mut r = bytes.into_response();
            let h = r.headers_mut();
            h.insert(header::CONTENT_TYPE, HeaderValue::from_str(&ct).unwrap_or(HeaderValue::from_static("application/octet-stream")));
            h.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("default-src 'none'; style-src 'unsafe-inline'; sandbox"));
            h.insert(header::CACHE_CONTROL, HeaderValue::from_static("public, max-age=300"));
            r
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            e.log();
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// Tenant theme tokens (M27 applies M01-F06 branding). Colours are validated hex values.
async fn theme_css(WebUser(p, _): WebUser) -> Response {
    let primary = p.primary_color.as_deref().filter(|c| c.len() == 7 && c.starts_with('#')).unwrap_or("#0B2130");
    let secondary = p.secondary_color.as_deref().filter(|c| c.len() == 7 && c.starts_with('#')).unwrap_or("#F26A21");
    let css = if p.role != Role::SuperAdmin {
        format!(":root {{ --brand-primary: {primary}; --brand-secondary: {secondary}; }}\n")
    } else {
        String::new()
    };
    let mut r = css.into_response();
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/css; charset=utf-8"));
    r.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// Development-only notification outbox (stand-in for the M25 email inbox). Disabled outside
/// APP_ENV=development and restricted to the Super Admin.
async fn dev_outbox(State(state): State<AppState>, SuperAdmin(p, _): SuperAdmin, IncomingFlash(flash): IncomingFlash) -> PageResult {
    if !state.config.app_env.is_development() {
        return Err(PageError(AppError::not_found("Page not found")));
    }
    let entries = super::infrastructure::adapters::list_outbox(&state.db.app, 200).await?;
    let had = flash.is_some();
    let v = views::OutboxView { page: PageCtx::for_user("Development outbox", &p, "outbox", flash, true), entries };
    Ok(render(&v, had))
}
