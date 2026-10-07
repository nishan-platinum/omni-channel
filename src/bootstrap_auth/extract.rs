//! Axum extractors for authentication, role authorization and CSRF (Tower/Axum translation of
//! "Laravel middleware"). Hiding UI elements is never authorization — these run on every route.

use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::header;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Redirect, Response};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::app::AppState;
use crate::platform::errors::AppError;
use crate::platform::observability::RequestContext;
use crate::platform::security::constant_time_eq;
use crate::web_support::{cookie_value, PageError, SESSION_COOKIE};

use super::service::{Principal, Role, SessionKind};

fn note_identity(parts: &Parts, p: &Principal) {
    if let Some(ctx) = parts.extensions.get::<RequestContext>() {
        ctx.set_identity(p.tenant_id, Some(p.user_id));
    }
}

pub fn request_meta(parts: &Parts) -> super::service::ClientMeta {
    let ctx = parts.extensions.get::<RequestContext>();
    super::service::ClientMeta { ip: ctx.and_then(|c| c.ip.clone()), user_agent: ctx.and_then(|c| c.user_agent.clone()) }
}

/// Browser session principal (cookie). Unauthenticated → redirect to /login.
pub struct WebUser(pub Principal, pub RequestContext);

pub enum WebAuthRejection {
    Login,
    Error(PageError),
}

impl IntoResponse for WebAuthRejection {
    fn into_response(self) -> Response {
        match self {
            Self::Login => Redirect::to("/login").into_response(),
            Self::Error(e) => e.into_response(),
        }
    }
}

impl FromRequestParts<AppState> for WebUser {
    type Rejection = WebAuthRejection;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let token = cookie_value(&parts.headers, SESSION_COOKIE).ok_or(WebAuthRejection::Login)?;
        let ctx = parts.extensions.get::<RequestContext>().cloned().ok_or(WebAuthRejection::Login)?;
        match state.auth.authenticate(&token, SessionKind::Browser).await {
            Ok(p) => {
                note_identity(parts, &p);
                Ok(Self(p, ctx))
            }
            Err(e) if e.code == crate::platform::errors::ErrorCode::Unauthenticated => Err(WebAuthRejection::Login),
            Err(e) => Err(WebAuthRejection::Error(PageError(e))),
        }
    }
}

/// Super Admin only (server-side check).
pub struct SuperAdmin(pub Principal, pub RequestContext);

impl FromRequestParts<AppState> for SuperAdmin {
    type Rejection = WebAuthRejection;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let WebUser(p, ctx) = WebUser::from_request_parts(parts, state).await?;
        if p.role != Role::SuperAdmin {
            return Err(WebAuthRejection::Error(PageError(AppError::forbidden("Platform Super Admin role required"))));
        }
        Ok(Self(p, ctx))
    }
}

/// Tenant Admin only. The tenant comes from the session — routes take no tenant id.
pub struct TenantAdmin(pub Principal, pub RequestContext);

impl FromRequestParts<AppState> for TenantAdmin {
    type Rejection = WebAuthRejection;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let WebUser(p, ctx) = WebUser::from_request_parts(parts, state).await?;
        if p.role != Role::TenantAdmin || p.tenant_id.is_none() {
            return Err(WebAuthRejection::Error(PageError(AppError::forbidden("Tenant Admin role required"))));
        }
        Ok(Self(p, ctx))
    }
}

/// API bearer principal (no cookies are accepted on /v1, so the API is not CSRF-exposed).
pub struct ApiUser(pub Principal, pub RequestContext);

impl FromRequestParts<AppState> for ApiUser {
    type Rejection = AppError;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::trim)
            .ok_or_else(|| AppError::unauthenticated("Missing or invalid credentials"))?;
        let ctx = parts.extensions.get::<RequestContext>().cloned().ok_or_else(|| AppError::unauthenticated("Missing request context"))?;
        let p = state.auth.authenticate(token, SessionKind::Api).await?;
        note_identity(parts, &p);
        Ok(Self(p, ctx))
    }
}

#[derive(Deserialize)]
struct CsrfField {
    #[serde(rename = "_csrf")]
    csrf: Option<String>,
}

pub fn csrf_matches(p: &Principal, token: Option<&str>) -> bool {
    token.is_some_and(|t| constant_time_eq(t.as_bytes(), p.csrf_token.as_bytes()))
}

/// URL-encoded form + authenticated user + verified synchronizer CSRF token (SEC-103).
/// The token is read from the `_csrf` field or the `X-CSRF-Token` header (HTMX).
pub struct CsrfForm<T> {
    pub user: Principal,
    pub ctx: RequestContext,
    pub form: T,
}

impl<T: DeserializeOwned + Send> FromRequest<AppState> for CsrfForm<T> {
    type Rejection = WebAuthRejection;

    async fn from_request(req: Request, state: &AppState) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = req.into_parts();
        let WebUser(user, ctx) = WebUser::from_request_parts(&mut parts, state).await?;
        let header_token = parts.headers.get("x-csrf-token").and_then(|v| v.to_str().ok()).map(str::to_string);
        let req = Request::from_parts(parts, body);
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|_| WebAuthRejection::Error(PageError(AppError::validation("body", "Request body too large or unreadable"))))?;
        let field: CsrfField = serde_urlencoded::from_bytes(&bytes).unwrap_or(CsrfField { csrf: None });
        let token = field.csrf.or(header_token);
        if !csrf_matches(&user, token.as_deref()) {
            tracing::warn!(user_id = %user.user_id, "CSRF token missing or invalid");
            return Err(WebAuthRejection::Error(PageError(AppError::forbidden(
                "Invalid or missing CSRF token; reload the page and try again",
            ))));
        }
        let form: T = serde_urlencoded::from_bytes(&bytes)
            .map_err(|e| WebAuthRejection::Error(PageError(AppError::validation("form", format!("Invalid form submission: {e}")))))?;
        Ok(Self { user, ctx, form })
    }
}
