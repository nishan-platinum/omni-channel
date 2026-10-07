//! Login / logout / invitation acceptance pages and the bootstrap API token endpoint.

use askama::Template;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::app::AppState;
use crate::platform::errors::{AppError, AppResult};
use crate::platform::security::{constant_time_eq, random_token};
use crate::web_support::{
    cookie_value, redirect_with, render, set_cookie, IncomingFlash, PageCtx, PageError, PageResult, LOGIN_CSRF_COOKIE, SESSION_COOKIE,
};

use super::extract::{request_meta, CsrfForm};
use super::service::{ClientMeta, Role, SessionKind};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/", get(home))
        .route("/login", get(login_page).post(login_submit))
        .route("/logout", post(logout))
        .route("/invitations/accept", get(accept_page).post(accept_submit))
        .route("/v1/bootstrap/token", post(api_token))
}

#[derive(Template)]
#[template(path = "auth/login.html")]
struct LoginPage {
    page: PageCtx,
    login_csrf: String,
    email: String,
    tenant_code: String,
    error: Option<String>,
    brand_name: Option<String>,
    brand_message: Option<String>,
    brand_logo: Option<String>,
    brand_primary: Option<String>,
}

#[derive(Deserialize, Default)]
struct LoginQuery {
    code: Option<String>,
}

async fn home(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match cookie_value(&headers, SESSION_COOKIE) {
        Some(t) => match state.auth.authenticate(&t, SessionKind::Browser).await {
            Ok(p) if p.role == Role::SuperAdmin => Redirect::to("/admin").into_response(),
            Ok(_) => Redirect::to("/tenant").into_response(),
            Err(_) => Redirect::to("/login").into_response(),
        },
        None => Redirect::to("/login").into_response(),
    }
}

/// Branded login when served on a verified, active custom domain (R019 "login page").
async fn branding_for_host(state: &AppState, headers: &HeaderMap) -> Option<(String, Option<String>, Option<String>, String, String)> {
    let host = headers.get(header::HOST)?.to_str().ok()?;
    let (tenant, active) = state.m01.branding.resolve_host(host).await.ok()??;
    if !active {
        return None;
    }
    let info = state.m01.deps.branding.get(&crate::platform::db::AccessScope::System, tenant).await.ok()?;
    let t = state.m01.deps.tenants.get(&crate::platform::db::AccessScope::System, tenant).await.ok()??;
    let logo = info.logo_object_key.as_ref().map(|_| crate::modules::m01_tenancy::application::branding::logo_path(tenant));
    Some((t.name, info.login_message, logo, info.primary_color, t.code.to_string()))
}

async fn login_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<LoginQuery>,
    IncomingFlash(flash): IncomingFlash,
) -> Response {
    let token = random_token(24);
    let brand = branding_for_host(&state, &headers).await;
    let mut page = PageCtx::anonymous("Sign in");
    page.flash = flash;
    let had_flash = page.flash.is_some();
    let p = LoginPage {
        page,
        login_csrf: token.clone(),
        email: String::new(),
        tenant_code: brand.as_ref().map(|b| b.4.clone()).or(q.code).unwrap_or_default(),
        error: None,
        brand_name: brand.as_ref().map(|b| b.0.clone()),
        brand_message: brand.as_ref().and_then(|b| b.1.clone()),
        brand_logo: brand.as_ref().and_then(|b| b.2.clone()),
        brand_primary: brand.as_ref().map(|b| b.3.clone()),
    };
    let mut r = render(&p, had_flash);
    r.headers_mut().append(header::SET_COOKIE, set_cookie(LOGIN_CSRF_COOKIE, &token, 1800, state.config.cookie_secure));
    r
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
    #[serde(default)]
    tenant_code: String,
    #[serde(rename = "_csrf", default)]
    csrf: String,
}

async fn login_submit(State(state): State<AppState>, headers: HeaderMap, req_parts: axum::extract::Request) -> Response {
    let (parts, body) = req_parts.into_parts();
    let meta = request_meta(&parts);
    let bytes = match axum::body::to_bytes(body, 64 * 1024).await {
        Ok(b) => b,
        Err(_) => return PageError(AppError::validation("body", "Invalid request")).into_response(),
    };
    let Ok(f) = serde_urlencoded::from_bytes::<LoginForm>(&bytes) else {
        return PageError(AppError::validation("form", "Invalid form submission")).into_response();
    };
    // Double-submit CSRF for the pre-session login form.
    let cookie = cookie_value(&headers, LOGIN_CSRF_COOKIE).unwrap_or_default();
    if cookie.is_empty() || !constant_time_eq(cookie.as_bytes(), f.csrf.as_bytes()) {
        return PageError(AppError::forbidden("Invalid or missing CSRF token; reload the login page")).into_response();
    }
    let code = Some(f.tenant_code.as_str()).filter(|c| !c.trim().is_empty());
    match state.auth.login(&f.email, &f.password, code, SessionKind::Browser, &meta).await {
        Ok((token, p)) => {
            let to = if p.role == Role::SuperAdmin { "/admin" } else { "/tenant" };
            let mut r = Redirect::to(to).into_response();
            let max_age = state.config.session_absolute_hours * 3600;
            r.headers_mut().append(header::SET_COOKIE, set_cookie(SESSION_COOKIE, &token, max_age, state.config.cookie_secure));
            r.headers_mut().append(header::SET_COOKIE, set_cookie(LOGIN_CSRF_COOKIE, "", 0, state.config.cookie_secure));
            tracing::info!(user_id = %p.user_id, tenant_id = ?p.tenant_id, "login succeeded");
            r
        }
        Err(e) => {
            tracing::info!(code = e.code.as_str(), "login refused");
            let token = random_token(24);
            let brand = branding_for_host(&state, &headers).await;
            let p = LoginPage {
                page: PageCtx::anonymous("Sign in"),
                login_csrf: token.clone(),
                email: f.email.chars().take(320).collect(),
                tenant_code: f.tenant_code.chars().take(32).collect(),
                error: Some(format!("{} ({})", e.message, e.code.as_str())),
                brand_name: brand.as_ref().map(|b| b.0.clone()),
                brand_message: brand.as_ref().and_then(|b| b.1.clone()),
                brand_logo: brand.as_ref().and_then(|b| b.2.clone()),
                brand_primary: brand.as_ref().map(|b| b.3.clone()),
            };
            let mut r = render(&p, false);
            *r.status_mut() = e.status();
            r.headers_mut().append(header::SET_COOKIE, set_cookie(LOGIN_CSRF_COOKIE, &token, 1800, state.config.cookie_secure));
            r
        }
    }
}

#[derive(Deserialize)]
struct Empty {}

async fn logout(State(state): State<AppState>, f: CsrfForm<Empty>) -> PageResult {
    let _ = f.form;
    state.auth.logout(f.user.session_id).await?;
    let mut r = redirect_with("/login", "success", "You have been signed out.");
    r.headers_mut().append(header::SET_COOKIE, set_cookie(SESSION_COOKIE, "", 0, state.config.cookie_secure));
    Ok(r)
}

#[derive(Template)]
#[template(path = "auth/accept_invite.html")]
struct AcceptPage {
    page: PageCtx,
    token: String,
    login_csrf: String,
    email: String,
    tenant_name: String,
    tenant_code: String,
    min_length: usize,
    error: Option<String>,
}

#[derive(Deserialize)]
struct TokenQuery {
    token: String,
}

async fn accept_page(State(state): State<AppState>, Query(q): Query<TokenQuery>) -> PageResult {
    let (email, info, _) = state.auth.invitation(&q.token).await?;
    let csrf = random_token(24);
    let p = AcceptPage {
        page: PageCtx::anonymous("Set your password"),
        token: q.token,
        login_csrf: csrf.clone(),
        email,
        tenant_name: info.as_ref().map(|i| i.name.clone()).unwrap_or_default(),
        tenant_code: info.as_ref().map(|i| i.code.clone()).unwrap_or_default(),
        min_length: info.as_ref().map(|i| i.password_min_length).unwrap_or(12).max(12),
        error: None,
    };
    let mut r = render(&p, false);
    r.headers_mut().append(header::SET_COOKIE, set_cookie(LOGIN_CSRF_COOKIE, &csrf, 1800, state.config.cookie_secure));
    Ok(r)
}

#[derive(Deserialize)]
struct AcceptForm {
    token: String,
    password: String,
    confirm: String,
    #[serde(rename = "_csrf", default)]
    csrf: String,
}

async fn accept_submit(State(state): State<AppState>, headers: HeaderMap, Form(f): Form<AcceptForm>) -> PageResult {
    let cookie = cookie_value(&headers, LOGIN_CSRF_COOKIE).unwrap_or_default();
    if cookie.is_empty() || !constant_time_eq(cookie.as_bytes(), f.csrf.as_bytes()) {
        return Err(AppError::forbidden("Invalid or missing CSRF token; reload the page").into());
    }
    match state.auth.accept_invitation(&f.token, &f.password, &f.confirm).await {
        Ok(_) => Ok(redirect_with("/login", "success", "Password set. You can sign in once your tenant is active.")),
        Err(e) if e.code == crate::platform::errors::ErrorCode::ValidationFailed => {
            let (email, info, _) = state.auth.invitation(&f.token).await?;
            let csrf = random_token(24);
            let p = AcceptPage {
                page: PageCtx::anonymous("Set your password"),
                token: f.token,
                login_csrf: csrf.clone(),
                email,
                tenant_name: info.as_ref().map(|i| i.name.clone()).unwrap_or_default(),
                tenant_code: info.as_ref().map(|i| i.code.clone()).unwrap_or_default(),
                min_length: info.as_ref().map(|i| i.password_min_length).unwrap_or(12).max(12),
                error: Some(e.message),
            };
            let mut r = render(&p, false);
            *r.status_mut() = axum::http::StatusCode::BAD_REQUEST;
            r.headers_mut().append(header::SET_COOKIE, set_cookie(LOGIN_CSRF_COOKIE, &csrf, 1800, state.config.cookie_secure));
            Ok(r)
        }
        Err(e) => Err(e.into()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenRequest {
    email: String,
    password: String,
    tenant_code: Option<String>,
}

/// Bootstrap bearer token for the `/v1` API (stand-in for OAuth2, API-002; ADR-0007).
async fn api_token(State(state): State<AppState>, req: axum::extract::Request) -> AppResult<Response> {
    let (parts, body) = req.into_parts();
    let meta: ClientMeta = request_meta(&parts);
    let bytes = axum::body::to_bytes(body, 64 * 1024).await.map_err(|_| AppError::validation("body", "Invalid request body"))?;
    let r: TokenRequest = serde_json::from_slice(&bytes).map_err(|e| AppError::validation("body", format!("Invalid JSON: {e}")))?;
    let (token, p) = state.auth.login(&r.email, &r.password, r.tenant_code.as_deref(), SessionKind::Api, &meta).await?;
    Ok(Json(json!({
        "data": {
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": state.config.api_token_ttl_minutes * 60,
            "scopes": p.scopes,
            "tenant_id": p.tenant_id,
            "role": p.role,
        },
        "meta": { "correlation_id": crate::platform::observability::current_correlation_id() }
    }))
    .into_response())
}
