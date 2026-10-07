//! Shared server-rendered UI support: layout context, flash messages (short-lived cookie),
//! HTML error pages, cookie helpers. Askama auto-escaping is always on.

use askama::Template;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};

use crate::bootstrap_auth::service::{Principal, Role};
use crate::platform::errors::{AppError, ErrorCode};
use crate::platform::observability::current_correlation_id;

pub const SESSION_COOKIE: &str = "occ_session";
pub const LOGIN_CSRF_COOKIE: &str = "occ_login_csrf";
pub const FLASH_COOKIE: &str = "occ_flash";

pub fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_string())
}

pub fn set_cookie(name: &str, value: &str, max_age_secs: i64, secure: bool) -> HeaderValue {
    let s = format!("{name}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age_secs}{}", if secure { "; Secure" } else { "" });
    HeaderValue::from_str(&s).unwrap_or_else(|_| HeaderValue::from_static("invalid=1; Max-Age=0"))
}

#[derive(Debug, Clone)]
pub struct Flash {
    pub kind: String,
    pub message: String,
}

fn encode_flash(kind: &str, msg: &str) -> String {
    use base64::Engine;
    let raw = format!("{kind}|{}", msg.chars().take(300).collect::<String>());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

fn decode_flash(v: &str) -> Option<Flash> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(v).ok()?;
    let s = String::from_utf8(raw).ok()?;
    let (k, m) = s.split_once('|')?;
    let kind = if k == "error" { "error" } else { "success" };
    Some(Flash { kind: kind.into(), message: m.into() })
}

/// Extracts (and implicitly consumes) the flash message of the previous POST/redirect.
pub struct IncomingFlash(pub Option<Flash>);

impl<S: Send + Sync> FromRequestParts<S> for IncomingFlash {
    type Rejection = std::convert::Infallible;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(cookie_value(&parts.headers, FLASH_COOKIE).and_then(|v| decode_flash(&v))))
    }
}

/// POST/redirect/GET with a flash message.
pub fn redirect_with(to: &str, kind: &str, message: &str) -> Response {
    let mut r = Redirect::to(to).into_response();
    r.headers_mut().append(header::SET_COOKIE, set_cookie(FLASH_COOKIE, &encode_flash(kind, message), 30, false));
    r
}

#[derive(Debug, Clone)]
pub struct NavUser {
    pub display_name: String,
    pub email: String,
    pub role_label: &'static str,
    pub is_super_admin: bool,
    pub tenant_name: Option<String>,
    pub tenant_code: Option<String>,
    pub tenant_status: Option<String>,
    pub read_only: bool,
}

impl NavUser {
    pub fn from(p: &Principal) -> Self {
        Self {
            display_name: p.display_name.clone(),
            email: p.email.clone(),
            role_label: p.role.label(),
            is_super_admin: p.role == Role::SuperAdmin,
            tenant_name: p.tenant_name.clone(),
            tenant_code: p.tenant_code.clone(),
            tenant_status: p.tenant_status.clone(),
            read_only: p.read_only,
        }
    }
}

/// Layout context shared by every page (`layouts/base.html`).
#[derive(Debug, Clone)]
pub struct PageCtx {
    pub title: String,
    pub user: Option<NavUser>,
    pub csrf: String,
    pub flash: Option<Flash>,
    pub nav: &'static str,
    pub dev_outbox: bool,
    pub theme: bool,
}

impl PageCtx {
    pub fn anonymous(title: &str) -> Self {
        Self { title: title.into(), user: None, csrf: String::new(), flash: None, nav: "", dev_outbox: false, theme: false }
    }

    pub fn for_user(title: impl Into<String>, p: &Principal, nav: &'static str, flash: Option<Flash>, dev_outbox: bool) -> Self {
        Self {
            title: title.into(),
            user: Some(NavUser::from(p)),
            csrf: p.csrf_token.clone(),
            flash,
            nav,
            dev_outbox: dev_outbox && p.role == Role::SuperAdmin,
            theme: p.role == Role::TenantAdmin,
        }
    }
}

/// Renders a template; a flash cookie is cleared once shown.
pub fn render<T: Template>(t: &T, clear_flash: bool) -> Response {
    match t.render() {
        Ok(html) => {
            let mut r = Html(html).into_response();
            if clear_flash {
                r.headers_mut().append(header::SET_COOKIE, set_cookie(FLASH_COOKIE, "", 0, false));
            }
            r
        }
        Err(e) => {
            tracing::error!(error = %e, correlation_id = %current_correlation_id(), "template render failed");
            (StatusCode::INTERNAL_SERVER_ERROR, Html("<h1>Unexpected server error</h1>".to_string())).into_response()
        }
    }
}

#[derive(Template)]
#[template(path = "error.html")]
pub struct ErrorPage {
    pub page: PageCtx,
    pub status: u16,
    pub code: String,
    pub message: String,
    pub correlation_id: String,
}

/// HTML rendering of an AppError (never exposes causes; shows the correlation id, STD-002).
pub struct PageError(pub AppError);

impl From<AppError> for PageError {
    fn from(e: AppError) -> Self {
        Self(e)
    }
}

impl From<sqlx::Error> for PageError {
    fn from(e: sqlx::Error) -> Self {
        Self(AppError::from(e))
    }
}

impl IntoResponse for PageError {
    fn into_response(self) -> Response {
        let e = self.0;
        e.log();
        if e.code == ErrorCode::Unauthenticated {
            return Redirect::to("/login").into_response();
        }
        let page = ErrorPage {
            page: PageCtx::anonymous("Error"),
            status: e.status().as_u16(),
            code: e.code.as_str().into(),
            message: e.message.clone(),
            correlation_id: current_correlation_id(),
        };
        let mut r = render(&page, false);
        *r.status_mut() = e.status();
        r
    }
}

pub type PageResult = Result<Response, PageError>;

pub async fn not_found() -> Response {
    PageError(AppError::not_found("Page not found")).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flash_roundtrip_and_cookie_parse() {
        let enc = encode_flash("success", "Tenant <b>created</b>");
        let f = decode_flash(&enc).unwrap();
        assert_eq!(f.message, "Tenant <b>created</b>");
        let mut h = HeaderMap::new();
        h.insert(header::COOKIE, HeaderValue::from_static("a=1; occ_session=abc; b=2"));
        assert_eq!(cookie_value(&h, "occ_session").as_deref(), Some("abc"));
        assert_eq!(cookie_value(&h, "missing"), None);
    }

    #[test]
    fn cookies_are_httponly_samesite() {
        let c = set_cookie(SESSION_COOKIE, "x", 60, true);
        let s = c.to_str().unwrap();
        assert!(s.contains("HttpOnly") && s.contains("SameSite=Lax") && s.contains("Secure"));
    }
}
