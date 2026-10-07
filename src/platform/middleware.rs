//! Tower/Axum middleware: correlation id (API-003/ERR-001), structured access log, per-tenant
//! request metrics, and security headers (SEC-104).

use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;

use super::observability::{with_correlation_id, RequestContext, RequestIdentity, TenantMetrics};
use super::time::Clock;

pub static X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
pub static X_CORRELATION_ID: HeaderName = HeaderName::from_static("x-correlation-id");

#[derive(Clone)]
pub struct ObservabilityState {
    pub metrics: Arc<TenantMetrics>,
    pub clock: Arc<dyn Clock>,
    pub access_log: bool,
}

fn sanitize_request_id(raw: Option<&HeaderValue>) -> Option<String> {
    let v = raw?.to_str().ok()?.trim();
    let ok = !v.is_empty() && v.len() <= 64 && v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    ok.then(|| v.to_string())
}

fn client_ip(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().chars().take(64).collect())
}

pub async fn request_context(State(obs): State<ObservabilityState>, mut req: Request, next: Next) -> Response {
    let started = Instant::now();
    let correlation_id = sanitize_request_id(req.headers().get(&X_REQUEST_ID)).unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
    // Path only — never the query string (invitation tokens travel in the query).
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().chars().take(200).collect());
    let method = req.method().clone();
    let ctx = RequestContext {
        correlation_id: correlation_id.clone(),
        identity: Arc::new(Mutex::new(RequestIdentity::default())),
        ip: client_ip(req.headers()),
        user_agent: req.headers().get(header::USER_AGENT).and_then(|v| v.to_str().ok()).map(|s| s.chars().take(256).collect()),
    };
    req.extensions_mut().insert(ctx.clone());

    let mut resp = with_correlation_id(correlation_id.clone(), next.run(req)).await;

    if let Ok(v) = HeaderValue::from_str(&correlation_id) {
        resp.headers_mut().insert(X_CORRELATION_ID.clone(), v.clone());
        resp.headers_mut().insert(X_REQUEST_ID.clone(), v);
    }
    if let Some((limit, remaining, reset)) = ctx.rate() {
        let h = resp.headers_mut();
        for (name, v) in [("x-ratelimit-limit", limit), ("x-ratelimit-remaining", remaining), ("x-ratelimit-reset", reset)] {
            if let Ok(val) = HeaderValue::from_str(&v.to_string()) {
                h.insert(HeaderName::from_static(name), val);
            }
        }
    }
    let status = resp.status();
    let (tenant_id, actor_id) = ctx.identity();
    if let Some(t) = tenant_id {
        obs.metrics.record(t, obs.clock.now(), status.is_server_error());
    }
    if obs.access_log {
        tracing::info!(
            correlation_id = %correlation_id,
            method = %method,
            route = %route,
            status = status.as_u16(),
            latency_ms = started.elapsed().as_secs_f64() * 1000.0,
            tenant_id = tenant_id.map(|t| t.to_string()).unwrap_or_default(),
            actor_id = actor_id.map(|a| a.to_string()).unwrap_or_default(),
            "request"
        );
    }
    resp
}

#[derive(Clone, Copy)]
pub struct SecurityHeaderConfig {
    pub hsts: bool,
}

pub async fn security_headers(State(cfg): State<SecurityHeaderConfig>, req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    let h = resp.headers_mut();
    h.entry(header::CONTENT_SECURITY_POLICY).or_insert(HeaderValue::from_static(
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
         object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'",
    ));
    h.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(header::REFERRER_POLICY, HeaderValue::from_static("same-origin"));
    h.insert(HeaderName::from_static("permissions-policy"), HeaderValue::from_static("camera=(), microphone=(), geolocation=()"));
    if cfg.hsts {
        h.insert(header::STRICT_TRANSPORT_SECURITY, HeaderValue::from_static("max-age=31536000; includeSubDomains"));
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_sanitised() {
        assert_eq!(sanitize_request_id(Some(&HeaderValue::from_static("abc-123"))), Some("abc-123".into()));
        assert_eq!(sanitize_request_id(Some(&HeaderValue::from_static("a b"))), None);
        let long = "a".repeat(65);
        assert_eq!(sanitize_request_id(Some(&HeaderValue::from_str(&long).unwrap())), None);
    }
}
