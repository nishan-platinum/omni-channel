//! Shared error model. Codes and HTTP statuses follow the unified error catalogue (spec §7.5 /
//! Part D §60, ADR-0006). Internal causes are logged with the correlation id and never returned.

use std::fmt;

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::observability::current_correlation_id;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ErrorCode {
    ValidationFailed,
    Unauthenticated,
    Forbidden,
    NotFound,
    Conflict,
    RateLimited,
    Internal,
    TenantSuspended,
    QuotaExceeded,
    DomainNotVerified,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ValidationFailed => "VALIDATION_FAILED",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::Forbidden => "FORBIDDEN",
            Self::NotFound => "NOT_FOUND",
            Self::Conflict => "CONFLICT",
            Self::RateLimited => "RATE_LIMITED",
            Self::Internal => "INTERNAL",
            Self::TenantSuspended => "TENANT_SUSPENDED",
            Self::QuotaExceeded => "QUOTA_EXCEEDED",
            Self::DomainNotVerified => "DOMAIN_NOT_VERIFIED",
        }
    }

    pub fn status(self) -> StatusCode {
        match self {
            Self::ValidationFailed => StatusCode::BAD_REQUEST,
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::Forbidden | Self::TenantSuspended | Self::DomainNotVerified => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict => StatusCode::CONFLICT,
            Self::RateLimited | Self::QuotaExceeded => StatusCode::TOO_MANY_REQUESTS,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FieldError {
    pub field: String,
    pub code: String,
    pub message: String,
}

impl FieldError {
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self { field: field.into(), code: "VALIDATION_FAILED".to_string(), message: message.into() }
    }
}

#[derive(Debug)]
pub struct AppError {
    pub code: ErrorCode,
    pub message: String,
    pub details: Vec<FieldError>,
    pub retry_after_secs: Option<u64>,
    source: Option<anyhow::Error>,
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for AppError {}

impl AppError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), details: Vec::new(), retry_after_secs: None, source: None }
    }

    pub fn validation(field: impl Into<String>, message: impl Into<String>) -> Self {
        let message = message.into();
        let mut e = Self::new(ErrorCode::ValidationFailed, message.clone());
        e.details.push(FieldError::new(field, message));
        e
    }

    pub fn validation_many(details: Vec<FieldError>) -> Self {
        let message = if details.len() == 1 { details[0].message.clone() } else { "One or more fields failed validation".to_string() };
        let mut e = Self::new(ErrorCode::ValidationFailed, message);
        e.details = details;
        e
    }

    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthenticated, message)
    }
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Forbidden, message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, message)
    }
    pub fn tenant_suspended() -> Self {
        Self::new(ErrorCode::TenantSuspended, "Tenant is suspended; access blocked")
    }
    pub fn rate_limited(message: impl Into<String>, retry_after_secs: u64) -> Self {
        let mut e = Self::new(ErrorCode::RateLimited, message);
        e.retry_after_secs = Some(retry_after_secs.max(1));
        e
    }
    pub fn quota_exceeded(message: impl Into<String>, retry_after_secs: u64) -> Self {
        let mut e = Self::new(ErrorCode::QuotaExceeded, message);
        e.retry_after_secs = Some(retry_after_secs.max(1));
        e
    }
    pub fn domain_not_verified(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::DomainNotVerified, message)
    }

    /// Unexpected failure: the cause is kept for logs only.
    pub fn internal(source: impl Into<anyhow::Error>) -> Self {
        Self {
            code: ErrorCode::Internal,
            message: "Unexpected server error".to_string(),
            details: Vec::new(),
            retry_after_secs: None,
            source: Some(source.into()),
        }
    }

    pub fn status(&self) -> StatusCode {
        self.code.status()
    }

    pub fn field_message(&self, field: &str) -> Option<&str> {
        self.details.iter().find(|d| d.field == field).map(|d| d.message.as_str())
    }

    /// Logs internal causes exactly once, with correlation id. Never logs request payloads.
    pub fn log(&self) {
        if let Some(src) = &self.source {
            tracing::error!(
                correlation_id = %current_correlation_id(),
                code = self.code.as_str(),
                error = %format!("{src:#}"),
                "internal error"
            );
        }
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        AppError::internal(e)
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        match e.downcast::<AppError>() {
            Ok(app) => app,
            Err(other) => AppError::internal(other),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'static str,
    message: &'a str,
    details: &'a [FieldError],
    correlation_id: String,
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorBody<'a>,
}

/// JSON error envelope per API-007: `{"error":{code,message,details[],correlation_id}}`.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        self.log();
        let body = ErrorEnvelope {
            error: ErrorBody {
                code: self.code.as_str(),
                message: &self.message,
                details: &self.details,
                correlation_id: current_correlation_id(),
            },
        };
        let mut resp = (self.status(), axum::Json(body)).into_response();
        if let Some(secs) = self.retry_after_secs {
            if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
                resp.headers_mut().insert(header::RETRY_AFTER, v);
            }
        }
        resp
    }
}

pub type AppResult<T> = Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_follow_catalogue() {
        assert_eq!(ErrorCode::ValidationFailed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(ErrorCode::TenantSuspended.status(), StatusCode::FORBIDDEN);
        assert_eq!(ErrorCode::QuotaExceeded.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(ErrorCode::DomainNotVerified.status(), StatusCode::FORBIDDEN);
        assert_eq!(ErrorCode::Conflict.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn internal_hides_cause() {
        let e = AppError::internal(anyhow::anyhow!("password=hunter2 connection refused"));
        assert_eq!(e.message, "Unexpected server error");
        assert!(!format!("{e}").contains("hunter2"));
    }

    #[test]
    fn rate_limited_sets_retry_after() {
        let e = AppError::quota_exceeded("x", 0);
        assert_eq!(e.retry_after_secs, Some(1));
    }
}
