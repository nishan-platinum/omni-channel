//! Domain errors (pure; mapped to API/HTML errors in the application layer).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub field: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    #[error("validation failed")]
    Validation(Vec<Violation>),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{message}")]
    QuotaExceeded { metric: String, message: String, retry_after_secs: u64 },
    #[error("{0}")]
    DomainNotVerified(String),
}

impl DomainError {
    pub fn field(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Validation(vec![Violation { field: field.into(), message: message.into() }])
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }
}

/// Accumulates field violations so a form can show every error at once (STD-001).
#[derive(Debug, Default)]
pub struct Violations(Vec<Violation>);

impl Violations {
    pub fn push(&mut self, field: &str, message: impl Into<String>) {
        self.0.push(Violation { field: field.to_string(), message: message.into() });
    }

    pub fn capture<T>(&mut self, r: Result<T, DomainError>) -> Option<T> {
        match r {
            Ok(v) => Some(v),
            Err(DomainError::Validation(v)) => {
                self.0.extend(v);
                None
            }
            Err(other) => {
                self.0.push(Violation { field: "_".into(), message: other.to_string() });
                None
            }
        }
    }

    pub fn into_result(self) -> Result<(), DomainError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(DomainError::Validation(self.0))
        }
    }
}
