//! Per-tenant encryption keys (OCC-M01-R010, P2; SEC-111/112). Envelope encryption: a per-tenant
//! data key (DEK) wrapped by a key-encryption key held by the KMS. BYOK only for Regulated tier.

use std::sync::OnceLock;

use chrono::{DateTime, Duration, Utc};
use regex::Regex;
use serde::Serialize;

use super::errors::DomainError;
use super::storage::Tier;

/// SEC-112: data keys rotate at most every 90 days.
pub const ROTATION_DAYS: i64 = 90;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyKind {
    PlatformManaged,
    CustomerSupplied,
}

impl KeyKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlatformManaged => "platform_managed",
            Self::CustomerSupplied => "customer_supplied",
        }
    }
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "platform_managed" => Ok(Self::PlatformManaged),
            "customer_supplied" => Ok(Self::CustomerSupplied),
            _ => Err(DomainError::field("key_kind", "Unknown key kind")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyState {
    PendingValidation,
    Active,
    Retired,
    Destroyed,
    Failed,
}

impl KeyState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PendingValidation => "pending_validation",
            Self::Active => "active",
            Self::Retired => "retired",
            Self::Destroyed => "destroyed",
            Self::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "pending_validation" => Ok(Self::PendingValidation),
            "active" => Ok(Self::Active),
            "retired" => Ok(Self::Retired),
            "destroyed" => Ok(Self::Destroyed),
            "failed" => Ok(Self::Failed),
            _ => Err(DomainError::field("state", "Unknown key state")),
        }
    }
}

fn byok_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^byok:[A-Za-z0-9:/_.\-]{8,200}$").expect("static regex"))
}

/// Customer-supplied key references are only for Regulated tenants (UJ-19 E2).
pub fn validate_byok(tier: Tier, key_ref: &str) -> Result<String, DomainError> {
    if tier != Tier::Regulated {
        return Err(DomainError::forbidden("Customer-supplied keys (BYOK) are available to Regulated tenants only"));
    }
    let s = key_ref.trim();
    if byok_re().is_match(s) {
        Ok(s.to_string())
    } else {
        Err(DomainError::field("key_ref", "Key reference must look like byok:<provider>/<key-id> (8-200 safe characters)"))
    }
}

pub fn rotate_after(from: DateTime<Utc>) -> DateTime<Utc> {
    from + Duration::days(ROTATION_DAYS)
}

pub fn rotation_due(rotate_after: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now >= rotate_after
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byok_only_regulated() {
        assert!(validate_byok(Tier::Premium, "byok:aws-kms/arn-1234").is_err());
        assert!(validate_byok(Tier::Regulated, "byok:aws-kms/arn-1234").is_ok());
        assert!(validate_byok(Tier::Regulated, "plain-key-material").is_err());
        assert!(validate_byok(Tier::Regulated, "byok:short").is_err(), "too short");
        assert!(validate_byok(Tier::Regulated, "byok:a b c d e f").is_err());
    }

    #[test]
    fn rotation_window() {
        let now = Utc::now();
        assert!(!rotation_due(rotate_after(now), now));
        assert!(rotation_due(rotate_after(now), now + Duration::days(91)));
    }
}
