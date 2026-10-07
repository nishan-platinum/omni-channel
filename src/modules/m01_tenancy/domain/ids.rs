//! Identifiers and the tenant code value object (BR-M01-001).

use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::errors::DomainError;

/// Tenant identifier. Physically `tenantadm.tenants.id`, exposed as `tenant_id` (ADR-0003).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(pub Uuid);

impl TenantId {
    /// UUID v7, application-generated (DBS-002).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Server-generated identifier for per-tenant schemas/databases: `tn_<32 hex>`.
    pub fn storage_ident(&self) -> String {
        format!("tn_{}", self.0.simple())
    }
}

impl Default for TenantId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TenantId {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s.trim()).map(Self).map_err(|_| DomainError::field("tenant_id", "Invalid tenant id"))
    }
}

fn code_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-z0-9-]{3,32}$").expect("static regex"))
}

/// `tenant_code`: 3–32 chars of `^[a-z0-9-]+$`, unique platform-wide, immutable after creation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantCode(String);

pub const TENANT_CODE_ERROR: &str = "Code must be 3-32 lowercase alphanumeric or hyphen";

impl TenantCode {
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        let s = raw.trim();
        if code_re().is_match(s) {
            Ok(Self(s.to_string()))
        } else {
            Err(DomainError::field("tenant_code", TENANT_CODE_ERROR))
        }
    }

    /// Auto-suggestion from the organisation name (screen: "auto-suggested from name, editable").
    pub fn suggest_from_name(name: &str) -> Self {
        let mut out = String::with_capacity(32);
        let mut last_dash = true;
        for ch in name.trim().chars().flat_map(|c| c.to_lowercase()) {
            if ch.is_ascii_alphanumeric() {
                out.push(ch);
                last_dash = false;
            } else if !last_dash {
                out.push('-');
                last_dash = true;
            }
            if out.len() >= 32 {
                break;
            }
        }
        let mut s = out.trim_matches('-').to_string();
        s.truncate(32);
        let s = s.trim_end_matches('-').to_string();
        let s = if s.len() < 3 { format!("{s}-org").trim_start_matches('-').to_string() } else { s };
        let s = if s.len() < 3 { "tenant".to_string() } else { s };
        Self(s)
    }

    /// Appends a numeric suffix while staying within 32 characters (used when a suggestion clashes
    /// and for sandbox codes).
    pub fn with_suffix(&self, suffix: &str) -> Self {
        let max_base = 32usize.saturating_sub(suffix.len() + 1);
        let base: String = self.0.chars().take(max_base).collect();
        Self(format!("{}-{}", base.trim_end_matches('-'), suffix))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_validation_matches_field_rule() {
        assert!(TenantCode::parse("acme-retail").is_ok());
        assert!(TenantCode::parse("abc").is_ok());
        assert!(TenantCode::parse(&"a".repeat(32)).is_ok());
        assert!(TenantCode::parse("ab").is_err());
        assert!(TenantCode::parse(&"a".repeat(33)).is_err());
        assert!(TenantCode::parse("Acme").is_err());
        assert!(TenantCode::parse("acme_retail").is_err());
        assert!(TenantCode::parse("acme retail").is_err());
        assert!(TenantCode::parse("acme'; drop").is_err());
        match TenantCode::parse("x") {
            Err(DomainError::Validation(v)) => assert_eq!(v[0].message, TENANT_CODE_ERROR),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn suggestion_is_always_valid() {
        for name in
            ["Acme Retail Sdn Bhd", "  ", "A", "Ünïcode Ltd!!", "a very long organisation name that keeps going and going forever", "--x--"]
        {
            let c = TenantCode::suggest_from_name(name);
            assert!(TenantCode::parse(c.as_str()).is_ok(), "{name:?} -> {c}");
        }
        assert_eq!(TenantCode::suggest_from_name("Acme Retail Sdn Bhd").as_str(), "acme-retail-sdn-bhd");
    }

    #[test]
    fn suffix_respects_length() {
        let c = TenantCode::parse(&"a".repeat(32)).unwrap();
        let s = c.with_suffix("sbx1");
        assert!(s.as_str().len() <= 32);
        assert!(s.as_str().ends_with("-sbx1"));
        assert!(TenantCode::parse(s.as_str()).is_ok());
    }

    #[test]
    fn storage_ident_is_safe() {
        let id = TenantId::new();
        let s = id.storage_ident();
        assert!(s.starts_with("tn_"));
        assert_eq!(s.len(), 35);
        assert!(s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
    }
}
