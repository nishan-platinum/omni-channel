//! White-label branding (M01-F06; OCC-M01-R006, R019; BR-M01-005; FD-010/011).

use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

use super::errors::DomainError;

pub const DEFAULT_PRIMARY: &str = "#0B2130";
pub const DEFAULT_SECONDARY: &str = "#F26A21";
pub const MAX_LOGO_BYTES: usize = 2 * 1024 * 1024;
pub const LOGO_ERROR: &str = "Logo must be PNG/SVG under 2MB";
pub const COLOUR_ERROR: &str = "Colour must be a 6-digit hex";

fn hex_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^#[0-9A-Fa-f]{6}$").expect("static regex"))
}

fn email_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$")
            .expect("static regex")
    })
}

/// `^#[0-9A-Fa-f]{6}$` (spec field rule for primary_color; applied to secondary as well).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HexColor(String);

impl HexColor {
    pub fn parse(field: &str, raw: &str) -> Result<Self, DomainError> {
        let s = raw.trim();
        if hex_re().is_match(s) {
            Ok(Self(s.to_ascii_uppercase()))
        } else {
            Err(DomainError::field(field, COLOUR_ERROR))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Email address (`varchar(320)`), used for primary admin email and the sender identity.
pub fn parse_email(field: &str, raw: &str, message: &str) -> Result<String, DomainError> {
    let s = raw.trim();
    if s.len() <= 320 && email_re().is_match(s) {
        Ok(s.to_ascii_lowercase())
    } else {
        Err(DomainError::field(field, message))
    }
}

pub fn email_domain(email: &str) -> Option<&str> {
    email.rsplit_once('@').map(|(_, d)| d)
}

/// Fully qualified domain name for the portal custom domain (FD-010).
pub fn parse_fqdn(field: &str, raw: &str) -> Result<String, DomainError> {
    let s = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    let labels: Vec<&str> = s.split('.').collect();
    let label_ok = |l: &&str| {
        !l.is_empty()
            && l.len() <= 63
            && !l.starts_with('-')
            && !l.ends_with('-')
            && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    };
    let tld_ok = labels.last().is_some_and(|t| t.len() >= 2 && t.bytes().all(|b| b.is_ascii_alphabetic()));
    if s.len() <= 253 && labels.len() >= 2 && labels.iter().all(label_ok) && tld_ok {
        Ok(s)
    } else {
        Err(DomainError::field(field, "Custom domain must be a fully qualified domain name"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LogoKind {
    Png,
    Svg,
}

impl LogoKind {
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Svg => "image/svg+xml",
        }
    }
    pub fn extension(self) -> &'static str {
        match self {
            Self::Png => "png",
            Self::Svg => "svg",
        }
    }
}

const PNG_MAGIC: &[u8] = &[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'];

/// Validates an uploaded logo by content (not by the client's declared type): PNG magic bytes or
/// a conservative SVG with no active content. SVGs are additionally served with a sandboxing CSP
/// and only referenced through `<img>` (scripts never execute there).
pub fn validate_logo(bytes: &[u8]) -> Result<LogoKind, DomainError> {
    if bytes.is_empty() || bytes.len() > MAX_LOGO_BYTES {
        return Err(DomainError::field("logo", LOGO_ERROR));
    }
    if bytes.starts_with(PNG_MAGIC) {
        return Ok(LogoKind::Png);
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Err(DomainError::field("logo", LOGO_ERROR));
    };
    let lower = text.to_ascii_lowercase();
    if !lower.contains("<svg") {
        return Err(DomainError::field("logo", LOGO_ERROR));
    }
    const FORBIDDEN: &[&str] = &[
        "<script",
        "javascript:",
        "<foreignobject",
        "<!entity",
        "<!doctype",
        "<iframe",
        "<embed",
        "<object",
        "data:text/html",
        "href=\"http",
        "href='http",
        "<use",
        "@import",
        "<style",
    ];
    if FORBIDDEN.iter().any(|f| lower.contains(f)) {
        return Err(DomainError::field("logo", "SVG logo contains active or external content"));
    }
    // Event-handler attributes (onload=, onclick=, …).
    let bytes_l = lower.as_bytes();
    for i in 0..bytes_l.len().saturating_sub(3) {
        if bytes_l[i].is_ascii_whitespace() && bytes_l[i + 1] == b'o' && bytes_l[i + 2] == b'n' {
            let rest = &lower[i + 3..];
            let name_len = rest.bytes().take_while(|b| b.is_ascii_alphabetic()).count();
            if name_len > 0 && rest[name_len..].trim_start().starts_with('=') {
                return Err(DomainError::field("logo", "SVG logo contains active or external content"));
            }
        }
    }
    Ok(LogoKind::Svg)
}

pub fn validate_text(field: &str, raw: Option<&str>, max: usize) -> Result<Option<String>, DomainError> {
    match raw.map(str::trim) {
        None | Some("") => Ok(None),
        Some(s) if s.chars().count() <= max => Ok(Some(s.to_string())),
        Some(_) => Err(DomainError::field(field, format!("Must be at most {max} characters"))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainStatus {
    None,
    Pending,
    Verified,
    Failed,
}

impl DomainStatus {
    pub fn parse(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "verified" => Self::Verified,
            "failed" => Self::Failed,
            _ => Self::None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pending => "pending",
            Self::Verified => "verified",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DnsRecord {
    pub record_type: &'static str,
    pub name: String,
    pub value: String,
}

/// FD-010: records the tenant must publish for the portal domain.
pub fn custom_domain_records(domain: &str, tenant_code: &str, platform_domain: &str, token: &str) -> Vec<DnsRecord> {
    vec![
        DnsRecord { record_type: "CNAME", name: domain.to_string(), value: format!("{tenant_code}.{platform_domain}") },
        DnsRecord { record_type: "TXT", name: format!("_omni-verify.{domain}"), value: format!("omni-verify={token}") },
    ]
}

/// FD-011: SPF/DKIM records for the sender domain.
pub fn sender_domain_records(domain: &str, platform_domain: &str, selector: &str) -> (String, String) {
    (
        format!("v=spf1 include:_spf.{platform_domain} ~all"),
        format!("{selector}._domainkey.{domain} CNAME {selector}.dkim.{platform_domain}"),
    )
}

/// BR-M01-005: activating (serving) a custom domain requires verification.
pub fn ensure_domain_can_activate(status: DomainStatus) -> Result<(), DomainError> {
    if status == DomainStatus::Verified {
        Ok(())
    } else {
        Err(DomainError::conflict("Domain not verified"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colours() {
        assert_eq!(HexColor::parse("primary_color", "#0b2130").unwrap().as_str(), "#0B2130");
        for bad in ["0B2130", "#0B213", "#0B21300", "#GGGGGG", "red", "#0B2130;}body{"] {
            assert!(HexColor::parse("primary_color", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn domains() {
        assert_eq!(parse_fqdn("d", "Care.Acme.com.").unwrap(), "care.acme.com");
        for bad in ["localhost", "acme", "-a.com", "a..com", "a.c", "a.123", "exa mple.com", "a/b.com"] {
            assert!(parse_fqdn("d", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn emails() {
        assert_eq!(parse_email("e", "Care@Acme.com", "bad").unwrap(), "care@acme.com");
        assert!(parse_email("e", "no-at-sign", "bad").is_err());
        assert!(parse_email("e", "a@b", "bad").is_err());
        assert_eq!(email_domain("care@acme.com"), Some("acme.com"));
    }

    #[test]
    fn logo_png_and_size() {
        let mut png = PNG_MAGIC.to_vec();
        png.extend_from_slice(&[0u8; 32]);
        assert_eq!(validate_logo(&png).unwrap(), LogoKind::Png);
        let big = vec![0u8; MAX_LOGO_BYTES + 1];
        assert!(validate_logo(&big).is_err());
        assert!(validate_logo(b"GIF89a....").is_err());
        assert!(validate_logo(&[]).is_err());
    }

    #[test]
    fn svg_active_content_rejected() {
        let ok = br##"<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"><rect width="10" height="10" fill="#0B2130"/></svg>"##;
        assert_eq!(validate_logo(ok).unwrap(), LogoKind::Svg);
        for bad in [
            &br#"<svg><script>alert(1)</script></svg>"#[..],
            br#"<svg onload="alert(1)"></svg>"#,
            br#"<svg><a href="javascript:alert(1)">x</a></svg>"#,
            br#"<svg><foreignObject><iframe/></foreignObject></svg>"#,
            br#"<svg><image href="http://evil/x.png"/></svg>"#,
            br#"<!DOCTYPE svg [<!ENTITY x "y">]><svg/>"#,
        ] {
            assert!(validate_logo(bad).is_err(), "{}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn domain_activation_requires_verification() {
        assert!(ensure_domain_can_activate(DomainStatus::Pending).is_err());
        assert!(ensure_domain_can_activate(DomainStatus::Verified).is_ok());
    }

    #[test]
    fn dns_records_shape() {
        let r = custom_domain_records("care.acme.com", "acme", "tenants.omni.local", "tok");
        assert_eq!(r[0].record_type, "CNAME");
        assert_eq!(r[0].value, "acme.tenants.omni.local");
        assert_eq!(r[1].name, "_omni-verify.care.acme.com");
    }
}
