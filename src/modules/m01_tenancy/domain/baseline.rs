//! Configuration baselines: export/import with diff (OCC-M01-R022; UJ-15 E2 rollback; UJ-19
//! sandbox → production promotion).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::errors::DomainError;

pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrandingBaseline {
    pub primary_color: String,
    pub secondary_color: String,
    pub email_footer: Option<String>,
    pub login_message: Option<String>,
    pub pdf_letterhead: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseBaseline {
    pub ring: String,
    pub maintenance_day: i16,
    pub maintenance_start_hour_utc: i16,
    pub maintenance_duration_min: i32,
}

/// Portable, secret-free configuration document. Branding assets/domains/senders are excluded
/// because they require per-environment verification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineDocument {
    pub format_version: u32,
    pub source_tenant_code: String,
    pub config: BTreeMap<String, Value>,
    pub feature_flags: BTreeMap<String, bool>,
    pub branding: BrandingBaseline,
    pub release: ReleaseBaseline,
}

impl BaselineDocument {
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        if raw.len() > 256 * 1024 {
            return Err(DomainError::field("baseline", "Baseline document is too large"));
        }
        let doc: Self = serde_json::from_str(raw).map_err(|e| DomainError::field("baseline", format!("Invalid baseline document: {e}")))?;
        if doc.format_version != FORMAT_VERSION {
            return Err(DomainError::field("baseline", "Unsupported baseline format version"));
        }
        Ok(doc)
    }

    /// Canonical JSON (BTreeMap ordering) used for the SHA-256 checksum.
    pub fn canonical_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffKind {
    Added,
    Removed,
    Changed,
}

impl DiffKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Removed => "removed",
            Self::Changed => "changed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DiffEntry {
    pub section: &'static str,
    pub key: String,
    pub kind: DiffKind,
    pub before: Option<String>,
    pub after: Option<String>,
}

fn show(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn diff_maps<V: PartialEq>(
    section: &'static str,
    current: &BTreeMap<String, V>,
    incoming: &BTreeMap<String, V>,
    fmt: impl Fn(&V) -> String,
    out: &mut Vec<DiffEntry>,
) {
    for (k, new) in incoming {
        match current.get(k) {
            None => out.push(DiffEntry { section, key: k.clone(), kind: DiffKind::Added, before: None, after: Some(fmt(new)) }),
            Some(old) if old != new => {
                out.push(DiffEntry { section, key: k.clone(), kind: DiffKind::Changed, before: Some(fmt(old)), after: Some(fmt(new)) })
            }
            _ => {}
        }
    }
    for (k, old) in current {
        if !incoming.contains_key(k) {
            out.push(DiffEntry { section, key: k.clone(), kind: DiffKind::Removed, before: Some(fmt(old)), after: None });
        }
    }
}

fn field_diff(section: &'static str, key: &str, a: Option<String>, b: Option<String>, out: &mut Vec<DiffEntry>) {
    if a != b {
        let kind = match (&a, &b) {
            (None, Some(_)) => DiffKind::Added,
            (Some(_), None) => DiffKind::Removed,
            _ => DiffKind::Changed,
        };
        out.push(DiffEntry { section, key: key.to_string(), kind, before: a, after: b });
    }
}

/// Diff of `incoming` relative to `current` (what applying `incoming` would change).
pub fn diff(current: &BaselineDocument, incoming: &BaselineDocument) -> Vec<DiffEntry> {
    let mut out = Vec::new();
    diff_maps("config", &current.config, &incoming.config, show, &mut out);
    diff_maps("feature_flags", &current.feature_flags, &incoming.feature_flags, |b| b.to_string(), &mut out);
    let (cb, ib) = (&current.branding, &incoming.branding);
    field_diff("branding", "primary_color", Some(cb.primary_color.clone()), Some(ib.primary_color.clone()), &mut out);
    field_diff("branding", "secondary_color", Some(cb.secondary_color.clone()), Some(ib.secondary_color.clone()), &mut out);
    field_diff("branding", "email_footer", cb.email_footer.clone(), ib.email_footer.clone(), &mut out);
    field_diff("branding", "login_message", cb.login_message.clone(), ib.login_message.clone(), &mut out);
    field_diff("branding", "pdf_letterhead", cb.pdf_letterhead.clone(), ib.pdf_letterhead.clone(), &mut out);
    let (cr, ir) = (&current.release, &incoming.release);
    field_diff("release", "ring", Some(cr.ring.clone()), Some(ir.ring.clone()), &mut out);
    field_diff("release", "maintenance_day", Some(cr.maintenance_day.to_string()), Some(ir.maintenance_day.to_string()), &mut out);
    field_diff(
        "release",
        "maintenance_start_hour_utc",
        Some(cr.maintenance_start_hour_utc.to_string()),
        Some(ir.maintenance_start_hour_utc.to_string()),
        &mut out,
    );
    field_diff(
        "release",
        "maintenance_duration_min",
        Some(cr.maintenance_duration_min.to_string()),
        Some(ir.maintenance_duration_min.to_string()),
        &mut out,
    );
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    pub fn doc() -> BaselineDocument {
        BaselineDocument {
            format_version: 1,
            source_tenant_code: "acme".into(),
            config: [("locale.currency".to_string(), json!("MYR"))].into_iter().collect(),
            feature_flags: [("module.crm_contacts".to_string(), true)].into_iter().collect(),
            branding: BrandingBaseline {
                primary_color: "#0B2130".into(),
                secondary_color: "#F26A21".into(),
                email_footer: None,
                login_message: None,
                pdf_letterhead: None,
            },
            release: ReleaseBaseline {
                ring: "general".into(),
                maintenance_day: 7,
                maintenance_start_hour_utc: 18,
                maintenance_duration_min: 120,
            },
        }
    }

    #[test]
    fn identical_documents_have_no_diff() {
        assert!(diff(&doc(), &doc()).is_empty());
    }

    #[test]
    fn detects_added_changed_removed() {
        let a = doc();
        let mut b = doc();
        b.config.insert("locale.currency".into(), json!("SGD"));
        b.config.insert("locale.timezone".into(), json!("Asia/Singapore"));
        b.feature_flags.clear();
        b.branding.email_footer = Some("Footer".into());
        let d = diff(&a, &b);
        assert!(d.iter().any(|e| e.key == "locale.currency" && e.kind == DiffKind::Changed));
        assert!(d.iter().any(|e| e.key == "locale.timezone" && e.kind == DiffKind::Added));
        assert!(d.iter().any(|e| e.key == "module.crm_contacts" && e.kind == DiffKind::Removed));
        assert!(d.iter().any(|e| e.key == "email_footer" && e.kind == DiffKind::Added));
    }

    #[test]
    fn parse_roundtrip_and_rejects_unknown_fields() {
        let s = doc().canonical_json();
        assert_eq!(BaselineDocument::parse(&s).unwrap(), doc());
        assert!(BaselineDocument::parse("{\"format_version\":1}").is_err());
        let mut v: Value = serde_json::from_str(&s).unwrap();
        v["secret"] = json!("x");
        assert!(BaselineDocument::parse(&v.to_string()).is_err());
    }
}
