//! Typed, namespaced per-tenant configuration (field `config_key`/`config_value`; OCC-M01-R004,
//! R020 locale defaults, R021 security-policy boundary, R027 analytics opt-out).

use std::collections::BTreeMap;
use std::net::IpAddr;

use serde_json::Value;

use super::errors::{DomainError, Violations};
use super::storage::Tier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Editor {
    /// Tenant Admin may edit (within plan); Super Admin may edit anything.
    TenantAdmin,
    /// Only Super Admin (stand-ins for values owned by other modules / host policy).
    SuperAdminOnly,
}

#[derive(Debug, Clone, Copy)]
pub enum ConfigType {
    Bool,
    Int { min: i64, max: i64 },
    Enum(&'static [&'static str]),
    EnumList { allowed: &'static [&'static str], min_items: usize },
    CidrList { max_items: usize },
}

#[derive(Debug, Clone, Copy)]
pub struct ConfigKeyDef {
    pub key: &'static str,
    pub label: &'static str,
    pub help: &'static str,
    pub ty: ConfigType,
    pub editor: Editor,
    pub default: fn() -> Value,
    /// Restrict editing to tenants of this tier (R027: only Regulated tenants may opt out).
    pub only_tier: Option<Tier>,
    /// Module that enforces this setting (M01 stores it; enforcement may be pending).
    pub enforced_by: &'static str,
}

pub const TIMEZONES: &[&str] = &[
    "Asia/Kuala_Lumpur",
    "Asia/Singapore",
    "Asia/Jakarta",
    "Asia/Bangkok",
    "Asia/Manila",
    "Asia/Hong_Kong",
    "Asia/Tokyo",
    "Asia/Kolkata",
    "Australia/Sydney",
    "Europe/London",
    "UTC",
];
pub const CURRENCIES: &[&str] = &["MYR", "SGD", "USD", "IDR", "THB", "PHP", "AUD", "EUR", "GBP"];
pub const DATE_FORMATS: &[&str] = &["DD-MMM-YYYY", "DD/MM/YYYY", "YYYY-MM-DD", "MM/DD/YYYY"];
pub const NUMBER_FORMATS: &[&str] = &["1,234.56", "1.234,56", "1 234,56"];
/// NFR-012: EN + BM at launch.
pub const LANGUAGES: &[&str] = &["en", "ms"];

fn v_str(s: &'static str) -> Value {
    Value::String(s.to_string())
}

pub const CONFIG_KEYS: &[ConfigKeyDef] = &[
    ConfigKeyDef {
        key: "locale.timezone",
        label: "Default timezone",
        help: "Tenant default; users may override.",
        ty: ConfigType::Enum(TIMEZONES),
        editor: Editor::TenantAdmin,
        default: || v_str("Asia/Kuala_Lumpur"),
        only_tier: None,
        enforced_by: "M01/M27",
    },
    ConfigKeyDef {
        key: "locale.currency",
        label: "Default currency",
        help: "ISO-4217 code.",
        ty: ConfigType::Enum(CURRENCIES),
        editor: Editor::TenantAdmin,
        default: || v_str("MYR"),
        only_tier: None,
        enforced_by: "M01/M27",
    },
    ConfigKeyDef {
        key: "locale.date_format",
        label: "Date format",
        help: "Display format; storage is always UTC.",
        ty: ConfigType::Enum(DATE_FORMATS),
        editor: Editor::TenantAdmin,
        default: || v_str("DD-MMM-YYYY"),
        only_tier: None,
        enforced_by: "M27",
    },
    ConfigKeyDef {
        key: "locale.number_format",
        label: "Number format",
        help: "Grouping and decimal separators.",
        ty: ConfigType::Enum(NUMBER_FORMATS),
        editor: Editor::TenantAdmin,
        default: || v_str("1,234.56"),
        only_tier: None,
        enforced_by: "M27",
    },
    ConfigKeyDef {
        key: "locale.default_language",
        label: "Default UI language",
        help: "Must be one of the allowed languages.",
        ty: ConfigType::Enum(LANGUAGES),
        editor: Editor::TenantAdmin,
        default: || v_str("en"),
        only_tier: None,
        enforced_by: "M27",
    },
    ConfigKeyDef {
        key: "locale.allowed_languages",
        label: "Allowed UI languages",
        help: "Users may choose within this set.",
        ty: ConfigType::EnumList { allowed: LANGUAGES, min_items: 1 },
        editor: Editor::TenantAdmin,
        default: || Value::Array(vec![v_str("en"), v_str("ms")]),
        only_tier: None,
        enforced_by: "M27",
    },
    ConfigKeyDef {
        key: "security.session_idle_timeout_minutes",
        label: "Session idle timeout (minutes)",
        help: "SEC-004; default 30. Applied by bootstrap auth.",
        ty: ConfigType::Int { min: 5, max: 480 },
        editor: Editor::TenantAdmin,
        default: || Value::from(30),
        only_tier: None,
        enforced_by: "bootstrap auth (M02 later)",
    },
    ConfigKeyDef {
        key: "security.password_min_length",
        label: "Minimum password length",
        help: "Applied when setting passwords.",
        ty: ConfigType::Int { min: 12, max: 128 },
        editor: Editor::TenantAdmin,
        default: || Value::from(12),
        only_tier: None,
        enforced_by: "bootstrap auth (M02 later)",
    },
    ConfigKeyDef {
        key: "security.mfa_policy",
        label: "MFA policy",
        help: "SEC-002: mandatory for admins at minimum.",
        ty: ConfigType::Enum(&["admins", "all"]),
        editor: Editor::TenantAdmin,
        default: || v_str("admins"),
        only_tier: None,
        enforced_by: "M02 (pending)",
    },
    ConfigKeyDef {
        key: "security.ip_allowlist",
        label: "Admin/API IP allow-list (CIDR)",
        help: "Stored and validated by M01; enforced by M02 (pending).",
        ty: ConfigType::CidrList { max_items: 50 },
        editor: Editor::TenantAdmin,
        default: || Value::Array(vec![]),
        only_tier: None,
        enforced_by: "M02 (pending)",
    },
    ConfigKeyDef {
        key: "security.sso_provider",
        label: "SSO identity provider",
        help: "Tenant's own SAML/OIDC IdP (SEC-001); connection handled by M02.",
        ty: ConfigType::Enum(&["none", "saml", "oidc"]),
        editor: Editor::TenantAdmin,
        default: || v_str("none"),
        only_tier: None,
        enforced_by: "M02 (pending)",
    },
    ConfigKeyDef {
        key: "security.api_clients_enabled",
        label: "API client management enabled",
        help: "Scoped API credentials (M02/M22).",
        ty: ConfigType::Bool,
        editor: Editor::TenantAdmin,
        default: || Value::Bool(true),
        only_tier: None,
        enforced_by: "M02/M22 (pending)",
    },
    ConfigKeyDef {
        key: "channel.whatsapp.bsp_connection_verified",
        label: "WhatsApp BSP connection verified",
        help: "Set by host once M05 BSP onboarding completes (stand-in).",
        ty: ConfigType::Bool,
        editor: Editor::SuperAdminOnly,
        default: || Value::Bool(false),
        only_tier: None,
        enforced_by: "M05 (stand-in)",
    },
    ConfigKeyDef {
        key: "lifecycle.suspend_session_policy",
        label: "On suspension",
        help: "revoke = end sessions immediately; drain = block new sessions only.",
        ty: ConfigType::Enum(&["revoke", "drain"]),
        editor: Editor::SuperAdminOnly,
        default: || v_str("revoke"),
        only_tier: None,
        enforced_by: "M01",
    },
    ConfigKeyDef {
        key: "analytics.cross_tenant_opt_out",
        label: "Opt out of host aggregated analytics",
        help: "R027: Regulated tenants may opt out.",
        ty: ConfigType::Bool,
        editor: Editor::TenantAdmin,
        default: || Value::Bool(false),
        only_tier: Some(Tier::Regulated),
        enforced_by: "M01",
    },
];

pub const UNKNOWN_CONFIG_KEY: &str = "Unknown config key";

pub fn key_def(key: &str) -> Option<&'static ConfigKeyDef> {
    CONFIG_KEYS.iter().find(|d| d.key == key)
}

pub fn defaults() -> BTreeMap<String, Value> {
    CONFIG_KEYS.iter().map(|d| (d.key.to_string(), (d.default)())).collect()
}

fn valid_cidr(s: &str) -> bool {
    let (ip, prefix) = match s.split_once('/') {
        Some((ip, p)) => (ip, Some(p)),
        None => (s, None),
    };
    let Ok(addr) = ip.parse::<IpAddr>() else {
        return false;
    };
    match prefix {
        None => true,
        Some(p) => p.parse::<u8>().is_ok_and(|n| match addr {
            IpAddr::V4(_) => n <= 32,
            IpAddr::V6(_) => n <= 128,
        }),
    }
}

/// Validates the value of one key (type + range/enum) and returns its normalised form.
pub fn validate_value(def: &ConfigKeyDef, value: &Value) -> Result<Value, DomainError> {
    let field = format!("config.{}", def.key);
    let bad = |msg: String| DomainError::field(field.clone(), msg);
    match def.ty {
        ConfigType::Bool => match value {
            Value::Bool(_) => Ok(value.clone()),
            Value::String(s) if s == "true" || s == "false" => Ok(Value::Bool(s == "true")),
            _ => Err(bad("Must be true or false".into())),
        },
        ConfigType::Int { min, max } => {
            let n = match value {
                Value::Number(n) => n.as_i64(),
                Value::String(s) => s.trim().parse::<i64>().ok(),
                _ => None,
            };
            match n {
                Some(n) if (min..=max).contains(&n) => Ok(Value::from(n)),
                _ => Err(bad(format!("Must be a whole number between {min} and {max}"))),
            }
        }
        ConfigType::Enum(allowed) => match value.as_str() {
            Some(s) if allowed.contains(&s) => Ok(value.clone()),
            _ => Err(bad(format!("Must be one of: {}", allowed.join(", ")))),
        },
        ConfigType::EnumList { allowed, min_items } => {
            let items: Vec<String> = match value {
                Value::Array(a) => a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
                Value::String(s) => s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect(),
                _ => return Err(bad("Must be a list".into())),
            };
            let mut out: Vec<String> = Vec::new();
            for i in items {
                if !allowed.contains(&i.as_str()) {
                    return Err(bad(format!("Allowed values: {}", allowed.join(", "))));
                }
                if !out.contains(&i) {
                    out.push(i);
                }
            }
            if out.len() < min_items {
                return Err(bad(format!("At least {min_items} value(s) required")));
            }
            Ok(Value::Array(out.into_iter().map(Value::String).collect()))
        }
        ConfigType::CidrList { max_items } => {
            let items: Vec<String> = match value {
                Value::Array(a) => a.iter().filter_map(|v| v.as_str().map(|s| s.trim().to_string())).collect(),
                Value::String(s) => s.split([',', '\n']).map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect(),
                _ => return Err(bad("Must be a list of CIDR ranges".into())),
            };
            if items.len() > max_items {
                return Err(bad(format!("At most {max_items} entries")));
            }
            if let Some(badv) = items.iter().find(|i| !valid_cidr(i)) {
                return Err(bad(format!("'{}' is not a valid IP address or CIDR range", badv.chars().take(60).collect::<String>())));
            }
            Ok(Value::Array(items.into_iter().map(Value::String).collect()))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigActor {
    SuperAdmin,
    TenantAdmin,
}

/// Validates a batch of changes against key schema, editor permissions, tier restrictions and
/// cross-key rules. Returns the normalised changes.
pub fn validate_changes(
    changes: &BTreeMap<String, Value>,
    current: &BTreeMap<String, Value>,
    actor: ConfigActor,
    tier: Tier,
) -> Result<BTreeMap<String, Value>, DomainError> {
    let mut v = Violations::default();
    let mut out = BTreeMap::new();
    for (k, val) in changes {
        let Some(def) = key_def(k) else {
            v.push(&format!("config.{k}"), UNKNOWN_CONFIG_KEY);
            continue;
        };
        if def.editor == Editor::SuperAdminOnly && actor != ConfigActor::SuperAdmin {
            return Err(DomainError::forbidden(format!("'{}' can only be changed by the platform operator", def.key)));
        }
        if let Some(t) = def.only_tier {
            if t != tier {
                return Err(DomainError::forbidden(format!("'{}' is only available to {} tenants", def.key, t.label())));
            }
        }
        if let Some(n) = v.capture(validate_value(def, val)) {
            out.insert(k.clone(), n);
        }
    }
    v.into_result()?;

    // Cross-key rule (R020): default language must be within the allowed set.
    let merged = |k: &str| out.get(k).or_else(|| current.get(k)).cloned();
    if let (Some(Value::String(lang)), Some(Value::Array(allowed))) =
        (merged("locale.default_language"), merged("locale.allowed_languages"))
    {
        if !allowed.iter().any(|a| a.as_str() == Some(lang.as_str())) {
            return Err(DomainError::field("config.locale.default_language", "Default language must be one of the allowed languages"));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn changes(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn unknown_key_rejected() {
        let err = validate_changes(&changes(&[("foo.bar", json!(1))]), &defaults(), ConfigActor::SuperAdmin, Tier::Standard).unwrap_err();
        match err {
            DomainError::Validation(v) => assert_eq!(v[0].message, UNKNOWN_CONFIG_KEY),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn typed_values_enforced() {
        assert!(validate_changes(
            &changes(&[("security.session_idle_timeout_minutes", json!(2))]),
            &defaults(),
            ConfigActor::TenantAdmin,
            Tier::Standard
        )
        .is_err());
        let ok = validate_changes(
            &changes(&[("security.session_idle_timeout_minutes", json!("45"))]),
            &defaults(),
            ConfigActor::TenantAdmin,
            Tier::Standard,
        )
        .unwrap();
        assert_eq!(ok["security.session_idle_timeout_minutes"], json!(45));
        assert!(validate_changes(&changes(&[("locale.currency", json!("XXX"))]), &defaults(), ConfigActor::TenantAdmin, Tier::Standard)
            .is_err());
    }

    #[test]
    fn super_admin_only_keys() {
        let c = changes(&[("channel.whatsapp.bsp_connection_verified", json!(true))]);
        assert!(matches!(validate_changes(&c, &defaults(), ConfigActor::TenantAdmin, Tier::Premium), Err(DomainError::Forbidden(_))));
        assert!(validate_changes(&c, &defaults(), ConfigActor::SuperAdmin, Tier::Premium).is_ok());
    }

    #[test]
    fn analytics_opt_out_only_for_regulated() {
        let c = changes(&[("analytics.cross_tenant_opt_out", json!(true))]);
        assert!(matches!(validate_changes(&c, &defaults(), ConfigActor::TenantAdmin, Tier::Standard), Err(DomainError::Forbidden(_))));
        assert!(validate_changes(&c, &defaults(), ConfigActor::TenantAdmin, Tier::Regulated).is_ok());
    }

    #[test]
    fn default_language_must_be_allowed() {
        let c = changes(&[("locale.allowed_languages", json!(["ms"]))]);
        assert!(validate_changes(&c, &defaults(), ConfigActor::TenantAdmin, Tier::Standard).is_err());
        let c = changes(&[("locale.allowed_languages", json!(["ms"])), ("locale.default_language", json!("ms"))]);
        assert!(validate_changes(&c, &defaults(), ConfigActor::TenantAdmin, Tier::Standard).is_ok());
    }

    #[test]
    fn cidr_validation() {
        let def = key_def("security.ip_allowlist").unwrap();
        assert!(validate_value(def, &json!("10.0.0.0/8, 192.168.1.10")).is_ok());
        assert!(validate_value(def, &json!(["10.0.0.0/33"])).is_err());
        assert!(validate_value(def, &json!(["not-an-ip"])).is_err());
        assert!(validate_value(def, &json!(["2001:db8::/32"])).is_ok());
    }

    #[test]
    fn every_default_is_valid() {
        for d in CONFIG_KEYS {
            assert!(validate_value(d, &(d.default)()).is_ok(), "{}", d.key);
        }
    }
}
