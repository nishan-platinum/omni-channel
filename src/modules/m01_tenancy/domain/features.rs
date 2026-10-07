//! Feature / module entitlement catalogue (OCC-M01-R004, R015; BR-M01-003; FD-008).
//! The catalogue names the modules/channels/AI/app features that can be switched per tenant.
//! M01 stores their state generically; the owning modules (M03…M40) are not implemented here.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::errors::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureCategory {
    Module,
    Channel,
    Ai,
    Apps,
}

impl FeatureCategory {
    pub fn label(self) -> &'static str {
        match self {
            Self::Module => "Modules",
            Self::Channel => "Channels",
            Self::Ai => "AI features",
            Self::Apps => "App ecosystem",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FeatureDef {
    pub key: &'static str,
    pub label: &'static str,
    pub category: FeatureCategory,
    /// Module that owns the behaviour behind the flag (not implemented in this prototype).
    pub owner: &'static str,
    /// Dependency that must be satisfied before enabling (spec F04 step 3). Config key that must be `true`.
    pub requires_config: Option<(&'static str, &'static str)>,
}

pub const FEATURES: &[FeatureDef] = &[
    FeatureDef {
        key: "module.crm_contacts",
        label: "Contacts & Accounts",
        category: FeatureCategory::Module,
        owner: "M13",
        requires_config: None,
    },
    FeatureDef {
        key: "module.crm_sales",
        label: "Sales & Pipeline",
        category: FeatureCategory::Module,
        owner: "M14",
        requires_config: None,
    },
    FeatureDef {
        key: "module.service_ticketing",
        label: "Service & Ticketing",
        category: FeatureCategory::Module,
        owner: "M15",
        requires_config: None,
    },
    FeatureDef {
        key: "module.activities",
        label: "Activities & Tasks",
        category: FeatureCategory::Module,
        owner: "M16",
        requires_config: None,
    },
    FeatureDef {
        key: "module.customer_portal",
        label: "Customer Portal",
        category: FeatureCategory::Module,
        owner: "M17",
        requires_config: None,
    },
    FeatureDef {
        key: "module.contact_centre",
        label: "Cloud Contact Centre",
        category: FeatureCategory::Module,
        owner: "M10",
        requires_config: None,
    },
    FeatureDef {
        key: "module.flow_builder",
        label: "Flow Builder & BPM",
        category: FeatureCategory::Module,
        owner: "M09",
        requires_config: None,
    },
    FeatureDef {
        key: "module.automated_marketing",
        label: "Automated Marketing",
        category: FeatureCategory::Module,
        owner: "M12",
        requires_config: None,
    },
    FeatureDef {
        key: "module.reporting",
        label: "Reporting & Dashboards",
        category: FeatureCategory::Module,
        owner: "M24",
        requires_config: None,
    },
    FeatureDef { key: "module.documents", label: "Documents", category: FeatureCategory::Module, owner: "M32", requires_config: None },
    FeatureDef {
        key: "module.workforce",
        label: "Workforce & Scheduling",
        category: FeatureCategory::Module,
        owner: "M33",
        requires_config: None,
    },
    FeatureDef {
        key: "module.projects",
        label: "Projects & Delivery",
        category: FeatureCategory::Module,
        owner: "M34",
        requires_config: None,
    },
    FeatureDef { key: "channel.voice", label: "Voice", category: FeatureCategory::Channel, owner: "M03", requires_config: None },
    FeatureDef { key: "channel.sms", label: "SMS & A2P", category: FeatureCategory::Channel, owner: "M04", requires_config: None },
    FeatureDef {
        key: "channel.whatsapp",
        label: "WhatsApp",
        category: FeatureCategory::Channel,
        owner: "M05",
        requires_config: Some(("channel.whatsapp.bsp_connection_verified", "WhatsApp requires a verified BSP connection")),
    },
    FeatureDef {
        key: "channel.social",
        label: "Social & OTT messaging",
        category: FeatureCategory::Channel,
        owner: "M05",
        requires_config: None,
    },
    FeatureDef { key: "channel.email", label: "Email", category: FeatureCategory::Channel, owner: "M06", requires_config: None },
    FeatureDef { key: "channel.video", label: "Video & WebRTC", category: FeatureCategory::Channel, owner: "M07", requires_config: None },
    FeatureDef { key: "channel.webchat", label: "Web chat", category: FeatureCategory::Channel, owner: "M08", requires_config: None },
    FeatureDef { key: "ai.conversational", label: "Conversational AI", category: FeatureCategory::Ai, owner: "M11", requires_config: None },
    FeatureDef { key: "ai.copilot", label: "Agent copilot", category: FeatureCategory::Ai, owner: "M11", requires_config: None },
    FeatureDef { key: "apps.marketplace", label: "App marketplace", category: FeatureCategory::Apps, owner: "M26", requires_config: None },
];

pub fn feature_def(key: &str) -> Option<&'static FeatureDef> {
    FEATURES.iter().find(|f| f.key == key)
}

pub fn is_channel(key: &str) -> bool {
    feature_def(key).is_some_and(|f| f.category == FeatureCategory::Channel)
}

pub const NOT_ENTITLED: &str = "Feature not included in your plan";

/// Validates enabling/disabling one feature for a tenant (BR-M01-003).
///
/// * unknown feature → VALIDATION_FAILED
/// * enable beyond plan entitlement → FORBIDDEN "Feature not included in your plan"
/// * unmet dependency → VALIDATION_FAILED with the dependency message
pub fn validate_feature_change(
    key: &str,
    enable: bool,
    entitlements: &BTreeSet<String>,
    config: &BTreeMap<String, Value>,
) -> Result<(), DomainError> {
    let def = feature_def(key).ok_or_else(|| DomainError::field(format!("feature_flags.{key}"), "Unknown feature"))?;
    if !enable {
        return Ok(());
    }
    if !entitlements.contains(key) {
        return Err(DomainError::forbidden(NOT_ENTITLED));
    }
    if let Some((cfg_key, message)) = def.requires_config {
        if config.get(cfg_key) != Some(&Value::Bool(true)) {
            return Err(DomainError::field(format!("feature_flags.{key}"), message));
        }
    }
    Ok(())
}

/// FD-008: the feature list offered for a plan (entitled first; others shown greyed as upsell).
pub fn features_for_plan(entitlements: &BTreeSet<String>) -> Vec<(&'static FeatureDef, bool)> {
    FEATURES.iter().map(|f| (f, entitlements.contains(f.key))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ents(keys: &[&str]) -> BTreeSet<String> {
        keys.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn cannot_enable_beyond_plan() {
        let e = ents(&["module.crm_contacts"]);
        let cfg = BTreeMap::new();
        assert!(validate_feature_change("module.crm_contacts", true, &e, &cfg).is_ok());
        assert_eq!(validate_feature_change("module.automated_marketing", true, &e, &cfg), Err(DomainError::Forbidden(NOT_ENTITLED.into())));
        // Disabling is always allowed.
        assert!(validate_feature_change("module.automated_marketing", false, &e, &cfg).is_ok());
    }

    #[test]
    fn unknown_feature_rejected() {
        assert!(matches!(validate_feature_change("module.nope", true, &ents(&[]), &BTreeMap::new()), Err(DomainError::Validation(_))));
    }

    #[test]
    fn whatsapp_requires_verified_bsp() {
        let e = ents(&["channel.whatsapp"]);
        let mut cfg = BTreeMap::new();
        assert!(matches!(validate_feature_change("channel.whatsapp", true, &e, &cfg), Err(DomainError::Validation(_))));
        cfg.insert("channel.whatsapp.bsp_connection_verified".into(), Value::Bool(true));
        assert!(validate_feature_change("channel.whatsapp", true, &e, &cfg).is_ok());
    }

    #[test]
    fn catalogue_keys_unique() {
        let keys: BTreeSet<_> = FEATURES.iter().map(|f| f.key).collect();
        assert_eq!(keys.len(), FEATURES.len());
        assert!(is_channel("channel.sms"));
        assert!(!is_channel("module.crm_sales"));
    }
}
