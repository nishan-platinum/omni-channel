//! Typed M01 domain events. Names follow the spec convention `<entity>.<action>`; `tenant.created`
//! and `tenant.terminated` are named explicitly in the spec (F01 step 5, F03 step 4).

use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::ids::TenantId;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum TenantEvent {
    Created { tenant_code: String, region: String, plan_id: Uuid, storage_strategy: String, template: Option<String> },
    StatusChanged { event_type: &'static str, from: String, to: String, reason: Option<String> },
    ConfigChanged { keys: Vec<String> },
    FeatureChanged { feature: String, enabled: bool },
    QuotaWarning { metric: String, usage: i64, limit: i64 },
    QuotaExhausted { metric: String, usage: i64, limit: i64 },
    QuotaLimitsChanged { metrics: Vec<String> },
    BrandingChanged { fields: Vec<String> },
    CustomDomainVerified { domain: String },
    SupportAccessRequested { grant_id: Uuid },
    SupportAccessGranted { grant_id: Uuid, expires_at: String },
    SupportAccessUsed { grant_id: Uuid },
    SandboxCreated { sandbox_id: TenantId },
    BaselineImported { baseline_id: Uuid, changes: usize },
    KeyRotated { key_version: i32 },
    IsolationCheckFailed { failures: Vec<String> },
    ExportGenerated { export_id: Uuid },
    ReleaseScheduled { release_version: String, scheduled_for: String },
}

impl TenantEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Created { .. } => "tenant.created",
            Self::StatusChanged { event_type, .. } => event_type,
            Self::ConfigChanged { .. } => "tenant.config_changed",
            Self::FeatureChanged { .. } => "tenant.feature_changed",
            Self::QuotaWarning { .. } => "tenant.quota_warning",
            Self::QuotaExhausted { .. } => "tenant.quota_exhausted",
            Self::QuotaLimitsChanged { .. } => "tenant.quota_limits_changed",
            Self::BrandingChanged { .. } => "tenant.branding_changed",
            Self::CustomDomainVerified { .. } => "tenant.domain_verified",
            Self::SupportAccessRequested { .. } => "tenant.support_access_requested",
            Self::SupportAccessGranted { .. } => "tenant.support_access_granted",
            Self::SupportAccessUsed { .. } => "tenant.support_access_used",
            Self::SandboxCreated { .. } => "tenant.sandbox_created",
            Self::BaselineImported { .. } => "tenant.baseline_imported",
            Self::KeyRotated { .. } => "tenant.key_rotated",
            Self::IsolationCheckFailed { .. } => "tenant.isolation_check_failed",
            Self::ExportGenerated { .. } => "tenant.export_generated",
            Self::ReleaseScheduled { .. } => "tenant.release_scheduled",
        }
    }

    pub fn payload(&self, tenant_id: TenantId) -> Value {
        let body = serde_json::to_value(self).unwrap_or(Value::Null);
        // Externally-tagged enum → take the inner object.
        let data = match body {
            Value::Object(mut m) if m.len() == 1 => m.iter_mut().next().map(|(_, v)| v.take()).unwrap_or(Value::Null),
            other => other,
        };
        json!({ "tenant_id": tenant_id, "data": data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_entity_action() {
        let e = TenantEvent::Created {
            tenant_code: "acme".into(),
            region: "my-central".into(),
            plan_id: Uuid::nil(),
            storage_strategy: "shared_row_level".into(),
            template: None,
        };
        assert_eq!(e.event_type(), "tenant.created");
        let p = e.payload(TenantId(Uuid::nil()));
        assert_eq!(p["data"]["tenant_code"], "acme");
        let s = TenantEvent::StatusChanged { event_type: "tenant.terminated", from: "grace".into(), to: "terminated".into(), reason: None };
        assert_eq!(s.event_type(), "tenant.terminated");
    }
}
