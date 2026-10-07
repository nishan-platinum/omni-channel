//! HTML form payloads. Forms never carry a tenant id for Tenant Admin routes; for Super Admin
//! routes the tenant id is a path parameter authorised by role.

use serde::Deserialize;

#[derive(Debug, Deserialize, Default, Clone)]
pub struct CreateTenantForm {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub legal_name: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub plan_id: String,
    #[serde(default)]
    pub primary_admin_email: String,
    #[serde(default)]
    pub parent_tenant_id: String,
    #[serde(default)]
    pub tenant_code: String,
    #[serde(default)]
    pub template_code: String,
    #[serde(default)]
    pub db_target: String,
    #[serde(default)]
    pub inherit_branding: Option<String>,
    #[serde(default)]
    pub inherit_config: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StatusForm {
    pub status: String,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct DetailsForm {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub legal_name: String,
    /// Super Admin only; empty string = no parent. Absent = unchanged.
    pub parent_tenant_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EmptyForm {}

#[derive(Debug, Deserialize)]
pub struct ReasonForm {
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct FeatureToggleForm {
    pub enabled: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct BrandingForm {
    #[serde(default)]
    pub primary_color: String,
    #[serde(default)]
    pub secondary_color: String,
    #[serde(default)]
    pub custom_domain: String,
    #[serde(default)]
    pub custom_domain_active: Option<String>,
    #[serde(default)]
    pub email_from: String,
    #[serde(default)]
    pub email_footer: String,
    #[serde(default)]
    pub login_message: String,
    #[serde(default)]
    pub pdf_letterhead: String,
    #[serde(default)]
    pub remove_logo: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PreviewForm {
    #[serde(default)]
    pub primary_color: String,
    #[serde(default)]
    pub secondary_color: String,
}

#[derive(Debug, Deserialize)]
pub struct DomainForm {
    #[serde(default)]
    pub domain: String,
}

#[derive(Debug, Deserialize)]
pub struct MeteringForm {
    pub meter: String,
    pub amount: i64,
}

#[derive(Debug, Deserialize)]
pub struct ConsumeForm {
    pub metric: String,
    pub amount: i64,
}

#[derive(Debug, Deserialize)]
pub struct GrantRequestForm {
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub incident_ref: String,
    #[serde(default)]
    pub duration_minutes: String,
    #[serde(default)]
    pub named_approver_id: String,
}

#[derive(Debug, Deserialize)]
pub struct DecisionForm {
    #[serde(default)]
    pub note: String,
}

#[derive(Debug, Deserialize)]
pub struct BaselineExportForm {
    #[serde(default)]
    pub label: String,
}

#[derive(Debug, Deserialize)]
pub struct BaselineImportForm {
    #[serde(default)]
    pub document: String,
    #[serde(default)]
    pub change_record: String,
    /// "preview" or "apply".
    #[serde(default)]
    pub action: String,
}

#[derive(Debug, Deserialize)]
pub struct ChangeRecordForm {
    #[serde(default)]
    pub change_record: String,
    #[serde(default)]
    pub action: String,
}

#[derive(Debug, Deserialize)]
pub struct ReleasePrefForm {
    pub ring: Option<String>,
    pub maintenance_day: i16,
    pub maintenance_start_hour_utc: i16,
    pub maintenance_duration_min: i32,
}

#[derive(Debug, Deserialize)]
pub struct NewReleaseForm {
    #[serde(default)]
    pub release_version: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub notes: String,
    pub disruptive: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ByokForm {
    #[serde(default)]
    pub key_ref: String,
}

#[derive(Debug, Deserialize)]
pub struct LegalHoldForm {
    pub hold: String,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct DirectoryQuery {
    pub q: Option<String>,
    pub status: Option<String>,
    pub region: Option<String>,
    pub cursor: Option<String>,
}
