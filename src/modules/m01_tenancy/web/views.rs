//! Askama view models. Templates only render; all decisions are made in application services.

use std::collections::BTreeMap;

use askama::Template;
use chrono::{DateTime, Utc};

use crate::platform::audit::AuditRecord;
use crate::web_support::PageCtx;

use super::super::application::analytics::HostAnalytics;
use super::super::application::backup::DrRow;
use super::super::application::baselines::ImportPreview;
use super::super::application::branding::BrandingView;
use super::super::application::configuration::{FeatureEntry, TenantSettingsView};
use super::super::application::directory::{DirectoryRow, EntitlementMatrix, TenantDetail};
use super::super::application::ports::*;
use super::super::application::quotas::QuotaView;
use super::super::application::support::SupportView as SupportData;
use super::super::domain::release::DAY_NAMES;
use super::super::domain::support::SupportGrant;
use super::super::domain::tenant::TenantStatus;
use super::super::infrastructure::adapters::OutboxEntry;
use super::forms::{BrandingForm, CreateTenantForm};

pub fn fmt_dt(v: &DateTime<Utc>) -> String {
    v.format("%d-%b-%Y %H:%M UTC").to_string()
}

/// Accepts a timestamp by value or by (nested) reference — Askama auto-references arguments.
pub trait DtArg {
    fn value(&self) -> DateTime<Utc>;
}
impl DtArg for DateTime<Utc> {
    fn value(&self) -> DateTime<Utc> {
        *self
    }
}
impl<T: DtArg + ?Sized> DtArg for &T {
    fn value(&self) -> DateTime<Utc> {
        (**self).value()
    }
}

pub trait OptDtArg {
    fn value(&self) -> Option<DateTime<Utc>>;
}
impl OptDtArg for Option<DateTime<Utc>> {
    fn value(&self) -> Option<DateTime<Utc>> {
        *self
    }
}
impl<T: OptDtArg + ?Sized> OptDtArg for &T {
    fn value(&self) -> Option<DateTime<Utc>> {
        (**self).value()
    }
}

impl PageCtx {
    pub fn dt<T: DtArg>(&self, v: T) -> String {
        fmt_dt(&v.value())
    }
    pub fn odt<T: OptDtArg>(&self, v: T) -> String {
        v.value().as_ref().map(fmt_dt).unwrap_or_else(|| "—".into())
    }
    pub fn badge(&self, status: &str) -> &'static str {
        match status {
            "active" | "completed" | "passed" | "verified" | "ready" | "approved" => "badge-ok",
            "draft" | "pending" | "in_progress" | "requested" | "pending approval" | "scheduled" | "rolling_out" | "planned"
            | "not_run" => "badge-info",
            "suspended" | "grace" | "warning" | "retired" | "expired" => "badge-warn",
            "terminated" | "purged" | "failed" | "rejected" | "revoked" | "exhausted" | "destroyed" => "badge-bad",
            _ => "badge-muted",
        }
    }
}

/// Tabs for one tenant; `base` is `/admin/tenants/{id}` (SA) or `/tenant` (TA).
#[derive(Debug, Clone)]
pub struct TenantNav {
    pub base: String,
    pub id: String,
    pub code: String,
    pub name: String,
    pub status: String,
    pub status_term: String,
    pub active: &'static str,
    pub is_sa: bool,
    pub read_only: bool,
    pub writable: bool,
    pub is_sandbox: bool,
}

impl TenantNav {
    pub fn tabs(&self) -> Vec<(&'static str, &'static str, String)> {
        let b = &self.base;
        let mut v = vec![
            ("overview", "Overview", b.clone()),
            ("config", "Configuration & features", format!("{b}/config")),
            ("quotas", "Quotas & usage", format!("{b}/quotas")),
            ("branding", "Branding", format!("{b}/branding")),
            ("storage", "Storage & isolation", format!("{b}/storage")),
            ("support", "Support access", format!("{b}/support")),
            ("sandboxes", "Sandboxes", format!("{b}/sandboxes")),
            ("baselines", "Config baselines", format!("{b}/baselines")),
            ("release", "Releases & maintenance", format!("{b}/release")),
            ("keys", "Encryption keys", format!("{b}/keys")),
            ("offboarding", "Offboarding & export", format!("{b}/offboarding")),
            ("audit", "Audit history", format!("{b}/audit")),
        ];
        if self.is_sa {
            v.push(("backups", "Backup & restore", format!("{b}/backups")));
        }
        v
    }
}

#[derive(Template)]
#[template(path = "dev/outbox.html")]
pub struct OutboxView {
    pub page: PageCtx,
    pub entries: Vec<OutboxEntry>,
}

#[derive(Template)]
#[template(path = "admin/dashboard.html")]
pub struct DashboardView {
    pub page: PageCtx,
    pub counts: Vec<(String, i64)>,
    pub total: i64,
    pub security_events: Vec<AuditRecord>,
    pub failed: Vec<(String, String, DateTime<Utc>)>,
    pub chain_ok: bool,
}

#[derive(Template)]
#[template(path = "tenants/directory.html")]
pub struct DirectoryView {
    pub page: PageCtx,
    pub rows: Vec<DirectoryRow>,
    pub q: String,
    pub status: String,
    pub region: String,
    pub next_cursor: Option<String>,
    pub statuses: Vec<&'static str>,
    pub regions: Vec<&'static str>,
}

#[derive(Template)]
#[template(path = "tenants/_rows.html")]
pub struct DirectoryRowsPartial {
    pub page: PageCtx,
    pub rows: Vec<DirectoryRow>,
    pub q: String,
    pub status: String,
    pub region: String,
    pub next_cursor: Option<String>,
}

#[derive(Template)]
#[template(path = "tenants/new.html")]
pub struct NewTenantView {
    pub page: PageCtx,
    pub form: CreateTenantForm,
    pub errors: BTreeMap<String, String>,
    pub general_error: Option<String>,
    pub plans: Vec<(String, String, String)>,
    pub templates: Vec<ProvisioningTemplate>,
    pub targets: Vec<DbTargetInfo>,
    pub parents: Vec<TenantSummary>,
    pub regions: Vec<(&'static str, &'static str)>,
}

impl NewTenantView {
    pub fn err(&self, field: &str) -> String {
        self.errors.get(field).cloned().unwrap_or_default()
    }
}

#[derive(Template)]
#[template(path = "tenants/_plan_features.html")]
pub struct PlanFeaturesPartial {
    pub tier: String,
    pub features: Vec<(String, String, bool)>,
}

#[derive(Debug, Clone)]
pub struct TransitionAction {
    pub to: &'static str,
    pub label: String,
    pub destructive: bool,
    pub requires_reason: bool,
}

pub fn transition_actions(next: &[TenantStatus]) -> Vec<TransitionAction> {
    next.iter()
        .map(|s| TransitionAction {
            to: s.as_str(),
            label: match s {
                TenantStatus::Active => "Activate / reinstate".to_string(),
                TenantStatus::Suspended => "Suspend".to_string(),
                TenantStatus::Grace => "Start grace (offboarding)".to_string(),
                TenantStatus::Terminated => "Terminate".to_string(),
                TenantStatus::Purged => "Purge permanently".to_string(),
                TenantStatus::Draft => "Draft".to_string(),
            },
            destructive: s.is_destructive_target(),
            requires_reason: s.requires_reason(),
        })
        .collect()
}

#[derive(Template)]
#[template(path = "tenants/overview.html")]
pub struct OverviewView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub d: TenantDetail,
    pub actions: Vec<TransitionAction>,
    pub admins: Vec<TenantUser>,
    pub notes: Vec<Rollout>,
    pub quota_lines: Vec<(String, f64, String)>,
    pub retention_hours: i64,
}

#[derive(Template)]
#[template(path = "tenants/config.html")]
pub struct ConfigView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub v: TenantSettingsView,
    pub errors: BTreeMap<String, String>,
    pub general_error: Option<String>,
}

impl ConfigView {
    pub fn err(&self, key: &str) -> String {
        self.errors.get(&format!("config.{key}")).cloned().unwrap_or_default()
    }
}

#[derive(Template)]
#[template(path = "tenants/_feature_row.html")]
pub struct FeatureRowPartial {
    pub page: PageCtx,
    pub base: String,
    pub f: FeatureEntry,
    pub writable: bool,
    pub error: Option<String>,
}

#[derive(Template)]
#[template(path = "tenants/quotas.html")]
pub struct QuotasView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub q: QuotaView,
    pub statement: Vec<(String, i64)>,
    pub month: String,
    pub meters: Vec<&'static str>,
}

#[derive(Template)]
#[template(path = "tenants/branding.html")]
pub struct BrandingPage {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub v: BrandingView,
    pub form: BrandingForm,
    pub errors: BTreeMap<String, String>,
    pub general_error: Option<String>,
}

impl BrandingPage {
    pub fn err(&self, field: &str) -> String {
        self.errors.get(field).cloned().unwrap_or_default()
    }
}

#[derive(Template)]
#[template(path = "tenants/_preview.html")]
pub struct PreviewPartial {
    pub primary: String,
    pub secondary: String,
    pub error: Option<String>,
}

#[derive(Template)]
#[template(path = "tenants/storage.html")]
pub struct StorageView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub conn: Option<ConnectionProfile>,
    pub checks: Vec<IsolationCheckRecord>,
    pub run: Option<ProvisioningRun>,
}

#[derive(Template)]
#[template(path = "tenants/support.html")]
pub struct SupportPage {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub grants: Vec<(SupportGrant, &'static str)>,
    pub approvers: Vec<TenantUser>,
    pub regulated: bool,
    pub view: Option<SupportData>,
    pub me: String,
}

#[derive(Template)]
#[template(path = "tenants/sandboxes.html")]
pub struct SandboxesView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub sandboxes: Vec<TenantSummary>,
    pub production: Option<String>,
    pub preview: Option<ImportPreview>,
}

#[derive(Template)]
#[template(path = "tenants/baselines.html")]
pub struct BaselinesView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub baselines: Vec<StoredBaseline>,
    pub preview: Option<ImportPreview>,
    pub document: String,
    pub change_record: String,
    pub current_json: String,
    pub error: Option<String>,
}

#[derive(Template)]
#[template(path = "tenants/release.html")]
pub struct ReleaseView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub pref: ReleasePreference,
    pub notes: Vec<Rollout>,
    pub days: Vec<(i16, &'static str)>,
}

pub fn days() -> Vec<(i16, &'static str)> {
    DAY_NAMES.iter().enumerate().map(|(i, d)| ((i + 1) as i16, *d)).collect()
}

#[derive(Template)]
#[template(path = "tenants/keys.html")]
pub struct KeysView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub keys: Vec<TenantKey>,
    pub regulated: bool,
    pub now: DateTime<Utc>,
}

#[derive(Template)]
#[template(path = "tenants/offboarding.html")]
pub struct OffboardingView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub exports: Vec<ExportRecord>,
    pub certificate: Option<DestructionCertificate>,
    pub legal_hold: bool,
    pub grace_until: Option<DateTime<Utc>>,
    pub purge_after: Option<DateTime<Utc>>,
}

#[derive(Template)]
#[template(path = "tenants/audit.html")]
pub struct AuditView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub rows: Vec<AuditRecord>,
    pub next_before: Option<i64>,
}

#[derive(Template)]
#[template(path = "tenants/backups.html")]
pub struct BackupsView {
    pub page: PageCtx,
    pub nav: TenantNav,
    pub backups: Vec<BackupRecord>,
}

#[derive(Template)]
#[template(path = "admin/releases.html")]
pub struct ReleasesAdminView {
    pub page: PageCtx,
    pub releases: Vec<(PlatformRelease, i64, i64)>,
}

#[derive(Template)]
#[template(path = "admin/analytics.html")]
pub struct AnalyticsView {
    pub page: PageCtx,
    pub a: HostAnalytics,
}

impl AnalyticsView {
    pub fn groups(&self) -> Vec<(&'static str, &Vec<super::super::domain::analytics::Bucket>)> {
        vec![
            ("By status", &self.a.by_status),
            ("By tier", &self.a.by_tier),
            ("By region", &self.a.by_region),
            ("Quota utilisation", &self.a.quota_utilisation),
        ]
    }
}

#[derive(Template)]
#[template(path = "admin/entitlements.html")]
pub struct EntitlementsView {
    pub page: PageCtx,
    pub m: EntitlementMatrix,
}

#[derive(Template)]
#[template(path = "admin/dr.html")]
pub struct DrView {
    pub page: PageCtx,
    pub rows: Vec<DrRow>,
}
