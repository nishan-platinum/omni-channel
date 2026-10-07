//! Ports: repository interfaces (implemented with SQLx in `infrastructure::persistence`) and
//! external-module ports (implemented by clearly-labelled reference adapters in
//! `infrastructure::adapters`). Application services depend only on these traits.
// Repository methods mirror single atomic commands (state + audit + events in one transaction),
// so some legitimately take more than seven arguments.
#![allow(clippy::too_many_arguments)]

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::platform::audit::AuditEntry;
use crate::platform::db::AccessScope;
use crate::platform::errors::AppResult;
use crate::platform::events::EventEnvelope;

use super::super::domain::baseline::BaselineDocument;
use super::super::domain::branding::DomainStatus;
use super::super::domain::keys::{KeyKind, KeyState};
use super::super::domain::plan::Plan;
use super::super::domain::quota::{QuotaDecision, QuotaMetric, QuotaState};
use super::super::domain::release::Ring;
use super::super::domain::storage::{DatabaseEngine, Region, StorageStrategy};
use super::super::domain::support::SupportGrant;
use super::super::domain::tenant::{IsolationCheckStatus, ProvisioningStatus, Tenant, TenantStatus, TransitionPlan};
use super::super::domain::TenantId;

// ------------------------------------------------------------------------------------------------
// Shared write envelope: audit rows + domain events committed in the same transaction as a change.
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct ChangeSet {
    pub audit: Vec<AuditEntry>,
    pub events: Vec<EventEnvelope>,
}

impl ChangeSet {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_audit(mut self, e: AuditEntry) -> Self {
        self.audit.push(e);
        self
    }
    pub fn with_event(mut self, e: EventEnvelope) -> Self {
        self.events.push(e);
        self
    }
    pub fn push_audit(&mut self, e: AuditEntry) {
        self.audit.push(e);
    }
    pub fn push_event(&mut self, e: EventEnvelope) {
        self.events.push(e);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

// ------------------------------------------------------------------------------------------------
// Tenants
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct TenantFilter {
    pub q: Option<String>,
    pub status: Option<TenantStatus>,
    pub region: Option<Region>,
    pub cursor: Option<String>,
    pub limit: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TenantSummary {
    pub id: TenantId,
    pub code: String,
    pub name: String,
    pub legal_name: Option<String>,
    pub region: String,
    pub status: TenantStatus,
    pub provisioning_status: ProvisioningStatus,
    pub isolation_check_status: IsolationCheckStatus,
    pub plan_code: String,
    pub tier: String,
    pub storage_strategy: String,
    pub is_sandbox: bool,
    pub platform_version: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionProfile {
    pub tenant_id: TenantId,
    pub engine: DatabaseEngine,
    pub storage_strategy: StorageStrategy,
    pub target_name: String,
    pub host: Option<String>,
    pub port: Option<i32>,
    pub database_name: String,
    pub schema_name: Option<String>,
    pub region: Region,
    /// Reference into the secret store (`env:TENANT_DB_*`). Never a password; not serialised.
    #[serde(skip)]
    pub secret_ref: Option<String>,
    pub status: String,
    pub last_checked_at: Option<DateTime<Utc>>,
    pub last_check_ok: Option<bool>,
    pub last_check_message: Option<String>,
    pub last_latency_ms: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct NewKey {
    pub id: Uuid,
    pub kind: KeyKind,
    pub key_ref: String,
    pub key_version: i32,
    pub state: KeyState,
    pub wrapped_dek: Option<Vec<u8>>,
    pub rotate_after: DateTime<Utc>,
}

/// Everything created atomically in the central DB when a tenant is provisioned.
#[derive(Debug, Clone)]
pub struct NewTenantBundle {
    pub tenant: Tenant,
    pub config: BTreeMap<String, Value>,
    pub flags: BTreeMap<String, bool>,
    pub quotas: Vec<QuotaState>,
    pub connection: ConnectionProfile,
    pub key: NewKey,
    pub run_id: Uuid,
    pub created_by: Option<Uuid>,
}

#[async_trait]
pub trait TenantRepository: Send + Sync {
    async fn code_exists(&self, code: &str) -> AppResult<bool>;
    async fn insert_provisioned(&self, scope: &AccessScope, bundle: &NewTenantBundle, changes: ChangeSet) -> AppResult<()>;
    async fn get(&self, scope: &AccessScope, id: TenantId) -> AppResult<Option<Tenant>>;
    async fn list(&self, scope: &AccessScope, filter: &TenantFilter) -> AppResult<Page<TenantSummary>>;
    async fn list_all_ids(&self, scope: &AccessScope) -> AppResult<Vec<(TenantId, TenantStatus)>>;
    async fn ancestry(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<TenantId>>;
    async fn subtree_height(&self, scope: &AccessScope, id: TenantId) -> AppResult<usize>;
    async fn children(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<TenantSummary>>;
    async fn sandboxes_of(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<TenantSummary>>;
    async fn apply_transition(
        &self,
        scope: &AccessScope,
        id: TenantId,
        expected_version: i32,
        plan: &TransitionPlan,
        changes: ChangeSet,
    ) -> AppResult<Tenant>;
    async fn update_details(
        &self,
        scope: &AccessScope,
        id: TenantId,
        expected_version: i32,
        name: &str,
        legal_name: Option<&str>,
        parent: Option<Option<TenantId>>,
        changes: ChangeSet,
    ) -> AppResult<Tenant>;
    async fn set_provisioning_status(&self, scope: &AccessScope, id: TenantId, status: ProvisioningStatus) -> AppResult<()>;
    async fn record_isolation_check(
        &self,
        scope: &AccessScope,
        id: TenantId,
        passed: bool,
        results: Value,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()>;
    async fn list_isolation_checks(&self, scope: &AccessScope, id: TenantId, limit: i64) -> AppResult<Vec<IsolationCheckRecord>>;
    async fn discard_draft(&self, scope: &AccessScope, id: TenantId, changes: ChangeSet) -> AppResult<()>;
    async fn set_legal_hold(&self, scope: &AccessScope, id: TenantId, hold: bool, changes: ChangeSet) -> AppResult<()>;
    async fn set_platform_version(&self, scope: &AccessScope, id: TenantId, version: &str) -> AppResult<()>;
    async fn status_counts(&self, scope: &AccessScope) -> AppResult<Vec<(TenantStatus, i64)>>;
    /// Control-plane RLS probe: under a foreign tenant scope, rows of `own` must be invisible.
    async fn control_plane_probe(&self, own: TenantId, foreign: TenantId) -> AppResult<bool>;
    /// Tenants in `grace` past `grace_until` and `terminated` past `purge_after` (scheduler).
    async fn due_lifecycle(&self, now: DateTime<Utc>) -> AppResult<(Vec<TenantId>, Vec<TenantId>)>;
}

#[derive(Debug, Clone, Serialize)]
pub struct IsolationCheckRecord {
    pub id: Uuid,
    pub passed: bool,
    pub results: Value,
    pub created_at: DateTime<Utc>,
}

// ------------------------------------------------------------------------------------------------
// Configuration & feature flags
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct StoredSettings {
    pub config: BTreeMap<String, Value>,
    pub flags: BTreeMap<String, bool>,
}

#[async_trait]
pub trait ConfigRepository: Send + Sync {
    async fn load(&self, scope: &AccessScope, id: TenantId) -> AppResult<StoredSettings>;
    async fn apply(
        &self,
        scope: &AccessScope,
        id: TenantId,
        config: &BTreeMap<String, Value>,
        flags: &BTreeMap<String, bool>,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()>;
    async fn enabled_flags_for(&self, scope: &AccessScope, ids: &[TenantId]) -> AppResult<Vec<(TenantId, String)>>;
}

// ------------------------------------------------------------------------------------------------
// Quotas & metering
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct QuotaConsumption {
    pub decision: QuotaDecision,
    pub state: QuotaState,
}

#[async_trait]
pub trait QuotaRepository: Send + Sync {
    async fn list(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<QuotaState>>;
    /// Atomically evaluates (domain rule) and records consumption; writes warning/exhausted
    /// markers and the corresponding domain events in the same transaction.
    async fn consume(
        &self,
        scope: &AccessScope,
        id: TenantId,
        metric: QuotaMetric,
        amount: i64,
        now: DateTime<Utc>,
        correlation_id: Option<String>,
    ) -> AppResult<QuotaConsumption>;
    async fn set_static_usage(&self, scope: &AccessScope, id: TenantId, metric: QuotaMetric, usage: i64) -> AppResult<()>;
    async fn set_limits(
        &self,
        scope: &AccessScope,
        id: TenantId,
        limits: &[(QuotaMetric, i64, f64)],
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()>;
    async fn add_meter(&self, scope: &AccessScope, id: TenantId, meter: &str, month: NaiveDate, delta: i64) -> AppResult<()>;
    async fn meters(&self, scope: &AccessScope, id: TenantId, month: NaiveDate) -> AppResult<Vec<(String, i64)>>;
    async fn max_utilisation(&self, scope: &AccessScope, ids: &[TenantId], now: DateTime<Utc>) -> AppResult<Vec<(TenantId, f64)>>;
}

// ------------------------------------------------------------------------------------------------
// Branding
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct Branding {
    pub tenant_id: TenantId,
    pub logo_object_key: Option<String>,
    pub logo_content_type: Option<String>,
    pub primary_color: String,
    pub secondary_color: String,
    pub custom_domain: Option<String>,
    pub custom_domain_status: DomainStatus,
    pub custom_domain_active: bool,
    pub custom_domain_token: Option<String>,
    pub custom_domain_checked_at: Option<DateTime<Utc>>,
    pub custom_domain_message: Option<String>,
    pub email_from: Option<String>,
    pub email_footer: Option<String>,
    pub login_message: Option<String>,
    pub pdf_letterhead: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub version: i32,
}

#[derive(Debug, Clone, Default)]
pub struct BrandingWrite {
    pub logo: Option<Option<(String, String, i64)>>,
    pub primary_color: Option<String>,
    pub secondary_color: Option<String>,
    /// Some(None) clears; Some(Some((domain, token))) sets a new pending domain.
    pub custom_domain: Option<Option<(String, String)>>,
    pub custom_domain_active: Option<bool>,
    pub email_from: Option<Option<String>>,
    pub email_footer: Option<Option<String>>,
    pub login_message: Option<Option<String>>,
    pub pdf_letterhead: Option<Option<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SenderDomain {
    pub id: Uuid,
    pub domain: String,
    pub status: String,
    pub spf_record: String,
    pub dkim_selector: String,
    pub dkim_record: String,
    pub verified_at: Option<DateTime<Utc>>,
    pub message: Option<String>,
}

#[async_trait]
pub trait BrandingRepository: Send + Sync {
    async fn get(&self, scope: &AccessScope, id: TenantId) -> AppResult<Branding>;
    /// Returns Conflict("Domain already claimed") on a duplicate custom domain.
    async fn update(
        &self,
        scope: &AccessScope,
        id: TenantId,
        write: &BrandingWrite,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<Branding>;
    async fn set_domain_status(
        &self,
        scope: &AccessScope,
        id: TenantId,
        status: DomainStatus,
        message: &str,
        at: DateTime<Utc>,
        changes: ChangeSet,
    ) -> AppResult<()>;
    async fn sender_domains(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<SenderDomain>>;
    async fn upsert_sender_domain(
        &self,
        scope: &AccessScope,
        id: TenantId,
        domain: &str,
        spf: &str,
        selector: &str,
        dkim: &str,
        changes: ChangeSet,
    ) -> AppResult<()>;
    async fn set_sender_status(
        &self,
        scope: &AccessScope,
        id: TenantId,
        domain: &str,
        verified: bool,
        message: &str,
        at: DateTime<Utc>,
        changes: ChangeSet,
    ) -> AppResult<()>;
    /// Host-based lookup (system scope): which tenant claims this domain, and is it active?
    async fn find_by_custom_domain(&self, domain: &str) -> AppResult<Option<(TenantId, bool)>>;
}

// ------------------------------------------------------------------------------------------------
// Connection profiles & provisioning runs
// ------------------------------------------------------------------------------------------------

#[async_trait]
pub trait ConnectionRepository: Send + Sync {
    async fn get(&self, scope: &AccessScope, id: TenantId) -> AppResult<Option<ConnectionProfile>>;
    async fn record_check(
        &self,
        scope: &AccessScope,
        id: TenantId,
        ok: bool,
        message: &str,
        latency_ms: i32,
        at: DateTime<Utc>,
    ) -> AppResult<()>;
    async fn set_status(&self, scope: &AccessScope, id: TenantId, status: &str) -> AppResult<()>;
    async fn health(&self, scope: &AccessScope, ids: &[TenantId]) -> AppResult<Vec<(TenantId, Option<bool>)>>;
}

#[derive(Debug, Clone, Serialize)]
pub struct ProvisioningStep {
    pub step: String,
    pub status: String,
    pub detail: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProvisioningRun {
    pub id: Uuid,
    pub status: String,
    pub template_code: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub error_summary: Option<String>,
    pub steps: Vec<ProvisioningStep>,
}

#[async_trait]
pub trait ProvisioningRepository: Send + Sync {
    async fn start_run(&self, scope: &AccessScope, tenant: TenantId, template: Option<&str>, actor: Option<Uuid>) -> AppResult<Uuid>;
    async fn add_step(
        &self,
        scope: &AccessScope,
        run: Uuid,
        tenant: TenantId,
        step: &str,
        status: &str,
        detail: Option<&str>,
        started_at: DateTime<Utc>,
    ) -> AppResult<()>;
    async fn finish_run(
        &self,
        scope: &AccessScope,
        run: Uuid,
        tenant: TenantId,
        ok: bool,
        duration_ms: i64,
        error: Option<&str>,
    ) -> AppResult<()>;
    async fn latest_run(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Option<ProvisioningRun>>;
    async fn failed_runs(&self, scope: &AccessScope, limit: i64) -> AppResult<Vec<(TenantId, String, DateTime<Utc>)>>;
}

// ------------------------------------------------------------------------------------------------
// Support grants
// ------------------------------------------------------------------------------------------------

#[async_trait]
pub trait SupportGrantRepository: Send + Sync {
    async fn insert(&self, scope: &AccessScope, grant: &SupportGrant, changes: ChangeSet) -> AppResult<()>;
    async fn get(&self, scope: &AccessScope, id: Uuid) -> AppResult<Option<SupportGrant>>;
    async fn list(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<SupportGrant>>;
    async fn decide(&self, scope: &AccessScope, grant: &SupportGrant, changes: ChangeSet) -> AppResult<()>;
    async fn record_use(&self, scope: &AccessScope, id: Uuid, at: DateTime<Utc>, changes: ChangeSet) -> AppResult<()>;
}

// ------------------------------------------------------------------------------------------------
// Baselines
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct StoredBaseline {
    pub id: Uuid,
    pub version_no: i32,
    pub label: String,
    pub source: String,
    pub sha256: String,
    pub change_record: Option<String>,
    pub created_at: DateTime<Utc>,
    pub content: BaselineDocument,
}

#[async_trait]
pub trait BaselineRepository: Send + Sync {
    async fn insert(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        label: &str,
        source: &str,
        doc: &BaselineDocument,
        change_record: Option<&str>,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<StoredBaseline>;
    async fn list(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<StoredBaseline>>;
    async fn get(&self, scope: &AccessScope, tenant: TenantId, id: Uuid) -> AppResult<Option<StoredBaseline>>;
    /// Applies a validated baseline (config, flags, branding text, release preferences) atomically.
    async fn apply(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        doc: &BaselineDocument,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()>;
}

// ------------------------------------------------------------------------------------------------
// Releases
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct ReleasePreference {
    pub ring: Ring,
    pub maintenance_day: i16,
    pub maintenance_start_hour_utc: i16,
    pub maintenance_duration_min: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlatformRelease {
    pub id: Uuid,
    pub release_version: String,
    pub title: String,
    pub notes: String,
    pub disruptive: bool,
    pub status: String,
    pub current_ring: Option<Ring>,
    pub created_at: DateTime<Utc>,
}

impl PlatformRelease {
    /// A release can advance until it has reached every tenant.
    pub fn can_advance(&self) -> bool {
        self.status != "completed" && self.current_ring != Some(Ring::General)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Rollout {
    pub release_id: Uuid,
    pub tenant_id: TenantId,
    pub release_version: String,
    pub title: String,
    pub notes: String,
    pub disruptive: bool,
    pub ring: Ring,
    pub scheduled_for: DateTime<Utc>,
    pub status: String,
    pub completed_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait ReleaseRepository: Send + Sync {
    async fn preference(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<ReleasePreference>;
    async fn set_preference(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        pref: &ReleasePreference,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()>;
    async fn create_release(
        &self,
        scope: &AccessScope,
        release: &PlatformRelease,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()>;
    async fn releases(&self, scope: &AccessScope) -> AppResult<Vec<PlatformRelease>>;
    async fn release(&self, scope: &AccessScope, id: Uuid) -> AppResult<Option<PlatformRelease>>;
    async fn set_release_ring(&self, scope: &AccessScope, id: Uuid, ring: Ring, status: &str, changes: ChangeSet) -> AppResult<()>;
    /// Tenants (with their preferences) eligible for a ring, excluding purged/terminated.
    async fn tenants_in_rings(&self, scope: &AccessScope, rings: &[Ring]) -> AppResult<Vec<(TenantId, ReleasePreference)>>;
    async fn schedule_rollout(
        &self,
        scope: &AccessScope,
        release: Uuid,
        tenant: TenantId,
        ring: Ring,
        at: DateTime<Utc>,
    ) -> AppResult<bool>;
    async fn due_rollouts(&self, now: DateTime<Utc>) -> AppResult<Vec<Rollout>>;
    async fn complete_rollout(&self, scope: &AccessScope, release: Uuid, tenant: TenantId, at: DateTime<Utc>) -> AppResult<()>;
    async fn rollouts_for_tenant(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<Rollout>>;
    async fn rollout_counts(&self, scope: &AccessScope, release: Uuid) -> AppResult<(i64, i64)>;
}

// ------------------------------------------------------------------------------------------------
// Keys, exports, backups, purge
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct TenantKey {
    pub id: Uuid,
    pub kind: KeyKind,
    pub key_ref: String,
    pub key_version: i32,
    pub state: KeyState,
    #[serde(skip)]
    pub wrapped_dek: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
    pub rotate_after: DateTime<Utc>,
}

#[async_trait]
pub trait KeyRepository: Send + Sync {
    async fn list(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<TenantKey>>;
    async fn by_version(&self, scope: &AccessScope, tenant: TenantId, version: i32) -> AppResult<Option<TenantKey>>;
    /// Retires the current active key and inserts/activates `new_key` atomically.
    async fn rotate(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        new_key: &NewKey,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()>;
}

#[derive(Debug, Clone, Serialize)]
pub struct ExportRecord {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub reason: String,
    pub object_key: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub key_version: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackupRecord {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub object_key: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub key_version: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DrReportRow {
    pub tenant_id: TenantId,
    pub tenant_code: String,
    pub tier: String,
    pub last_backup_at: Option<DateTime<Utc>>,
    pub last_restore_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestructionCertificate {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub tenant_code: String,
    pub manifest: Value,
    pub manifest_sha256: String,
    pub issued_at: DateTime<Utc>,
}

#[async_trait]
pub trait ExportRepository: Send + Sync {
    /// M01-owned control-plane snapshot of one tenant (no secrets).
    async fn snapshot(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Value>;
    async fn insert_export(&self, scope: &AccessScope, rec: &ExportRecord, actor: Option<Uuid>, changes: ChangeSet) -> AppResult<()>;
    async fn exports(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<ExportRecord>>;
    async fn export(&self, scope: &AccessScope, id: Uuid) -> AppResult<Option<ExportRecord>>;
    async fn insert_backup(&self, scope: &AccessScope, rec: &BackupRecord, actor: Option<Uuid>, changes: ChangeSet) -> AppResult<()>;
    async fn backups(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<BackupRecord>>;
    async fn record_restore(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        backup: Uuid,
        started: DateTime<Utc>,
        completed: DateTime<Utc>,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()>;
    async fn dr_report(&self, scope: &AccessScope) -> AppResult<Vec<DrReportRow>>;
    async fn certificate(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Option<DestructionCertificate>>;
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PurgeCounts {
    pub control_plane_rows: BTreeMap<String, i64>,
}

#[async_trait]
pub trait PurgeRepository: Send + Sync {
    /// Deletes all M01 control-plane data of the tenant, crypto-shreds keys, turns the tenant row
    /// into a tombstone (status purged), writes the destruction certificate, audit and event —
    /// all in one transaction. `manifest_extra` carries counts from the data plane/object store.
    async fn purge(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        plan: &TransitionPlan,
        manifest_extra: Value,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<DestructionCertificate>;
}

// ------------------------------------------------------------------------------------------------
// Analytics (P2)
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Default)]
pub struct AnalyticsRaw {
    pub participants: i64,
    pub opted_out: i64,
    pub by_status: Vec<(String, i64)>,
    pub by_tier: Vec<(String, i64)>,
    pub by_region: Vec<(String, i64)>,
    pub feature_enabled: Vec<(String, i64)>,
    pub feature_entitled: Vec<(String, i64)>,
    pub utilisation_buckets: Vec<(String, i64)>,
    pub api_calls_this_month: i64,
}

#[async_trait]
pub trait AnalyticsRepository: Send + Sync {
    async fn aggregate(&self, month: NaiveDate, now: DateTime<Utc>) -> AppResult<AnalyticsRaw>;
}

// ------------------------------------------------------------------------------------------------
// External module ports (reference adapters in infrastructure/adapters)
// ------------------------------------------------------------------------------------------------

/// M19 plans & entitlements.
#[async_trait]
pub trait PlanCatalog: Send + Sync {
    async fn list_active(&self) -> AppResult<Vec<Plan>>;
    async fn get(&self, id: Uuid) -> AppResult<Option<Plan>>;
}

#[derive(Debug, Clone, Serialize)]
pub struct ProvisioningTemplate {
    pub id: Uuid,
    pub code: String,
    pub name: String,
    pub description: String,
    pub features: BTreeSet<String>,
    pub config_defaults: BTreeMap<String, Value>,
    pub packs: Vec<String>,
}

#[async_trait]
pub trait TemplateCatalog: Send + Sync {
    async fn list(&self) -> AppResult<Vec<ProvisioningTemplate>>;
    async fn get(&self, code: &str) -> AppResult<Option<ProvisioningTemplate>>;
}

#[derive(Debug, Clone, Serialize)]
pub struct TenantUser {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub status: String,
}

/// M02 identity (bootstrap_auth reference implementation).
#[async_trait]
pub trait IdentityPort: Send + Sync {
    /// Creates (or re-invites) the initial Tenant Admin; returns the user id and, unless the user
    /// is already active, a fresh raw invitation token (never persisted raw, never logged).
    async fn invite_tenant_admin(&self, tenant: TenantId, email: &str, display_name: &str) -> AppResult<(Uuid, Option<String>)>;
    async fn revoke_sessions(&self, tenant: TenantId) -> AppResult<u64>;
    async fn count_users(&self, tenant: TenantId) -> AppResult<i64>;
    async fn tenant_admins(&self, tenant: TenantId) -> AppResult<Vec<TenantUser>>;
    async fn purge_tenant(&self, tenant: TenantId) -> AppResult<u64>;
}

#[derive(Debug, Clone)]
pub struct Notification {
    pub tenant_id: Option<TenantId>,
    /// Spec notification id (NT-001…) or an implementation id (NT-M01-…).
    pub notification_id: String,
    pub template_key: String,
    pub recipient: String,
    pub channels: String,
    pub priority: String,
    pub subject: String,
    pub body: String,
}

/// M25 notifications.
#[async_trait]
pub trait NotificationPort: Send + Sync {
    async fn send(&self, n: Notification) -> AppResult<()>;
}

/// Downstream provisioning of template packs (M23 numbers/channels, M02 roles/teams, M09 BPM,
/// M24 reports, M15 SLA, M31 dropdowns).
#[async_trait]
pub trait DownstreamProvisioningPort: Send + Sync {
    async fn apply_pack(&self, tenant: TenantId, pack: &str) -> AppResult<String>;
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationResult {
    pub verified: bool,
    pub message: String,
    pub simulated: bool,
}

/// DNS CNAME/TXT verification for custom domains (shares infra with M06).
#[async_trait]
pub trait DomainVerificationPort: Send + Sync {
    async fn verify(&self, domain: &str, expected_cname: &str, token: &str) -> AppResult<VerificationResult>;
}

/// SPF/DKIM verification for the email sender domain (M06).
#[async_trait]
pub trait EmailSenderVerificationPort: Send + Sync {
    async fn verify(&self, domain: &str, dkim_selector: &str) -> AppResult<VerificationResult>;
}

#[derive(Debug, Clone)]
pub struct DataKey {
    pub plaintext: [u8; 32],
    pub wrapped: Vec<u8>,
}

/// KMS/HSM (SEC-112). Raw master keys never leave the adapter.
#[async_trait]
pub trait KeyManagementPort: Send + Sync {
    fn platform_key_ref(&self, tenant: TenantId) -> String;
    async fn generate_data_key(&self, key_ref: &str) -> AppResult<DataKey>;
    async fn unwrap_data_key(&self, key_ref: &str, wrapped: &[u8]) -> AppResult<[u8; 32]>;
    /// Validates that a customer-supplied key reference is reachable/usable.
    async fn validate_customer_key(&self, key_ref: &str) -> AppResult<VerificationResult>;
}

/// Object storage with tenant path separation (FR-ARC-004).
#[async_trait]
pub trait ObjectStoragePort: Send + Sync {
    async fn put(&self, key: &str, bytes: &[u8]) -> AppResult<()>;
    async fn get(&self, key: &str) -> AppResult<Option<Vec<u8>>>;
    async fn delete_prefix(&self, prefix: &str) -> AppResult<u64>;
    async fn list_prefix(&self, prefix: &str) -> AppResult<Vec<(String, u64)>>;
}

/// Release deployment (CI/CD, FR-OPS-121). The reference adapter only records.
#[async_trait]
pub trait ReleaseManagementPort: Send + Sync {
    async fn deploy(&self, tenant: TenantId, release_version: &str) -> AppResult<String>;
}

/// Anonymised data subset for sandboxes (M29/M38). M01 owns no business rows.
#[async_trait]
pub trait AnonymisedDataCopyPort: Send + Sync {
    async fn copy_subset(&self, from: TenantId, to: TenantId) -> AppResult<String>;
}

/// Export participants for data owned by future modules (each module registers one).
#[async_trait]
pub trait ExportParticipant: Send + Sync {
    fn module(&self) -> &'static str;
    async fn export(&self, tenant: TenantId) -> AppResult<Value>;
}

// ------------------------------------------------------------------------------------------------
// Tenant data plane
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct ProbeResult {
    pub check: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CanaryRow {
    pub id: String,
    pub tenant_id: String,
    pub marker: String,
}

/// One tenant's operational data store. Engine specifics stay inside implementations.
#[async_trait]
pub trait TenantDataStore: Send + Sync {
    fn engine(&self) -> DatabaseEngine;
    fn strategy(&self) -> StorageStrategy;
    async fn ping(&self) -> AppResult<u128>;
    async fn write_canary(&self, tenant: TenantId, marker: &str) -> AppResult<()>;
    async fn list_rows(&self, tenant: TenantId) -> AppResult<Vec<CanaryRow>>;
    async fn isolation_probe(&self, own: TenantId, foreign: TenantId) -> AppResult<Vec<ProbeResult>>;
    async fn purge_rows(&self, tenant: TenantId) -> AppResult<u64>;
}

#[derive(Debug, Clone, Serialize)]
pub struct DbTargetInfo {
    pub name: String,
    pub engine: DatabaseEngine,
    pub region: Region,
    pub host: String,
    pub port: u16,
}

/// Server-side routing from tenant → data store, plus provisioning/decommissioning.
#[async_trait]
pub trait TenantDataRouter: Send + Sync {
    fn dedicated_targets(&self) -> Vec<DbTargetInfo>;
    /// Computes the connection profile for a new tenant (no I/O). `target` only for dedicated.
    fn plan_profile(
        &self,
        tenant: TenantId,
        strategy: StorageStrategy,
        region: Region,
        target: Option<&str>,
    ) -> AppResult<ConnectionProfile>;
    async fn provision(&self, profile: &ConnectionProfile) -> AppResult<String>;
    async fn store_for(&self, profile: &ConnectionProfile) -> AppResult<std::sync::Arc<dyn TenantDataStore>>;
    /// Drops the tenant's schema/database (dedicated/schema) — irreversible.
    async fn decommission(&self, profile: &ConnectionProfile) -> AppResult<String>;
}
