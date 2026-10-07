//! Configuration baseline export/import with diff (OCC-M01-R022), rollback (UJ-15 E2) and sandbox →
//! production promotion with a change record (UJ-19 step 6, GOV-002).

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult};

use super::super::domain::baseline::{diff, BaselineDocument, BrandingBaseline, DiffEntry, ReleaseBaseline, FORMAT_VERSION};
use super::super::domain::branding::{validate_text, HexColor};
use super::super::domain::config::{self as cfgdom, ConfigActor};
use super::super::domain::errors::Violations;
use super::super::domain::events::TenantEvent;
use super::super::domain::features;
use super::super::domain::release::{MaintenanceWindow, Ring};
use super::super::domain::TenantId;
use super::context::{Access, Actor, M01Deps};
use super::ports::{ChangeSet, StoredBaseline};

#[derive(Debug, Clone, Serialize)]
pub struct ImportPreview {
    pub incoming: BaselineDocument,
    pub diff: Vec<DiffEntry>,
}

pub struct BaselineService {
    deps: Arc<M01Deps>,
}

impl BaselineService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps }
    }

    pub(crate) async fn current(&self, scope: &AccessScope, id: TenantId) -> AppResult<BaselineDocument> {
        let t = self.deps.load_tenant(scope, id).await?;
        let s = self.deps.configs.load(scope, id).await?;
        let mut config = cfgdom::defaults();
        config.extend(s.config);
        let b = self.deps.branding.get(scope, id).await?;
        let r = self.deps.releases.preference(scope, id).await?;
        Ok(BaselineDocument {
            format_version: FORMAT_VERSION,
            source_tenant_code: t.code.to_string(),
            config,
            feature_flags: s.flags,
            branding: BrandingBaseline {
                primary_color: b.primary_color,
                secondary_color: b.secondary_color,
                email_footer: b.email_footer,
                login_message: b.login_message,
                pdf_letterhead: b.pdf_letterhead,
            },
            release: ReleaseBaseline {
                ring: r.ring.as_str().into(),
                maintenance_day: r.maintenance_day,
                maintenance_start_hour_utc: r.maintenance_start_hour_utc,
                maintenance_duration_min: r.maintenance_duration_min,
            },
        })
    }

    pub async fn export(&self, actor: &Actor, id: TenantId, label: &str) -> AppResult<StoredBaseline> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let doc = self.current(&scope, id).await?;
        let label = if label.trim().is_empty() { "Manual export" } else { label.trim() };
        let label: String = label.chars().take(200).collect();
        let a = actor.audit(Some(id), "tenant_baseline", None, "tenant.baseline_exported");
        self.deps.baselines.insert(&scope, id, &label, "export", &doc, None, actor.user_id, ChangeSet::new().with_audit(a)).await
    }

    pub async fn list(&self, actor: &Actor, id: TenantId) -> AppResult<Vec<StoredBaseline>> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        self.deps.baselines.list(&scope, id).await
    }

    pub async fn get(&self, actor: &Actor, id: TenantId, baseline: Uuid) -> AppResult<StoredBaseline> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        self.deps.baselines.get(&scope, id, baseline).await?.ok_or_else(|| AppError::not_found("Baseline does not exist"))
    }

    /// Validates an incoming document for this tenant (plan entitlements, key schema, permissions).
    async fn validate(&self, actor: &Actor, scope: &AccessScope, id: TenantId, doc: &BaselineDocument) -> AppResult<BaselineDocument> {
        let t = self.deps.load_tenant(scope, id).await?;
        let plan = self.deps.plans.get(t.plan_id).await?.ok_or_else(|| AppError::not_found("Plan does not exist"))?;
        let who = if actor.is_tenant_admin() { ConfigActor::TenantAdmin } else { ConfigActor::SuperAdmin };
        let current = self.current(scope, id).await?;
        // Only keys that change are subject to editor permissions.
        let changed: BTreeMap<_, _> =
            doc.config.iter().filter(|(k, v)| current.config.get(*k) != Some(*v)).map(|(k, v)| (k.clone(), v.clone())).collect();
        let validated = cfgdom::validate_changes(&changed, &current.config, who, plan.tier)?;
        let mut config = current.config.clone();
        config.extend(validated);
        for (k, on) in &doc.feature_flags {
            features::validate_feature_change(k, *on, &plan.entitlements, &config)?;
        }
        let mut v = Violations::default();
        let p = v.capture(HexColor::parse("branding.primary_color", &doc.branding.primary_color));
        let s = v.capture(HexColor::parse("branding.secondary_color", &doc.branding.secondary_color));
        let footer = v.capture(validate_text("branding.email_footer", doc.branding.email_footer.as_deref(), 1000));
        let login = v.capture(validate_text("branding.login_message", doc.branding.login_message.as_deref(), 500));
        let pdf = v.capture(validate_text("branding.pdf_letterhead", doc.branding.pdf_letterhead.as_deref(), 500));
        let ring = v.capture(Ring::parse(&doc.release.ring));
        v.capture(MaintenanceWindow::new(
            doc.release.maintenance_day,
            doc.release.maintenance_start_hour_utc,
            doc.release.maintenance_duration_min,
        ));
        v.into_result()?;
        if actor.is_tenant_admin() && ring.is_some_and(|r| r.as_str() != current.release.ring) {
            return Err(AppError::forbidden("Release ring is assigned by the platform operator"));
        }
        Ok(BaselineDocument {
            format_version: FORMAT_VERSION,
            source_tenant_code: doc.source_tenant_code.clone(),
            config,
            feature_flags: doc.feature_flags.clone(),
            branding: BrandingBaseline {
                primary_color: p.map(|c| c.as_str().to_string()).unwrap_or_default(),
                secondary_color: s.map(|c| c.as_str().to_string()).unwrap_or_default(),
                email_footer: footer.flatten(),
                login_message: login.flatten(),
                pdf_letterhead: pdf.flatten(),
            },
            release: doc.release.clone(),
        })
    }

    pub async fn preview_import(&self, actor: &Actor, id: TenantId, raw: &str) -> AppResult<ImportPreview> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let incoming = BaselineDocument::parse(raw)?;
        let validated = self.validate(actor, &scope, id, &incoming).await?;
        let current = self.current(&scope, id).await?;
        Ok(ImportPreview { diff: diff(&current, &validated), incoming: validated })
    }

    /// Applies a baseline after snapshotting the current state (for rollback). Requires a change
    /// record reference (GOV-002) — free text in the prototype.
    pub async fn import(&self, actor: &Actor, id: TenantId, raw: &str, change_record: &str, source: &str) -> AppResult<StoredBaseline> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        self.deps.load_tenant(&scope, id).await?.ensure_config_writable()?;
        let change_record = change_record.trim();
        if change_record.len() < 3 || change_record.len() > 200 {
            return Err(AppError::validation("change_record", "A change record reference is required (3-200 characters)"));
        }
        let incoming = BaselineDocument::parse(raw)?;
        let validated = self.validate(actor, &scope, id, &incoming).await?;
        let current = self.current(&scope, id).await?;
        let d = diff(&current, &validated);
        let snap_audit = actor.audit(Some(id), "tenant_baseline", None, "tenant.baseline_snapshot");
        self.deps
            .baselines
            .insert(
                &scope,
                id,
                &format!("Before import ({change_record})"),
                "pre_import_snapshot",
                &current,
                Some(change_record),
                actor.user_id,
                ChangeSet::new().with_audit(snap_audit),
            )
            .await?;
        let mut a = actor.audit(Some(id), "tenant_baseline", None, "tenant.baseline_imported");
        a.reason = Some(change_record.to_string());
        a.after = Some(json!({ "changes": d.len(), "diff": d }));
        let stored = self
            .deps
            .baselines
            .insert(
                &scope,
                id,
                &format!("Imported ({change_record})"),
                source,
                &validated,
                Some(change_record),
                actor.user_id,
                ChangeSet::new(),
            )
            .await?;
        let changes = ChangeSet::new()
            .with_audit(a)
            .with_event(actor.event(id, &TenantEvent::BaselineImported { baseline_id: stored.id, changes: d.len() }));
        self.deps.baselines.apply(&scope, id, &validated, actor.user_id, changes).await?;
        let channels = validated.feature_flags.iter().filter(|(k, v)| **v && features::is_channel(k)).count() as i64;
        self.deps.quotas.set_static_usage(&scope, id, super::super::domain::quota::QuotaMetric::Channels, channels).await?;
        Ok(stored)
    }

    /// Rollback to a stored baseline (UJ-15 E2).
    pub async fn rollback(&self, actor: &Actor, id: TenantId, baseline: Uuid, change_record: &str) -> AppResult<StoredBaseline> {
        let b = self.get(actor, id, baseline).await?;
        let raw = serde_json::to_string(&b.content).map_err(AppError::internal)?;
        self.import(actor, id, &raw, change_record, "imported").await
    }

    /// Sandbox → production promotion: preview (diff) or apply.
    pub async fn promotion_preview(&self, actor: &Actor, sandbox: TenantId) -> AppResult<(TenantId, ImportPreview)> {
        let (prod, raw) = self.promotion_source(actor, sandbox).await?;
        Ok((prod, self.preview_import(actor, prod, &raw).await?))
    }

    pub async fn promote(&self, actor: &Actor, sandbox: TenantId, change_record: &str) -> AppResult<StoredBaseline> {
        let (prod, raw) = self.promotion_source(actor, sandbox).await?;
        self.import(actor, prod, &raw, change_record, "promotion").await
    }

    async fn promotion_source(&self, actor: &Actor, sandbox: TenantId) -> AppResult<(TenantId, String)> {
        let scope = actor.platform_scope().or_else(|_| {
            // A Tenant Admin of the production tenant may promote from its sandbox.
            actor.tenant_id.map(|t| AccessScope::Tenant(t.0)).ok_or_else(|| AppError::forbidden("No tenant context"))
        })?;
        let sbx = match self.deps.tenants.get(&AccessScope::Platform, sandbox).await? {
            Some(t) if t.is_sandbox => t,
            _ => return Err(AppError::not_found("Sandbox does not exist")),
        };
        let prod = sbx.sandbox_of_tenant_id.ok_or_else(|| AppError::not_found("Sandbox is not linked"))?;
        // Tenant Admins may promote only into their own production tenant.
        self.deps.authorize(actor, prod, Access::Write, false).await?;
        let _ = scope;
        let doc = self.current(&AccessScope::Platform, sandbox).await?;
        Ok((prod, serde_json::to_string(&doc).map_err(AppError::internal)?))
    }
}
