//! Tenant provisioning (M01-F01; OCC-M01-R001, R008, R013; UJ-19 steps 1–2; Part D §58).
//!
//! Saga: (1) validate → (2) one central-DB transaction creating tenant, config, flags, quotas,
//! branding, release preferences, connection profile, data key, provisioning run, audit and the
//! `tenant.created` outbox event → (3) post-commit steps, each recorded in
//! `tenantadm.provisioning_steps`: initial Tenant Admin + invitation, data-store provisioning,
//! template packs, isolation smoke test. A failed step leaves the tenant in `draft` with
//! `provisioning_status = failed`; the Super Admin can retry (idempotent steps) or discard the draft
//! (compensating delete — "rollback provisioning").

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult};

use super::super::domain::branding::parse_email;
use super::super::domain::config::{self as cfgdom, ConfigActor};
use super::super::domain::errors::Violations;
use super::super::domain::events::TenantEvent;
use super::super::domain::features::{self, is_channel};
use super::super::domain::hierarchy::validate_parent;
use super::super::domain::keys::{rotate_after, KeyKind, KeyState};
use super::super::domain::plan::Plan;
use super::super::domain::quota::{cycle_start, QuotaMetric, QuotaState, ALL_METRICS};
use super::super::domain::storage::{Region, StorageStrategy};
use super::super::domain::tenant::{IsolationCheckStatus, ProvisioningStatus, Tenant, TenantStatus};
use super::super::domain::{TenantCode, TenantId};
use super::context::{Actor, M01Deps};
use super::isolation::IsolationService;
use super::ports::*;

#[derive(Debug, Clone, Default)]
pub struct CreateTenantCommand {
    pub name: String,
    pub legal_name: Option<String>,
    pub region: Option<String>,
    pub plan_id: Option<String>,
    pub primary_admin_email: String,
    pub parent_tenant_id: Option<String>,
    /// Optional explicit code; otherwise suggested from the name.
    pub tenant_code: Option<String>,
    pub template_code: Option<String>,
    /// Dedicated database target (Regulated tier only).
    pub db_target: Option<String>,
    /// FD-009 reseller inheritance options (shown when a parent is set).
    pub inherit_branding: bool,
    pub inherit_config: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProvisioningOutcome {
    pub tenant: Tenant,
    pub run_id: Uuid,
    pub completed: bool,
    pub duration_ms: i64,
    pub notes: Vec<String>,
}

/// Internal options used by the sandbox service.
#[derive(Debug, Clone, Default)]
pub(crate) struct SandboxLink {
    pub production: Option<TenantId>,
    pub config: Option<BTreeMap<String, Value>>,
    pub flags: Option<BTreeMap<String, bool>>,
    pub target: Option<String>,
}

pub struct ProvisioningService {
    deps: Arc<M01Deps>,
    isolation: Arc<IsolationService>,
}

fn opt_trim(s: &Option<String>) -> Option<String> {
    s.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string)
}

impl ProvisioningService {
    pub fn new(deps: Arc<M01Deps>, isolation: Arc<IsolationService>) -> Self {
        Self { deps, isolation }
    }

    pub async fn create(&self, actor: &Actor, cmd: CreateTenantCommand) -> AppResult<ProvisioningOutcome> {
        actor.require_super_admin()?;
        self.create_internal(actor, cmd, SandboxLink::default()).await
    }

    pub(crate) async fn create_internal(
        &self,
        actor: &Actor,
        cmd: CreateTenantCommand,
        sandbox: SandboxLink,
    ) -> AppResult<ProvisioningOutcome> {
        let scope = actor.platform_scope()?;
        let started = Instant::now();
        let now = self.deps.clock.now();

        // ---- 1. Validation (all field errors at once, STD-001) ----
        let mut v = Violations::default();
        let name = cmd.name.trim().to_string();
        if name.is_empty() || name.chars().count() > 120 {
            v.push("name", "Name is required (max 120 chars)");
        }
        let legal_name = opt_trim(&cmd.legal_name);
        if legal_name.as_ref().is_some_and(|l| l.chars().count() > 200) {
            v.push("legal_name", "Legal name must be at most 200 characters");
        }
        let region = v.capture(Region::parse(cmd.region.as_deref().unwrap_or("my-central")));
        let email = v.capture(parse_email("primary_admin_email", &cmd.primary_admin_email, "Valid admin email required"));
        let explicit_code = match opt_trim(&cmd.tenant_code) {
            Some(c) => v.capture(TenantCode::parse(&c)),
            None => None,
        };
        let plan_uuid = match opt_trim(&cmd.plan_id) {
            Some(p) => match Uuid::parse_str(&p) {
                Ok(u) => Some(u),
                Err(_) => {
                    v.push("plan_id", "Plan does not exist");
                    None
                }
            },
            None => {
                v.push("plan_id", "Plan is required");
                None
            }
        };
        let parent_id = match opt_trim(&cmd.parent_tenant_id) {
            Some(p) => match Uuid::parse_str(&p) {
                Ok(u) => Some(TenantId(u)),
                Err(_) => {
                    v.push("parent_tenant_id", "Invalid parent tenant id");
                    None
                }
            },
            None => None,
        };
        v.into_result()?;
        let (region, email) = (region.unwrap_or_default(), email.unwrap_or_default());

        let plan = match plan_uuid {
            Some(id) => self.deps.plans.get(id).await?,
            None => None,
        };
        let plan = match plan {
            Some(p) if p.active => p,
            _ => return Err(AppError::not_found("Plan does not exist")),
        };

        let template = match opt_trim(&cmd.template_code) {
            Some(code) => Some(self.deps.templates.get(&code).await?.ok_or_else(|| AppError::not_found("Template does not exist"))?),
            None => None,
        };

        let id = TenantId::new();
        if let Some(parent) = parent_id {
            let p = self.deps.tenants.get(&scope, parent).await?.ok_or_else(|| AppError::not_found("Parent tenant does not exist"))?;
            if p.is_sandbox || matches!(p.status, TenantStatus::Terminated | TenantStatus::Purged) {
                return Err(AppError::conflict("Parent tenant cannot have sub-accounts"));
            }
            let mut ancestry = vec![parent];
            ancestry.extend(self.deps.tenants.ancestry(&scope, parent).await?);
            validate_parent(id, &ancestry, 1)?;
        }

        // Tenant code: explicit codes must be free (BR-M01-001); suggestions get a numeric suffix.
        let code = match explicit_code {
            Some(c) => {
                if self.deps.tenants.code_exists(c.as_str()).await? {
                    return Err(AppError::conflict("Tenant code already in use"));
                }
                c
            }
            None => self.free_code(TenantCode::suggest_from_name(&name)).await?,
        };

        // Storage strategy from tier (R008); dedicated targets must match the region (residency).
        let strategy = plan.tier.storage_strategy();
        let target = sandbox.target.clone().or_else(|| opt_trim(&cmd.db_target));
        if strategy == StorageStrategy::DedicatedDatabase && target.is_none() {
            return Err(AppError::validation("db_target", "Regulated tenants need a dedicated database target in their region"));
        }
        let connection = self.deps.data_router.plan_profile(id, strategy, region, target.as_deref())?;

        // ---- seed configuration, entitlements, quotas from plan + template (FD-008) ----
        let mut notes = Vec::new();
        let mut config = cfgdom::defaults();
        if let Some(t) = &template {
            let validated = cfgdom::validate_changes(&t.config_defaults, &config, ConfigActor::SuperAdmin, plan.tier)?;
            config.extend(validated);
        }
        if let Some(c) = &sandbox.config {
            config.extend(c.clone());
        }
        let flags = match &sandbox.flags {
            Some(f) => f.clone(),
            None => seed_flags(&plan, template.as_ref().map(|t| &t.features), &config, &mut notes),
        };
        let quotas = seed_quotas(&plan, &flags, now);

        let mut inheritance = serde_json::Map::new();
        if parent_id.is_some() {
            inheritance.insert("branding".into(), Value::Bool(cmd.inherit_branding));
            inheritance.insert("config".into(), Value::Bool(cmd.inherit_config));
        }

        let tenant = Tenant {
            id,
            code,
            name,
            legal_name,
            region,
            plan_id: plan.id,
            status: TenantStatus::Draft,
            parent_tenant_id: parent_id,
            primary_admin_email: email.clone(),
            storage_strategy: strategy,
            isolation_mode: strategy.isolation_mode(),
            is_sandbox: sandbox.production.is_some(),
            sandbox_of_tenant_id: sandbox.production,
            template_code: template.as_ref().map(|t| t.code.clone()),
            inheritance_flags: Value::Object(inheritance),
            provisioning_status: ProvisioningStatus::InProgress,
            isolation_check_status: IsolationCheckStatus::NotRun,
            isolation_checked_at: None,
            suspended_reason: None,
            status_reason: None,
            activated_at: None,
            grace_until: None,
            terminated_at: None,
            purge_after: None,
            purged_at: None,
            legal_hold: false,
            platform_version: "1.0.0".into(),
            created_at: now,
            updated_at: now,
            version: 1,
        };

        // Per-tenant data key (R010 envelope encryption).
        let key_ref = self.deps.kms.platform_key_ref(id);
        let dk = self.deps.kms.generate_data_key(&key_ref).await?;
        let key = NewKey {
            id: Uuid::now_v7(),
            kind: KeyKind::PlatformManaged,
            key_ref,
            key_version: 1,
            state: KeyState::Active,
            wrapped_dek: Some(dk.wrapped),
            rotate_after: rotate_after(now),
        };

        let run_id = Uuid::now_v7();
        let event = TenantEvent::Created {
            tenant_code: tenant.code.to_string(),
            region: region.as_str().into(),
            plan_id: plan.id,
            storage_strategy: strategy.as_str().into(),
            template: tenant.template_code.clone(),
        };
        let changes = ChangeSet::new()
            .with_audit(actor.tenant_audit(
                id,
                "tenant.created",
                None,
                Some(json!({
                    "tenant_code": tenant.code, "name": tenant.name, "region": region.as_str(),
                    "plan": plan.code, "storage_strategy": strategy.as_str(),
                    "template": tenant.template_code, "parent_tenant_id": parent_id,
                    "sandbox_of": sandbox.production, "db_target": target
                })),
            ))
            .with_event(actor.event(id, &event));
        let bundle = NewTenantBundle { tenant: tenant.clone(), config, flags, quotas, connection, key, run_id, created_by: actor.user_id };
        self.deps.tenants.insert_provisioned(&scope, &bundle, changes).await?;

        // ---- 3. post-commit saga steps ----
        let ok = self
            .run_steps(actor, &scope, &tenant, run_id, template.as_ref().map(|t| t.packs.clone()).unwrap_or_default(), &mut notes)
            .await;
        let duration_ms = started.elapsed().as_millis() as i64;
        self.deps
            .provisioning
            .finish_run(&scope, run_id, id, ok, duration_ms, if ok { None } else { Some("One or more provisioning steps failed") })
            .await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        Ok(ProvisioningOutcome { tenant, run_id, completed: ok, duration_ms, notes })
    }

    async fn free_code(&self, base: TenantCode) -> AppResult<TenantCode> {
        if !self.deps.tenants.code_exists(base.as_str()).await? {
            return Ok(base);
        }
        for n in 2..1000 {
            let c = base.with_suffix(&n.to_string());
            if !self.deps.tenants.code_exists(c.as_str()).await? {
                return Ok(c);
            }
        }
        Err(AppError::conflict("Tenant code already in use"))
    }

    /// Executes the idempotent post-commit steps; returns true when all succeeded.
    async fn run_steps(
        &self,
        actor: &Actor,
        scope: &AccessScope,
        tenant: &Tenant,
        run_id: Uuid,
        packs: Vec<String>,
        notes: &mut Vec<String>,
    ) -> bool {
        let d = &self.deps;
        let mut all_ok = true;

        // Step: initial Tenant Admin + activation invite (F01 step 4; M02 + M25 ports).
        let t0 = d.clock.now();
        let step = async {
            let (_uid, token) = d.identity.invite_tenant_admin(tenant.id, &tenant.primary_admin_email, "Tenant Administrator").await?;
            if let Some(token) = token {
                let link = format!("{}/invitations/accept?token={}", d.settings.public_base_url.trim_end_matches('/'), token);
                d.notifications
                    .send(Notification {
                        tenant_id: Some(tenant.id),
                        notification_id: "NT-M01-INVITE".into(),
                        template_key: "NT-TENANT-INVITE".into(),
                        recipient: tenant.primary_admin_email.clone(),
                        channels: "Email".into(),
                        priority: "High".into(),
                        subject: format!("You have been invited to administer {}", tenant.name),
                        body: format!(
                            "An organisation account \"{}\" (code {}) was created for you. Set your password: {}\n\
                             You can sign in once the platform operator activates the tenant.",
                            tenant.name, tenant.code, link
                        ),
                    })
                    .await?;
            }
            let users = d.identity.count_users(tenant.id).await?;
            d.quotas.set_static_usage(scope, tenant.id, QuotaMetric::Users, users).await?;
            Ok::<String, AppError>(format!("invited {}", tenant.primary_admin_email))
        }
        .await;
        all_ok &= self.record(scope, run_id, tenant.id, "identity.initial_admin", step, t0).await;

        // Step: tenant data store (shared rows / schema / dedicated database).
        let t0 = d.clock.now();
        let step = async {
            let profile = d
                .connections
                .get(scope, tenant.id)
                .await?
                .ok_or_else(|| AppError::internal(anyhow::anyhow!("connection profile missing")))?;
            let detail = d.data_router.provision(&profile).await?;
            d.connections.set_status(scope, tenant.id, "ready").await?;
            Ok::<String, AppError>(detail)
        }
        .await;
        let store_ok = self.record(scope, run_id, tenant.id, "data_store.provision", step, t0).await;
        all_ok &= store_ok;

        // Step: template packs handed to downstream module ports (R013).
        for pack in packs {
            let t0 = d.clock.now();
            let step = d.downstream.apply_pack(tenant.id, &pack).await;
            all_ok &= self.record(scope, run_id, tenant.id, &format!("template.{pack}"), step, t0).await;
        }

        // Step: isolation smoke test (R009; UJ-19 E1 — gates activation).
        if store_ok {
            let t0 = d.clock.now();
            let step = match self.isolation.run(actor, tenant.id).await {
                Ok(r) if r.passed => Ok("all isolation checks passed".to_string()),
                Ok(r) => Err(AppError::conflict(format!("isolation smoke test failed: {}", r.failures().join(", ")))),
                Err(e) => Err(e),
            };
            all_ok &= self.record(scope, run_id, tenant.id, "isolation.smoke_test", step, t0).await;
        } else {
            notes.push("isolation smoke test skipped because the data store is not ready".into());
            all_ok = false;
        }

        let status = if all_ok { ProvisioningStatus::Completed } else { ProvisioningStatus::Failed };
        if let Err(e) = d.tenants.set_provisioning_status(scope, tenant.id, status).await {
            e.log();
            all_ok = false;
        }
        all_ok
    }

    async fn record(
        &self,
        scope: &AccessScope,
        run: Uuid,
        tenant: TenantId,
        step: &str,
        result: AppResult<String>,
        started: chrono::DateTime<Utc>,
    ) -> bool {
        let (status, detail, ok) = match result {
            Ok(d) => ("completed", d, true),
            Err(e) => {
                e.log();
                ("failed", e.message.clone(), false)
            }
        };
        if let Err(e) = self.deps.provisioning.add_step(scope, run, tenant, step, status, Some(&detail), started).await {
            e.log();
        }
        ok
    }

    /// Re-runs the post-commit steps for a draft whose provisioning failed.
    pub async fn retry(&self, actor: &Actor, id: TenantId) -> AppResult<ProvisioningOutcome> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        if tenant.status != TenantStatus::Draft || tenant.provisioning_status == ProvisioningStatus::Completed {
            return Err(AppError::conflict("Only drafts with failed provisioning can be retried"));
        }
        let started = Instant::now();
        self.deps.tenants.set_provisioning_status(&scope, id, ProvisioningStatus::InProgress).await?;
        let run_id = self.deps.provisioning.start_run(&scope, id, tenant.template_code.as_deref(), actor.user_id).await?;
        let packs = match &tenant.template_code {
            Some(code) => self.deps.templates.get(code).await?.map(|t| t.packs).unwrap_or_default(),
            None => Vec::new(),
        };
        let mut notes = Vec::new();
        let ok = self.run_steps(actor, &scope, &tenant, run_id, packs, &mut notes).await;
        let duration_ms = started.elapsed().as_millis() as i64;
        self.deps.provisioning.finish_run(&scope, run_id, id, ok, duration_ms, if ok { None } else { Some("retry failed") }).await?;
        self.deps
            .audit
            .record(&scope, vec![actor.tenant_audit(id, "tenant.provisioning_retried", None, Some(json!({ "ok": ok })))])
            .await?;
        Ok(ProvisioningOutcome { tenant: self.deps.load_tenant(&scope, id).await?, run_id, completed: ok, duration_ms, notes })
    }

    /// Compensating rollback for a never-activated draft (Part D §58 "Rollback provisioning").
    pub async fn discard_draft(&self, actor: &Actor, id: TenantId) -> AppResult<()> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        if tenant.status != TenantStatus::Draft || tenant.activated_at.is_some() {
            return Err(AppError::conflict("Only never-activated drafts can be discarded"));
        }
        if !self.deps.tenants.sandboxes_of(&scope, id).await?.is_empty() || !self.deps.tenants.children(&scope, id).await?.is_empty() {
            return Err(AppError::conflict("Draft has linked tenants"));
        }
        if let Some(profile) = self.deps.connections.get(&scope, id).await? {
            self.deps.data_router.decommission(&profile).await?;
        }
        self.deps.objects.delete_prefix(&format!("tenants/{id}/")).await?;
        self.deps.identity.purge_tenant(id).await?;
        let changes = ChangeSet::new().with_audit(actor.tenant_audit(
            id,
            "tenant.draft_discarded",
            Some(json!({ "tenant_code": tenant.code, "status": "draft" })),
            None,
        ));
        self.deps.tenants.discard_draft(&scope, id, changes).await
    }
}

/// Seeds feature flags: template features ∩ plan entitlements (or all entitlements without a
/// template), skipping features whose prerequisites are unmet and channels beyond the quota.
fn seed_flags(
    plan: &Plan,
    template: Option<&BTreeSet<String>>,
    config: &BTreeMap<String, Value>,
    notes: &mut Vec<String>,
) -> BTreeMap<String, bool> {
    let mut flags = BTreeMap::new();
    let channel_limit = plan.quota(QuotaMetric::Channels);
    let mut channels = 0;
    for def in features::FEATURES {
        let wanted = match template {
            Some(t) => t.contains(def.key),
            None => plan.entitles(def.key),
        };
        let mut enabled = false;
        if wanted {
            match features::validate_feature_change(def.key, true, &plan.entitlements, config) {
                Ok(()) if is_channel(def.key) && channels >= channel_limit => {
                    notes.push(format!("{} not enabled: channel quota reached", def.key));
                }
                Ok(()) => {
                    enabled = true;
                    if is_channel(def.key) {
                        channels += 1;
                    }
                }
                Err(e) => notes.push(format!("{} not enabled: {}", def.key, describe(&e))),
            }
        }
        if plan.entitles(def.key) || enabled {
            flags.insert(def.key.to_string(), enabled);
        }
    }
    flags
}

fn describe(e: &super::super::domain::DomainError) -> String {
    match e {
        super::super::domain::DomainError::Validation(v) => v.iter().map(|x| x.message.clone()).collect::<Vec<_>>().join("; "),
        other => other.to_string(),
    }
}

fn seed_quotas(plan: &Plan, flags: &BTreeMap<String, bool>, now: chrono::DateTime<Utc>) -> Vec<QuotaState> {
    let channels = flags.iter().filter(|(k, v)| **v && is_channel(k)).count() as i64;
    ALL_METRICS
        .iter()
        .map(|m| QuotaState {
            metric: *m,
            limit: plan.quota(*m),
            usage: if *m == QuotaMetric::Channels { channels } else { 0 },
            soft_threshold: plan.soft_threshold,
            cycle_start: cycle_start(m.period(), now),
            warned_cycle_start: None,
            exhausted_cycle_start: None,
        })
        .collect()
}
