//! Tenant lifecycle orchestration (M01-F03; OCC-M01-R003, R014; BR-M01-002; NT-001).
//! Legality of transitions is decided only by `Tenant::plan_transition` (domain).

use std::sync::Arc;

use serde_json::json;

use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult};

use super::super::domain::events::TenantEvent;
use super::super::domain::tenant::{TenantStatus, TransitionEffect};
use super::super::domain::{Tenant, TenantId};
use super::context::{Access, Actor, M01Deps};
use super::offboarding::OffboardingService;
use super::ports::ChangeSet;

pub struct LifecycleService {
    deps: Arc<M01Deps>,
    offboarding: Arc<OffboardingService>,
}

impl LifecycleService {
    pub fn new(deps: Arc<M01Deps>, offboarding: Arc<OffboardingService>) -> Self {
        Self { deps, offboarding }
    }

    /// `PATCH /v1/tenants/{id}/status`. Only the Super Admin (or a system/billing service actor)
    /// may change status; Tenant Admins are refused.
    pub async fn change_status(&self, actor: &Actor, id: TenantId, to: &str, reason: Option<&str>) -> AppResult<Tenant> {
        let to = TenantStatus::parse(to)?;
        if actor.is_tenant_admin() {
            // Own-tenant check first so cross-tenant attempts are audited as such.
            self.deps.authorize(actor, id, Access::Write, false).await?;
            return Err(AppError::forbidden("Only the platform operator can change tenant status"));
        }
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        let now = self.deps.clock.now();
        let plan = tenant.plan_transition(to, reason, now, self.deps.settings.lifecycle)?;

        if plan.effects.contains(&TransitionEffect::PurgeData) {
            return self.offboarding.purge(actor, &tenant, &plan).await;
        }

        let mut audit = actor.tenant_audit(
            id,
            "tenant.status_changed",
            Some(json!({ "status": plan.from.as_str() })),
            Some(json!({ "status": plan.to.as_str(), "grace_until": plan.grace_until, "purge_after": plan.purge_after })),
        );
        audit.reason = plan.reason.clone();
        let event = TenantEvent::StatusChanged {
            event_type: plan.event_type,
            from: plan.from.as_str().into(),
            to: plan.to.as_str().into(),
            reason: plan.reason.clone(),
        };
        let changes = ChangeSet::new().with_audit(audit).with_event(actor.event(id, &event));
        let updated = self.deps.tenants.apply_transition(&scope, id, tenant.version, &plan, changes).await?;

        // Side effects after commit.
        for effect in &plan.effects {
            match effect {
                TransitionEffect::BlockNewSessions => {
                    let policy = self
                        .deps
                        .configs
                        .load(&scope, id)
                        .await?
                        .config
                        .get("lifecycle.suspend_session_policy")
                        .and_then(|v| v.as_str().map(str::to_string));
                    if policy.as_deref() != Some("drain") {
                        self.deps.identity.revoke_sessions(id).await?;
                    }
                }
                TransitionEffect::RevokeAllSessions => {
                    self.deps.identity.revoke_sessions(id).await?;
                }
                TransitionEffect::GenerateExport => {
                    let reason = if plan.to == TenantStatus::Grace { "grace" } else { "terminated" };
                    if let Err(e) = self.offboarding.generate_export(actor, id, reason).await {
                        // The transition stands; the export can be regenerated from the UI.
                        e.log();
                    }
                }
                _ => {}
            }
        }
        Ok(updated)
    }

    /// Scheduler: grace expiry → terminated; retention elapsed → purged (when auto-purge is on).
    pub async fn run_scheduler_tick(&self) -> AppResult<(usize, usize)> {
        self.run_scheduler_tick_for(None).await
    }

    /// Scheduler tick restricted to the given tenants (operations "process now"; tests).
    pub async fn run_scheduler_tick_for(&self, only: Option<&[TenantId]>) -> AppResult<(usize, usize)> {
        let actor = Actor::system("lifecycle-scheduler");
        let now = self.deps.clock.now();
        let (mut expired_grace, mut purge_due) = self.deps.tenants.due_lifecycle(now).await?;
        if let Some(ids) = only {
            expired_grace.retain(|t| ids.contains(t));
            purge_due.retain(|t| ids.contains(t));
        }
        let mut terminated = 0;
        for id in expired_grace {
            match self.change_status(&actor, id, "terminated", Some("Grace period expired")).await {
                Ok(_) => terminated += 1,
                Err(e) => e.log(),
            }
        }
        let mut purged = 0;
        if self.deps.settings.auto_purge {
            for id in purge_due {
                match self.change_status(&actor, id, "purged", Some("Retention window elapsed")).await {
                    Ok(_) => purged += 1,
                    Err(e) => e.log(),
                }
            }
        }
        Ok((terminated, purged))
    }

    /// Legal hold (Ch 72): overrides purge.
    pub async fn set_legal_hold(&self, actor: &Actor, id: TenantId, hold: bool, reason: Option<&str>) -> AppResult<()> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        if tenant.status == TenantStatus::Purged {
            return Err(AppError::conflict("Tenant has been purged"));
        }
        let mut a = actor.tenant_audit(
            id,
            "tenant.legal_hold_changed",
            Some(json!({ "legal_hold": tenant.legal_hold })),
            Some(json!({ "legal_hold": hold })),
        );
        a.reason = reason.map(str::to_string);
        self.deps.tenants.set_legal_hold(&scope, id, hold, ChangeSet::new().with_audit(a)).await
    }

    /// Update organisation name / legal name (TA within own tenant; SA any) and, for SA only, the
    /// reseller parent (cycle/depth validated).
    pub async fn update_details(
        &self,
        actor: &Actor,
        id: TenantId,
        name: &str,
        legal_name: Option<&str>,
        parent: Option<Option<TenantId>>,
    ) -> AppResult<Tenant> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        tenant.ensure_config_writable()?;
        let mut v = super::super::domain::errors::Violations::default();
        let name = name.trim();
        if name.is_empty() || name.chars().count() > 120 {
            v.push("name", "Name is required (max 120 chars)");
        }
        let legal = legal_name.map(str::trim).filter(|s| !s.is_empty());
        if legal.is_some_and(|l| l.chars().count() > 200) {
            v.push("legal_name", "Legal name must be at most 200 characters");
        }
        v.into_result()?;
        if parent.is_some() && !actor.is_super_admin() {
            return Err(AppError::forbidden("Only the platform operator can change the parent tenant"));
        }
        if let Some(Some(p)) = parent {
            let ps = AccessScope::Platform;
            self.deps.tenants.get(&ps, p).await?.ok_or_else(|| AppError::not_found("Parent tenant does not exist"))?;
            let mut ancestry = vec![p];
            ancestry.extend(self.deps.tenants.ancestry(&ps, p).await?);
            let height = self.deps.tenants.subtree_height(&ps, id).await?;
            super::super::domain::hierarchy::validate_parent(id, &ancestry, height)?;
        }
        let changes = ChangeSet::new().with_audit(actor.tenant_audit(
            id,
            "tenant.details_updated",
            Some(json!({ "name": tenant.name, "legal_name": tenant.legal_name, "parent_tenant_id": tenant.parent_tenant_id })),
            Some(json!({ "name": name, "legal_name": legal, "parent_tenant_id": parent.flatten() })),
        ));
        self.deps.tenants.update_details(&scope, id, tenant.version, name, legal, parent, changes).await
    }
}
