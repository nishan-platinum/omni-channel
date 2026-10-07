//! Break-glass support access (OCC-M01-R024; M18-R009 AT "access without approval denied and
//! logged"; SEC-121/144; ADR-0009).

use std::sync::Arc;

use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult};

use super::super::domain::events::TenantEvent;
use super::super::domain::support::{validate_request, GrantRequest, GrantState, GrantStatus, SupportGrant};
use super::super::domain::TenantId;
use super::context::{Access, Actor, M01Deps};
use super::ports::{CanaryRow, ChangeSet, Notification, TenantUser};

#[derive(Debug, Clone, Serialize)]
pub struct SupportView {
    pub grant: SupportGrant,
    pub rows: Vec<CanaryRow>,
}

pub struct SupportAccessService {
    deps: Arc<M01Deps>,
}

impl SupportAccessService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps }
    }

    async fn tier(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<super::super::domain::storage::Tier> {
        let t = self.deps.load_tenant(scope, tenant).await?;
        let plan = self.deps.plans.get(t.plan_id).await?.ok_or_else(|| AppError::not_found("Plan does not exist"))?;
        Ok(plan.tier)
    }

    pub async fn approvers(&self, actor: &Actor, tenant: TenantId) -> AppResult<Vec<TenantUser>> {
        self.deps.authorize(actor, tenant, Access::Read, false).await?;
        Ok(self.deps.identity.tenant_admins(tenant).await?.into_iter().filter(|u| u.status == "active").collect())
    }

    pub async fn request(&self, actor: &Actor, tenant: TenantId, req: GrantRequest) -> AppResult<SupportGrant> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let tier = self.tier(&scope, tenant).await?;
        let (reason, incident, duration) = validate_request(&req, tier)?;
        let admins = self.deps.identity.tenant_admins(tenant).await?;
        if let Some(named) = req.named_approver_id {
            if !admins.iter().any(|u| u.id == named && u.status == "active") {
                return Err(AppError::validation("named_approver_id", "Named approver must be an active Tenant Admin of this tenant"));
            }
        }
        let now = self.deps.clock.now();
        let grant = SupportGrant {
            id: Uuid::now_v7(),
            tenant_id: tenant,
            requested_by: actor.user_id.unwrap_or_else(Uuid::nil),
            reason,
            incident_ref: incident,
            duration_minutes: duration,
            named_approver_id: req.named_approver_id,
            status: GrantStatus::Requested,
            decided_by: None,
            decided_at: None,
            decision_note: None,
            starts_at: None,
            expires_at: None,
            revoked_at: None,
            use_count: 0,
            created_at: now,
        };
        let mut a = actor.audit(Some(tenant), "support_grant", Some(grant.id.to_string()), "support_access.requested");
        a.security_event = true;
        a.reason = Some(grant.reason.clone());
        a.after =
            Some(json!({ "incident_ref": grant.incident_ref, "duration_minutes": duration, "named_approver_id": grant.named_approver_id }));
        let changes =
            ChangeSet::new().with_audit(a).with_event(actor.event(tenant, &TenantEvent::SupportAccessRequested { grant_id: grant.id }));
        self.deps.grants.insert(&scope, &grant, changes).await.map_err(|e| {
            if e.code == crate::platform::errors::ErrorCode::Conflict {
                AppError::conflict("A grant for this incident is already pending or active")
            } else {
                e
            }
        })?;
        for admin in admins.iter().filter(|u| u.status == "active" && grant.named_approver_id.is_none_or(|n| n == u.id)) {
            self.deps
                .notifications
                .send(Notification {
                    tenant_id: Some(tenant),
                    notification_id: "NT-M01-SUPPORT".into(),
                    template_key: "NT-SUPPORT-REQUEST".into(),
                    recipient: admin.email.clone(),
                    channels: "Email + In-app".into(),
                    priority: "High".into(),
                    subject: "Platform support requests time-boxed access to your tenant".into(),
                    body: format!(
                        "Reason: {}. Incident: {}. Duration: {} minutes. Approve or reject it under Support access.",
                        grant.reason,
                        grant.incident_ref.as_deref().unwrap_or("-"),
                        grant.duration_minutes
                    ),
                })
                .await?;
        }
        Ok(grant)
    }

    pub async fn list(&self, actor: &Actor, tenant: TenantId) -> AppResult<Vec<SupportGrant>> {
        let scope = self.deps.authorize(actor, tenant, Access::Read, false).await?;
        self.deps.grants.list(&scope, tenant).await
    }

    async fn load_for_tenant_admin(&self, actor: &Actor, grant_id: Uuid) -> AppResult<(AccessScope, SupportGrant)> {
        let own = actor.tenant_id.ok_or_else(|| AppError::forbidden("Tenant Admin role required"))?;
        if !actor.is_tenant_admin() {
            return Err(AppError::forbidden("Tenant Admin role required"));
        }
        let scope = AccessScope::Tenant(own.0);
        // RLS: a grant of another tenant is simply invisible → 404.
        let g = self.deps.grants.get(&scope, grant_id).await?.ok_or_else(|| AppError::not_found("Grant does not exist"))?;
        Ok((scope, g))
    }

    /// Tenant Admin approval (tenant-approved, time-boxed). Starts the window.
    pub async fn approve(&self, actor: &Actor, grant_id: Uuid, note: Option<&str>) -> AppResult<SupportGrant> {
        let (scope, mut g) = self.load_for_tenant_admin(actor, grant_id).await?;
        let approver = actor.user_id.ok_or_else(|| AppError::forbidden("Approver identity required"))?;
        let now = self.deps.clock.now();
        let (starts, expires) = g.approve(approver, now)?;
        g.status = GrantStatus::Approved;
        g.decided_by = Some(approver);
        g.decided_at = Some(now);
        g.decision_note = note.map(|n| n.chars().take(500).collect());
        g.starts_at = Some(starts);
        g.expires_at = Some(expires);
        let mut a = actor.audit(Some(g.tenant_id), "support_grant", Some(g.id.to_string()), "support_access.approved");
        a.security_event = true;
        a.after = Some(json!({ "starts_at": starts, "expires_at": expires }));
        let changes = ChangeSet::new()
            .with_audit(a)
            .with_event(actor.event(g.tenant_id, &TenantEvent::SupportAccessGranted { grant_id: g.id, expires_at: expires.to_rfc3339() }));
        self.deps.grants.decide(&scope, &g, changes).await?;
        Ok(g)
    }

    pub async fn reject(&self, actor: &Actor, grant_id: Uuid, note: Option<&str>) -> AppResult<SupportGrant> {
        let (scope, mut g) = self.load_for_tenant_admin(actor, grant_id).await?;
        g.reject()?;
        let now = self.deps.clock.now();
        g.status = GrantStatus::Rejected;
        g.decided_by = actor.user_id;
        g.decided_at = Some(now);
        g.decision_note = note.map(|n| n.chars().take(500).collect());
        let mut a = actor.audit(Some(g.tenant_id), "support_grant", Some(g.id.to_string()), "support_access.rejected");
        a.security_event = true;
        self.deps.grants.decide(&scope, &g, ChangeSet::new().with_audit(a)).await?;
        Ok(g)
    }

    /// Revocation by the tenant (own grants) or the Super Admin.
    pub async fn revoke(&self, actor: &Actor, tenant: TenantId, grant_id: Uuid) -> AppResult<SupportGrant> {
        let scope = self.deps.authorize(actor, tenant, Access::Write, false).await?;
        let mut g = self
            .deps
            .grants
            .get(&scope, grant_id)
            .await?
            .filter(|g| g.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("Grant does not exist"))?;
        let now = self.deps.clock.now();
        g.revoke(now)?;
        g.status = GrantStatus::Revoked;
        g.revoked_at = Some(now);
        let mut a = actor.audit(Some(tenant), "support_grant", Some(g.id.to_string()), "support_access.revoked");
        a.security_event = true;
        self.deps.grants.decide(&scope, &g, ChangeSet::new().with_audit(a)).await?;
        Ok(g)
    }

    /// Finds an active grant or records a denied attempt (security event) and returns 403.
    pub async fn require_active_grant(&self, actor: &Actor, tenant: TenantId, purpose: &str) -> AppResult<SupportGrant> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let now = self.deps.clock.now();
        let active = self.deps.grants.list(&scope, tenant).await?.into_iter().find(|g| g.state_at(now) == GrantState::Active);
        match active {
            Some(g) => {
                let mut a = actor.audit(Some(tenant), "support_grant", Some(g.id.to_string()), "support_access.used");
                a.security_event = true;
                a.after = Some(json!({ "purpose": purpose }));
                let changes =
                    ChangeSet::new().with_audit(a).with_event(actor.event(tenant, &TenantEvent::SupportAccessUsed { grant_id: g.id }));
                self.deps.grants.record_use(&scope, g.id, now, changes).await?;
                Ok(g)
            }
            None => {
                let mut a = actor.audit(Some(tenant), "tenant", Some(tenant.to_string()), "support_access.denied");
                a.security_event = true;
                a.after = Some(json!({ "purpose": purpose }));
                self.deps.audit.record(&scope, vec![a]).await?;
                Err(AppError::forbidden("Support access requires an active, tenant-approved grant"))
            }
        }
    }

    /// The support view: tenant data-plane rows, readable only under an active grant. Opens a
    /// tenant-scoped data context explicitly for the duration of the read.
    pub async fn open_support_view(&self, actor: &Actor, tenant: TenantId) -> AppResult<SupportView> {
        let grant = self.require_active_grant(actor, tenant, "support_view").await?;
        let scope = actor.platform_scope()?;
        let profile = self.deps.connections.get(&scope, tenant).await?.ok_or_else(|| AppError::conflict("Tenant has no data store"))?;
        let store = self.deps.data_router.store_for(&profile).await?;
        let rows = store.list_rows(tenant).await?;
        Ok(SupportView { grant, rows })
    }
}
