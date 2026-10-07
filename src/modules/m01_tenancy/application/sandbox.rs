//! Sandbox tenants (OCC-M01-R017, P2): linked sandbox with configuration copy and an anonymised
//! data boundary (no production PII is copied; M01 owns no business rows).

use std::sync::Arc;

use serde::Serialize;
use serde_json::json;

use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult};

use super::super::domain::sandbox::{ensure_can_create, sandbox_code};
use super::super::domain::TenantId;
use super::context::{Access, Actor, M01Deps};
use super::lifecycle::LifecycleService;
use super::ports::TenantSummary;
use super::provisioning::{CreateTenantCommand, ProvisioningOutcome, ProvisioningService, SandboxLink};

#[derive(Debug, Clone, Serialize)]
pub struct SandboxCreated {
    pub outcome: ProvisioningOutcome,
    pub data_copy: String,
    pub activated: bool,
}

pub struct SandboxService {
    deps: Arc<M01Deps>,
    provisioning: Arc<ProvisioningService>,
    lifecycle: Arc<LifecycleService>,
}

impl SandboxService {
    pub fn new(deps: Arc<M01Deps>, provisioning: Arc<ProvisioningService>, lifecycle: Arc<LifecycleService>) -> Self {
        Self { deps, provisioning, lifecycle }
    }

    pub async fn list(&self, actor: &Actor, production: TenantId) -> AppResult<Vec<TenantSummary>> {
        let scope = self.deps.authorize(actor, production, Access::Read, false).await?;
        // Sandboxes are separate tenants; list through the platform view of the link only.
        let _ = scope;
        self.deps.tenants.sandboxes_of(&AccessScope::Platform, production).await
    }

    /// Creates a linked sandbox (SA, or the TA of the production tenant — self-service).
    pub async fn create(&self, actor: &Actor, production: TenantId) -> AppResult<SandboxCreated> {
        let scope = self.deps.authorize(actor, production, Access::Write, false).await?;
        let prod = self.deps.load_tenant(&scope, production).await?;
        let existing = self.deps.tenants.sandboxes_of(&AccessScope::Platform, production).await?;
        ensure_can_create(&prod, existing.len())?;

        let mut ordinal = existing.len() + 1;
        let code = loop {
            let c = sandbox_code(&prod.code, ordinal);
            if !self.deps.tenants.code_exists(c.as_str()).await? {
                break c;
            }
            ordinal += 1;
            if ordinal > 50 {
                return Err(AppError::conflict("Could not allocate a sandbox code"));
            }
        };
        let settings = self.deps.configs.load(&scope, production).await?;
        let target = self.deps.connections.get(&scope, production).await?.map(|c| c.target_name);
        let host = Actor::system("sandbox-provisioning");
        let host = Actor { user_id: actor.user_id, correlation_id: actor.correlation_id.clone(), ..host };
        let cmd = CreateTenantCommand {
            name: format!("{} (sandbox {ordinal})", prod.name).chars().take(120).collect(),
            legal_name: prod.legal_name.clone(),
            region: Some(prod.region.as_str().into()),
            plan_id: Some(prod.plan_id.to_string()),
            primary_admin_email: prod.primary_admin_email.clone(),
            tenant_code: Some(code.to_string()),
            ..Default::default()
        };
        let link = SandboxLink {
            production: Some(production),
            config: Some(settings.config),
            flags: Some(settings.flags),
            target: target.filter(|t| t != "shared" && t != "central-schema"),
        };
        let outcome = self.provisioning.create_internal(&host, cmd, link).await?;
        let sandbox_id = outcome.tenant.id;
        let data_copy = self.deps.anonymiser.copy_subset(production, sandbox_id).await?;

        // Sandbox goes live automatically once provisioning (incl. isolation test) succeeded.
        let mut activated = false;
        if outcome.completed {
            self.lifecycle
                .change_status(&host, sandbox_id, "active", Some("Sandbox auto-activation after successful provisioning"))
                .await?;
            activated = true;
        }
        self.deps
            .audit
            .record(
                &AccessScope::Platform,
                vec![actor.tenant_audit(
                    production,
                    "tenant.sandbox_created",
                    None,
                    Some(json!({ "sandbox_id": sandbox_id, "code": code, "data_copy": data_copy })),
                )],
            )
            .await?;
        Ok(SandboxCreated { outcome, data_copy, activated })
    }
}
