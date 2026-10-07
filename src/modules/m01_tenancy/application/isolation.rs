//! Isolation smoke test (OCC-M01-R009; UJ-19 E1). Probes the tenant's data store and the control
//! plane with a foreign tenant context; any visible/insertable cross-tenant row is a failure that
//! is recorded, audited as a security event and alerted.

use std::sync::Arc;

use serde::Serialize;
use serde_json::json;

use crate::platform::errors::AppResult;

use super::super::domain::events::TenantEvent;
use super::super::domain::TenantId;
use super::context::{Access, Actor, M01Deps};
use super::ports::{ChangeSet, ProbeResult};

#[derive(Debug, Clone, Serialize)]
pub struct IsolationReport {
    pub tenant_id: TenantId,
    pub passed: bool,
    pub results: Vec<ProbeResult>,
}

impl IsolationReport {
    pub fn failures(&self) -> Vec<String> {
        self.results.iter().filter(|r| !r.passed).map(|r| r.check.clone()).collect()
    }
}

pub struct IsolationService {
    deps: Arc<M01Deps>,
}

impl IsolationService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps }
    }

    pub async fn run(&self, actor: &Actor, tenant: TenantId) -> AppResult<IsolationReport> {
        actor.require_super_admin()?;
        let scope = self.deps.authorize(actor, tenant, Access::Write, false).await?;
        let profile = self
            .deps
            .connections
            .get(&scope, tenant)
            .await?
            .ok_or_else(|| crate::platform::errors::AppError::conflict("Tenant has no data store profile"))?;

        let mut results = Vec::new();
        // A random foreign tenant id: nothing of `tenant` may be visible or writable under it.
        let foreign = TenantId::new();
        match self.deps.data_router.store_for(&profile).await {
            Ok(store) => {
                let marker = format!("canary:{}", tenant);
                match store.write_canary(tenant, &marker).await {
                    Ok(()) => results.push(ProbeResult {
                        check: "canary.write_own_scope".into(),
                        passed: true,
                        detail: "canary written under own tenant context".into(),
                    }),
                    Err(e) => results.push(ProbeResult { check: "canary.write_own_scope".into(), passed: false, detail: e.message }),
                }
                match store.isolation_probe(tenant, foreign).await {
                    Ok(mut r) => results.append(&mut r),
                    Err(e) => results.push(ProbeResult { check: "data_store.probe".into(), passed: false, detail: e.message }),
                }
            }
            Err(e) => results.push(ProbeResult { check: "data_store.connect".into(), passed: false, detail: e.message }),
        }
        let cp = self.deps.tenants.control_plane_probe(tenant, foreign).await?;
        results.push(ProbeResult {
            check: "control_plane.rls_foreign_scope".into(),
            passed: cp,
            detail: if cp {
                "control-plane rows invisible to a foreign tenant scope".into()
            } else {
                "control-plane rows visible across tenants".into()
            },
        });

        let passed = results.iter().all(|r| r.passed);
        let report = IsolationReport { tenant_id: tenant, passed, results };
        let mut audit = actor.tenant_audit(
            tenant,
            if passed { "isolation.check_passed" } else { "isolation.check_failed" },
            None,
            Some(json!(report.results)),
        );
        audit.security_event = !passed;
        let mut changes = ChangeSet::new().with_audit(audit);
        if !passed {
            changes.push_event(actor.event(tenant, &TenantEvent::IsolationCheckFailed { failures: report.failures() }));
        }
        self.deps.tenants.record_isolation_check(&scope, tenant, passed, json!(report.results), actor.user_id, changes).await?;
        Ok(report)
    }
}
