//! Per-tenant encryption keys, rotation without downtime, BYOK for Regulated tenants
//! (OCC-M01-R010, P2; SEC-111/112). Old key versions stay available (retired) for decryption.

use std::sync::Arc;

use serde_json::json;
use uuid::Uuid;

use crate::platform::errors::{AppError, AppResult};

use super::super::domain::events::TenantEvent;
use super::super::domain::keys::{rotate_after, validate_byok, KeyKind, KeyState};
use super::super::domain::TenantId;
use super::context::{Access, Actor, M01Deps};
use super::ports::{ChangeSet, NewKey, TenantKey};

pub struct KeyService {
    deps: Arc<M01Deps>,
}

impl KeyService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps }
    }

    pub async fn list(&self, actor: &Actor, id: TenantId) -> AppResult<Vec<TenantKey>> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        self.deps.keys.list(&scope, id).await
    }

    /// Rotates the tenant data key (new DEK under the same key reference).
    pub async fn rotate(&self, actor: &Actor, id: TenantId) -> AppResult<TenantKey> {
        actor.require_super_admin()?;
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let keys = self.deps.keys.list(&scope, id).await?;
        let active = keys.iter().find(|k| k.state == KeyState::Active).ok_or_else(|| AppError::conflict("No active key to rotate"))?;
        self.install(actor, id, active.kind, active.key_ref.clone(), keys.iter().map(|k| k.key_version).max().unwrap_or(0) + 1).await
    }

    /// Registers a customer-supplied key reference (Regulated only) and makes it active.
    pub async fn register_byok(&self, actor: &Actor, id: TenantId, key_ref: &str) -> AppResult<TenantKey> {
        actor.require_super_admin()?;
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let t = self.deps.load_tenant(&scope, id).await?;
        let plan = self.deps.plans.get(t.plan_id).await?.ok_or_else(|| AppError::not_found("Plan does not exist"))?;
        let key_ref = validate_byok(plan.tier, key_ref)?;
        let check = self.deps.kms.validate_customer_key(&key_ref).await?;
        if !check.verified {
            return Err(AppError::conflict(format!("Customer key could not be validated: {}", check.message)));
        }
        let next = self.deps.keys.list(&scope, id).await?.iter().map(|k| k.key_version).max().unwrap_or(0) + 1;
        self.install(actor, id, KeyKind::CustomerSupplied, key_ref, next).await
    }

    async fn install(&self, actor: &Actor, id: TenantId, kind: KeyKind, key_ref: String, version: i32) -> AppResult<TenantKey> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let now = self.deps.clock.now();
        let dk = self.deps.kms.generate_data_key(&key_ref).await?;
        let key = NewKey {
            id: Uuid::now_v7(),
            kind,
            key_ref: key_ref.clone(),
            key_version: version,
            state: KeyState::Active,
            wrapped_dek: Some(dk.wrapped),
            rotate_after: rotate_after(now),
        };
        let mut a = actor.audit(Some(id), "tenant_key", Some(key.id.to_string()), "tenant.key_rotated");
        a.after = Some(json!({ "key_version": version, "kind": kind.as_str(), "key_ref": key_ref }));
        let changes = ChangeSet::new().with_audit(a).with_event(actor.event(id, &TenantEvent::KeyRotated { key_version: version }));
        self.deps.keys.rotate(&scope, id, &key, actor.user_id, changes).await?;
        self.deps.keys.by_version(&scope, id, version).await?.ok_or_else(|| AppError::internal(anyhow::anyhow!("rotated key not found")))
    }
}
