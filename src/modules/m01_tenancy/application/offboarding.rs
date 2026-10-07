//! Offboarding: encrypted export, certified purge with destruction certificate, export download
//! (OCC-M01-R016; Ch 72 lifecycle; ADR-0010).

use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult};
use crate::platform::security::{random_bytes, sha256_hex};

use super::super::domain::events::TenantEvent;
use super::super::domain::keys::KeyState;
use super::super::domain::tenant::{TenantStatus, TransitionPlan};
use super::super::domain::{Tenant, TenantId};
use super::context::{Access, Actor, M01Deps};
use super::ports::{ChangeSet, DestructionCertificate, ExportRecord, TenantKey};
use super::support::SupportAccessService;

const ENVELOPE_MAGIC: &[u8; 4] = b"OME1";

/// Encrypts with the tenant's active data key: `OME1 | key_version(be u32) | nonce(12) | ciphertext`.
pub(crate) async fn encrypt_for_tenant(
    deps: &M01Deps,
    scope: &AccessScope,
    tenant: TenantId,
    plaintext: &[u8],
) -> AppResult<(Vec<u8>, i32)> {
    let key = active_key(deps, scope, tenant).await?;
    let wrapped = key.wrapped_dek.as_deref().ok_or_else(|| AppError::conflict("Tenant data key is not available"))?;
    let dek = deps.kms.unwrap_data_key(&key.key_ref, wrapped).await?;
    let cipher = Aes256Gcm::new_from_slice(&dek).map_err(|e| AppError::internal(anyhow::anyhow!("{e}")))?;
    let nonce_bytes: [u8; 12] = random_bytes();
    let ct =
        cipher.encrypt(Nonce::from_slice(&nonce_bytes), plaintext).map_err(|_| AppError::internal(anyhow::anyhow!("encryption failed")))?;
    let mut out = Vec::with_capacity(ct.len() + 20);
    out.extend_from_slice(ENVELOPE_MAGIC);
    out.extend_from_slice(&(key.key_version as u32).to_be_bytes());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ct);
    Ok((out, key.key_version))
}

pub(crate) async fn decrypt_for_tenant(deps: &M01Deps, scope: &AccessScope, tenant: TenantId, blob: &[u8]) -> AppResult<Vec<u8>> {
    if blob.len() < 20 || &blob[..4] != ENVELOPE_MAGIC {
        return Err(AppError::conflict("Unrecognised encrypted object"));
    }
    let version = u32::from_be_bytes([blob[4], blob[5], blob[6], blob[7]]) as i32;
    let key = deps.keys.by_version(scope, tenant, version).await?.ok_or_else(|| AppError::conflict("Data key version not found"))?;
    let wrapped = key
        .wrapped_dek
        .as_deref()
        .filter(|_| key.state != KeyState::Destroyed)
        .ok_or_else(|| AppError::conflict("Data key has been destroyed (crypto-shredded)"))?;
    let dek = deps.kms.unwrap_data_key(&key.key_ref, wrapped).await?;
    let cipher = Aes256Gcm::new_from_slice(&dek).map_err(|e| AppError::internal(anyhow::anyhow!("{e}")))?;
    cipher.decrypt(Nonce::from_slice(&blob[8..20]), &blob[20..]).map_err(|_| AppError::conflict("Decryption failed (integrity check)"))
}

async fn active_key(deps: &M01Deps, scope: &AccessScope, tenant: TenantId) -> AppResult<TenantKey> {
    deps.keys
        .list(scope, tenant)
        .await?
        .into_iter()
        .find(|k| k.state == KeyState::Active)
        .ok_or_else(|| AppError::conflict("Tenant has no active data key"))
}

pub struct OffboardingService {
    deps: Arc<M01Deps>,
    support: Arc<SupportAccessService>,
}

impl OffboardingService {
    pub fn new(deps: Arc<M01Deps>, support: Arc<SupportAccessService>) -> Self {
        Self { deps, support }
    }

    /// Full M01 export: control-plane snapshot + data-plane rows + object manifest + participants.
    pub async fn generate_export(&self, actor: &Actor, tenant: TenantId, reason: &str) -> AppResult<ExportRecord> {
        let scope = self.deps.authorize(actor, tenant, Access::Write, false).await?;
        let t = self.deps.load_tenant(&scope, tenant).await?;
        if t.status == TenantStatus::Purged {
            return Err(AppError::conflict("Tenant has been purged"));
        }
        let reason = match reason {
            "grace" | "terminated" | "manual" => reason,
            _ => "manual",
        };
        let control = self.deps.exports.snapshot(&scope, tenant).await?;
        let data_plane = match self.deps.connections.get(&scope, tenant).await? {
            Some(profile) => match self.deps.data_router.store_for(&profile).await {
                Ok(store) => serde_json::to_value(store.list_rows(tenant).await?).unwrap_or(Value::Null),
                Err(e) => json!({ "error": e.message }),
            },
            None => Value::Null,
        };
        let objects = self.deps.objects.list_prefix(&format!("tenants/{tenant}/")).await?;
        let mut participants = serde_json::Map::new();
        for p in &self.deps.export_participants {
            participants.insert(p.module().into(), p.export(tenant).await?);
        }
        let now = self.deps.clock.now();
        let doc = json!({
            "format": "omni-m01-export/1",
            "generated_at": now,
            "tenant_id": tenant,
            "tenant_code": t.code,
            "reason": reason,
            "control_plane": control,
            "data_plane": { "isolation_canaries": data_plane },
            "object_manifest": objects.iter().map(|(k, s)| json!({ "key": k, "size": s })).collect::<Vec<_>>(),
            "module_exports": participants,
            "note": "Records, files and attachments of CRM modules M02-M40 are exported by their own ExportParticipant implementations (none registered in the M01 prototype)."
        });
        let plaintext = serde_json::to_vec_pretty(&doc).map_err(AppError::internal)?;
        let (blob, key_version) = encrypt_for_tenant(&self.deps, &scope, tenant, &plaintext).await?;
        let id = Uuid::now_v7();
        let object_key = format!("tenants/{tenant}/exports/{id}.json.enc");
        self.deps.objects.put(&object_key, &blob).await?;
        let rec = ExportRecord {
            id,
            tenant_id: tenant,
            reason: reason.into(),
            object_key,
            sha256: sha256_hex(&blob),
            size_bytes: blob.len() as i64,
            key_version,
            created_at: now,
        };
        let changes = ChangeSet::new()
            .with_audit(actor.tenant_audit(
                tenant,
                "tenant.export_generated",
                None,
                Some(json!({ "export_id": id, "reason": reason, "sha256": rec.sha256 })),
            ))
            .with_event(actor.event(tenant, &TenantEvent::ExportGenerated { export_id: id }));
        self.deps.exports.insert_export(&scope, &rec, actor.user_id, changes).await?;
        Ok(rec)
    }

    pub async fn list_exports(&self, actor: &Actor, tenant: TenantId) -> AppResult<Vec<ExportRecord>> {
        let scope = self.deps.authorize(actor, tenant, Access::Read, false).await?;
        self.deps.exports.exports(&scope, tenant).await
    }

    /// Decrypted download. Tenant Admin: own tenant. Super Admin: only with an active break-glass
    /// grant (the export contains tenant data).
    pub async fn download_export(&self, actor: &Actor, tenant: TenantId, export_id: Uuid) -> AppResult<(String, Vec<u8>)> {
        let scope = self.deps.authorize(actor, tenant, Access::Read, false).await?;
        if actor.is_super_admin() {
            self.support.require_active_grant(actor, tenant, "export.download").await?;
        }
        let rec = self
            .deps
            .exports
            .export(&scope, export_id)
            .await?
            .filter(|r| r.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("Export does not exist"))?;
        let blob = self.deps.objects.get(&rec.object_key).await?.ok_or_else(|| AppError::not_found("Export object missing"))?;
        if sha256_hex(&blob) != rec.sha256 {
            return Err(AppError::conflict("Export integrity check failed"));
        }
        let plain = decrypt_for_tenant(&self.deps, &scope, tenant, &blob).await?;
        self.deps
            .audit
            .record(&scope, vec![actor.tenant_audit(tenant, "tenant.export_downloaded", None, Some(json!({ "export_id": export_id })))])
            .await?;
        Ok((format!("tenant-export-{export_id}.json"), plain))
    }

    /// Irreversible purge (terminated → purged). Data plane and object store first; then the
    /// control-plane purge, tombstone, certificate, audit and event in one transaction.
    pub(crate) async fn purge(&self, actor: &Actor, tenant: &Tenant, plan: &TransitionPlan) -> AppResult<Tenant> {
        let scope = actor.platform_scope()?;
        let id = tenant.id;
        let mut manifest = serde_json::Map::new();
        if let Some(profile) = self.deps.connections.get(&scope, id).await? {
            let rows = match self.deps.data_router.store_for(&profile).await {
                Ok(store) => store.purge_rows(id).await?,
                Err(e) => return Err(e),
            };
            manifest.insert("data_plane_rows_deleted".into(), json!(rows));
            let detail = self.deps.data_router.decommission(&profile).await?;
            manifest.insert("data_store".into(), json!(detail));
            manifest.insert("storage_strategy".into(), json!(profile.storage_strategy.as_str()));
            manifest.insert("engine".into(), json!(profile.engine.as_str()));
        }
        let objects = self.deps.objects.delete_prefix(&format!("tenants/{id}/")).await?;
        manifest.insert("objects_deleted".into(), json!(objects));
        let identities = self.deps.identity.purge_tenant(id).await?;
        manifest.insert("identities_deleted".into(), json!(identities));
        manifest.insert("primary_admin_email_notified".into(), json!(tenant.primary_admin_email));

        let mut audit =
            actor.tenant_audit(id, "tenant.purged", Some(json!({ "status": "terminated" })), Some(json!({ "status": "purged" })));
        audit.reason = plan.reason.clone();
        let event = TenantEvent::StatusChanged {
            event_type: plan.event_type,
            from: plan.from.as_str().into(),
            to: plan.to.as_str().into(),
            reason: plan.reason.clone(),
        };
        let mut ev = actor.event(id, &event);
        // NT-019 needs a recipient after identities are gone.
        ev.payload["data"]["notify"] = json!(tenant.primary_admin_email);
        let changes = ChangeSet::new().with_audit(audit).with_event(ev);
        let _cert: DestructionCertificate =
            self.deps.purge.purge(&scope, id, plan, Value::Object(manifest), actor.user_id, changes).await?;
        self.deps.load_tenant(&scope, id).await
    }

    pub async fn certificate(&self, actor: &Actor, tenant: TenantId) -> AppResult<Option<DestructionCertificate>> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        self.deps.exports.certificate(&scope, tenant).await
    }
}
