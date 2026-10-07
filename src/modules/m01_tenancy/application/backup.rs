//! Per-tenant backup/restore and RPO/RTO reporting (OCC-M01-R011; NFR-007/008). REFERENCE
//! implementation: snapshots the tenant's M01 configuration (encrypted with the tenant key) into
//! object storage and restores it for that tenant only. Full database PITR is platform
//! infrastructure (pending) and is not simulated here.

use std::sync::Arc;

use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::platform::errors::{AppError, AppResult};
use crate::platform::security::sha256_hex;

use super::super::domain::baseline::BaselineDocument;
use super::super::domain::TenantId;
use super::baselines::BaselineService;
use super::context::{Access, Actor, M01Deps};
use super::offboarding::{decrypt_for_tenant, encrypt_for_tenant};
use super::ports::{BackupRecord, ChangeSet, DrReportRow};

/// NFR-007 targets.
pub const RPO_TARGET_MINUTES: i64 = 15;
pub const RTO_TARGET_MINUTES: i64 = 240;

#[derive(Debug, Clone, Serialize)]
pub struct DrRow {
    pub row: DrReportRow,
    pub rpo_minutes: Option<i64>,
    pub rpo_met: bool,
    pub rto_met: Option<bool>,
}

pub struct BackupService {
    deps: Arc<M01Deps>,
    baselines: Arc<BaselineService>,
}

impl BackupService {
    pub fn new(deps: Arc<M01Deps>, baselines: Arc<BaselineService>) -> Self {
        Self { deps, baselines }
    }

    pub async fn backup(&self, actor: &Actor, id: TenantId) -> AppResult<BackupRecord> {
        actor.require_super_admin()?;
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let doc = self.baselines.current(&scope, id).await?;
        let plain = serde_json::to_vec(&doc).map_err(AppError::internal)?;
        let (blob, key_version) = encrypt_for_tenant(&self.deps, &scope, id, &plain).await?;
        let bid = Uuid::now_v7();
        let key = format!("tenants/{id}/backups/{bid}.json.enc");
        self.deps.objects.put(&key, &blob).await?;
        let rec = BackupRecord {
            id: bid,
            tenant_id: id,
            object_key: key,
            sha256: sha256_hex(&blob),
            size_bytes: blob.len() as i64,
            key_version,
            created_at: self.deps.clock.now(),
        };
        let a = actor.tenant_audit(id, "tenant.backup_created", None, Some(json!({ "backup_id": bid, "sha256": rec.sha256 })));
        self.deps.exports.insert_backup(&scope, &rec, actor.user_id, ChangeSet::new().with_audit(a)).await?;
        Ok(rec)
    }

    pub async fn list(&self, actor: &Actor, id: TenantId) -> AppResult<Vec<BackupRecord>> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        self.deps.exports.backups(&scope, id).await
    }

    /// Restores one tenant's M01 configuration from a backup; other tenants are untouched.
    pub async fn restore(&self, actor: &Actor, id: TenantId, backup: Uuid) -> AppResult<i64> {
        actor.require_super_admin()?;
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let started = self.deps.clock.now();
        let t0 = std::time::Instant::now();
        let rec = self
            .deps
            .exports
            .backups(&scope, id)
            .await?
            .into_iter()
            .find(|b| b.id == backup)
            .ok_or_else(|| AppError::not_found("Backup does not exist"))?;
        let blob = self.deps.objects.get(&rec.object_key).await?.ok_or_else(|| AppError::not_found("Backup object missing"))?;
        if sha256_hex(&blob) != rec.sha256 {
            return Err(AppError::conflict("Backup integrity check failed"));
        }
        let plain = decrypt_for_tenant(&self.deps, &scope, id, &blob).await?;
        let doc: BaselineDocument = serde_json::from_slice(&plain).map_err(AppError::internal)?;
        let raw = serde_json::to_string(&doc).map_err(AppError::internal)?;
        self.baselines.import(actor, id, &raw, &format!("restore {backup}"), "imported").await?;
        let ms = t0.elapsed().as_millis() as i64;
        let completed = started + chrono::Duration::milliseconds(ms);
        let a = actor.tenant_audit(id, "tenant.restored", None, Some(json!({ "backup_id": backup, "duration_ms": ms })));
        self.deps.exports.record_restore(&scope, id, backup, started, completed, actor.user_id, ChangeSet::new().with_audit(a)).await?;
        Ok(ms)
    }

    pub async fn dr_report(&self, actor: &Actor) -> AppResult<Vec<DrRow>> {
        let scope = actor.platform_scope()?;
        let now = self.deps.clock.now();
        Ok(self
            .deps
            .exports
            .dr_report(&scope)
            .await?
            .into_iter()
            .map(|row| {
                let rpo = row.last_backup_at.map(|b| (now - b).num_minutes());
                DrRow {
                    rpo_minutes: rpo,
                    rpo_met: rpo.is_some_and(|m| m <= RPO_TARGET_MINUTES),
                    rto_met: row.last_restore_ms.map(|ms| ms <= RTO_TARGET_MINUTES * 60_000),
                    row,
                }
            })
            .collect())
    }
}
