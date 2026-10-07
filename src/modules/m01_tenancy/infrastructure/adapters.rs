//! REFERENCE ADAPTERS for ports owned by other modules or by platform infrastructure.
//! None of these is a production integration; each says so in its doc comment and outputs.

use std::path::{Component, Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use async_trait::async_trait;
use hkdf::Hkdf;
use sha2::Sha256;
use sqlx::PgPool;
use uuid::Uuid;

use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};
use crate::platform::security::random_bytes;

use super::super::application::ports::*;
use super::super::domain::TenantId;

// ------------------------------------------------------------------------------------------------
// M25 notifications — REFERENCE: persisted to shared.notification_outbox + structured log.
// ------------------------------------------------------------------------------------------------

pub struct OutboxNotificationAdapter {
    pool: PgPool,
    /// Bodies (which may contain invitation links) are stored only in development, where the
    /// `/dev/outbox` page stands in for an email inbox.
    store_bodies: bool,
}

impl OutboxNotificationAdapter {
    pub fn new(pool: PgPool, store_bodies: bool) -> Self {
        Self { pool, store_bodies }
    }
}

#[async_trait]
impl NotificationPort for OutboxNotificationAdapter {
    async fn send(&self, n: Notification) -> AppResult<()> {
        let scope = match n.tenant_id {
            Some(t) => AccessScope::Tenant(t.0),
            None => AccessScope::System,
        };
        let mut tx = scoped_tx(&self.pool, &scope).await?;
        sqlx::query(
            "INSERT INTO shared.notification_outbox (id, tenant_id, notification_id, template_key, recipient, channels, priority, subject, body)
             VALUES ($1,$2,$3,$4,$5::citext,$6,$7,$8,$9)",
        )
        .bind(Uuid::now_v7())
        .bind(n.tenant_id.map(|t| t.0))
        .bind(&n.notification_id)
        .bind(&n.template_key)
        .bind(&n.recipient)
        .bind(&n.channels)
        .bind(&n.priority)
        .bind(&n.subject)
        .bind(if self.store_bodies { Some(n.body.as_str()) } else { None })
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        // Never log the body: it can contain invitation tokens.
        tracing::info!(notification_id = %n.notification_id, template = %n.template_key, tenant_id = ?n.tenant_id.map(|t| t.0), "REFERENCE M25: notification queued to local outbox");
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct OutboxEntry {
    pub id: Uuid,
    pub tenant_id: Option<Uuid>,
    pub notification_id: String,
    pub template_key: String,
    pub recipient: String,
    pub priority: String,
    pub subject: String,
    pub body: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Development outbox listing (served only when APP_ENV=development, Super Admin only).
pub async fn list_outbox(pool: &PgPool, limit: i64) -> AppResult<Vec<OutboxEntry>> {
    let mut tx = scoped_tx(pool, &AccessScope::System).await?;
    let rows = sqlx::query_as::<_, OutboxEntry>(
        "SELECT id, tenant_id, notification_id, template_key, recipient::text AS recipient, priority, subject, body, created_at
           FROM shared.notification_outbox ORDER BY created_at DESC LIMIT $1",
    )
    .bind(limit.clamp(1, 500))
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

// ------------------------------------------------------------------------------------------------
// Downstream template packs (M23/M02/M09/M15/M24/M31) — REFERENCE: records only.
// ------------------------------------------------------------------------------------------------

pub struct ReferenceDownstreamProvisioning;

#[async_trait]
impl DownstreamProvisioningPort for ReferenceDownstreamProvisioning {
    async fn apply_pack(&self, tenant: TenantId, pack: &str) -> AppResult<String> {
        let (kind, name) = pack.split_once(':').unwrap_or((pack, "default"));
        let owner = match kind {
            "roles_teams" => "M02",
            "bpm" => "M09",
            "reports" => "M24",
            "sla" => "M15",
            "dropdowns" => "M31",
            _ => "M23",
        };
        tracing::info!(tenant_id = %tenant, pack, owner, "REFERENCE downstream provisioning: pack recorded (module not implemented)");
        Ok(format!("REFERENCE: {kind} pack '{name}' handed to {owner} (not implemented in M01 prototype)"))
    }
}

// ------------------------------------------------------------------------------------------------
// DNS / SPF-DKIM verification (M06 + DNS/TLS infra) — SIMULATED.
// ------------------------------------------------------------------------------------------------

pub struct SimulatedDnsVerifier {
    pub verified_suffixes: Vec<String>,
}

impl SimulatedDnsVerifier {
    fn passes(&self, domain: &str) -> bool {
        let d = domain.to_ascii_lowercase();
        self.verified_suffixes.iter().any(|s| d.ends_with(s.as_str()))
    }
}

#[async_trait]
impl DomainVerificationPort for SimulatedDnsVerifier {
    async fn verify(&self, domain: &str, expected_cname: &str, _token: &str) -> AppResult<VerificationResult> {
        let ok = self.passes(domain);
        Ok(VerificationResult {
            verified: ok,
            simulated: true,
            message: if ok {
                format!("SIMULATED: CNAME {domain} -> {expected_cname} and TXT token found; managed TLS would be issued (not implemented)")
            } else {
                format!(
                    "SIMULATED: CNAME/TXT records not found for {domain} (simulator verifies domains ending with {})",
                    self.verified_suffixes.join(", ")
                )
            },
        })
    }
}

#[async_trait]
impl EmailSenderVerificationPort for SimulatedDnsVerifier {
    async fn verify(&self, domain: &str, selector: &str) -> AppResult<VerificationResult> {
        let ok = self.passes(domain);
        Ok(VerificationResult {
            verified: ok,
            simulated: true,
            message: if ok {
                format!("SIMULATED: SPF include and DKIM selector {selector} found for {domain}")
            } else {
                format!("SIMULATED: SPF/DKIM not found for {domain}")
            },
        })
    }
}

// ------------------------------------------------------------------------------------------------
// KMS — REFERENCE local envelope encryption. The master key comes from the environment and is
// never stored in the database or source. KEKs are derived per key reference with HKDF.
// ------------------------------------------------------------------------------------------------

pub struct LocalKms {
    master: [u8; 32],
}

impl LocalKms {
    pub fn new(master: &[u8]) -> AppResult<Self> {
        let mut m = [0u8; 32];
        if master.len() != 32 {
            return Err(AppError::internal(anyhow::anyhow!("master key must be 32 bytes")));
        }
        m.copy_from_slice(master);
        Ok(Self { master: m })
    }

    fn kek(&self, key_ref: &str) -> AppResult<Aes256Gcm> {
        let hk = Hkdf::<Sha256>::new(Some(b"omni-m01-local-kms"), &self.master);
        let mut okm = [0u8; 32];
        hk.expand(key_ref.as_bytes(), &mut okm).map_err(|_| AppError::internal(anyhow::anyhow!("hkdf expand failed")))?;
        Aes256Gcm::new_from_slice(&okm).map_err(|e| AppError::internal(anyhow::anyhow!("{e}")))
    }
}

#[async_trait]
impl KeyManagementPort for LocalKms {
    fn platform_key_ref(&self, tenant: TenantId) -> String {
        format!("local-kms://tenants/{tenant}/kek")
    }

    async fn generate_data_key(&self, key_ref: &str) -> AppResult<DataKey> {
        let dek: [u8; 32] = random_bytes();
        let nonce: [u8; 12] = random_bytes();
        let ct = self
            .kek(key_ref)?
            .encrypt(Nonce::from_slice(&nonce), dek.as_slice())
            .map_err(|_| AppError::internal(anyhow::anyhow!("key wrap failed")))?;
        let mut wrapped = nonce.to_vec();
        wrapped.extend_from_slice(&ct);
        Ok(DataKey { plaintext: dek, wrapped })
    }

    async fn unwrap_data_key(&self, key_ref: &str, wrapped: &[u8]) -> AppResult<[u8; 32]> {
        if wrapped.len() < 12 + 32 {
            return Err(AppError::conflict("Wrapped key is malformed"));
        }
        let pt = self
            .kek(key_ref)?
            .decrypt(Nonce::from_slice(&wrapped[..12]), &wrapped[12..])
            .map_err(|_| AppError::conflict("Data key could not be unwrapped"))?;
        let mut k = [0u8; 32];
        k.copy_from_slice(&pt);
        Ok(k)
    }

    async fn validate_customer_key(&self, key_ref: &str) -> AppResult<VerificationResult> {
        // A real BYOK flow would call the customer's KMS. The reference adapter accepts any
        // well-formed reference and says so.
        Ok(VerificationResult {
            verified: true,
            simulated: true,
            message: format!("SIMULATED: customer key {key_ref} accepted by the local reference KMS (no external KMS call)"),
        })
    }
}

// ------------------------------------------------------------------------------------------------
// Object storage — REFERENCE local filesystem with tenant path separation (FR-ARC-004).
// ------------------------------------------------------------------------------------------------

pub struct LocalObjectStorage {
    root: PathBuf,
}

impl LocalObjectStorage {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Rejects absolute paths and `..` so keys can never escape the storage root.
    fn path(&self, key: &str) -> AppResult<PathBuf> {
        let p = Path::new(key);
        if key.is_empty() || p.is_absolute() || p.components().any(|c| !matches!(c, Component::Normal(_))) {
            return Err(AppError::internal(anyhow::anyhow!("invalid object key")));
        }
        Ok(self.root.join(p))
    }
}

#[async_trait]
impl ObjectStoragePort for LocalObjectStorage {
    async fn put(&self, key: &str, bytes: &[u8]) -> AppResult<()> {
        let path = self.path(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(AppError::internal)?;
        }
        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, bytes).await.map_err(AppError::internal)?;
        tokio::fs::rename(&tmp, &path).await.map_err(AppError::internal)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> AppResult<Option<Vec<u8>>> {
        match tokio::fs::read(self.path(key)?).await {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(AppError::internal(e)),
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> AppResult<u64> {
        let dir = self.path(prefix.trim_end_matches('/'))?;
        let listed = self.list_prefix(prefix).await?;
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(listed.len() as u64),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(AppError::internal(e)),
        }
    }

    async fn list_prefix(&self, prefix: &str) -> AppResult<Vec<(String, u64)>> {
        let dir = self.path(prefix.trim_end_matches('/'))?;
        let mut out = Vec::new();
        let mut stack = vec![dir];
        while let Some(d) = stack.pop() {
            let mut rd = match tokio::fs::read_dir(&d).await {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(AppError::internal(e)),
            };
            while let Some(entry) = rd.next_entry().await.map_err(AppError::internal)? {
                let meta = entry.metadata().await.map_err(AppError::internal)?;
                if meta.is_dir() {
                    stack.push(entry.path());
                } else if let Ok(rel) = entry.path().strip_prefix(&self.root) {
                    out.push((rel.to_string_lossy().to_string(), meta.len()));
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

// ------------------------------------------------------------------------------------------------
// Release deployment, anonymised data copy — REFERENCE.
// ------------------------------------------------------------------------------------------------

pub struct ReferenceReleaseManager;

#[async_trait]
impl ReleaseManagementPort for ReferenceReleaseManager {
    async fn deploy(&self, tenant: TenantId, release_version: &str) -> AppResult<String> {
        tracing::info!(tenant_id = %tenant, release_version, "REFERENCE release manager: rollout marked applied (no deployment performed)");
        Ok(format!("REFERENCE: release {release_version} marked applied for {tenant}"))
    }
}

pub struct ReferenceAnonymiser;

#[async_trait]
impl AnonymisedDataCopyPort for ReferenceAnonymiser {
    async fn copy_subset(&self, from: TenantId, to: TenantId) -> AppResult<String> {
        tracing::info!(%from, %to, "REFERENCE anonymiser: no business rows copied (M01 owns none; no production PII)");
        Ok("REFERENCE: no business data copied — M01 owns no CRM rows; future modules supply anonymised subsets".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn kms_wraps_and_unwraps() {
        let kms = LocalKms::new(&[7u8; 32]).unwrap();
        let dk = kms.generate_data_key("ref-a").await.unwrap();
        assert_eq!(kms.unwrap_data_key("ref-a", &dk.wrapped).await.unwrap(), dk.plaintext);
        assert!(kms.unwrap_data_key("ref-b", &dk.wrapped).await.is_err(), "KEK is bound to the key reference");
    }

    #[test]
    fn object_keys_cannot_escape_root() {
        let s = LocalObjectStorage::new(PathBuf::from("/tmp/x"));
        assert!(s.path("tenants/a/logo.png").is_ok());
        assert!(s.path("../etc/passwd").is_err());
        assert!(s.path("/etc/passwd").is_err());
        assert!(s.path("tenants/../../x").is_err());
    }

    #[tokio::test]
    async fn simulated_dns() {
        let v = SimulatedDnsVerifier { verified_suffixes: vec![".verified.test".into()] };
        assert!(DomainVerificationPort::verify(&v, "care.acme.verified.test", "x", "t").await.unwrap().verified);
        let r = DomainVerificationPort::verify(&v, "care.acme.com", "x", "t").await.unwrap();
        assert!(!r.verified && r.simulated);
    }
}
