//! Server-side catalogue of dedicated database targets and the secret resolver (ADR-0004).
//! HTTP input never carries connection details: the Super Admin picks a target by name.

use std::path::Path;

use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

use crate::platform::errors::{AppError, AppResult};

use super::super::super::domain::storage::{DatabaseEngine, Region};
use super::super::super::domain::TenantId;

#[derive(Debug, Clone, Deserialize)]
pub struct DbTarget {
    pub name: String,
    pub engine: String,
    pub region: String,
    pub host: String,
    pub port: u16,
    /// Maintenance database used by the admin connection (PostgreSQL only).
    #[serde(default = "default_admin_db")]
    pub admin_database: String,
    pub admin_user: String,
    /// `env:TENANT_DB_*` reference for the admin password.
    pub admin_secret_ref: String,
    /// `env:TENANT_DB_*` reference to the seed from which per-tenant runtime passwords derive.
    pub runtime_seed_ref: String,
}

fn default_admin_db() -> String {
    "postgres".into()
}

#[derive(Debug, Deserialize)]
struct TargetsFile {
    #[serde(default)]
    targets: Vec<DbTarget>,
}

impl DbTarget {
    pub fn engine(&self) -> AppResult<DatabaseEngine> {
        Ok(DatabaseEngine::parse(&self.engine)?)
    }
    pub fn region(&self) -> AppResult<Region> {
        Ok(Region::parse(&self.region)?)
    }
}

pub fn load_targets(path: &Path) -> anyhow::Result<Vec<DbTarget>> {
    if !path.exists() {
        tracing::warn!(path = %path.display(), "tenant DB targets file not found; dedicated tier unavailable");
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(path)?;
    let f: TargetsFile = toml::from_str(&raw)?;
    for t in &f.targets {
        DatabaseEngine::parse(&t.engine).map_err(|e| anyhow::anyhow!("target {}: {e:?}", t.name))?;
        Region::parse(&t.region).map_err(|e| anyhow::anyhow!("target {}: {e:?}", t.name))?;
        for r in [&t.admin_secret_ref, &t.runtime_seed_ref] {
            if !valid_secret_ref(r) {
                anyhow::bail!("target {}: secret refs must look like env:TENANT_DB_*", t.name);
            }
        }
    }
    Ok(f.targets)
}

pub fn valid_secret_ref(r: &str) -> bool {
    r.strip_prefix("env:TENANT_DB_")
        .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'))
}

/// REFERENCE secret adapter (stand-in for a vault, SEC-113): resolves `env:TENANT_DB_*` only, so
/// no other process secret (e.g. bootstrap passwords) can ever be read through a stored reference.
#[derive(Clone, Default)]
pub struct EnvSecretResolver;

impl EnvSecretResolver {
    pub fn resolve(&self, secret_ref: &str) -> AppResult<String> {
        if !valid_secret_ref(secret_ref) {
            return Err(AppError::internal(anyhow::anyhow!("secret reference rejected")));
        }
        let var = &secret_ref["env:".len()..];
        std::env::var(var).map_err(|_| AppError::internal(anyhow::anyhow!("secret {var} is not set")))
    }

    /// Per-tenant runtime credential derived from a seed (dynamic-credential stand-in): nothing
    /// tenant-specific is stored, and each tenant database gets its own login.
    pub fn derive_password(&self, seed_ref: &str, username: &str) -> AppResult<String> {
        let seed = self.resolve(seed_ref)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(seed.as_bytes()).map_err(|e| AppError::internal(anyhow::anyhow!("{e}")))?;
        mac.update(b"omni-m01-tenant-db:");
        mac.update(username.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

/// Per-tenant runtime login (≤ 32 chars for MySQL): `u` + 31 hex chars of the tenant UUID.
pub fn runtime_username(tenant: TenantId) -> String {
    let simple = tenant.0.simple().to_string();
    format!("u{}", &simple[1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_refs_are_restricted() {
        assert!(valid_secret_ref("env:TENANT_DB_PG_ADMIN_PASSWORD"));
        assert!(!valid_secret_ref("env:BOOTSTRAP_SUPERADMIN_PASSWORD"));
        assert!(!valid_secret_ref("env:TENANT_DB_"));
        assert!(!valid_secret_ref("file:/etc/passwd"));
        assert!(EnvSecretResolver.resolve("env:DATABASE_URL").is_err());
    }

    #[test]
    fn usernames_fit_mysql() {
        let u = runtime_username(TenantId::new());
        assert_eq!(u.len(), 32);
        assert!(u.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()));
    }

    #[test]
    fn derived_passwords_differ_per_user() {
        std::env::set_var("TENANT_DB_TEST_SEED_X", "seed-value");
        let r = EnvSecretResolver;
        let a = r.derive_password("env:TENANT_DB_TEST_SEED_X", "ua").unwrap();
        let b = r.derive_password("env:TENANT_DB_TEST_SEED_X", "ub").unwrap();
        assert_ne!(a, b);
        assert_eq!(a, r.derive_password("env:TENANT_DB_TEST_SEED_X", "ua").unwrap());
    }
}
