//! Cross-cutting security primitives: random tokens, hashing, Argon2id passwords, constant-time
//! comparison. Token values are never logged by callers.

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine;
use rand::RngCore;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// URL-safe random token with `bytes` bytes of entropy.
pub fn random_token(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut buf = [0u8; N];
    rand::thread_rng().fill_bytes(&mut buf);
    buf
}

pub fn sha256(data: &[u8]) -> Vec<u8> {
    Sha256::digest(data).to_vec()
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && bool::from(a.ct_eq(b))
}

fn argon2() -> Argon2<'static> {
    // OWASP-recommended Argon2id baseline (19 MiB, 2 iterations, 1 lane).
    let params = Params::new(19 * 1024, 2, 1, None).unwrap_or_default();
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

pub fn hash_password(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    argon2().hash_password(password.as_bytes(), &salt).map(|h| h.to_string()).map_err(|e| anyhow::anyhow!("password hashing failed: {e}"))
}

pub fn verify_password(password: &str, phc: &str) -> bool {
    match PasswordHash::new(phc) {
        Ok(parsed) => argon2().verify_password(password.as_bytes(), &parsed).is_ok(),
        Err(_) => false,
    }
}

/// A fixed hash used to equalise timing when the user does not exist.
pub fn dummy_verify(password: &str) {
    static DUMMY: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let h = DUMMY.get_or_init(|| hash_password("dummy-password-for-timing").unwrap_or_default());
    let _ = verify_password(password, h);
}

/// Argon2id is deliberately expensive (CPU + 19 MiB per call). Run it on the blocking pool so it
/// never stalls async workers, and bound concurrency so login floods cannot exhaust CPU/memory.
fn hashing_permits() -> &'static tokio::sync::Semaphore {
    static SEM: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    SEM.get_or_init(|| {
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).clamp(2, 8);
        tokio::sync::Semaphore::new(n)
    })
}

async fn off_runtime<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> anyhow::Result<T> {
    let _permit = hashing_permits().acquire().await.map_err(|e| anyhow::anyhow!("hashing pool closed: {e}"))?;
    tokio::task::spawn_blocking(f).await.map_err(|e| anyhow::anyhow!("hashing task failed: {e}"))
}

pub async fn hash_password_async(password: &str) -> anyhow::Result<String> {
    let p = password.to_string();
    off_runtime(move || hash_password(&p)).await?
}

pub async fn verify_password_async(password: &str, phc: &str) -> bool {
    let (p, h) = (password.to_string(), phc.to_string());
    off_runtime(move || verify_password(&p, &h)).await.unwrap_or(false)
}

pub async fn dummy_verify_async(password: &str) {
    let p = password.to_string();
    let _ = off_runtime(move || dummy_verify(&p)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_roundtrip() {
        let h = hash_password("correct horse battery staple").unwrap();
        assert!(h.starts_with("$argon2id$"));
        assert!(verify_password("correct horse battery staple", &h));
        assert!(!verify_password("wrong", &h));
        assert!(!verify_password("x", "not-a-hash"));
    }

    #[test]
    fn tokens_are_random_and_urlsafe() {
        let a = random_token(32);
        let b = random_token(32);
        assert_ne!(a, b);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn ct_eq() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
