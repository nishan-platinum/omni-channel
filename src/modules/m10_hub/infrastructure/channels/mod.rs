//! Channel adapters. Everything here is a **SIMULATED** stand-in (ADR-0012): no Meta, BSP, SBC or
//! carrier is contacted. Real adapters implement the same `ChannelAdapter` contract later.

pub mod sip_sim;
pub mod webchat;
pub mod whatsapp_sim;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::platform::security::constant_time_eq;

/// `sha256=<hex HMAC-SHA256(secret, body)>` — the X-Hub-Signature-256 format.
pub fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

pub fn signature_valid(secret: &[u8], body: &[u8], header: Option<&str>) -> bool {
    match header {
        Some(h) => constant_time_eq(h.trim().as_bytes(), sign(secret, body).as_bytes()),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_round_trip() {
        let s = sign(b"secret", b"{\"a\":1}");
        assert!(s.starts_with("sha256=") && s.len() == 7 + 64);
        assert!(signature_valid(b"secret", b"{\"a\":1}", Some(&s)));
        assert!(!signature_valid(b"other", b"{\"a\":1}", Some(&s)));
        assert!(!signature_valid(b"secret", b"{\"a\":2}", Some(&s)));
        assert!(!signature_valid(b"secret", b"{\"a\":1}", None));
    }
}
