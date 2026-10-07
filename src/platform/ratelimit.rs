//! In-process fixed-window rate limiter keyed by tenant (noisy-neighbour protection for API
//! requests per minute, OCC-M01-R012 / API-006). A multi-pod deployment would back this with Redis
//! (FR-PRF-101); the interface stays the same.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateDecision {
    pub allowed: bool,
    pub limit: u64,
    pub remaining: u64,
    pub reset_secs: u64,
}

#[derive(Default)]
pub struct TenantRateLimiter {
    windows: Mutex<HashMap<Uuid, (i64, u64)>>,
}

impl TenantRateLimiter {
    pub fn check(&self, tenant: Uuid, limit: u64, now: DateTime<Utc>) -> RateDecision {
        let minute = now.timestamp().div_euclid(60);
        let reset_secs = (60 - now.timestamp().rem_euclid(60)).max(1) as u64;
        let Ok(mut map) = self.windows.lock() else {
            // Fail closed if the lock is poisoned.
            return RateDecision { allowed: false, limit, remaining: 0, reset_secs };
        };
        if map.len() > 100_000 {
            map.retain(|_, (m, _)| *m == minute);
        }
        let entry = map.entry(tenant).or_insert((minute, 0));
        if entry.0 != minute {
            *entry = (minute, 0);
        }
        if entry.1 >= limit {
            return RateDecision { allowed: false, limit, remaining: 0, reset_secs };
        }
        entry.1 += 1;
        RateDecision { allowed: true, limit, remaining: limit - entry.1, reset_secs }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_after_limit_and_resets_next_minute() {
        let rl = TenantRateLimiter::default();
        let t = Uuid::now_v7();
        let now = DateTime::parse_from_rfc3339("2026-10-06T10:00:05Z").unwrap().with_timezone(&Utc);
        assert!(rl.check(t, 2, now).allowed);
        assert!(rl.check(t, 2, now).allowed);
        let d = rl.check(t, 2, now);
        assert!(!d.allowed);
        assert_eq!(d.reset_secs, 55);
        let other = Uuid::now_v7();
        assert!(rl.check(other, 2, now).allowed, "tenants are independent");
        assert!(rl.check(t, 2, now + chrono::Duration::seconds(60)).allowed);
    }
}
