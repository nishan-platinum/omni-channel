//! Host-level anonymised aggregate analytics (OCC-M01-R027, P2). Only M01 metadata is
//! aggregated — never tenant business rows — and small cohorts are suppressed (k-anonymity).

use serde::Serialize;

/// Minimum cohort size before a bucket is shown.
pub const K_ANONYMITY: i64 = 3;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Bucket {
    pub label: String,
    pub count: Option<i64>,
    pub suppressed: bool,
}

pub fn suppress(label: impl Into<String>, count: i64) -> Bucket {
    let suppressed = count > 0 && count < K_ANONYMITY;
    Bucket { label: label.into(), count: if suppressed { None } else { Some(count) }, suppressed }
}

/// Adoption percentage among participating tenants, suppressed below the k threshold.
pub fn adoption_pct(enabled: i64, participants: i64) -> Option<f64> {
    if participants < K_ANONYMITY {
        None
    } else {
        Some((enabled as f64 / participants as f64 * 1000.0).round() / 10.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_cohorts_suppressed() {
        assert!(suppress("x", 1).suppressed);
        assert!(suppress("x", 2).count.is_none());
        assert_eq!(suppress("x", 3).count, Some(3));
        assert_eq!(suppress("x", 0).count, Some(0));
        assert_eq!(adoption_pct(1, 2), None);
        assert_eq!(adoption_pct(1, 4), Some(25.0));
    }
}
