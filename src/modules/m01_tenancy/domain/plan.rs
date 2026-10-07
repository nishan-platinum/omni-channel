//! Plan view used by M01 (entity owned by M19; served by the reference `PlanCatalog`).

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use uuid::Uuid;

use super::quota::QuotaMetric;
use super::storage::Tier;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Plan {
    pub id: Uuid,
    pub code: String,
    pub name: String,
    pub tier: Tier,
    pub entitlements: BTreeSet<String>,
    pub quotas: BTreeMap<QuotaMetric, i64>,
    pub soft_threshold: f64,
    pub active: bool,
    pub version: i32,
}

impl Plan {
    pub fn entitles(&self, feature: &str) -> bool {
        self.entitlements.contains(feature)
    }

    pub fn quota(&self, metric: QuotaMetric) -> i64 {
        self.quotas.get(&metric).copied().unwrap_or(0)
    }
}
