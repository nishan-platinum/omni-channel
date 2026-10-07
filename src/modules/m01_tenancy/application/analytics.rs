//! Host-level anonymised, aggregated analytics (OCC-M01-R027, P2). Aggregates M01 metadata only
//! (never row-level tenant business data), excludes opted-out Regulated tenants and suppresses
//! cohorts smaller than k = 3.

use std::sync::Arc;

use serde::Serialize;

use crate::platform::errors::AppResult;

use super::super::domain::analytics::{adoption_pct, suppress, Bucket};
use super::super::domain::features::feature_def;
use super::context::{Actor, M01Deps};
use super::quotas::month_of;

#[derive(Debug, Clone, Serialize)]
pub struct FeatureAdoption {
    pub feature: String,
    pub label: String,
    pub adoption_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HostAnalytics {
    pub participants: i64,
    pub opted_out: i64,
    pub by_status: Vec<Bucket>,
    pub by_tier: Vec<Bucket>,
    pub by_region: Vec<Bucket>,
    pub quota_utilisation: Vec<Bucket>,
    pub adoption: Vec<FeatureAdoption>,
    pub api_calls_this_month: Option<i64>,
}

pub struct AnalyticsService {
    deps: Arc<M01Deps>,
}

impl AnalyticsService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps }
    }

    pub async fn host_analytics(&self, actor: &Actor) -> AppResult<HostAnalytics> {
        actor.require_super_admin()?;
        let now = self.deps.clock.now();
        let raw = self.deps.analytics.aggregate(month_of(now), now).await?;
        let adoption = raw
            .feature_entitled
            .iter()
            .map(|(f, entitled)| {
                let enabled = raw.feature_enabled.iter().find(|(k, _)| k == f).map(|(_, n)| *n).unwrap_or(0);
                FeatureAdoption {
                    feature: f.clone(),
                    label: feature_def(f).map(|d| d.label.to_string()).unwrap_or_else(|| f.clone()),
                    adoption_pct: adoption_pct(enabled, *entitled),
                }
            })
            .collect();
        let buckets = |v: &Vec<(String, i64)>| v.iter().map(|(l, n)| suppress(l.clone(), *n)).collect::<Vec<_>>();
        Ok(HostAnalytics {
            participants: raw.participants,
            opted_out: raw.opted_out,
            by_status: buckets(&raw.by_status),
            by_tier: buckets(&raw.by_tier),
            by_region: buckets(&raw.by_region),
            quota_utilisation: buckets(&raw.utilisation_buckets),
            adoption,
            api_calls_this_month: (raw.participants >= super::super::domain::analytics::K_ANONYMITY).then_some(raw.api_calls_this_month),
        })
    }
}
