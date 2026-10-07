//! Quotas and metering guardrails (M01-F05; OCC-M01-R005, R012; BR-M01-004; NT-002/NT-003).

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};

use super::errors::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaMetric {
    Users,
    Numbers,
    Channels,
    VolumeMonth,
    ApiRequestsPerMinute,
    CampaignSendsPerHour,
    ReportQueryCostPerHour,
    StorageGb,
    BpmExecutionsPerHour,
    EmailsMonth,
    AiTokensMonth,
}

pub const ALL_METRICS: [QuotaMetric; 11] = [
    QuotaMetric::Users,
    QuotaMetric::Numbers,
    QuotaMetric::Channels,
    QuotaMetric::VolumeMonth,
    QuotaMetric::ApiRequestsPerMinute,
    QuotaMetric::CampaignSendsPerHour,
    QuotaMetric::ReportQueryCostPerHour,
    QuotaMetric::StorageGb,
    QuotaMetric::BpmExecutionsPerHour,
    QuotaMetric::EmailsMonth,
    QuotaMetric::AiTokensMonth,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaPeriod {
    /// Current-state counts (users, numbers, channels, storage).
    Static,
    /// Resets each billing cycle (calendar month, UTC).
    Monthly,
    /// Rolling hourly window (rate guardrails).
    Hourly,
    /// Enforced in-process per minute (API rate limit).
    PerMinute,
}

impl QuotaPeriod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Static => "static",
            Self::Monthly => "monthly",
            Self::Hourly => "hourly",
            Self::PerMinute => "per_minute",
        }
    }
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "static" => Ok(Self::Static),
            "monthly" => Ok(Self::Monthly),
            "hourly" => Ok(Self::Hourly),
            "per_minute" => Ok(Self::PerMinute),
            _ => Err(DomainError::field("period", "Unknown quota period")),
        }
    }
}

impl QuotaMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Users => "users",
            Self::Numbers => "numbers",
            Self::Channels => "channels",
            Self::VolumeMonth => "volume_month",
            Self::ApiRequestsPerMinute => "api_requests_per_minute",
            Self::CampaignSendsPerHour => "campaign_sends_per_hour",
            Self::ReportQueryCostPerHour => "report_query_cost_per_hour",
            Self::StorageGb => "storage_gb",
            Self::BpmExecutionsPerHour => "bpm_executions_per_hour",
            Self::EmailsMonth => "emails_month",
            Self::AiTokensMonth => "ai_tokens_month",
        }
    }

    pub fn parse(s: &str) -> Result<Self, DomainError> {
        ALL_METRICS.iter().copied().find(|m| m.as_str() == s).ok_or_else(|| DomainError::field("metric", "Unknown quota metric"))
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Users => "Users",
            Self::Numbers => "Phone numbers",
            Self::Channels => "Enabled channels",
            Self::VolumeMonth => "Calls + messages / month",
            Self::ApiRequestsPerMinute => "API requests / minute",
            Self::CampaignSendsPerHour => "Campaign sends / hour",
            Self::ReportQueryCostPerHour => "Report query cost / hour",
            Self::StorageGb => "Storage (GB)",
            Self::BpmExecutionsPerHour => "BPM executions / hour",
            Self::EmailsMonth => "Emails / month",
            Self::AiTokensMonth => "AI tokens / month",
        }
    }

    pub fn period(self) -> QuotaPeriod {
        match self {
            Self::Users | Self::Numbers | Self::Channels | Self::StorageGb => QuotaPeriod::Static,
            Self::VolumeMonth | Self::EmailsMonth | Self::AiTokensMonth => QuotaPeriod::Monthly,
            Self::CampaignSendsPerHour | Self::ReportQueryCostPerHour | Self::BpmExecutionsPerHour => QuotaPeriod::Hourly,
            Self::ApiRequestsPerMinute => QuotaPeriod::PerMinute,
        }
    }

    /// Error raised when the limit is hit. Users use the field-specific RATE_LIMITED message
    /// from the field dictionary; everything else uses QUOTA_EXCEEDED (Part D §60).
    pub fn blocked_message(self) -> &'static str {
        match self {
            Self::Users => "User quota reached; upgrade plan",
            _ => "Plan quota reached; upgrade required",
        }
    }
}

/// Start of the cycle containing `now` for a period.
pub fn cycle_start(period: QuotaPeriod, now: DateTime<Utc>) -> DateTime<Utc> {
    match period {
        QuotaPeriod::Monthly => Utc.with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0).single().unwrap_or(now),
        QuotaPeriod::Hourly => now.with_minute(0).and_then(|t| t.with_second(0)).and_then(|t| t.with_nanosecond(0)).unwrap_or(now),
        QuotaPeriod::PerMinute => now.with_second(0).and_then(|t| t.with_nanosecond(0)).unwrap_or(now),
        QuotaPeriod::Static => now,
    }
}

/// End of the cycle that started at `start` (used for Retry-After).
pub fn cycle_end(period: QuotaPeriod, start: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match period {
        QuotaPeriod::Monthly => {
            let (y, m) = if start.month() == 12 { (start.year() + 1, 1) } else { (start.year(), start.month() + 1) };
            Utc.with_ymd_and_hms(y, m, 1, 0, 0, 0).single()
        }
        QuotaPeriod::Hourly => Some(start + Duration::hours(1)),
        QuotaPeriod::PerMinute => Some(start + Duration::minutes(1)),
        QuotaPeriod::Static => None,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QuotaState {
    pub metric: QuotaMetric,
    pub limit: i64,
    pub usage: i64,
    pub soft_threshold: f64,
    pub cycle_start: DateTime<Utc>,
    pub warned_cycle_start: Option<DateTime<Utc>>,
    pub exhausted_cycle_start: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaLevel {
    Ok,
    Warning,
    Exhausted,
}

impl QuotaLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Exhausted => "exhausted",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuotaDecision {
    Allowed {
        new_usage: i64,
        cycle_start: DateTime<Utc>,
        reset_cycle: bool,
        /// First crossing of the soft threshold in this cycle → NT-002 + `tenant.quota_warning`.
        raise_warning: bool,
        /// Usage reached exactly 100% in this cycle → NT-003 + `tenant.quota_exhausted`.
        raise_exhausted: bool,
    },
    Blocked {
        retry_after_secs: u64,
        cycle_start: DateTime<Utc>,
        reset_cycle: bool,
        /// First block in this cycle → alert (NT-003) if not already raised.
        raise_exhausted: bool,
    },
}

pub fn validate_threshold(t: f64) -> Result<f64, DomainError> {
    if t.is_finite() && (0.0..=1.0).contains(&t) {
        Ok(t)
    } else {
        Err(DomainError::field("soft_threshold", "Soft threshold must be between 0 and 1"))
    }
}

pub fn validate_limit(metric: QuotaMetric, limit: i64) -> Result<i64, DomainError> {
    let min = if metric == QuotaMetric::Users { 1 } else { 0 };
    if limit >= min {
        Ok(limit)
    } else {
        Err(DomainError::field(format!("limits.{}", metric.as_str()), format!("Limit must be >= {min}")))
    }
}

impl QuotaState {
    fn effective_cycle(&self, now: DateTime<Utc>) -> (DateTime<Utc>, i64, bool) {
        let period = self.metric.period();
        if period == QuotaPeriod::Static {
            return (self.cycle_start, self.usage, false);
        }
        let current = cycle_start(period, now);
        if current > self.cycle_start {
            (current, 0, true)
        } else {
            (self.cycle_start, self.usage, false)
        }
    }

    /// Usage as seen at `now` (expired windows count as zero).
    pub fn usage_at(&self, now: DateTime<Utc>) -> i64 {
        self.effective_cycle(now).1
    }

    pub fn utilisation_at(&self, now: DateTime<Utc>) -> f64 {
        if self.limit <= 0 {
            if self.usage_at(now) > 0 {
                1.0
            } else {
                0.0
            }
        } else {
            self.usage_at(now) as f64 / self.limit as f64
        }
    }

    pub fn level_at(&self, now: DateTime<Utc>) -> QuotaLevel {
        let u = self.usage_at(now);
        if self.limit == 0 || u >= self.limit {
            if self.limit == 0 && u == 0 {
                QuotaLevel::Ok
            } else {
                QuotaLevel::Exhausted
            }
        } else if (u as f64) >= self.soft_threshold * self.limit as f64 {
            QuotaLevel::Warning
        } else {
            QuotaLevel::Ok
        }
    }

    /// Evaluates a quota-bound action consuming `amount` units (BR-M01-004: warn at the soft
    /// threshold, block once the action would exceed 100%).
    pub fn evaluate(&self, amount: i64, now: DateTime<Utc>) -> QuotaDecision {
        let (cycle, usage, reset) = self.effective_cycle(now);
        let warned = !reset && self.warned_cycle_start == Some(cycle);
        let exhausted = !reset && self.exhausted_cycle_start == Some(cycle);
        let new_usage = usage.saturating_add(amount.max(0));
        if new_usage > self.limit {
            let retry = cycle_end(self.metric.period(), cycle).map(|end| (end - now).num_seconds().max(1) as u64).unwrap_or(3600);
            return QuotaDecision::Blocked { retry_after_secs: retry, cycle_start: cycle, reset_cycle: reset, raise_exhausted: !exhausted };
        }
        let threshold = self.soft_threshold * self.limit as f64;
        QuotaDecision::Allowed {
            new_usage,
            cycle_start: cycle,
            reset_cycle: reset,
            raise_warning: !warned && self.limit > 0 && (new_usage as f64) >= threshold,
            raise_exhausted: !exhausted && self.limit > 0 && new_usage == self.limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn q(metric: QuotaMetric, limit: i64, usage: i64, now: DateTime<Utc>) -> QuotaState {
        QuotaState {
            metric,
            limit,
            usage,
            soft_threshold: 0.8,
            cycle_start: cycle_start(metric.period(), now),
            warned_cycle_start: None,
            exhausted_cycle_start: None,
        }
    }

    #[test]
    fn warns_at_80_percent_once() {
        let now = at("2026-10-06T10:00:00Z");
        let s = q(QuotaMetric::VolumeMonth, 100, 79, now);
        match s.evaluate(1, now) {
            QuotaDecision::Allowed { raise_warning, new_usage, .. } => {
                assert!(raise_warning);
                assert_eq!(new_usage, 80);
            }
            other => panic!("{other:?}"),
        }
        let mut s2 = q(QuotaMetric::VolumeMonth, 100, 85, now);
        s2.warned_cycle_start = Some(s2.cycle_start);
        assert!(matches!(s2.evaluate(1, now), QuotaDecision::Allowed { raise_warning: false, .. }));
    }

    #[test]
    fn blocks_above_100_percent_with_retry_after() {
        let now = at("2026-10-06T10:00:00Z");
        let s = q(QuotaMetric::VolumeMonth, 100, 99, now);
        assert!(matches!(s.evaluate(1, now), QuotaDecision::Allowed { raise_exhausted: true, .. }));
        let full = q(QuotaMetric::VolumeMonth, 100, 100, now);
        match full.evaluate(1, now) {
            QuotaDecision::Blocked { retry_after_secs, raise_exhausted, .. } => {
                assert!(raise_exhausted);
                // Next cycle starts 2026-11-01T00:00:00Z.
                assert_eq!(retry_after_secs, (at("2026-11-01T00:00:00Z") - now).num_seconds() as u64);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn monthly_counters_reset_each_cycle() {
        let start = at("2026-09-15T00:00:00Z");
        let s = q(QuotaMetric::VolumeMonth, 100, 100, start);
        let next = at("2026-10-01T00:00:01Z");
        assert_eq!(s.usage_at(next), 0);
        assert!(matches!(s.evaluate(5, next), QuotaDecision::Allowed { new_usage: 5, reset_cycle: true, .. }));
    }

    #[test]
    fn static_counts_never_reset() {
        let now = at("2026-10-06T10:00:00Z");
        let s = q(QuotaMetric::Users, 2, 2, now);
        assert!(matches!(s.evaluate(1, now + Duration::days(40)), QuotaDecision::Blocked { .. }));
    }

    #[test]
    fn hourly_window() {
        let now = at("2026-10-06T10:59:00Z");
        let s = q(QuotaMetric::CampaignSendsPerHour, 10, 10, now);
        match s.evaluate(1, now) {
            QuotaDecision::Blocked { retry_after_secs, .. } => assert_eq!(retry_after_secs, 60),
            other => panic!("{other:?}"),
        }
        assert!(matches!(s.evaluate(1, at("2026-10-06T11:00:00Z")), QuotaDecision::Allowed { .. }));
    }

    #[test]
    fn levels_and_validation() {
        let now = Utc::now();
        assert_eq!(q(QuotaMetric::Numbers, 10, 5, now).level_at(now), QuotaLevel::Ok);
        assert_eq!(q(QuotaMetric::Numbers, 10, 8, now).level_at(now), QuotaLevel::Warning);
        assert_eq!(q(QuotaMetric::Numbers, 10, 10, now).level_at(now), QuotaLevel::Exhausted);
        assert!(validate_limit(QuotaMetric::Users, 0).is_err());
        assert!(validate_limit(QuotaMetric::Numbers, 0).is_ok());
        assert!(validate_threshold(1.2).is_err());
        assert!(validate_threshold(0.8).is_ok());
        assert_eq!(QuotaMetric::parse("volume_month").unwrap(), QuotaMetric::VolumeMonth);
    }
}
