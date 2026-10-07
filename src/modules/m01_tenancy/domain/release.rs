//! Ring-based release management and maintenance windows (OCC-M01-R026, P2; FR-OPS-121).

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use serde::Serialize;

use super::errors::{DomainError, Violations};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Ring {
    HostSandbox,
    EarlyAdopter,
    General,
}

pub const RINGS: [Ring; 3] = [Ring::HostSandbox, Ring::EarlyAdopter, Ring::General];

impl Ring {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "host_sandbox" => Ok(Self::HostSandbox),
            "early_adopter" => Ok(Self::EarlyAdopter),
            "general" => Ok(Self::General),
            _ => Err(DomainError::field("ring", "Ring must be host_sandbox, early_adopter or general")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HostSandbox => "host_sandbox",
            Self::EarlyAdopter => "early_adopter",
            Self::General => "general",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::HostSandbox => "Host sandbox",
            Self::EarlyAdopter => "Early adopters",
            Self::General => "All tenants",
        }
    }
    pub fn next(self) -> Option<Ring> {
        match self {
            Self::HostSandbox => Some(Self::EarlyAdopter),
            Self::EarlyAdopter => Some(Self::General),
            Self::General => None,
        }
    }
}

/// Whether a release currently at `release_ring` reaches a tenant in `tenant_ring`.
pub fn ring_reaches(release_ring: Ring, tenant_ring: Ring) -> bool {
    tenant_ring <= release_ring
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MaintenanceWindow {
    /// ISO weekday 1 = Monday … 7 = Sunday.
    pub day: i16,
    pub start_hour_utc: i16,
    pub duration_min: i32,
}

pub const DAY_NAMES: [&str; 7] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

impl MaintenanceWindow {
    pub fn new(day: i16, start_hour_utc: i16, duration_min: i32) -> Result<Self, DomainError> {
        let mut v = Violations::default();
        if !(1..=7).contains(&day) {
            v.push("maintenance_day", "Day must be 1 (Monday) to 7 (Sunday)");
        }
        if !(0..=23).contains(&start_hour_utc) {
            v.push("maintenance_start_hour_utc", "Start hour must be 0-23 (UTC)");
        }
        if !(30..=480).contains(&duration_min) {
            v.push("maintenance_duration_min", "Duration must be 30-480 minutes");
        }
        v.into_result()?;
        Ok(Self { day, start_hour_utc, duration_min })
    }

    pub fn day_name(&self) -> &'static str {
        DAY_NAMES[((self.day.clamp(1, 7)) - 1) as usize]
    }

    /// Start of the next window at or after `after` (a window already in progress counts).
    pub fn next_start(&self, after: DateTime<Utc>) -> DateTime<Utc> {
        let today = after.date_naive();
        for offset in 0..=7 {
            let date = today + Duration::days(offset);
            if date.weekday().number_from_monday() as i16 != self.day {
                continue;
            }
            if let Some(start) = Utc.with_ymd_and_hms(date.year(), date.month(), date.day(), self.start_hour_utc as u32, 0, 0).single() {
                let end = start + Duration::minutes(i64::from(self.duration_min));
                if after < end {
                    return start.max(after);
                }
            }
        }
        after + Duration::days(7)
    }
}

/// When a release should reach a tenant: disruptive changes honour the tenant's maintenance
/// window; non-disruptive changes roll out immediately.
pub fn schedule_for(window: &MaintenanceWindow, disruptive: bool, now: DateTime<Utc>) -> DateTime<Utc> {
    if disruptive {
        window.next_start(now)
    } else {
        now
    }
}

pub fn validate_version(v: &str) -> Result<String, DomainError> {
    let s = v.trim();
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() == 3 && parts.iter().all(|p| !p.is_empty() && p.len() <= 5 && p.bytes().all(|b| b.is_ascii_digit())) {
        Ok(s.to_string())
    } else {
        Err(DomainError::field("release_version", "Version must look like 1.2.3"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn ring_progression() {
        assert!(ring_reaches(Ring::HostSandbox, Ring::HostSandbox));
        assert!(!ring_reaches(Ring::HostSandbox, Ring::EarlyAdopter));
        assert!(ring_reaches(Ring::EarlyAdopter, Ring::HostSandbox));
        assert!(ring_reaches(Ring::General, Ring::General));
        assert_eq!(Ring::HostSandbox.next(), Some(Ring::EarlyAdopter));
        assert_eq!(Ring::General.next(), None);
    }

    #[test]
    fn disruptive_change_waits_for_window() {
        // 2026-10-06 is a Tuesday. Window: Sunday 18:00 UTC for 2h.
        let w = MaintenanceWindow::new(7, 18, 120).unwrap();
        let now = at("2026-10-06T10:00:00Z");
        assert_eq!(schedule_for(&w, true, now), at("2026-10-11T18:00:00Z"));
        assert_eq!(schedule_for(&w, false, now), now);
        // Inside the window: start immediately.
        assert_eq!(w.next_start(at("2026-10-11T19:00:00Z")), at("2026-10-11T19:00:00Z"));
        // Just after the window: next week.
        assert_eq!(w.next_start(at("2026-10-11T20:00:00Z")), at("2026-10-18T18:00:00Z"));
    }

    #[test]
    fn window_validation() {
        assert!(MaintenanceWindow::new(0, 18, 120).is_err());
        assert!(MaintenanceWindow::new(1, 24, 120).is_err());
        assert!(MaintenanceWindow::new(1, 2, 10).is_err());
        assert_eq!(MaintenanceWindow::new(1, 2, 60).unwrap().day_name(), "Monday");
        assert!(validate_version("1.2.3").is_ok());
        assert!(validate_version("1.2").is_err());
    }
}
