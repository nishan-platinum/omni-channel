//! Injectable clock so time-boxed rules (support grants, grace, retention, quota cycles) are testable.

use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Test clock that only moves when told to.
pub struct ManualClock {
    now: Mutex<DateTime<Utc>>,
}

impl ManualClock {
    pub fn new(start: DateTime<Utc>) -> Self {
        Self { now: Mutex::new(start) }
    }

    pub fn advance(&self, by: Duration) {
        if let Ok(mut g) = self.now.lock() {
            *g += by;
        }
    }

    pub fn set(&self, to: DateTime<Utc>) {
        if let Ok(mut g) = self.now.lock() {
            *g = to;
        }
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        self.now.lock().map(|g| *g).unwrap_or_else(|_| Utc::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_advances() {
        let start = Utc::now();
        let c = ManualClock::new(start);
        c.advance(Duration::hours(5));
        assert_eq!(c.now(), start + Duration::hours(5));
    }
}
