//! Tracing setup, correlation ids (ERR-001) and lightweight per-tenant request metrics used by the
//! host tenant directory health column (OCC-M01-R023). No secrets are ever recorded here.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;

use super::config::LogFormat;

tokio::task_local! {
    static CORRELATION_ID: String;
}

/// Runs `fut` with the correlation id available to error rendering and logs.
pub async fn with_correlation_id<F: std::future::Future>(id: String, fut: F) -> F::Output {
    CORRELATION_ID.scope(id, fut).await
}

pub fn current_correlation_id() -> String {
    CORRELATION_ID.try_with(|c| c.clone()).unwrap_or_else(|_| "none".to_string())
}

pub fn init_tracing(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,tower_http=warn"));
    let builder = fmt().with_env_filter(filter).with_target(false);
    let res = match format {
        LogFormat::Json => builder.json().flatten_event(true).try_init(),
        LogFormat::Pretty => builder.compact().try_init(),
    };
    // Tests may initialise more than once; ignore "already set".
    let _ = res;
}

/// Mutable per-request slot filled by the auth extractors so the access-log middleware can record
/// the resolved tenant/actor after the handler ran.
#[derive(Debug, Default)]
pub struct RequestIdentity {
    pub tenant_id: Option<Uuid>,
    pub actor_id: Option<Uuid>,
    /// (limit, remaining, reset seconds) of the tenant API rate limit (API-003 headers).
    pub rate: Option<(u64, u64, u64)>,
}

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub correlation_id: String,
    pub identity: Arc<Mutex<RequestIdentity>>,
    pub ip: Option<String>,
    pub user_agent: Option<String>,
}

impl RequestContext {
    pub fn set_identity(&self, tenant_id: Option<Uuid>, actor_id: Option<Uuid>) {
        if let Ok(mut g) = self.identity.lock() {
            g.tenant_id = tenant_id;
            g.actor_id = actor_id;
        }
    }

    pub fn set_rate(&self, limit: u64, remaining: u64, reset: u64) {
        if let Ok(mut g) = self.identity.lock() {
            g.rate = Some((limit, remaining, reset));
        }
    }

    pub fn rate(&self) -> Option<(u64, u64, u64)> {
        self.identity.lock().ok().and_then(|g| g.rate)
    }

    pub fn identity(&self) -> (Option<Uuid>, Option<Uuid>) {
        self.identity.lock().map(|g| (g.tenant_id, g.actor_id)).unwrap_or((None, None))
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct MinuteBucket {
    minute: i64,
    requests: u64,
    errors: u64,
}

/// Rolling 60-minute request/error counters per tenant (in-process; a production deployment would
/// export these to Prometheus per FR-OPS-130).
#[derive(Default)]
pub struct TenantMetrics {
    inner: Mutex<HashMap<Uuid, VecDeque<MinuteBucket>>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TenantHealthSample {
    pub requests_last_hour: u64,
    pub errors_last_hour: u64,
}

impl TenantHealthSample {
    pub fn error_rate(&self) -> f64 {
        if self.requests_last_hour == 0 {
            0.0
        } else {
            self.errors_last_hour as f64 / self.requests_last_hour as f64
        }
    }
}

impl TenantMetrics {
    pub fn record(&self, tenant: Uuid, now: DateTime<Utc>, is_error: bool) {
        let minute = now.timestamp() / 60;
        if let Ok(mut map) = self.inner.lock() {
            let q = map.entry(tenant).or_default();
            match q.back_mut() {
                Some(b) if b.minute == minute => {
                    b.requests += 1;
                    b.errors += u64::from(is_error);
                }
                _ => q.push_back(MinuteBucket { minute, requests: 1, errors: u64::from(is_error) }),
            }
            while q.front().is_some_and(|b| b.minute <= minute - 60) {
                q.pop_front();
            }
        }
    }

    pub fn sample(&self, tenant: Uuid, now: DateTime<Utc>) -> TenantHealthSample {
        let minute = now.timestamp() / 60;
        self.inner
            .lock()
            .ok()
            .and_then(|map| {
                map.get(&tenant).map(|q| {
                    q.iter().filter(|b| b.minute > minute - 60).fold(TenantHealthSample::default(), |acc, b| TenantHealthSample {
                        requests_last_hour: acc.requests_last_hour + b.requests,
                        errors_last_hour: acc.errors_last_hour + b.errors,
                    })
                })
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_roll_over_an_hour() {
        let m = TenantMetrics::default();
        let t = Uuid::now_v7();
        let now = Utc::now();
        m.record(t, now, false);
        m.record(t, now, true);
        assert_eq!(m.sample(t, now).requests_last_hour, 2);
        assert!((m.sample(t, now).error_rate() - 0.5).abs() < 1e-9);
        let later = now + chrono::Duration::minutes(61);
        assert_eq!(m.sample(t, later).requests_last_hour, 0);
    }

    #[tokio::test]
    async fn correlation_id_scoped() {
        let id = with_correlation_id("abc".into(), async { current_correlation_id() }).await;
        assert_eq!(id, "abc");
        assert_eq!(current_correlation_id(), "none");
    }
}
