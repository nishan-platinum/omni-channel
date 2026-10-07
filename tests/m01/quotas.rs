//! M01-F05 quotas & metering: OCC-M01-R005, R012, R025; BR-M01-004; NT-002/NT-003.

use axum::http::StatusCode;
use chrono::{Datelike, Duration, TimeZone, Utc};
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn warn_at_80_block_above_100() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let tenant = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&id).unwrap());
    let sa_actor = omni_m01::modules::m01_tenancy::application::Actor {
        user_id: None,
        role: omni_m01::modules::m01_tenancy::application::ActorRole::SuperAdmin,
        tenant_id: None,
        email: None,
        correlation_id: None,
        ip: None,
        user_agent: None,
    };
    use omni_m01::modules::m01_tenancy::domain::quota::QuotaMetric;
    app.state.m01.quotas.set_limits(&sa_actor, tenant, vec![(QuotaMetric::VolumeMonth, 10, 0.8)]).await.unwrap();

    let r = app.post(&format!("/v1/tenants/{id}/quota/consume"), &ta, json!({ "metric": "volume_month", "amount": 7 })).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert_eq!(r.data()["warning"], false);
    let r = app.post(&format!("/v1/tenants/{id}/quota/consume"), &ta, json!({ "metric": "volume_month", "amount": 1 })).await;
    assert_eq!(r.data()["warning"], true, "80% reached");
    let r = app.post(&format!("/v1/tenants/{id}/quota/consume"), &ta, json!({ "metric": "volume_month", "amount": 2 })).await;
    assert_eq!(r.status, StatusCode::OK, "exactly 100% is allowed");
    let r = app.post(&format!("/v1/tenants/{id}/quota/consume"), &ta, json!({ "metric": "volume_month", "amount": 1 })).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS, "101% → 429");
    assert_eq!(r.code(), "QUOTA_EXCEEDED");
    let retry: i64 = r.headers.get("retry-after").unwrap().to_str().unwrap().parse().unwrap();
    assert!(retry > 0);

    let q = app.get(&format!("/v1/tenants/{id}/quota"), &ta).await;
    assert_eq!(q.data()["usage"]["volume_month"], 10);
    assert_eq!(q.data()["levels"]["volume_month"], "exhausted");

    // Warning raised once, exhausted alert raised (events → NT-002 / NT-003).
    let events = app.event_types(&id).await;
    assert_eq!(events.iter().filter(|e| *e == "tenant.quota_warning").count(), 1);
    assert!(events.contains(&"tenant.quota_exhausted".to_string()));
    app.dispatch().await;
    let nts = app.outbox_ids(&id).await;
    assert!(nts.contains(&"NT-002".to_string()) && nts.contains(&"NT-003".to_string()), "{nts:?}");

    // Monthly counters reset in the next cycle.
    let now = app.clock.now_for_tests();
    let next = if now.month() == 12 {
        Utc.with_ymd_and_hms(now.year() + 1, 1, 1, 0, 0, 1)
    } else {
        Utc.with_ymd_and_hms(now.year(), now.month() + 1, 1, 0, 0, 1)
    }
    .unwrap();
    app.clock.set(next);
    let ta2 = app.admin_token(&t).await;
    let r = app.post(&format!("/v1/tenants/{id}/quota/consume"), &ta2, json!({ "metric": "volume_month", "amount": 1 })).await;
    assert_eq!(r.status, StatusCode::OK, "new cycle: {}", r.text);
    assert_eq!(r.data()["usage"], 1);
}

trait NowForTests {
    fn now_for_tests(&self) -> chrono::DateTime<Utc>;
}
impl NowForTests for omni_m01::platform::time::ManualClock {
    fn now_for_tests(&self) -> chrono::DateTime<Utc> {
        use omni_m01::platform::time::Clock;
        self.now()
    }
}

#[tokio::test]
async fn user_quota_uses_rate_limited_message() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let tenant = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&id).unwrap());
    let sa_actor = omni_m01::modules::m01_tenancy::application::Actor {
        user_id: None,
        role: omni_m01::modules::m01_tenancy::application::ActorRole::SuperAdmin,
        tenant_id: None,
        email: None,
        correlation_id: None,
        ip: None,
        user_agent: None,
    };
    use omni_m01::modules::m01_tenancy::domain::quota::QuotaMetric;
    app.state.m01.quotas.set_limits(&sa_actor, tenant, vec![(QuotaMetric::Users, 1, 0.8)]).await.unwrap();
    let r = app.post(&format!("/v1/tenants/{id}/quota/consume"), &ta, json!({ "metric": "users", "amount": 1 })).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(r.code(), "RATE_LIMITED");
    assert_eq!(r.body["error"]["message"], "User quota reached; upgrade plan");
    // Users limit must stay >= 1.
    assert!(app.state.m01.quotas.set_limits(&sa_actor, tenant, vec![(QuotaMetric::Users, 0, 0.8)]).await.is_err());
}

#[tokio::test]
async fn api_rate_limit_per_tenant_is_noisy_neighbour_safe() {
    // R012 / API-006: per-tenant API requests per minute.
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (a, _ta_a) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let (b, ta_b) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let tenant = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&tid(&a)).unwrap());
    let sa_actor = omni_m01::modules::m01_tenancy::application::Actor {
        user_id: None,
        role: omni_m01::modules::m01_tenancy::application::ActorRole::SuperAdmin,
        tenant_id: None,
        email: None,
        correlation_id: None,
        ip: None,
        user_agent: None,
    };
    use omni_m01::modules::m01_tenancy::domain::quota::QuotaMetric;
    app.state.m01.quotas.set_limits(&sa_actor, tenant, vec![(QuotaMetric::ApiRequestsPerMinute, 3, 0.8)]).await.unwrap();
    // Align to the start of a minute so the window does not roll over mid-test.
    let now = {
        use omni_m01::platform::time::Clock;
        app.clock.now()
    };
    app.clock.set(now + Duration::seconds(60 - (now.timestamp() % 60)));
    let ta_a = app.admin_token(&a).await;
    let ta_b2 = app.admin_token(&b).await;
    let _ = ta_b;
    let mut statuses = Vec::new();
    for _ in 0..5 {
        statuses.push(app.get(&format!("/v1/tenants/{}", tid(&a)), &ta_a).await);
    }
    let limited = statuses.iter().filter(|r| r.status == StatusCode::TOO_MANY_REQUESTS).count();
    assert!(limited >= 1, "{:?}", statuses.iter().map(|r| r.status).collect::<Vec<_>>());
    let last = statuses.last().unwrap();
    assert_eq!(last.code(), "RATE_LIMITED");
    assert!(last.headers.get("retry-after").is_some());
    assert!(statuses[0].headers.get("x-ratelimit-limit").is_some());
    // The other tenant is unaffected.
    assert_eq!(app.get(&format!("/v1/tenants/{}", tid(&b)), &ta_b2).await.status, StatusCode::OK);
}

#[tokio::test]
async fn metering_statement_and_reference_feed() {
    // R025
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let r = app.post(&format!("/v1/reference/metering/{id}"), &sa, json!({ "meter": "emails_sent", "amount": 120 })).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    let r = app.post(&format!("/v1/reference/metering/{id}"), &ta, json!({ "meter": "emails_sent", "amount": 1 })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "metering feed is host/system only");
    app.post(&format!("/v1/reference/metering/{id}"), &sa, json!({ "meter": "ai_tokens", "amount": 1 })).await;
    let s = app.get(&format!("/v1/tenants/{id}/usage/statement"), &ta).await;
    assert_eq!(s.status, StatusCode::OK);
    assert_eq!(s.data()["meters"]["emails_sent"], 120);
    assert_eq!(s.data()["meters"]["active_users"], 1);
    assert!(s.data()["meters"]["api_calls"].as_i64().unwrap_or(0) >= 1, "API calls are metered: {}", s.text);
}
