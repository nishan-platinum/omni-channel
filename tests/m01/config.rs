//! M01-F04 configuration & feature flags: OCC-M01-R004, R015, R020, R021; BR-M01-003; FD-008.

use axum::http::StatusCode;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn config_get_and_patch_contract() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let r = app
        .patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "config": { "locale.timezone": "Asia/Singapore", "security.session_idle_timeout_minutes": 45 }, "feature_flags": { "module.crm_sales": false } }))
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert_eq!(r.data()["config"]["locale.timezone"], "Asia/Singapore");
    assert_eq!(r.data()["config"]["security.session_idle_timeout_minutes"], 45);
    assert_eq!(r.data()["feature_flags"]["module.crm_sales"], false);
    let actions = app.audit_actions(&id).await;
    assert!(actions.contains(&"tenant.config_changed".to_string()));
    assert!(actions.contains(&"tenant.feature_changed".to_string()));
    let events = app.event_types(&id).await;
    assert!(events.contains(&"tenant.config_changed".to_string()) && events.contains(&"tenant.feature_changed".to_string()));
}

#[tokio::test]
async fn unknown_key_and_bad_types_are_rejected() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let r = app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "config": { "channel.unknown.thing": true } })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.body["error"]["details"][0]["message"], "Unknown config key");
    let r =
        app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "config": { "security.session_idle_timeout_minutes": 99999 } })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r =
        app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "config": { "security.ip_allowlist": ["10.0.0.0/8", "bad"] } })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "config": { "locale.allowed_languages": ["ms"] } })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "default language must stay within the allowed set");
}

#[tokio::test]
async fn features_cannot_exceed_plan_entitlement() {
    // BR-M01-003: enable non-entitled feature → 403 'Feature not included in your plan'
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    for token in [&ta, &sa] {
        let r =
            app.patch(&format!("/v1/tenants/{id}/config"), token, json!({ "feature_flags": { "module.automated_marketing": true } })).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text);
        assert_eq!(r.body["error"]["message"], "Feature not included in your plan");
    }
    let cfg = app.get(&format!("/v1/tenants/{id}/config"), &ta).await;
    assert_eq!(cfg.data()["feature_flags"]["module.automated_marketing"], false);
}

#[tokio::test]
async fn feature_dependencies_and_super_admin_only_keys() {
    // F04 step 3: WhatsApp requires a verified BSP connection (set by the host).
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_PREMIUM, json!({})).await;
    let id = tid(&t);
    let r = app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "feature_flags": { "channel.whatsapp": true } })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.text.contains("verified BSP connection"));
    let r = app
        .patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "config": { "channel.whatsapp.bsp_connection_verified": true } }))
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "TA cannot set host-only keys");
    let r = app
        .patch(&format!("/v1/tenants/{id}/config"), &sa, json!({ "config": { "channel.whatsapp.bsp_connection_verified": true } }))
        .await;
    assert_eq!(r.status, StatusCode::OK);
    // Channel quota (7 in Premium): disable one first so enabling WhatsApp stays within it.
    let r = app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "feature_flags": { "channel.whatsapp": true } })).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
}

#[tokio::test]
async fn channel_count_is_quota_bound() {
    // R005 "channels" limit: exceeding it returns 429 QUOTA_EXCEEDED.
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({ "template_code": "tpl-minimal" })).await;
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
    app.state.m01.quotas.set_limits(&sa_actor, tenant, vec![(QuotaMetric::Channels, 1, 0.8)]).await.unwrap();
    let ok = app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "feature_flags": { "channel.email": true } })).await;
    assert_eq!(ok.status, StatusCode::OK, "{}", ok.text);
    let blocked = app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "feature_flags": { "channel.sms": true } })).await;
    assert_eq!(blocked.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(blocked.code(), "QUOTA_EXCEEDED");
    assert!(blocked.headers.get("retry-after").is_some());
}

#[tokio::test]
async fn analytics_opt_out_only_for_regulated() {
    // R027
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (std_t, std_ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let r = app
        .patch(&format!("/v1/tenants/{}/config", tid(&std_t)), &std_ta, json!({ "config": { "analytics.cross_tenant_opt_out": true } }))
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let (reg, reg_ta) = app.active_tenant_with_admin(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-pg-my-central" })).await;
    let r = app
        .patch(&format!("/v1/tenants/{}/config", tid(&reg)), &reg_ta, json!({ "config": { "analytics.cross_tenant_opt_out": true } }))
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
}

#[tokio::test]
async fn tenant_security_policy_drives_bootstrap_auth() {
    // R021 boundary: session idle timeout & password length come from tenant configuration.
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    app.patch(&format!("/v1/tenants/{id}/config"), &sa, json!({ "config": { "security.password_min_length": 20 } })).await;
    let uid = uuid::Uuid::parse_str(&id).unwrap();
    let (_, token) = app.state.auth.invite_tenant_admin(uid, t["primary_admin_email"].as_str().unwrap(), "TA").await.unwrap();
    let err = app.state.auth.accept_invitation(token.as_deref().unwrap(), "short-but-12", "short-but-12").await.unwrap_err();
    assert!(err.message.contains("20-128"), "{}", err.message);
}
