//! M01-F06 branding: OCC-M01-R006, R019; BR-M01-005; FD-010/011; DOMAIN_NOT_VERIFIED.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn colours_validated_and_saved() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let r = app.patch(&format!("/v1/tenants/{id}/branding"), &ta, json!({ "primary_color": "red" })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.body["error"]["details"][0]["message"], "Colour must be a 6-digit hex");
    let r = app
        .patch(
            &format!("/v1/tenants/{id}/branding"),
            &ta,
            json!({ "primary_color": "#112233", "secondary_color": "#aabbcc", "email_footer": "Acme · KL" }),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert_eq!(r.data()["branding"]["primary_color"], "#112233");
    assert_eq!(r.data()["branding"]["secondary_color"], "#AABBCC");
    assert!(app.audit_actions(&id).await.contains(&"tenant.branding_changed".to_string()));
    let r = app.patch(&format!("/v1/tenants/{id}/branding"), &ta, json!({ "logo_url": "https://evil.example/x.png" })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "logos must be uploaded");
}

#[tokio::test]
async fn custom_domain_requires_verification_before_serving() {
    // BR-M01-005 + FD-010
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let domain = format!("care.{}.verified.test", unique_code("d"));
    let r = app.patch(&format!("/v1/tenants/{id}/branding"), &ta, json!({ "custom_domain": domain, "custom_domain_active": true })).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "activation before verification: {}", r.text);
    assert_eq!(r.body["error"]["message"], "Domain not verified");
    let r = app.patch(&format!("/v1/tenants/{id}/branding"), &ta, json!({ "custom_domain": domain })).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.data()["verification_status"]["custom_domain_status"], "pending");
    assert_eq!(r.data()["dns_records"][0]["record_type"], "CNAME");

    // Unverified domain → not served (421).
    let req = |host: &str| Request::get("/login").header(header::HOST, host).body(Body::empty()).unwrap();
    assert_eq!(app.raw(req(&domain)).await.status, StatusCode::MISDIRECTED_REQUEST);

    let tenant = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&id).unwrap());
    let ta_actor = omni_m01::modules::m01_tenancy::application::Actor {
        user_id: None,
        role: omni_m01::modules::m01_tenancy::application::ActorRole::TenantAdmin,
        tenant_id: Some(tenant),
        email: None,
        correlation_id: None,
        ip: None,
        user_agent: None,
    };
    let v = app.state.m01.branding.verify_domain(&ta_actor, tenant).await.unwrap();
    assert_eq!(v.verification_status.custom_domain_status, "verified");
    let r = app
        .patch(&format!("/v1/tenants/{id}/branding"), &ta, json!({ "custom_domain_active": true, "login_message": "Welcome to Acme care" }))
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    let served = app.raw(req(&domain)).await;
    assert_eq!(served.status, StatusCode::OK);
    assert!(served.text.contains("Welcome to Acme care"), "branded login page");

    // Another tenant cannot claim the same domain.
    let (t2, ta2) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let r = app.patch(&format!("/v1/tenants/{}/branding", tid(&t2)), &ta2, json!({ "custom_domain": domain })).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["error"]["message"], "Domain already claimed");

    // A domain outside the simulator's verified suffix fails verification.
    let bad = format!("care.{}.example", unique_code("d"));
    app.patch(&format!("/v1/tenants/{id}/branding"), &ta, json!({ "custom_domain": bad })).await;
    let v = app.state.m01.branding.verify_domain(&ta_actor, tenant).await.unwrap();
    assert_eq!(v.verification_status.custom_domain_status, "failed");
    assert!(!v.branding.custom_domain_active);
}

#[tokio::test]
async fn email_sender_requires_spf_dkim_verification() {
    // FD-011, DOMAIN_NOT_VERIFIED (403)
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let domain = format!("{}.verified.test", unique_code("mail"));
    let r = app.patch(&format!("/v1/tenants/{id}/branding"), &ta, json!({ "email_from": format!("care@{domain}") })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(r.code(), "DOMAIN_NOT_VERIFIED");
    let tenant = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&id).unwrap());
    let ta_actor = omni_m01::modules::m01_tenancy::application::Actor {
        user_id: None,
        role: omni_m01::modules::m01_tenancy::application::ActorRole::TenantAdmin,
        tenant_id: Some(tenant),
        email: None,
        correlation_id: None,
        ip: None,
        user_agent: None,
    };
    app.state.m01.branding.add_sender_domain(&ta_actor, tenant, &domain).await.unwrap();
    let v = app.state.m01.branding.verify_sender_domain(&ta_actor, tenant, &domain).await.unwrap();
    assert!(v.sender_domains.iter().any(|s| s.status == "verified"));
    let r = app.patch(&format!("/v1/tenants/{id}/branding"), &ta, json!({ "email_from": format!("care@{domain}") })).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert_eq!(r.data()["verification_status"]["email_from_status"], "verified");
}

#[tokio::test]
async fn logo_upload_validates_content_and_is_served_safely() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let tenant = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&id).unwrap());
    let ta_actor = omni_m01::modules::m01_tenancy::application::Actor {
        user_id: None,
        role: omni_m01::modules::m01_tenancy::application::ActorRole::TenantAdmin,
        tenant_id: Some(tenant),
        email: None,
        correlation_id: None,
        ip: None,
        user_agent: None,
    };
    let evil = br#"<svg xmlns="http://www.w3.org/2000/svg" onload="alert(1)"></svg>"#;
    assert!(app.state.m01.branding.upload_logo(&ta_actor, tenant, evil).await.is_err());
    let too_big = vec![0u8; 2 * 1024 * 1024 + 1];
    assert!(app.state.m01.branding.upload_logo(&ta_actor, tenant, &too_big).await.is_err());
    let svg = br##"<svg xmlns="http://www.w3.org/2000/svg" width="4" height="4"><rect width="4" height="4" fill="#0B2130"/></svg>"##;
    let v = app.state.m01.branding.upload_logo(&ta_actor, tenant, svg).await.unwrap();
    assert_eq!(v.logo_url.as_deref(), Some(format!("/assets/tenants/{id}/logo").as_str()));
    let served = app.raw(Request::get(format!("/assets/tenants/{id}/logo")).body(Body::empty()).unwrap()).await;
    assert_eq!(served.status, StatusCode::OK);
    assert_eq!(served.headers.get(header::CONTENT_TYPE).unwrap(), "image/svg+xml");
    assert!(served.headers.get(header::CONTENT_SECURITY_POLICY).unwrap().to_str().unwrap().contains("sandbox"));
}
