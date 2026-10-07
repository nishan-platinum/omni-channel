//! Break-glass support access: OCC-M01-R024; M18-R009 AT "access without approval denied and logged".

use axum::http::StatusCode;
use chrono::Duration;
use serde_json::json;

use omni_m01::modules::m01_tenancy::domain::support::GrantRequest;

use crate::common::*;

#[tokio::test]
async fn host_needs_tenant_approved_time_boxed_grant() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let tenant = tenant_id(&t);
    let host = sa_actor();

    // No grant → denied and logged as a security event.
    let denied = app.state.m01.support.open_support_view(&host, tenant).await.unwrap_err();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert!(app.audit_actions(&tid(&t)).await.contains(&"support_access.denied".to_string()));

    let g = app
        .state
        .m01
        .support
        .request(
            &host,
            tenant,
            GrantRequest {
                reason: "Investigate failed webhooks".into(),
                incident_ref: None,
                duration_minutes: None,
                named_approver_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(g.duration_minutes, 240, "default 4 h");
    // Pending grant is not usable.
    assert!(app.state.m01.support.open_support_view(&host, tenant).await.is_err());
    // Host cannot approve its own request.
    assert!(app.state.m01.support.approve(&host, g.id, None).await.is_err());

    let admin = admin_user_id(&app, &t).await;
    let approved = app.state.m01.support.approve(&ta_actor(&t, admin), g.id, Some("ok")).await.unwrap();
    assert!(approved.expires_at.is_some());
    let view = app.state.m01.support.open_support_view(&host, tenant).await.unwrap();
    assert!(!view.rows.is_empty(), "support view reads the tenant data plane");
    assert!(view.rows.iter().all(|r| r.tenant_id == tid(&t)));
    let actions = app.audit_actions(&tid(&t)).await;
    assert!(actions.contains(&"support_access.approved".to_string()) && actions.contains(&"support_access.used".to_string()));

    // Time box: after 4 h the grant expires.
    app.clock.advance(Duration::hours(4) + Duration::minutes(1));
    assert_eq!(app.state.m01.support.open_support_view(&host, tenant).await.unwrap_err().status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn regulated_tenants_need_incident_and_named_approver() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-pg-my-central" })).await;
    let tenant = tenant_id(&t);
    let host = sa_actor();
    let missing = app
        .state
        .m01
        .support
        .request(
            &host,
            tenant,
            GrantRequest { reason: "Investigate incident".into(), incident_ref: None, duration_minutes: Some(60), named_approver_id: None },
        )
        .await
        .unwrap_err();
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    let admin = admin_user_id(&app, &t).await;
    let g = app
        .state
        .m01
        .support
        .request(
            &host,
            tenant,
            GrantRequest {
                reason: "Investigate incident".into(),
                incident_ref: Some("INC-42".into()),
                duration_minutes: Some(60),
                named_approver_id: Some(admin),
            },
        )
        .await
        .unwrap();
    // One grant per incident.
    let dup = app
        .state
        .m01
        .support
        .request(
            &host,
            tenant,
            GrantRequest {
                reason: "Investigate incident".into(),
                incident_ref: Some("INC-42".into()),
                duration_minutes: Some(60),
                named_approver_id: Some(admin),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(dup.status(), StatusCode::CONFLICT);
    // A different user (not the named approver) cannot approve.
    assert!(app.state.m01.support.approve(&ta_actor(&t, uuid::Uuid::now_v7()), g.id, None).await.is_err());
    let ok = app.state.m01.support.approve(&ta_actor(&t, admin), g.id, None).await.unwrap();
    assert_eq!(ok.expires_at.unwrap() - ok.starts_at.unwrap(), Duration::minutes(60));
    // Tenant can revoke.
    app.state.m01.support.revoke(&ta_actor(&t, admin), tenant, g.id).await.unwrap();
    assert!(app.state.m01.support.open_support_view(&host, tenant).await.is_err());
}

#[tokio::test]
async fn host_export_download_needs_grant() {
    // R016 + R024: exports contain tenant data.
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let tenant = tenant_id(&t);
    let host = sa_actor();
    let export = app.state.m01.offboarding.generate_export(&host, tenant, "manual").await.unwrap();
    assert_eq!(app.state.m01.offboarding.download_export(&host, tenant, export.id).await.unwrap_err().status(), StatusCode::FORBIDDEN);
    let admin = admin_user_id(&app, &t).await;
    let (_, bytes) = app.state.m01.offboarding.download_export(&ta_actor(&t, admin), tenant, export.id).await.unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(doc["tenant_code"], t["tenant_code"]);
    assert!(doc["control_plane"]["audit_log"].as_array().is_some_and(|a| !a.is_empty()));
}
