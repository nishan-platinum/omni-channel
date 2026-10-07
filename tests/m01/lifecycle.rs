//! M01-F03 lifecycle: OCC-M01-R003, R014, R016; BR-M01-002; state machine §10.4.

use axum::http::StatusCode;
use chrono::Duration;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn full_lifecycle_with_side_effects() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    let get = app.get(&format!("/v1/tenants/{id}"), &sa).await;
    assert_eq!(get.data()["status"], "active");
    assert!(get.data()["activated_at"].is_string());

    // Suspend: reason required (400), then suspended; TA session revoked and login refused (403).
    let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "suspended" })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = app
        .patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "suspended", "reason": "Non-payment of invoice INV-2026-0042" }))
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert_eq!(r.data()["status"], "suspended");
    assert!(r.data()["updated_at"].is_string());
    let blocked = app.get(&format!("/v1/tenants/{id}"), &ta).await;
    assert_eq!(blocked.status, StatusCode::UNAUTHORIZED, "existing API session revoked on suspension");
    let login = app.token(t["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, Some(t["tenant_code"].as_str().unwrap())).await;
    assert_eq!(login.status, StatusCode::FORBIDDEN, "BR-M01-002: login to suspended tenant → 403");
    assert_eq!(login.code(), "TENANT_SUSPENDED");
    app.dispatch().await;
    assert!(app.outbox_ids(&id).await.contains(&"NT-001".to_string()), "NT-001 sent to Tenant Admin");

    // Reinstate.
    let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "active" })).await;
    assert_eq!(r.status, StatusCode::OK);
    let login = app.token(t["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, Some(t["tenant_code"].as_str().unwrap())).await;
    assert_eq!(login.status, StatusCode::OK);
    let ta = login.data()["access_token"].as_str().unwrap().to_string();

    // Grace: read-only, reason required, export generated, TA may still log in.
    let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "grace", "reason": "Offboarding requested" })).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    let ro = app.patch(&format!("/v1/tenants/{id}/config"), &ta, json!({ "config": { "locale.currency": "SGD" } })).await;
    assert_eq!(ro.status, StatusCode::CONFLICT, "grace is read-only: {}", ro.text);
    assert_eq!(app.get(&format!("/v1/tenants/{id}"), &ta).await.status, StatusCode::OK, "read access during grace");

    // Recovery before expiry, then grace again → terminated → purged after retention.
    assert_eq!(app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "active" })).await.status, StatusCode::OK);
    assert_eq!(
        app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "grace", "reason": "Non-payment" })).await.status,
        StatusCode::OK
    );
    let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "terminated" })).await;
    assert_eq!(r.status, StatusCode::OK);
    let term = app.get(&format!("/v1/tenants/{id}"), &sa).await;
    assert!(term.data()["terminated_at"].is_string());
    assert!(term.data()["purge_after"].is_string());
    let login = app.token(t["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, Some(t["tenant_code"].as_str().unwrap())).await;
    assert_eq!(login.status, StatusCode::FORBIDDEN, "terminated tenant denied");

    // Every transition published its domain event (checked before the purge removes delivered ones).
    let events = app.event_types(&id).await;
    for e in ["tenant.activated", "tenant.suspended", "tenant.reinstated", "tenant.grace_started", "tenant.recovered", "tenant.terminated"]
    {
        assert!(events.contains(&e.to_string()), "missing {e} in {events:?}");
    }

    // Purge only after the retention window (1 h in tests).
    let early = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "purged" })).await;
    assert_eq!(early.status, StatusCode::CONFLICT, "{}", early.text);
    app.clock.advance(Duration::hours(2));
    let sa = app.sa_token().await;
    let purged = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "purged" })).await;
    assert_eq!(purged.status, StatusCode::OK, "{}", purged.text);
    let after = app.get(&format!("/v1/tenants/{id}"), &sa).await;
    assert_eq!(after.data()["status"], "purged");
    assert_eq!(after.data()["name"], "[purged]");
    assert_eq!(after.data()["tenant_code"], t["tenant_code"], "code stays reserved (BR-M01-001)");
    let cfg = app.get(&format!("/v1/tenants/{id}/config"), &sa).await;
    assert_eq!(cfg.data()["feature_flags"]["module.crm_contacts"], false, "config purged");

    // Every transition audited with reason and actor; events published.
    let actions = app.audit_actions(&id).await;
    assert!(actions.iter().filter(|a| *a == "tenant.status_changed").count() >= 7, "{actions:?}");
    assert!(actions.contains(&"tenant.purged".to_string()));
    // Delivered events are removed by the purge; the purge event itself is pending delivery.
    assert!(app.event_types(&id).await.contains(&"tenant.purged".to_string()));
    // Destruction certificate with a manifest digest.
    let tid_ = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&id).unwrap());
    let cert = app.state.m01.deps.exports.certificate(&omni_m01::platform::db::AccessScope::Platform, tid_).await.unwrap().unwrap();
    assert_eq!(cert.manifest_sha256.len(), 64);
    assert!(cert.manifest["control_plane_rows_deleted"]["tenantadm.tenant_configs"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn illegal_transitions_are_conflicts() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    for to in ["suspended", "grace", "terminated", "purged", "draft"] {
        let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": to, "reason": "x" })).await;
        assert_eq!(r.status, StatusCode::CONFLICT, "draft → {to}: {}", r.text);
        assert_eq!(r.body["error"]["message"], "Illegal status transition");
    }
    app.activate(&sa, &id).await;
    for to in ["terminated", "purged", "draft", "active"] {
        let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": to, "reason": "x" })).await;
        assert_eq!(r.status, StatusCode::CONFLICT, "active → {to}");
    }
    let bad = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "archived" })).await;
    assert_eq!(bad.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn activation_requires_passed_isolation_test() {
    // UJ-19 E1
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    sqlx::query("UPDATE tenantadm.tenants SET isolation_check_status = 'failed' WHERE id = $1::uuid")
        .bind(&id)
        .execute(&app.state.db.owner)
        .await
        .unwrap();
    let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "active" })).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    // Re-running the smoke test turns it green and unblocks activation.
    let rerun = app.post(&format!("/v1/tenants/{id}/isolation-check"), &sa, json!({})).await;
    assert_eq!(rerun.status, StatusCode::OK, "{}", rerun.text);
    assert_eq!(rerun.data()["passed"], true);
    app.activate(&sa, &id).await;
}

#[tokio::test]
async fn tenant_admin_cannot_change_status() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let r = app.patch(&format!("/v1/tenants/{}/status", tid(&t)), &ta, json!({ "status": "suspended", "reason": "self" })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn legal_hold_blocks_purge_and_scheduler_expires_grace() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "grace", "reason": "offboarding" })).await;
    let sa_actor = omni_m01::modules::m01_tenancy::application::Actor {
        user_id: None,
        role: omni_m01::modules::m01_tenancy::application::ActorRole::SuperAdmin,
        tenant_id: None,
        email: None,
        correlation_id: None,
        ip: None,
        user_agent: None,
    };
    let tenant = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&id).unwrap());
    app.state.m01.lifecycle.set_legal_hold(&sa_actor, tenant, true, Some("litigation")).await.unwrap();

    // Grace period (24 h in tests) expires → the scheduler terminates.
    app.clock.advance(Duration::hours(25));
    app.state.m01.lifecycle.run_scheduler_tick_for(Some(&[tenant])).await.unwrap();
    let sa = app.sa_token().await;
    assert_eq!(app.get(&format!("/v1/tenants/{id}"), &sa).await.data()["status"], "terminated");
    // Recovery after expiry is impossible (terminated → active is illegal).
    assert_eq!(app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "active" })).await.status, StatusCode::CONFLICT);

    // Retention elapsed but legal hold → scheduler and manual purge blocked.
    app.clock.advance(Duration::hours(2));
    app.state.m01.lifecycle.run_scheduler_tick_for(Some(&[tenant])).await.unwrap();
    let sa = app.sa_token().await;
    assert_eq!(app.get(&format!("/v1/tenants/{id}"), &sa).await.data()["status"], "terminated");
    let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "purged" })).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert!(r.text.contains("legal hold"));

    // Released → auto-purge on the next tick.
    app.state.m01.lifecycle.set_legal_hold(&sa_actor, tenant, false, None).await.unwrap();
    app.state.m01.lifecycle.run_scheduler_tick_for(Some(&[tenant])).await.unwrap();
    assert_eq!(app.get(&format!("/v1/tenants/{id}"), &sa).await.data()["status"], "purged");
}
