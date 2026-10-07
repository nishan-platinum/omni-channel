//! M01-F01 provisioning: OCC-M01-R001, R008, R013; BR-M01-001; field rules; API-003 idempotency.

use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn create_tenant_returns_201_draft_and_seeds_everything() {
    // OCC-M01-R001, spec §10.6 POST /v1/tenants → 201 {tenant_id, tenant_code, status:'draft'}
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({ "template_code": "tpl-contact-centre" })).await;
    assert_eq!(t["status"], "draft");
    assert_eq!(t["provisioning_status"], "completed");
    assert_eq!(t["isolation_check_status"], "passed");
    assert_eq!(t["storage_strategy"], "shared_row_level");
    assert_eq!(t["isolation_mode"], "row_level");
    let id = tid(&t);
    // tenant_id is a UUID v7 (ADR-0003).
    assert_eq!(uuid::Uuid::parse_str(&id).unwrap().get_version_num(), 7);

    // Seeded config + flags filtered to the plan (FD-008): non-entitled template features are off.
    let cfg = app.get(&format!("/v1/tenants/{id}/config"), &sa).await;
    assert_eq!(cfg.status, StatusCode::OK);
    assert_eq!(cfg.data()["config"]["locale.currency"], "MYR");
    assert_eq!(cfg.data()["feature_flags"]["module.crm_contacts"], true);
    assert_eq!(cfg.data()["feature_flags"]["module.contact_centre"], false, "contact centre is not in the Standard plan");

    // Quotas seeded from plan limits.
    let q = app.get(&format!("/v1/tenants/{id}/quota"), &sa).await;
    assert_eq!(q.data()["limits"]["users"], 50);
    assert_eq!(q.data()["thresholds"]["volume_month"], 0.8);
    assert_eq!(q.data()["usage"]["users"], 1, "initial Tenant Admin counts as a user");

    // Invitation sent (NotificationPort) and tenant.created published (outbox).
    assert!(app.outbox_ids(&id).await.contains(&"NT-M01-INVITE".to_string()));
    assert!(app.event_types(&id).await.contains(&"tenant.created".to_string()));
    assert!(app.audit_actions(&id).await.contains(&"tenant.created".to_string()));
}

#[tokio::test]
async fn duplicate_code_is_conflict_409() {
    // BR-M01-001
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let r = app
        .post("/v1/tenants", &sa, json!({ "name": "Dup", "region": "my-central", "plan_id": PLAN_STANDARD, "primary_admin_email": "x@dup.example", "tenant_code": t["tenant_code"] }))
        .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.code(), "CONFLICT");
    assert_eq!(r.body["error"]["message"], "Tenant code already in use");
}

#[tokio::test]
async fn field_validation_errors() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let bad = app
        .post(
            "/v1/tenants",
            &sa,
            json!({ "name": "", "region": "us-east", "plan_id": PLAN_STANDARD, "primary_admin_email": "nope", "tenant_code": "AB" }),
        )
        .await;
    assert_eq!(bad.status, StatusCode::BAD_REQUEST);
    assert_eq!(bad.code(), "VALIDATION_FAILED");
    let msgs: Vec<String> =
        bad.body["error"]["details"].as_array().unwrap().iter().map(|d| d["message"].as_str().unwrap().to_string()).collect();
    assert!(msgs.contains(&"Name is required (max 120 chars)".to_string()), "{msgs:?}");
    assert!(msgs.contains(&"Unsupported region".to_string()));
    assert!(msgs.contains(&"Valid admin email required".to_string()));
    assert!(msgs.contains(&"Code must be 3-32 lowercase alphanumeric or hyphen".to_string()));
    assert!(bad.body["error"]["correlation_id"].is_string());

    // Unknown and retired plans → NOT_FOUND 'Plan does not exist'.
    for plan in ["01920000-0000-7000-8000-0000000000ff", PLAN_RETIRED] {
        let r = app
            .post("/v1/tenants", &sa, json!({ "name": "X", "region": "my-central", "plan_id": plan, "primary_admin_email": "a@x.example" }))
            .await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text);
        assert_eq!(r.body["error"]["message"], "Plan does not exist");
    }
    // Unknown fields (e.g. a smuggled tenant_id) are rejected.
    let r = app
        .post(
            "/v1/tenants",
            &sa,
            json!({ "name": "X", "plan_id": PLAN_STANDARD, "primary_admin_email": "a@x.example", "tenant_id": "abc" }),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn code_is_suggested_from_name_and_made_unique() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let name = format!("Suggest {}", unique_code("n"));
    let body = json!({ "name": name, "region": "my-central", "plan_id": PLAN_STANDARD, "primary_admin_email": "a@s.example" });
    let a = app.post("/v1/tenants", &sa, body.clone()).await;
    let b = app.post("/v1/tenants", &sa, body).await;
    assert_eq!(a.status, StatusCode::CREATED);
    assert_eq!(b.status, StatusCode::CREATED);
    let (ca, cb) = (a.data()["tenant_code"].as_str().unwrap(), b.data()["tenant_code"].as_str().unwrap());
    assert!(ca.starts_with("suggest-"));
    assert_ne!(ca, cb);
    assert!(cb.ends_with("-2"));
}

#[tokio::test]
async fn hierarchy_depth_and_cycles() {
    // parent_tenant_id: max depth 3, no cycles → CONFLICT 'Circular or too-deep hierarchy'
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let a = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let b = app.create_tenant(&sa, PLAN_STANDARD, json!({ "parent_tenant_id": tid(&a) })).await;
    let c = app.create_tenant(&sa, PLAN_STANDARD, json!({ "parent_tenant_id": tid(&b) })).await;
    assert_eq!(c["parent_tenant_id"], json!(tid(&b)));
    let d = app
        .post(
            "/v1/tenants",
            &sa,
            json!({ "name": "Too deep", "plan_id": PLAN_STANDARD, "primary_admin_email": "d@d.example", "parent_tenant_id": tid(&c) }),
        )
        .await;
    assert_eq!(d.status, StatusCode::CONFLICT);
    assert_eq!(d.body["error"]["message"], "Circular or too-deep hierarchy");

    // Re-parenting A under C would create a cycle.
    let sa_actor = omni_m01::modules::m01_tenancy::application::Actor {
        user_id: None,
        role: omni_m01::modules::m01_tenancy::application::ActorRole::SuperAdmin,
        tenant_id: None,
        email: None,
        correlation_id: None,
        ip: None,
        user_agent: None,
    };
    let aid = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&tid(&a)).unwrap());
    let cid = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&tid(&c)).unwrap());
    let err = app.state.m01.lifecycle.update_details(&sa_actor, aid, "A", None, Some(Some(cid))).await.unwrap_err();
    assert_eq!(err.message, "Circular or too-deep hierarchy");
}

#[tokio::test]
async fn idempotency_key_replays_and_detects_misuse() {
    // API-003
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let key = unique_code("idem");
    let code = unique_code("idem");
    let body = json!({ "name": "Idem Org", "region": "my-central", "plan_id": PLAN_STANDARD, "primary_admin_email": "a@idem.example", "tenant_code": code });
    let r1 = app.send(Method::POST, "/v1/tenants", Some(&sa), Some(body.clone()), &[("idempotency-key", &key)]).await;
    let r2 = app.send(Method::POST, "/v1/tenants", Some(&sa), Some(body.clone()), &[("idempotency-key", &key)]).await;
    assert_eq!(r1.status, StatusCode::CREATED);
    assert_eq!(r2.status, StatusCode::CREATED);
    assert_eq!(r1.data()["tenant_id"], r2.data()["tenant_id"], "replayed original response");
    let mut other = body;
    other["name"] = json!("Changed");
    let r3 = app.send(Method::POST, "/v1/tenants", Some(&sa), Some(other), &[("idempotency-key", &key)]).await;
    assert_eq!(r3.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn storage_strategy_follows_tier_and_residency() {
    // OCC-M01-R008 + residency pinning (UJ-19 E2)
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let p = app.create_tenant(&sa, PLAN_PREMIUM, json!({})).await;
    assert_eq!(p["storage_strategy"], "schema_per_tenant");
    let r = app.create_tenant(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-pg-my-central" })).await;
    assert_eq!(r["storage_strategy"], "dedicated_database");
    assert_eq!(r["isolation_mode"], "database_per_tenant");
    let missing =
        app.post("/v1/tenants", &sa, json!({ "name": "Reg", "plan_id": PLAN_REGULATED, "primary_admin_email": "a@r.example" })).await;
    assert_eq!(missing.status, StatusCode::BAD_REQUEST);
    let wrong_region = app
        .post("/v1/tenants", &sa, json!({ "name": "Reg", "region": "sg", "plan_id": PLAN_REGULATED, "primary_admin_email": "a@r.example", "db_target": "dedicated-pg-my-central" }))
        .await;
    assert_eq!(wrong_region.status, StatusCode::BAD_REQUEST);
    assert!(wrong_region.text.contains("residency"));
}

#[tokio::test]
async fn template_provisioning_is_fast_and_recorded() {
    // OCC-M01-R013: provisioning from a template, automated, ≤ 30 minutes.
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_PREMIUM, json!({ "template_code": "tpl-contact-centre" })).await;
    assert!(t["provisioning"]["duration_ms"].as_i64().unwrap() < 30 * 60 * 1000);
    let id = omni_m01::modules::m01_tenancy::domain::TenantId(uuid::Uuid::parse_str(&tid(&t)).unwrap());
    let run = app.state.m01.deps.provisioning.latest_run(&omni_m01::platform::db::AccessScope::Platform, id).await.unwrap().unwrap();
    let steps: Vec<_> = run.steps.iter().map(|s| s.step.clone()).collect();
    assert!(steps.contains(&"identity.initial_admin".into()));
    assert!(steps.contains(&"data_store.provision".into()));
    assert!(steps.contains(&"template.roles_teams:contact-centre".into()));
    assert!(steps.contains(&"isolation.smoke_test".into()));
    assert!(run.steps.iter().all(|s| s.status == "completed"));
}

#[tokio::test]
async fn tenant_admin_cannot_create_tenants() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (_t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let r = app.post("/v1/tenants", &ta, json!({ "name": "X", "plan_id": PLAN_STANDARD, "primary_admin_email": "a@x.example" })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
}
