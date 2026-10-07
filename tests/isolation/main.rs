//! Tenant-isolation suite (OCC-M01-R002, R007, R009; FR-TST-104; SEC-102). RELEASE BLOCKER:
//! a failure here is never "flaky" — fix the code.
#[path = "../common/mod.rs"]
mod common;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use serde_json::json;
use sqlx::Row;

use common::*;
use omni_m01::platform::db::{scoped_tx, AccessScope};

struct Pair {
    app: TestApp,
    sa: String,
    a: serde_json::Value,
    ta_a: String,
    b: serde_json::Value,
    ta_b: String,
}

async fn pair() -> Pair {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (a, ta_a) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let (b, ta_b) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    Pair { app, sa, a, ta_a, b, ta_b }
}

#[tokio::test]
async fn tenant_a_cannot_read_tenant_b_via_api() {
    let p = pair().await;
    let b = tid(&p.b);
    for path in [
        format!("/v1/tenants/{b}"),
        format!("/v1/tenants/{b}/config"),
        format!("/v1/tenants/{b}/quota"),
        format!("/v1/tenants/{b}/usage/statement"),
    ] {
        let r = p.app.get(&path, &p.ta_a).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{path}: {}", r.text);
        assert!(!r.text.contains(p.b["tenant_code"].as_str().unwrap()), "no data leaked: {path}");
    }
    // The attempt is recorded as a security audit event in A's audit trail (M01-F02 step 4).
    let actions = p.app.audit_actions(&tid(&p.a)).await;
    assert!(actions.contains(&"security.cross_tenant_attempt".to_string()), "{actions:?}");
}

#[tokio::test]
async fn tenant_a_cannot_update_or_delete_tenant_b() {
    let p = pair().await;
    let b = tid(&p.b);
    let r = p.app.patch(&format!("/v1/tenants/{b}/config"), &p.ta_a, json!({ "config": { "locale.currency": "USD" } })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = p.app.patch(&format!("/v1/tenants/{b}/branding"), &p.ta_a, json!({ "primary_color": "#000000" })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = p.app.patch(&format!("/v1/tenants/{b}/status"), &p.ta_a, json!({ "status": "suspended", "reason": "attack" })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = p.app.post(&format!("/v1/tenants/{b}/quota/consume"), &p.ta_a, json!({ "metric": "volume_month", "amount": 1 })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    // "Delete" of a tenant exists only as host draft-discard/purge; tenant users cannot reach it.
    let (session, csrf) =
        p.app.browser_login(p.a["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, Some(p.a["tenant_code"].as_str().unwrap())).await;
    let r = p.app.form(&format!("/admin/tenants/{b}/discard"), &session, &[("_csrf", &csrf)]).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    // B is untouched.
    let cfg = p.app.get(&format!("/v1/tenants/{b}/config"), &p.ta_b).await;
    assert_eq!(cfg.data()["config"]["locale.currency"], "MYR");
    assert_eq!(p.app.get(&format!("/v1/tenants/{b}"), &p.sa).await.data()["status"], "active");
}

#[tokio::test]
async fn tenant_id_in_request_never_overrides_session_tenant() {
    let p = pair().await;
    let b = tid(&p.b);
    // JSON body: unknown field rejected; B unchanged.
    let r = p
        .app
        .patch(&format!("/v1/tenants/{}/config", tid(&p.a)), &p.ta_a, json!({ "tenant_id": b, "config": { "locale.currency": "USD" } }))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    // Query string and form fields on Tenant Admin pages are ignored: the page shows A.
    let (session, csrf) =
        p.app.browser_login(p.a["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, Some(p.a["tenant_code"].as_str().unwrap())).await;
    let page = p.app.page(&format!("/tenant?tenant_id={b}"), &session).await;
    assert_eq!(page.status, StatusCode::OK);
    assert!(page.text.contains(p.a["tenant_code"].as_str().unwrap()));
    assert!(!page.text.contains(p.b["tenant_code"].as_str().unwrap()));
    let r = p.app.form("/tenant/config", &session, &[("_csrf", &csrf), ("tenant_id", &b), ("cfg:locale.currency", "USD")]).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    assert_eq!(p.app.get(&format!("/v1/tenants/{b}/config"), &p.ta_b).await.data()["config"]["locale.currency"], "MYR", "B untouched");
    assert_eq!(
        p.app.get(&format!("/v1/tenants/{}/config", tid(&p.a)), &p.ta_a).await.data()["config"]["locale.currency"],
        "USD",
        "A changed"
    );
}

#[tokio::test]
async fn manipulated_record_ids_do_not_leak() {
    let p = pair().await;
    let host = sa_actor();
    // B has a baseline, an export and a support grant.
    let b_admin = admin_user_id(&p.app, &p.b).await;
    let baseline = p.app.state.m01.baselines.export(&ta_actor(&p.b, b_admin), tenant_id(&p.b), "b").await.unwrap();
    let export = p.app.state.m01.offboarding.generate_export(&host, tenant_id(&p.b), "manual").await.unwrap();
    let grant = p
        .app
        .state
        .m01
        .support
        .request(
            &host,
            tenant_id(&p.b),
            omni_m01::modules::m01_tenancy::domain::support::GrantRequest {
                reason: "incident for B".into(),
                incident_ref: None,
                duration_minutes: None,
                named_approver_id: None,
            },
        )
        .await
        .unwrap();
    let (session, csrf) =
        p.app.browser_login(p.a["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, Some(p.a["tenant_code"].as_str().unwrap())).await;
    let r = p.app.page(&format!("/tenant/baselines/{}/download", baseline.id), &session).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "B's baseline id via A's session");
    let r = p.app.page(&format!("/tenant/offboarding/exports/{}", export.id), &session).await;
    assert_ne!(r.status, StatusCode::OK);
    assert!(!r.text.contains("control_plane"));
    let r = p.app.form(&format!("/tenant/support/{}/approve", grant.id), &session, &[("_csrf", &csrf), ("note", "")]).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    let g = p.app.state.m01.deps.grants.get(&AccessScope::Platform, grant.id).await.unwrap().unwrap();
    assert_eq!(g.status.as_str(), "requested", "A cannot approve B's grant");
}

#[tokio::test]
async fn direct_repository_access_is_scoped() {
    let p = pair().await;
    let deps = &p.app.state.m01.deps;
    let a = AccessScope::Tenant(tenant_id(&p.a).0);
    assert!(deps.tenants.get(&a, tenant_id(&p.b)).await.unwrap().is_none());
    assert!(deps.configs.load(&a, tenant_id(&p.b)).await.unwrap().config.is_empty());
    assert!(deps.quotas.list(&a, tenant_id(&p.b)).await.unwrap().is_empty());
    assert!(deps.connections.get(&a, tenant_id(&p.b)).await.unwrap().is_none());
    assert!(deps.branding.get(&a, tenant_id(&p.b)).await.is_err());
    // Tenant-scoped list shows only the own tenant.
    let page = deps.tenants.list(&a, &Default::default()).await.unwrap();
    assert!(page.items.iter().all(|t| t.id == tenant_id(&p.a)));
}

#[tokio::test]
async fn postgres_rls_blocks_cross_tenant_access_with_non_bypass_role() {
    let p = pair().await;
    let pool = &p.app.state.db.app; // role crm_app
    let attrs = sqlx::query("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user").fetch_one(pool).await.unwrap();
    assert!(!attrs.get::<bool, _>("rolsuper") && !attrs.get::<bool, _>("rolbypassrls"), "runtime role must not bypass RLS");
    let (a, b) = (tenant_id(&p.a).0, tenant_id(&p.b).0);

    // Missing tenant context fails closed.
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM tenantadm.tenant_configs").fetch_one(pool).await.unwrap();
    assert_eq!(n, 0);
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM tenant_data.isolation_canaries").fetch_one(pool).await.unwrap();
    assert_eq!(n, 0);

    // Tenant A context: B's rows invisible; writing B's rows rejected.
    let mut tx = scoped_tx(pool, &AccessScope::Tenant(a)).await.unwrap();
    let n: i64 =
        sqlx::query_scalar("SELECT count(*) FROM tenantadm.tenant_configs WHERE tenant_id = $1").bind(b).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(n, 0);
    let own: i64 =
        sqlx::query_scalar("SELECT count(*) FROM tenantadm.tenant_configs WHERE tenant_id = $1").bind(a).fetch_one(&mut *tx).await.unwrap();
    assert!(own > 0);
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM tenant_data.isolation_canaries WHERE tenant_id = $1")
        .bind(b)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(n, 0);
    tx.rollback().await.unwrap();
    let mut tx = scoped_tx(pool, &AccessScope::Tenant(a)).await.unwrap();
    let w = sqlx::query("INSERT INTO tenantadm.tenant_configs (id, tenant_id, config_key, config_value) VALUES ($1, $2, 'x.y', 'true')")
        .bind(uuid::Uuid::now_v7())
        .bind(b)
        .execute(&mut *tx)
        .await;
    assert!(w.is_err(), "RLS WITH CHECK rejects cross-tenant insert");
    tx.rollback().await.unwrap();
    let mut tx = scoped_tx(pool, &AccessScope::Tenant(a)).await.unwrap();
    let u = sqlx::query("UPDATE tenantadm.tenants SET name = 'pwned' WHERE id = $1").bind(b).execute(&mut *tx).await.unwrap();
    assert_eq!(u.rows_affected(), 0);
    let d = sqlx::query("DELETE FROM tenantadm.tenant_branding WHERE tenant_id = $1").bind(b).execute(&mut *tx).await.unwrap();
    assert_eq!(d.rows_affected(), 0);
    tx.rollback().await.unwrap();

    // Host (platform) scope reads M01 metadata but never the tenant data plane.
    let mut tx = scoped_tx(pool, &AccessScope::Platform).await.unwrap();
    let meta: i64 = sqlx::query_scalar("SELECT count(*) FROM tenantadm.tenants WHERE id = $1").bind(b).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(meta, 1);
    let data: i64 = sqlx::query_scalar("SELECT count(*) FROM tenant_data.isolation_canaries WHERE tenant_id = $1")
        .bind(b)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(data, 0);
    tx.rollback().await.unwrap();

    // Transaction-local context does not leak through pooled connections.
    for _ in 0..10 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM tenantadm.tenant_configs").fetch_one(pool).await.unwrap();
        assert_eq!(n, 0);
    }
}

#[tokio::test]
async fn schema_and_dedicated_stores_isolate_with_foreign_context() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    for (plan, extra) in [
        (PLAN_PREMIUM, json!({})),
        (PLAN_REGULATED, json!({ "db_target": "dedicated-pg-my-central" })),
        (PLAN_REGULATED, json!({ "db_target": "dedicated-mysql-my-central" })),
    ] {
        let t = app.create_tenant(&sa, plan, extra).await;
        let r = app.post(&format!("/v1/tenants/{}/isolation-check", tid(&t)), &sa, json!({})).await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text);
        assert_eq!(r.data()["passed"], true, "{}", r.text);
        let checks: Vec<String> =
            r.data()["results"].as_array().unwrap().iter().map(|c| c["check"].as_str().unwrap().to_string()).collect();
        assert!(checks.len() >= 5, "{checks:?}");
    }
}

#[tokio::test]
async fn suspended_terminated_and_purged_tenants_are_denied() {
    let p = pair().await;
    let a = tid(&p.a);
    p.app.patch(&format!("/v1/tenants/{a}/status"), &p.sa, json!({ "status": "suspended", "reason": "non-payment" })).await;
    let r = p.app.get(&format!("/v1/tenants/{a}"), &p.ta_a).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "session revoked");
    let r = p.app.token(p.a["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, Some(p.a["tenant_code"].as_str().unwrap())).await;
    assert_eq!(r.code(), "TENANT_SUSPENDED");
    // With the "drain" policy, existing sessions survive suspension but are still refused per request.
    let b = tid(&p.b);
    p.app.patch(&format!("/v1/tenants/{b}/config"), &p.sa, json!({ "config": { "lifecycle.suspend_session_policy": "drain" } })).await;
    p.app.patch(&format!("/v1/tenants/{b}/status"), &p.sa, json!({ "status": "suspended", "reason": "non-payment" })).await;
    let r = p.app.get(&format!("/v1/tenants/{b}"), &p.ta_b).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(r.code(), "TENANT_SUSPENDED");
    // Terminated.
    p.app.patch(&format!("/v1/tenants/{b}/status"), &p.sa, json!({ "status": "active" })).await;
    p.app.patch(&format!("/v1/tenants/{b}/status"), &p.sa, json!({ "status": "grace", "reason": "offboarding" })).await;
    p.app.patch(&format!("/v1/tenants/{b}/status"), &p.sa, json!({ "status": "terminated" })).await;
    let r = p.app.token(p.b["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, Some(p.b["tenant_code"].as_str().unwrap())).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn super_admin_elevation_is_explicit_and_audited() {
    let p = pair().await;
    // Tenant Admins cannot use host routes or platform scopes.
    let r = p.app.get("/v1/tenants", &p.ta_a).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = p.app.post(&format!("/v1/reference/metering/{}", tid(&p.a)), &p.ta_a, json!({ "meter": "volume", "amount": 1 })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    // Super Admin cross-tenant read works and is recorded as elevated access.
    let r = p.app.get(&format!("/v1/tenants/{}", tid(&p.b)), &p.sa).await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(p.app.audit_actions(&tid(&p.b)).await.contains(&"platform.elevated_access".to_string()));
}

#[tokio::test]
async fn unauthenticated_access_is_refused() {
    let app = TestApp::new().await;
    let r = app.raw(Request::get("/v1/tenants/00000000-0000-7000-8000-000000000000").body(Body::empty()).unwrap()).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert_eq!(r.code(), "UNAUTHENTICATED");
    let r = app.raw(Request::get("/v1/tenants").header(header::AUTHORIZATION, "Bearer forged-token").body(Body::empty()).unwrap()).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    for p in ["/admin", "/tenant", "/admin/tenants/x/config"] {
        let r = app.raw(Request::get(p).body(Body::empty()).unwrap()).await;
        assert_eq!(r.status, StatusCode::SEE_OTHER, "{p}");
        assert_eq!(r.headers.get(header::LOCATION).unwrap(), "/login");
    }
    // Cookies are never accepted by the API (no CSRF exposure on /v1).
    let sa_email = app.sa_email.clone();
    let sa_pw = app.sa_password.clone();
    let (session, _) = app.browser_login(&sa_email, &sa_pw, None).await;
    let r = app.raw(Request::get("/v1/tenants").header(header::COOKIE, session).body(Body::empty()).unwrap()).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn wrong_password_lockout() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let email = t["primary_admin_email"].as_str().unwrap();
    for _ in 0..5 {
        let r = app.token(email, "wrong-password-123", None).await;
        assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    }
    let r = app.token(email, TA_PASSWORD, None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "locked after 5 failures");
    assert!(r.text.contains("locked"));
}
