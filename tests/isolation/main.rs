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

// ------------------------------------------------------------------------------------------------
// M10 hub (gateway slice): the same isolation guarantees for conversations, messages and agents.
// ------------------------------------------------------------------------------------------------

mod hub_isolation {
    use axum::http::StatusCode;
    use serde_json::json;
    use sqlx::Row;
    use uuid::Uuid;

    use super::common::hub::*;
    use omni_m01::platform::db::{scoped_tx, AccessScope};

    /// Two hub tenants, each with one support agent online and one assigned WhatsApp conversation.
    struct Two {
        h: HubApp,
        a: HubTenant,
        b: HubTenant,
        agent_a: String,
        agent_b: String,
        ws_a: Ws,
        conv_a: String,
        conv_b: String,
    }

    async fn two() -> Two {
        let h = HubApp::new().await;
        let a = h.tenant().await;
        let b = h.tenant().await;
        let (_, agent_a) = h.agent(&a, &["support"], 3).await;
        let (_, agent_b) = h.agent(&b, &["support"], 3).await;
        let (mut ws_a, _) = h.agent_online(&agent_a, true).await;
        let (mut ws_b, _) = h.agent_online(&agent_b, true).await;
        h.whatsapp_inbound(&a.whatsapp, "60190000001", "tenant A secret", &wamid()).await;
        h.whatsapp_inbound(&b.whatsapp, "60190000001", "tenant B secret", &wamid()).await;
        let conv_a = recv_type(&mut ws_a, "conversation.assigned").await["conversation"]["id"].as_str().unwrap().to_string();
        let conv_b = recv_type(&mut ws_b, "conversation.assigned").await["conversation"]["id"].as_str().unwrap().to_string();
        Two { h, a, b, agent_a, agent_b, ws_a, conv_a, conv_b }
    }

    #[tokio::test]
    async fn agent_cannot_read_or_write_another_tenants_conversation() {
        let mut x = two().await;
        // Read over the API → not found (RLS hides the row; no existence oracle).
        let r = x.h.app.get(&format!("/v1/hub/conversations/{}/messages", x.conv_b), &x.agent_a).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND);
        let r = x.h.app.get(&format!("/v1/hub/conversations/{}/messages", x.conv_b), &x.a.ta).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND);
        // Write over the WebSocket → refused, nothing stored in B.
        send(&mut x.ws_a, json!({ "type": "message.send", "conversation_id": x.conv_b, "client_msg_id": "x1", "body": "injected" })).await;
        let e = recv_type(&mut x.ws_a, "error").await;
        assert_eq!(e["code"], "NOT_FOUND");
        send(&mut x.ws_a, json!({ "type": "conversation.close", "conversation_id": x.conv_b })).await;
        assert_eq!(recv_type(&mut x.ws_a, "error").await["code"], "NOT_FOUND");
        let r = x.h.app.get(&format!("/v1/hub/conversations/{}/messages", x.conv_b), &x.agent_b).await;
        let bodies: Vec<String> =
            r.data()["messages"].as_array().unwrap().iter().map(|m| m["body"].as_str().unwrap().to_string()).collect();
        assert_eq!(bodies, vec!["tenant B secret"]);
        // Lists only ever show the caller's tenant.
        let mine = x.h.app.get("/v1/hub/conversations", &x.a.ta).await;
        assert!(mine.data().as_array().unwrap().iter().all(|c| c["id"] != x.conv_b.as_str()));
        assert!(!mine.text.contains("tenant B secret"));
    }

    #[tokio::test]
    async fn agent_sees_only_conversations_assigned_to_them() {
        let x = two().await;
        let (_, other_agent) = x.h.agent(&x.a, &["support"], 3).await;
        let r = x.h.app.get(&format!("/v1/hub/conversations/{}/messages", x.conv_a), &other_agent).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND);
        let (mut ws, _) = x.h.agent_online(&other_agent, false).await;
        send(&mut ws, json!({ "type": "message.send", "conversation_id": x.conv_a, "client_msg_id": "y1", "body": "not mine" })).await;
        assert_eq!(recv_type(&mut ws, "error").await["code"], "FORBIDDEN");
    }

    #[tokio::test]
    async fn rls_hides_hub_rows_across_tenants_and_from_platform_scope() {
        let x = two().await;
        let pool = &x.h.app.state.db.app;
        for table in ["conversations", "messages", "agents", "agent_presence", "channel_endpoints", "message_status_events"] {
            let mut tx = scoped_tx(pool, &AccessScope::Tenant(x.a.tenant_id)).await.unwrap();
            let foreign: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM hub.{table} WHERE tenant_id = $1"))
                .bind(x.b.tenant_id)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
            assert_eq!(foreign, 0, "tenant A sees tenant B rows in hub.{table}");
            tx.rollback().await.unwrap();
            // Platform (Super Admin) scope never reads tenant engagement data.
            let mut tx = scoped_tx(pool, &AccessScope::Platform).await.unwrap();
            let any: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM hub.{table}")).fetch_one(&mut *tx).await.unwrap();
            assert_eq!(any, 0, "platform scope reads hub.{table}");
            tx.rollback().await.unwrap();
            // No scope at all → fail closed.
            let mut tx = pool.begin().await.unwrap();
            let any: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM hub.{table}")).fetch_one(&mut *tx).await.unwrap();
            assert_eq!(any, 0, "unscoped transaction reads hub.{table}");
            tx.rollback().await.unwrap();
        }
        // Writing a row for another tenant from a tenant scope is rejected by WITH CHECK.
        let mut tx = scoped_tx(pool, &AccessScope::Tenant(x.a.tenant_id)).await.unwrap();
        let r =
            sqlx::query("INSERT INTO hub.channel_endpoints (id, tenant_id, channel, address, label) VALUES ($1, $2, 'whatsapp', $3, 'x')")
                .bind(Uuid::now_v7())
                .bind(x.b.tenant_id)
                .bind(format!("rls-{}", Uuid::new_v4().simple()))
                .execute(&mut *tx)
                .await;
        assert!(r.is_err(), "cross-tenant insert must be rejected by RLS");
    }

    #[tokio::test]
    async fn inbound_tenant_comes_only_from_the_endpoint_registry() {
        let x = two().await;
        let row = sqlx::query("SELECT tenant_id FROM hub.conversations WHERE id = $1")
            .bind(Uuid::parse_str(&x.conv_b).unwrap())
            .fetch_one(&x.h.app.state.db.owner)
            .await
            .unwrap();
        let t: Uuid = row.try_get("tenant_id").unwrap();
        assert_eq!(t, x.b.tenant_id);
        // A payload cannot smuggle a tenant: unknown fields are ignored, the number decides.
        let mut body = omni_m01::modules::m10_hub::infrastructure::channels::whatsapp_sim::inbound_payload(
            &x.b.whatsapp,
            "60190000002",
            "Z",
            "hi",
            &wamid(),
        );
        body["tenant_id"] = json!(x.a.tenant_id);
        let raw = body.to_string();
        let sig = omni_m01::modules::m10_hub::infrastructure::channels::sign(
            x.h.app.state.config.hub_sim_whatsapp_app_secret.as_bytes(),
            raw.as_bytes(),
        );
        let (st, _) = x.h.raw_post("/v1/hub/channels/whatsapp/webhook", &raw, &[("x-hub-signature-256", &sig)]).await;
        assert_eq!(st, StatusCode::OK);
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM hub.conversations WHERE tenant_id = $1 AND customer_address = '60190000002'")
            .bind(x.a.tenant_id)
            .fetch_one(&x.h.app.state.db.owner)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn hub_roles_are_separated_from_m01_administration() {
        let x = two().await;
        // Agent tokens have no M01 scopes and no hub admin rights.
        assert_eq!(x.h.app.get(&format!("/v1/tenants/{}", x.a.tenant_id), &x.agent_a).await.status, StatusCode::FORBIDDEN);
        assert_eq!(x.h.app.get("/v1/hub/agents", &x.agent_a).await.status, StatusCode::FORBIDDEN);
        assert_eq!(x.h.app.post("/v1/hub/channels/simulated", &x.agent_a, json!({})).await.status, StatusCode::FORBIDDEN);
        // Tenant Admin / Super Admin cannot open an agent socket.
        let mut req =
            tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(format!("ws://{}/v1/hub/ws/agent", x.h.addr))
                .unwrap();
        req.headers_mut().insert("authorization", format!("Bearer {}", x.a.ta).parse().unwrap());
        assert!(tokio_tungstenite::connect_async(req).await.is_err());
        let mut req =
            tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(format!("ws://{}/v1/hub/ws/agent", x.h.addr))
                .unwrap();
        req.headers_mut().insert("authorization", format!("Bearer {}", x.h.sa).parse().unwrap());
        assert!(tokio_tungstenite::connect_async(req).await.is_err());
        // Super Admin has no hub tenant API either (platform scope never reads engagement data).
        assert_eq!(x.h.app.get("/v1/hub/conversations", &x.h.sa).await.status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn suspended_tenant_stops_ingest_and_agent_access() {
        let x = two().await;
        let r =
            x.h.app
                .patch(&format!("/v1/tenants/{}/status", x.a.tenant_id), &x.h.sa, json!({ "status": "suspended", "reason": "test" }))
                .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text);
        let (st, body) = x.h.whatsapp_inbound(&x.a.whatsapp, "60190000003", "hello?", &wamid()).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "TENANT_SUSPENDED");
        let mut req =
            tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(format!("ws://{}/v1/hub/ws/agent", x.h.addr))
                .unwrap();
        req.headers_mut().insert("authorization", format!("Bearer {}", x.agent_a).parse().unwrap());
        assert!(tokio_tungstenite::connect_async(req).await.is_err(), "suspended tenant's agent must not connect");
        // An already-open socket cannot send any more either.
        let mut ws = x.ws_a;
        send(&mut ws, json!({ "type": "message.send", "conversation_id": x.conv_a, "client_msg_id": "s1", "body": "after suspension" }))
            .await;
        assert_eq!(recv_type(&mut ws, "error").await["code"], "TENANT_SUSPENDED");
        // Tenant B is unaffected.
        let (st, _) = x.h.whatsapp_inbound(&x.b.whatsapp, "60190000003", "hello?", &wamid()).await;
        assert_eq!(st, StatusCode::OK);
    }
}
