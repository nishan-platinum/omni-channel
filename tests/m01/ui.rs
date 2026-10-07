//! Server-rendered UI smoke tests (forms, CSRF, role routing) — deterministic HTTP, no browser.

use axum::http::{header, StatusCode};
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn super_admin_pages_render() {
    let app = TestApp::new().await;
    let sa_email = app.sa_email.clone();
    let sa_pw = app.sa_password.clone();
    let (session, csrf) = app.browser_login(&sa_email, &sa_pw, None).await;
    assert!(!csrf.is_empty());
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    for p in ["/admin", "/admin/tenants", "/admin/tenants/new", "/admin/releases", "/admin/analytics", "/admin/entitlements", "/admin/dr"] {
        let r = app.page(p, &session).await;
        assert_eq!(r.status, StatusCode::OK, "{p}");
        assert!(r.text.contains("<main"), "{p}");
    }
    for tab in [
        "",
        "/config",
        "/quotas",
        "/branding",
        "/storage",
        "/support",
        "/sandboxes",
        "/baselines",
        "/release",
        "/keys",
        "/offboarding",
        "/audit",
        "/backups",
    ] {
        let r = app.page(&format!("/admin/tenants/{id}{tab}"), &session).await;
        assert_eq!(r.status, StatusCode::OK, "tab {tab}: {}", r.text.chars().take(400).collect::<String>());
    }
    // Directory search via HTMX returns only the rows partial.
    let r = app
        .raw(
            axum::http::Request::get(format!("/admin/tenants?q={}", t["tenant_code"].as_str().unwrap()))
                .header(header::COOKIE, &session)
                .header("hx-request", "true")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
    assert!(r.text.contains(t["tenant_code"].as_str().unwrap()));
    assert!(!r.text.contains("<html"));
}

#[tokio::test]
async fn create_tenant_through_the_form_and_activate() {
    let app = TestApp::new().await;
    let (sa_email, sa_pw) = (app.sa_email.clone(), app.sa_password.clone());
    let (session, csrf) = app.browser_login(&sa_email, &sa_pw, None).await;
    let code = unique_code("ui");
    let r = app
        .form(
            "/admin/tenants",
            &session,
            &[
                ("_csrf", &csrf),
                ("name", "UI Org"),
                ("region", "my-central"),
                ("plan_id", PLAN_STANDARD),
                ("primary_admin_email", "ui@ui.example"),
                ("tenant_code", &code),
            ],
        )
        .await;
    assert_eq!(r.status, StatusCode::SEE_OTHER, "{}", r.text.chars().take(500).collect::<String>());
    let loc = r.headers.get(header::LOCATION).unwrap().to_str().unwrap().to_string();
    let detail = app.page(&loc, &session).await;
    assert!(detail.text.contains(&code));
    assert!(detail.text.contains("Activate / reinstate"), "only permitted transitions offered");
    let act = app.form(&format!("{loc}/status"), &session, &[("_csrf", &csrf), ("status", "active"), ("reason", "")]).await;
    assert_eq!(act.status, StatusCode::SEE_OTHER);
    let detail = app.page(&loc, &session).await;
    assert!(detail.text.contains("Suspend"));

    // Validation errors re-render the form with the input preserved (STD-001).
    let r = app
        .form(
            "/admin/tenants",
            &session,
            &[
                ("_csrf", &csrf),
                ("name", "Keep Me"),
                ("region", "my-central"),
                ("plan_id", PLAN_STANDARD),
                ("primary_admin_email", "bad"),
                ("tenant_code", ""),
            ],
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.text.contains("Valid admin email required"));
    assert!(r.text.contains("value=\"Keep Me\""));
}

#[tokio::test]
async fn csrf_is_required_for_state_changes() {
    let app = TestApp::new().await;
    let (sa_email, sa_pw) = (app.sa_email.clone(), app.sa_password.clone());
    let (session, _csrf) = app.browser_login(&sa_email, &sa_pw, None).await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let r = app.form(&format!("/admin/tenants/{}/status", tid(&t)), &session, &[("status", "active")]).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = app.form(&format!("/admin/tenants/{}/status", tid(&t)), &session, &[("_csrf", "forged"), ("status", "active")]).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(app.get(&format!("/v1/tenants/{}", tid(&t)), &sa).await.data()["status"], "draft");
    // Login form also needs its double-submit token.
    let r = app.form("/login", "occ_login_csrf=abc", &[("email", &sa_email), ("password", &sa_pw), ("_csrf", "xyz")]).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn tenant_admin_invitation_login_and_self_service() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let id = tid(&t);
    // Invitation page renders and the password can be set even before activation.
    let uid = uuid::Uuid::parse_str(&id).unwrap();
    let (_, token) = app.state.auth.invite_tenant_admin(uid, t["primary_admin_email"].as_str().unwrap(), "TA").await.unwrap();
    let token = token.unwrap();
    let page =
        app.raw(axum::http::Request::get(format!("/invitations/accept?token={token}")).body(axum::body::Body::empty()).unwrap()).await;
    assert_eq!(page.status, StatusCode::OK);
    assert!(page.text.contains(t["tenant_code"].as_str().unwrap()));
    app.state.auth.accept_invitation(&token, TA_PASSWORD, TA_PASSWORD).await.unwrap();
    // Draft tenant: login refused until activation.
    let r = app.token(t["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    app.activate(&sa, &id).await;
    let (session, csrf) = app.browser_login(t["primary_admin_email"].as_str().unwrap(), TA_PASSWORD, None).await;
    for p in [
        "/tenant",
        "/tenant/config",
        "/tenant/quotas",
        "/tenant/branding",
        "/tenant/storage",
        "/tenant/support",
        "/tenant/sandboxes",
        "/tenant/baselines",
        "/tenant/release",
        "/tenant/keys",
        "/tenant/offboarding",
        "/tenant/audit",
    ] {
        let r = app.page(p, &session).await;
        assert_eq!(r.status, StatusCode::OK, "{p}");
    }
    assert_eq!(app.page("/admin", &session).await.status, StatusCode::FORBIDDEN, "hidden menus are not authorization");
    assert_eq!(app.page(&format!("/admin/tenants/{id}"), &session).await.status, StatusCode::FORBIDDEN);
    // Feature toggle through HTMX returns the row partial; non-entitled feature → error shown.
    let r = app
        .raw(
            axum::http::Request::post("/tenant/features/module.automated_marketing")
                .header(header::COOKIE, &session)
                .header("hx-request", "true")
                .header("x-csrf-token", &csrf)
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(axum::body::Body::from("enabled=true"))
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.text.contains("Upgrade plan"));
    // Config save through the form.
    let r = app.form("/tenant/config", &session, &[("_csrf", &csrf), ("cfg:locale.currency", "SGD")]).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    // Logout revokes the session.
    let r = app.form("/logout", &session, &[("_csrf", &csrf)]).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    let r = app.page("/tenant", &session).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER, "redirected to login after logout");
}

#[tokio::test]
async fn dev_outbox_hidden_outside_development() {
    let app = TestApp::new().await; // APP_ENV=test
    let (sa_email, sa_pw) = (app.sa_email.clone(), app.sa_password.clone());
    let (session, _) = app.browser_login(&sa_email, &sa_pw, None).await;
    assert_eq!(app.page("/dev/outbox", &session).await.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn health_and_readiness() {
    let app = TestApp::new().await;
    let r = app.raw(axum::http::Request::get("/health").body(axum::body::Body::empty()).unwrap()).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = app.raw(axum::http::Request::get("/ready").body(axum::body::Body::empty()).unwrap()).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["central_db"], "ok");
    assert!(r.headers.get("x-correlation-id").is_some());
    assert!(r.headers.get(header::CONTENT_SECURITY_POLICY).is_some());
    // X-Request-Id is echoed (API-003).
    let r = app.raw(axum::http::Request::get("/health").header("x-request-id", "abc-123").body(axum::body::Body::empty()).unwrap()).await;
    assert_eq!(r.headers.get("x-request-id").unwrap(), "abc-123");
}
