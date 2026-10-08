//! Shared integration-test harness. Uses the real router (`tower::ServiceExt::oneshot`) against the
//! Docker-hosted databases (see `.env` / `.env.example`). Each test creates its own tenants with
//! unique codes, so tests run in parallel without resetting the databases.
#![allow(dead_code)]

pub mod hub;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use chrono::Utc;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

use omni_m01::app::{build_router, build_state, AppState};
use omni_m01::platform::config::{AppConfig, AppEnv};
use omni_m01::platform::db::AccessScope;
use omni_m01::platform::time::ManualClock;

pub const PLAN_STANDARD: &str = "01920000-0000-7000-8000-000000000001";
pub const PLAN_PREMIUM: &str = "01920000-0000-7000-8000-000000000002";
pub const PLAN_REGULATED: &str = "01920000-0000-7000-8000-000000000003";
pub const PLAN_RETIRED: &str = "01920000-0000-7000-8000-000000000009";
pub const TA_PASSWORD: &str = "Tenant-Admin-Passw0rd!";

pub struct TestApp {
    pub state: AppState,
    pub router: Router,
    pub clock: Arc<ManualClock>,
    pub sa_email: String,
    pub sa_password: String,
}

pub struct Resp {
    pub status: StatusCode,
    pub headers: axum::http::HeaderMap,
    pub body: Value,
    pub text: String,
}

impl Resp {
    pub fn code(&self) -> &str {
        self.body["error"]["code"].as_str().unwrap_or("")
    }
    pub fn data(&self) -> &Value {
        &self.body["data"]
    }
}

pub fn unique_code(prefix: &str) -> String {
    let u = Uuid::new_v4().simple().to_string();
    format!("{}-{}", prefix, &u[..10])
}

impl TestApp {
    pub async fn new() -> Self {
        Self::with_config(|_| {}).await
    }

    /// Like `new`, with a hook to adjust the configuration before the state is built.
    pub async fn with_config(adjust: impl FnOnce(&mut AppConfig)) -> Self {
        let _ = dotenvy::dotenv();
        let mut cfg = AppConfig::from_env().expect("test configuration (copy .env.example to .env and start the Docker databases)");
        cfg.app_env = AppEnv::Test;
        cfg.db_max_connections = 5;
        cfg.access_log = false;
        cfg.scheduler_enabled = false;
        cfg.grace_period_hours = 24;
        cfg.retention_hours = 1;
        cfg.auto_purge = true;
        cfg.data_dir = std::env::temp_dir().join("omni-m01-tests");
        cfg.hub_demo_seed = false;
        if std::path::Path::new("config/tenant-db-targets.local.toml").exists() && std::env::var("TENANT_DB_TARGETS_FILE").is_err() {
            cfg.tenant_db_targets_file = "config/tenant-db-targets.local.toml".into();
        }
        adjust(&mut cfg);
        let clock = Arc::new(ManualClock::new(Utc::now()));
        let state = build_state(cfg.clone(), clock.clone()).await.expect("build state (are the Docker databases up?)");
        let router = build_router(state.clone());
        Self {
            state,
            router,
            clock,
            sa_email: cfg.bootstrap_superadmin_email.clone().expect("BOOTSTRAP_SUPERADMIN_EMAIL"),
            sa_password: cfg.bootstrap_superadmin_password.clone().expect("BOOTSTRAP_SUPERADMIN_PASSWORD"),
        }
    }

    pub async fn send(&self, method: Method, path: &str, token: Option<&str>, body: Option<Value>, extra: &[(&str, &str)]) -> Resp {
        let mut b = Request::builder().method(method).uri(path);
        if let Some(t) = token {
            b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        for (k, v) in extra {
            b = b.header(*k, *v);
        }
        let req = match body {
            Some(v) => b.header(header::CONTENT_TYPE, "application/json").body(Body::from(v.to_string())).unwrap(),
            None => b.body(Body::empty()).unwrap(),
        };
        self.raw(req).await
    }

    pub async fn raw(&self, req: Request<Body>) -> Resp {
        let resp = self.router.clone().oneshot(req).await.expect("router response");
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.into_body().collect().await.expect("body").to_bytes();
        let text = String::from_utf8_lossy(&bytes).to_string();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        Resp { status, headers, body, text }
    }

    pub async fn get(&self, path: &str, token: &str) -> Resp {
        self.send(Method::GET, path, Some(token), None, &[]).await
    }
    pub async fn post(&self, path: &str, token: &str, body: Value) -> Resp {
        self.send(Method::POST, path, Some(token), Some(body), &[]).await
    }
    pub async fn patch(&self, path: &str, token: &str, body: Value) -> Resp {
        self.send(Method::PATCH, path, Some(token), Some(body), &[]).await
    }

    pub async fn token(&self, email: &str, password: &str, code: Option<&str>) -> Resp {
        let mut body = json!({ "email": email, "password": password });
        if let Some(c) = code {
            body["tenant_code"] = json!(c);
        }
        self.send(Method::POST, "/v1/bootstrap/token", None, Some(body), &[]).await
    }

    pub async fn sa_token(&self) -> String {
        let r = self.token(&self.sa_email.clone(), &self.sa_password.clone(), None).await;
        assert_eq!(r.status, StatusCode::OK, "SA token: {}", r.text);
        r.data()["access_token"].as_str().unwrap().to_string()
    }

    /// Creates a tenant through the spec API; returns the response data.
    pub async fn create_tenant(&self, sa: &str, plan: &str, extra: Value) -> Value {
        let code = unique_code("t");
        let mut body = json!({
            "name": format!("Test Org {code}"),
            "region": "my-central",
            "plan_id": plan,
            "primary_admin_email": format!("admin@{code}.example"),
            "tenant_code": code,
        });
        if let (Some(b), Some(e)) = (body.as_object_mut(), extra.as_object()) {
            for (k, v) in e {
                b.insert(k.clone(), v.clone());
            }
        }
        let r = self.post("/v1/tenants", sa, body).await;
        assert_eq!(r.status, StatusCode::CREATED, "create tenant: {}", r.text);
        assert_eq!(r.data()["provisioning"]["completed"], json!(true), "provisioning: {}", r.text);
        r.data().clone()
    }

    pub async fn activate(&self, sa: &str, id: &str) {
        let r = self.patch(&format!("/v1/tenants/{id}/status"), sa, json!({ "status": "active" })).await;
        assert_eq!(r.status, StatusCode::OK, "activate: {}", r.text);
    }

    /// Creates + activates a tenant and returns (tenant data, Tenant Admin token).
    pub async fn active_tenant_with_admin(&self, sa: &str, plan: &str, extra: Value) -> (Value, String) {
        let t = self.create_tenant(sa, plan, extra).await;
        let id = t["tenant_id"].as_str().unwrap().to_string();
        self.activate(sa, &id).await;
        let token = self.admin_token(&t).await;
        (t, token)
    }

    /// Accepts the Tenant Admin invitation (re-issued for the test) and logs in via the API.
    pub async fn admin_token(&self, t: &Value) -> String {
        let id = Uuid::parse_str(t["tenant_id"].as_str().unwrap()).unwrap();
        let email = t["primary_admin_email"].as_str().unwrap();
        let (_, token) = self.state.auth.invite_tenant_admin(id, email, "Tenant Administrator").await.unwrap();
        if let Some(tok) = token {
            self.state.auth.accept_invitation(&tok, TA_PASSWORD, TA_PASSWORD).await.unwrap();
        }
        let r = self.token(email, TA_PASSWORD, Some(t["tenant_code"].as_str().unwrap())).await;
        assert_eq!(r.status, StatusCode::OK, "TA token: {}", r.text);
        r.data()["access_token"].as_str().unwrap().to_string()
    }

    /// Runs one outbox dispatch cycle (events → notifications).
    pub async fn dispatch(&self) {
        for _ in 0..5 {
            if self.state.dispatcher.run_once().await.unwrap() == 0 {
                break;
            }
        }
    }

    pub async fn audit_actions(&self, tenant: &str) -> Vec<String> {
        let id = Uuid::parse_str(tenant).unwrap();
        omni_m01::platform::audit::list_for_tenant(&self.state.db.app, &AccessScope::Platform, id, None, 200)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.action)
            .collect()
    }

    pub async fn outbox_ids(&self, tenant: &str) -> Vec<String> {
        let id = Uuid::parse_str(tenant).unwrap();
        sqlx::query_scalar::<_, String>("SELECT notification_id FROM shared.notification_outbox WHERE tenant_id = $1 ORDER BY created_at")
            .bind(id)
            .fetch_all(&self.state.db.owner)
            .await
            .unwrap()
    }

    pub async fn event_types(&self, tenant: &str) -> Vec<String> {
        let id = Uuid::parse_str(tenant).unwrap();
        sqlx::query_scalar::<_, String>("SELECT event_type FROM shared.event_outbox WHERE tenant_id = $1 ORDER BY occurred_at")
            .bind(id)
            .fetch_all(&self.state.db.owner)
            .await
            .unwrap()
    }

    // ---- browser helpers -------------------------------------------------------------------

    /// Logs in through the HTML form (double-submit CSRF) and returns (session cookie, csrf token).
    pub async fn browser_login(&self, email: &str, password: &str, code: Option<&str>) -> (String, String) {
        let page = self.raw(Request::get("/login").body(Body::empty()).unwrap()).await;
        let pre = page
            .headers
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|c| c.starts_with("occ_login_csrf="))
            .and_then(|c| c.split(';').next())
            .and_then(|c| c.split_once('='))
            .map(|(_, v)| v.to_string())
            .expect("login csrf cookie");
        let form =
            format!("email={}&password={}&tenant_code={}&_csrf={}", urlenc(email), urlenc(password), urlenc(code.unwrap_or("")), pre);
        let r = self
            .raw(
                Request::post("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .header(header::COOKIE, format!("occ_login_csrf={pre}"))
                    .body(Body::from(form))
                    .unwrap(),
            )
            .await;
        assert_eq!(r.status, StatusCode::SEE_OTHER, "browser login: {}", r.text);
        let session = r
            .headers
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|c| c.starts_with("occ_session="))
            .and_then(|c| c.split(';').next())
            .expect("session cookie")
            .to_string();
        let home = self.page("/", &session).await;
        let to = home.headers.get(header::LOCATION).and_then(|v| v.to_str().ok()).unwrap_or("/admin").to_string();
        let p = self.page(&to, &session).await;
        let csrf = extract_csrf(&p.text).expect("csrf token on page");
        (session, csrf)
    }

    pub async fn page(&self, path: &str, session: &str) -> Resp {
        self.raw(Request::get(path).header(header::COOKIE, session).body(Body::empty()).unwrap()).await
    }

    pub async fn form(&self, path: &str, session: &str, fields: &[(&str, &str)]) -> Resp {
        let body = fields.iter().map(|(k, v)| format!("{}={}", urlenc(k), urlenc(v))).collect::<Vec<_>>().join("&");
        self.raw(
            Request::post(path)
                .header(header::COOKIE, session)
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
    }
}

pub fn extract_csrf(html: &str) -> Option<String> {
    let i = html.find("name=\"_csrf\" value=\"")? + "name=\"_csrf\" value=\"".len();
    let rest = &html[i..];
    Some(rest[..rest.find('"')?].to_string())
}

pub fn urlenc(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

pub fn tid(t: &Value) -> String {
    t["tenant_id"].as_str().unwrap().to_string()
}

pub fn tenant_id(t: &Value) -> omni_m01::modules::m01_tenancy::domain::TenantId {
    omni_m01::modules::m01_tenancy::domain::TenantId(Uuid::parse_str(t["tenant_id"].as_str().unwrap()).unwrap())
}

pub fn sa_actor() -> omni_m01::modules::m01_tenancy::application::Actor {
    omni_m01::modules::m01_tenancy::application::Actor {
        user_id: Some(Uuid::now_v7()),
        role: omni_m01::modules::m01_tenancy::application::ActorRole::SuperAdmin,
        tenant_id: None,
        email: None,
        correlation_id: Some("test".into()),
        ip: None,
        user_agent: None,
    }
}

pub fn ta_actor(t: &Value, user: Uuid) -> omni_m01::modules::m01_tenancy::application::Actor {
    omni_m01::modules::m01_tenancy::application::Actor {
        user_id: Some(user),
        role: omni_m01::modules::m01_tenancy::application::ActorRole::TenantAdmin,
        tenant_id: Some(tenant_id(t)),
        email: None,
        correlation_id: Some("test".into()),
        ip: None,
        user_agent: None,
    }
}

/// Id of the (single) Tenant Admin of a tenant.
pub async fn admin_user_id(app: &TestApp, t: &Value) -> Uuid {
    app.state.auth.tenant_admins(tenant_id(t).0).await.unwrap()[0].0
}
