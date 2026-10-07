//! Dedicated MySQL tenant adapter: provisioning, connectivity, routing and the database/login
//! boundary. MySQL has NO row-level security — these tests prove the dedicated-database boundary.
#[path = "../common/mod.rs"]
mod common;

use axum::http::StatusCode;
use serde_json::json;
use sqlx::mysql::MySqlConnectOptions;
use sqlx::{ConnectOptions, Connection};

use common::*;
use omni_m01::modules::m01_tenancy::infrastructure::tenant_data::targets::{runtime_username, EnvSecretResolver};

#[tokio::test]
async fn dedicated_mysql_tenant_lifecycle_and_boundary() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (a, _ta) = app.active_tenant_with_admin(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-mysql-my-central" })).await;
    let b = app.create_tenant(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-mysql-my-central" })).await;
    let host = sa_actor();

    let pa = app.state.m01.directory.check_connectivity(&host, tenant_id(&a)).await.unwrap();
    assert_eq!(pa.engine.as_str(), "mysql");
    assert_eq!(pa.last_check_ok, Some(true), "{:?}", pa.last_check_message);
    let pb = app.state.m01.directory.check_connectivity(&host, tenant_id(&b)).await.unwrap();
    assert_ne!(pa.database_name, pb.database_name, "database per tenant");

    // Routing: A's store only serves A (repository guard), and holds only A's rows.
    let store = app.state.m01.deps.data_router.store_for(&pa).await.unwrap();
    assert!(store.list_rows(tenant_id(&a)).await.unwrap().iter().all(|r| r.tenant_id == tid(&a)));
    assert_eq!(store.list_rows(tenant_id(&b)).await.unwrap_err().status(), StatusCode::FORBIDDEN);
    assert!(store.write_canary(tenant_id(&b), "x").await.is_err());

    // A's runtime login cannot open B's database (dedicated boundary, no RLS involved).
    let secrets = EnvSecretResolver;
    let user = runtime_username(tenant_id(&a));
    let pw = secrets.derive_password("env:TENANT_DB_RUNTIME_SEED", &user).unwrap();
    let opts = MySqlConnectOptions::new().host("127.0.0.1").port(53306).username(&user).password(&pw);
    let mut own = opts.clone().database(&pa.database_name).connect().await.expect("own database reachable");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM isolation_canaries").fetch_one(&mut own).await.unwrap();
    assert!(n >= 1);
    own.close().await.unwrap();
    let other = opts.database(&pb.database_name).connect().await;
    assert!(other.is_err(), "access to another tenant database must be denied");

    // Isolation probe via the API.
    let r = app.post(&format!("/v1/tenants/{}/isolation-check", tid(&a)), &sa, json!({})).await;
    assert_eq!(r.data()["passed"], true, "{}", r.text);

    // Decommission (draft discard) drops B's database and login.
    app.state.m01.provisioning.discard_draft(&host, tenant_id(&b)).await.unwrap();
    let admin_pw = std::env::var("TENANT_DB_MYSQL_ADMIN_PASSWORD").unwrap();
    let mut admin = MySqlConnectOptions::new().host("127.0.0.1").port(53306).username("root").password(&admin_pw).connect().await.unwrap();
    let dbs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?")
        .bind(&pb.database_name)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(dbs, 0);
    let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mysql.user WHERE user = ?")
        .bind(runtime_username(tenant_id(&b)))
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(users, 0);
}

#[tokio::test]
async fn unreachable_tenant_database_does_not_affect_readiness() {
    // A tenant DB outage is reported per tenant, never via /ready.
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-mysql-my-central" })).await;
    sqlx::query("UPDATE tenantadm.tenant_database_connections SET port = 1 WHERE tenant_id = $1::uuid")
        .bind(tid(&t))
        .execute(&app.state.db.owner)
        .await
        .unwrap();
    let fresh = TestApp::new().await; // new router cache so the broken profile is used
    let p = fresh.state.m01.directory.check_connectivity(&sa_actor(), tenant_id(&t)).await.unwrap();
    assert_eq!(p.last_check_ok, Some(false));
    let r = fresh.raw(axum::http::Request::get("/ready").body(axum::body::Body::empty()).unwrap()).await;
    assert_eq!(r.status, StatusCode::OK);
}
