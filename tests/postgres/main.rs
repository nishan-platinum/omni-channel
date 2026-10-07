//! PostgreSQL control plane + PostgreSQL tenant adapters: roles, RLS objects, optimistic locking,
//! uniqueness, append-only audit, schema-per-tenant and dedicated databases.
#[path = "../common/mod.rs"]
mod common;

use axum::http::StatusCode;
use serde_json::json;

use common::*;
use omni_m01::platform::db::{scoped_tx, AccessScope};

#[tokio::test]
async fn migrations_are_idempotent_and_rls_is_forced() {
    let app = TestApp::new().await;
    omni_m01::platform::db::migrate(&app.state.db.owner).await.unwrap();
    let rows: Vec<(String, bool, bool)> = sqlx::query_as(
        "SELECT c.relname::text, c.relrowsecurity, c.relforcerowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname IN ('tenantadm', 'identity', 'tenant_data') AND c.relkind = 'r' AND c.relname NOT IN ('plans', 'provisioning_templates', 'platform_releases')",
    )
    .fetch_all(&app.state.db.owner)
    .await
    .unwrap();
    assert!(rows.len() > 20);
    for (t, rls, force) in rows {
        assert!(rls && force, "{t} must have ENABLE+FORCE RLS");
    }
}

#[tokio::test]
async fn optimistic_locking_rejects_stale_versions() {
    // STD-003
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let tenant = app.state.m01.deps.tenants.get(&AccessScope::Platform, tenant_id(&t)).await.unwrap().unwrap();
    let plan = tenant
        .plan_transition(
            omni_m01::modules::m01_tenancy::domain::TenantStatus::Active,
            None,
            chrono::Utc::now(),
            app.state.m01.deps.settings.lifecycle,
        )
        .unwrap();
    let ok =
        app.state.m01.deps.tenants.apply_transition(&AccessScope::Platform, tenant.id, tenant.version, &plan, Default::default()).await;
    assert!(ok.is_ok());
    let stale = app
        .state
        .m01
        .deps
        .tenants
        .apply_transition(&AccessScope::Platform, tenant.id, tenant.version, &plan, Default::default())
        .await
        .unwrap_err();
    assert_eq!(stale.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn unique_constraints_hold_at_database_level() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let r = sqlx::query("UPDATE tenantadm.tenants SET tenant_code = $1 WHERE id <> $2::uuid AND id = (SELECT id FROM tenantadm.tenants WHERE id <> $2::uuid LIMIT 1)")
        .bind(t["tenant_code"].as_str().unwrap())
        .bind(tid(&t))
        .execute(&app.state.db.owner)
        .await;
    assert!(r.is_err(), "uq_tenants_tenant_code");
    let r = sqlx::query("INSERT INTO tenantadm.tenants (id, tenant_code, name, plan_id, primary_admin_email, storage_strategy) VALUES (gen_random_uuid(), 'BAD CODE', 'x', $1::uuid, 'a@b.c', 'shared_row_level')")
        .bind(PLAN_STANDARD)
        .execute(&app.state.db.owner)
        .await;
    assert!(r.is_err(), "ck_tenants_tenant_code");
}

#[tokio::test]
async fn audit_log_is_append_only_and_chained() {
    // SEC-140/141
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_STANDARD, json!({})).await;
    let upd =
        sqlx::query("UPDATE shared.audit_log SET action = 'x' WHERE tenant_id = $1::uuid").bind(tid(&t)).execute(&app.state.db.owner).await;
    assert!(upd.is_err());
    let del = sqlx::query("DELETE FROM shared.audit_log WHERE tenant_id = $1::uuid").bind(tid(&t)).execute(&app.state.db.owner).await;
    assert!(del.is_err());
    let hash: Option<Vec<u8>> = sqlx::query_scalar("SELECT hash FROM shared.audit_log WHERE tenant_id = $1::uuid ORDER BY id DESC LIMIT 1")
        .bind(tid(&t))
        .fetch_one(&app.state.db.owner)
        .await
        .unwrap();
    assert_eq!(hash.unwrap().len(), 32);
    assert!(omni_m01::platform::audit::verify_chain(&app.state.db.app, 200).await.unwrap());
    // The runtime role cannot even attempt to modify audit rows.
    let mut tx = scoped_tx(&app.state.db.app, &AccessScope::Platform).await.unwrap();
    assert!(sqlx::query("UPDATE shared.audit_log SET action = 'x'").execute(&mut *tx).await.is_err());
}

#[tokio::test]
async fn schema_per_tenant_store_is_created_and_dropped() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let t = app.create_tenant(&sa, PLAN_PREMIUM, json!({})).await;
    let schema = format!("tn_{}", uuid::Uuid::parse_str(&tid(&t)).unwrap().simple());
    let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)")
        .bind(&schema)
        .fetch_one(&app.state.db.owner)
        .await
        .unwrap();
    assert!(exists);
    // crm_app without tenant context sees nothing in the tenant schema.
    let n: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {schema}.isolation_canaries")).fetch_one(&app.state.db.app).await.unwrap();
    assert_eq!(n, 0);
    let host = sa_actor();
    app.state.m01.provisioning.discard_draft(&host, tenant_id(&t)).await.unwrap();
    let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)")
        .bind(&schema)
        .fetch_one(&app.state.db.owner)
        .await
        .unwrap();
    assert!(!exists, "schema dropped by provisioning rollback");
    // The code is free again after a discarded draft.
    assert!(!app.state.m01.deps.tenants.code_exists(t["tenant_code"].as_str().unwrap()).await.unwrap());
}

#[tokio::test]
async fn dedicated_postgres_database_per_tenant() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-pg-my-central" })).await;
    let host = sa_actor();
    let profile = app.state.m01.directory.check_connectivity(&host, tenant_id(&t)).await.unwrap();
    assert_eq!(profile.last_check_ok, Some(true));
    assert_eq!(profile.engine.as_str(), "postgres");
    assert!(profile.database_name.starts_with("tn_"));
    let store = app.state.m01.deps.data_router.store_for(&profile).await.unwrap();
    let rows = store.list_rows(tenant_id(&t)).await.unwrap();
    assert!(rows.iter().all(|r| r.tenant_id == tid(&t)) && !rows.is_empty());
    // Terminate + purge drops the dedicated database.
    let id = tid(&t);
    app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "grace", "reason": "exit" })).await;
    app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "terminated" })).await;
    app.clock.advance(chrono::Duration::hours(2));
    let sa = app.sa_token().await;
    let r = app.patch(&format!("/v1/tenants/{id}/status"), &sa, json!({ "status": "purged" })).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    let cert = app.state.m01.deps.exports.certificate(&AccessScope::Platform, tenant_id(&t)).await.unwrap().unwrap();
    assert!(cert.manifest["data_plane_and_objects"]["data_store"].as_str().unwrap().contains("dropped"));
}
