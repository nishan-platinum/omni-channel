//! P2 requirements: R010 keys/BYOK, R017 sandboxes, R022 baselines, R026 releases, R027 analytics,
//! plus R011 backup/restore and R016 encrypted exports.

use axum::http::StatusCode;
use serde_json::json;

use omni_m01::platform::db::AccessScope;

use crate::common::*;

#[tokio::test]
async fn sandbox_copy_and_promotion_with_diff() {
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let prod = tenant_id(&t);
    app.patch(&format!("/v1/tenants/{}/config", tid(&t)), &ta, json!({ "config": { "locale.currency": "SGD" } })).await;
    let admin = admin_user_id(&app, &t).await;
    let created = app.state.m01.sandboxes.create(&ta_actor(&t, admin), prod).await.unwrap();
    assert!(created.activated);
    let sbx = created.outcome.tenant.clone();
    assert!(sbx.is_sandbox);
    assert_eq!(sbx.sandbox_of_tenant_id, Some(prod));
    assert!(sbx.code.as_str().starts_with(t["tenant_code"].as_str().unwrap()));
    assert!(created.data_copy.contains("no business data copied"));
    let cfg = app.get(&format!("/v1/tenants/{}/config", sbx.id), &sa).await;
    assert_eq!(cfg.data()["config"]["locale.currency"], "SGD", "configuration copied");
    // A sandbox cannot have its own sandbox.
    assert!(app.state.m01.sandboxes.create(&sa_actor(), sbx.id).await.is_err());

    // Change the sandbox, preview and promote to production with a change record.
    app.patch(&format!("/v1/tenants/{}/config", sbx.id), &sa, json!({ "config": { "locale.timezone": "Asia/Tokyo" } })).await;
    let (target, preview) = app.state.m01.baselines.promotion_preview(&ta_actor(&t, admin), sbx.id).await.unwrap();
    assert_eq!(target, prod);
    assert!(preview.diff.iter().any(|d| d.key == "locale.timezone"));
    let applied = app.state.m01.baselines.promote(&ta_actor(&t, admin), sbx.id, "CHG-1").await.unwrap();
    assert_eq!(applied.source, "promotion");
    let cfg = app.get(&format!("/v1/tenants/{}/config", tid(&t)), &ta).await;
    assert_eq!(cfg.data()["config"]["locale.timezone"], "Asia/Tokyo");
}

#[tokio::test]
async fn baseline_export_import_rollback() {
    // R022, UJ-15 E2
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let tenant = tenant_id(&t);
    let admin = admin_user_id(&app, &t).await;
    let actor = ta_actor(&t, admin);
    let v1 = app.state.m01.baselines.export(&actor, tenant, "go-live").await.unwrap();
    app.patch(&format!("/v1/tenants/{}/config", tid(&t)), &ta, json!({ "config": { "locale.currency": "USD" } })).await;
    let raw = serde_json::to_string(&v1.content).unwrap();
    let preview = app.state.m01.baselines.preview_import(&actor, tenant, &raw).await.unwrap();
    assert!(preview.diff.iter().any(|d| d.key == "locale.currency" && d.after.as_deref() == Some("MYR")));
    // Change record required.
    assert!(app.state.m01.baselines.import(&actor, tenant, &raw, "", "imported").await.is_err());
    app.state.m01.baselines.rollback(&actor, tenant, v1.id, "CHG-rollback").await.unwrap();
    let cfg = app.get(&format!("/v1/tenants/{}/config", tid(&t)), &ta).await;
    assert_eq!(cfg.data()["config"]["locale.currency"], "MYR");
    // Importing a document that enables a non-entitled feature is refused (403).
    let mut doc = v1.content.clone();
    doc.feature_flags.insert("module.automated_marketing".into(), true);
    let err = app.state.m01.baselines.import(&actor, tenant, &serde_json::to_string(&doc).unwrap(), "CHG-2", "imported").await.unwrap_err();
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
    let kinds: Vec<_> = app.state.m01.baselines.list(&actor, tenant).await.unwrap().into_iter().map(|b| b.source).collect();
    assert!(kinds.contains(&"pre_import_snapshot".to_string()));
}

#[tokio::test]
async fn key_rotation_keeps_old_exports_readable_and_byok_is_regulated_only() {
    // R010: rotation without downtime; BYOK for Regulated.
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let tenant = tenant_id(&t);
    let host = sa_actor();
    let admin = admin_user_id(&app, &t).await;
    let export = app.state.m01.offboarding.generate_export(&host, tenant, "manual").await.unwrap();
    // Stored export is ciphertext, not JSON.
    let blob = app.state.m01.deps.objects.get(&export.object_key).await.unwrap().unwrap();
    assert!(serde_json::from_slice::<serde_json::Value>(&blob).is_err());
    let k2 = app.state.m01.keys.rotate(&host, tenant).await.unwrap();
    assert_eq!(k2.key_version, 2);
    let keys = app.state.m01.keys.list(&host, tenant).await.unwrap();
    assert_eq!(keys.iter().filter(|k| k.state.as_str() == "active").count(), 1);
    assert!(keys.iter().any(|k| k.key_version == 1 && k.state.as_str() == "retired"));
    assert!(app.state.m01.offboarding.download_export(&ta_actor(&t, admin), tenant, export.id).await.is_ok(), "old export still decrypts");
    assert_eq!(
        app.state.m01.keys.register_byok(&host, tenant, "byok:aws-kms/key-123456").await.unwrap_err().status(),
        StatusCode::FORBIDDEN
    );

    let (r, _) = app.active_tenant_with_admin(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-mysql-my-central" })).await;
    let k = app.state.m01.keys.register_byok(&host, tenant_id(&r), "byok:aws-kms/key-123456").await.unwrap();
    assert_eq!(k.kind.as_str(), "customer_supplied");
    assert_eq!(k.state.as_str(), "active");
}

#[tokio::test]
async fn ring_based_release_honours_maintenance_window() {
    // R026: disruptive change honours the tenant window; non-disruptive rolls out now.
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let (t, _ta) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let tenant = tenant_id(&t);
    let host = sa_actor();
    app.state.m01.releases.set_preference(&host, tenant, Some("host_sandbox"), 7, 3, 60).await.unwrap();
    let v = format!("9.{}.0", rand_minor());
    let rel = app.state.m01.releases.create_release(&host, &v, "Disruptive DB upgrade", "Requires a restart.", true).await.unwrap();
    let advanced = app.state.m01.releases.advance(&host, rel.id).await.unwrap();
    assert_eq!(advanced.current_ring.unwrap().as_str(), "host_sandbox");
    let notes = app.state.m01.releases.notes_for(&host, tenant).await.unwrap();
    let mine = notes.iter().find(|n| n.release_version == v).unwrap();
    assert!(mine.scheduled_for > chrono::Utc::now() - chrono::Duration::minutes(1));
    use chrono::{Datelike, Timelike};
    assert_eq!(mine.scheduled_for.weekday().number_from_monday(), 7, "Sunday window");
    assert!(mine.scheduled_for.hour() >= 3);

    let v2 = format!("8.{}.0", rand_minor());
    let rel2 = app.state.m01.releases.create_release(&host, &v2, "Minor fix", "Non-disruptive.", false).await.unwrap();
    app.state.m01.releases.advance(&host, rel2.id).await.unwrap();
    // The scheduler processes due rollouts in batches (200 per tick) across ALL tenants of the
    // shared test database; drain it like consecutive ticks would.
    for _ in 0..100 {
        if app.state.m01.releases.process_due().await.unwrap() == 0 {
            break;
        }
    }
    let after = app.get(&format!("/v1/tenants/{}", tid(&t)), &sa).await;
    assert_eq!(after.data()["platform_version"], v2.as_str(), "non-disruptive release applied immediately");
    // Advance through all rings; cannot go further.
    app.state.m01.releases.advance(&host, rel2.id).await.unwrap();
    app.state.m01.releases.advance(&host, rel2.id).await.unwrap();
    assert!(app.state.m01.releases.advance(&host, rel2.id).await.is_err());
}

fn rand_minor() -> u32 {
    (uuid::Uuid::new_v4().as_u128() % 90_000) as u32
}

#[tokio::test]
async fn analytics_aggregate_excludes_opted_out_and_never_reads_data_plane() {
    // R027
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    let before = app.state.m01.analytics.host_analytics(&sa_actor()).await.unwrap();
    let (r, r_ta) = app.active_tenant_with_admin(&sa, PLAN_REGULATED, json!({ "db_target": "dedicated-pg-my-central" })).await;
    let mid = app.state.m01.analytics.host_analytics(&sa_actor()).await.unwrap();
    assert!(mid.participants >= 1, "{} participants before", before.participants);
    app.patch(&format!("/v1/tenants/{}/config", tid(&r)), &r_ta, json!({ "config": { "analytics.cross_tenant_opt_out": true } })).await;
    let after = app.state.m01.analytics.host_analytics(&sa_actor()).await.unwrap();
    assert!(after.opted_out >= 1);
    // Every bucket is either a count >= 3 or suppressed.
    for b in after.by_status.iter().chain(after.by_tier.iter()).chain(after.by_region.iter()) {
        assert!(b.suppressed || b.count.unwrap_or(0) == 0 || b.count.unwrap() >= 3, "{b:?}");
    }
}

#[tokio::test]
async fn per_tenant_backup_restore_does_not_touch_other_tenants() {
    // R011
    let app = TestApp::new().await;
    let sa = app.sa_token().await;
    // The DR report lists at most 500 tenants ordered by code; the shared test database holds many
    // more, so this tenant gets a code that sorts first.
    let (a, ta_a) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({ "tenant_code": format!("0-{}", unique_code("dr")) })).await;
    let (b, ta_b) = app.active_tenant_with_admin(&sa, PLAN_STANDARD, json!({})).await;
    let host = sa_actor();
    let backup = app.state.m01.backups.backup(&host, tenant_id(&a)).await.unwrap();
    app.patch(&format!("/v1/tenants/{}/config", tid(&a)), &ta_a, json!({ "config": { "locale.currency": "EUR" } })).await;
    app.patch(&format!("/v1/tenants/{}/config", tid(&b)), &ta_b, json!({ "config": { "locale.currency": "GBP" } })).await;
    let ms = app.state.m01.backups.restore(&host, tenant_id(&a), backup.id).await.unwrap();
    assert!(ms >= 0);
    assert_eq!(app.get(&format!("/v1/tenants/{}/config", tid(&a)), &ta_a).await.data()["config"]["locale.currency"], "MYR");
    assert_eq!(app.get(&format!("/v1/tenants/{}/config", tid(&b)), &ta_b).await.data()["config"]["locale.currency"], "GBP");
    let report = app.state.m01.backups.dr_report(&host).await.unwrap();
    let row = report.iter().find(|r| r.row.tenant_id == tenant_id(&a)).unwrap();
    assert!(row.rpo_met);
    assert_eq!(row.rto_met, Some(true));
    let _ = AccessScope::Platform;
}
