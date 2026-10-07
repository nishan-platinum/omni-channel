//! Super Admin (host console) routes: dashboard, tenant directory, provisioning, per-tenant
//! management tabs, releases, analytics, entitlement matrix, DR report.

use std::collections::BTreeMap;

use axum::extract::{Multipart, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use serde::Deserialize;

use crate::app::AppState;
use crate::bootstrap_auth::extract::{csrf_matches, CsrfForm, SuperAdmin, WebUser};
use crate::platform::audit;
use crate::platform::errors::{AppError, ErrorCode};
use crate::web_support::{redirect_with, render, IncomingFlash, PageCtx, PageError, PageResult};

use super::super::application::ports::TenantFilter;
use super::super::application::provisioning::CreateTenantCommand;
use super::super::domain::features::FEATURES;
use super::super::domain::storage::{Region, ALL_REGIONS};
use super::super::domain::tenant::ALL_STATUSES;
use super::super::domain::TenantStatus;
use super::actor;
use super::forms::*;
use super::shared::{self, Ctx};
use super::views::*;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/admin", get(dashboard))
        .route("/admin/tenants", get(directory).post(create_tenant))
        .route("/admin/tenants/new", get(new_tenant))
        .route("/admin/plans/{id}/features", get(plan_features))
        .route("/admin/releases", get(releases).post(release_create))
        .route("/admin/releases/{id}/advance", post(release_advance))
        .route("/admin/analytics", get(analytics))
        .route("/admin/entitlements", get(entitlements))
        .route("/admin/dr", get(dr_report))
        .route("/branding/preview", post(branding_preview))
        .route("/admin/tenants/{id}", get(overview))
        .route("/admin/tenants/{id}/status", post(status))
        .route("/admin/tenants/{id}/details", post(details))
        .route("/admin/tenants/{id}/provisioning/retry", post(retry))
        .route("/admin/tenants/{id}/discard", post(discard))
        .route("/admin/tenants/{id}/isolation-check", post(isolation_check))
        .route("/admin/tenants/{id}/storage", get(storage))
        .route("/admin/tenants/{id}/storage/check", post(storage_check))
        .route("/admin/tenants/{id}/legal-hold", post(legal_hold))
        .route("/admin/tenants/{id}/config", get(config).post(config_save))
        .route("/admin/tenants/{id}/features/{key}", post(feature_toggle))
        .route("/admin/tenants/{id}/quotas", get(quotas))
        .route("/admin/tenants/{id}/quotas/limits", post(quota_limits))
        .route("/admin/tenants/{id}/quotas/metering", post(metering))
        .route("/admin/tenants/{id}/quotas/consume", post(consume))
        .route("/admin/tenants/{id}/quotas/statement.csv", get(statement))
        .route("/admin/tenants/{id}/branding", get(branding).post(branding_save))
        .route("/admin/tenants/{id}/branding/logo", post(branding_logo))
        .route("/admin/tenants/{id}/branding/verify", post(branding_verify))
        .route("/admin/tenants/{id}/branding/senders", post(sender_add))
        .route("/admin/tenants/{id}/branding/senders/verify", post(sender_verify))
        .route("/admin/tenants/{id}/support", get(support).post(support_request))
        .route("/admin/tenants/{id}/support/open", post(support_open))
        .route("/admin/tenants/{id}/support/{gid}/revoke", post(support_revoke))
        .route("/admin/tenants/{id}/sandboxes", get(sandboxes).post(sandbox_create))
        .route("/admin/tenants/{id}/sandboxes/{sid}/promote", post(sandbox_promote))
        .route("/admin/tenants/{id}/baselines", get(baselines))
        .route("/admin/tenants/{id}/baselines/export", post(baseline_export))
        .route("/admin/tenants/{id}/baselines/import", post(baseline_import))
        .route("/admin/tenants/{id}/baselines/{bid}/rollback", post(baseline_rollback))
        .route("/admin/tenants/{id}/baselines/{bid}/download", get(baseline_download))
        .route("/admin/tenants/{id}/release", get(release).post(release_save))
        .route("/admin/tenants/{id}/keys", get(keys))
        .route("/admin/tenants/{id}/keys/rotate", post(key_rotate))
        .route("/admin/tenants/{id}/keys/byok", post(key_byok))
        .route("/admin/tenants/{id}/offboarding", get(offboarding))
        .route("/admin/tenants/{id}/offboarding/export", post(export_generate))
        .route("/admin/tenants/{id}/offboarding/exports/{eid}", get(export_download))
        .route("/admin/tenants/{id}/audit", get(audit_page))
        .route("/admin/tenants/{id}/backups", get(backups).post(backup_create))
        .route("/admin/tenants/{id}/backups/{bid}/restore", post(backup_restore))
}

fn dev(s: &AppState) -> bool {
    s.config.app_env.is_development()
}

async fn dashboard(State(s): State<AppState>, SuperAdmin(p, rc): SuperAdmin, IncomingFlash(f): IncomingFlash) -> PageResult {
    let a = actor(&p, &rc);
    let counts = s.m01.directory.status_counts(&a).await?;
    let total = counts.values().sum();
    let security_events = audit::recent_security_events(&s.db.app, 10).await?;
    let failed = s.m01.directory.failed_provisioning(&a).await?.into_iter().map(|(id, code, at)| (id.to_string(), code, at)).collect();
    let chain_ok = audit::verify_chain(&s.db.app, 500).await?;
    let had = f.is_some();
    let counts = ALL_STATUSES.iter().map(|st| (st.as_str().to_string(), counts.get(st.as_str()).copied().unwrap_or(0))).collect();
    let v = DashboardView {
        page: PageCtx::for_user("Host console", &p, "dashboard", f, dev(&s)),
        counts,
        total,
        security_events,
        failed,
        chain_ok,
    };
    Ok(render(&v, had))
}

async fn directory(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    headers: HeaderMap,
    Query(q): Query<DirectoryQuery>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let a = actor(&p, &rc);
    let filter = TenantFilter {
        q: q.q.clone().filter(|v| !v.trim().is_empty()),
        status: q.status.as_deref().filter(|v| !v.is_empty()).map(TenantStatus::parse).transpose().map_err(AppError::from)?,
        region: q.region.as_deref().filter(|v| !v.is_empty()).map(Region::parse).transpose().map_err(AppError::from)?,
        cursor: q.cursor.clone().filter(|v| !v.is_empty()),
        limit: 25,
    };
    let page = s.m01.directory.directory(&a, filter).await?;
    let (qs, st, rg) = (q.q.unwrap_or_default(), q.status.unwrap_or_default(), q.region.unwrap_or_default());
    if headers.get("hx-request").is_some() {
        let v = DirectoryRowsPartial {
            page: PageCtx::for_user("", &p, "tenants", None, false),
            rows: page.items,
            q: qs,
            status: st,
            region: rg,
            next_cursor: page.next_cursor,
        };
        return Ok(render(&v, false));
    }
    let had = f.is_some();
    let v = DirectoryView {
        page: PageCtx::for_user("Tenant directory", &p, "tenants", f, dev(&s)),
        rows: page.items,
        q: qs,
        status: st,
        region: rg,
        next_cursor: page.next_cursor,
        statuses: ALL_STATUSES.iter().map(|s| s.as_str()).collect(),
        regions: ALL_REGIONS.iter().map(|r| r.as_str()).collect(),
    };
    Ok(render(&v, had))
}

async fn new_tenant_view(
    s: &AppState,
    p: &crate::bootstrap_auth::service::Principal,
    rc: &crate::platform::observability::RequestContext,
    form: CreateTenantForm,
    errors: BTreeMap<String, String>,
    general: Option<String>,
) -> Result<NewTenantView, PageError> {
    let a = actor(p, rc);
    let plans =
        s.m01.directory.plans().await?.into_iter().map(|pl| (pl.id.to_string(), pl.name.clone(), pl.tier.label().to_string())).collect();
    let templates = s.m01.directory.templates().await?;
    let parents = s
        .m01
        .directory
        .list(&a, TenantFilter { limit: 200, ..Default::default() })
        .await?
        .items
        .into_iter()
        .filter(|t| !t.is_sandbox && matches!(t.status, TenantStatus::Draft | TenantStatus::Active | TenantStatus::Suspended))
        .collect();
    Ok(NewTenantView {
        page: PageCtx::for_user("New tenant", p, "tenants", None, dev(s)),
        form,
        errors,
        general_error: general,
        plans,
        templates,
        targets: s.m01.directory.dedicated_targets(),
        parents,
        regions: ALL_REGIONS.iter().map(|r| (r.as_str(), r.label())).collect(),
    })
}

async fn new_tenant(State(s): State<AppState>, SuperAdmin(p, rc): SuperAdmin) -> PageResult {
    let form = CreateTenantForm { region: "my-central".into(), ..Default::default() };
    let v = new_tenant_view(&s, &p, &rc, form, BTreeMap::new(), None).await?;
    Ok(render(&v, false))
}

async fn create_tenant(State(s): State<AppState>, f: CsrfForm<CreateTenantForm>) -> PageResult {
    if f.user.role != crate::bootstrap_auth::service::Role::SuperAdmin {
        return Err(shared::not_permitted());
    }
    let form = f.form.clone();
    let cmd = CreateTenantCommand {
        name: form.name.clone(),
        legal_name: Some(form.legal_name.clone()),
        region: Some(form.region.clone()),
        plan_id: Some(form.plan_id.clone()),
        primary_admin_email: form.primary_admin_email.clone(),
        parent_tenant_id: Some(form.parent_tenant_id.clone()),
        tenant_code: Some(form.tenant_code.clone()),
        template_code: Some(form.template_code.clone()),
        db_target: Some(form.db_target.clone()),
        inherit_branding: form.inherit_branding.is_some(),
        inherit_config: form.inherit_config.is_some(),
    };
    match s.m01.provisioning.create(&actor(&f.user, &f.ctx), cmd).await {
        Ok(out) => {
            let msg = if out.completed {
                format!(
                    "Tenant {} created in Draft (provisioned in {} ms). Review it, then activate. The invitation is in the outbox.",
                    out.tenant.code, out.duration_ms
                )
            } else {
                format!("Tenant {} created in Draft but provisioning failed — see the provisioning steps and retry.", out.tenant.code)
            };
            Ok(redirect_with(&format!("/admin/tenants/{}", out.tenant.id), if out.completed { "success" } else { "error" }, &msg))
        }
        Err(e) => {
            let errors: BTreeMap<String, String> = e.details.iter().map(|d| (d.field.clone(), d.message.clone())).collect();
            let general = Some(format!("{} ({})", e.message, e.code.as_str()));
            let status = e.status();
            if e.code == ErrorCode::Internal {
                e.log();
            }
            let v = new_tenant_view(&s, &f.user, &f.ctx, form, errors, general).await?;
            let mut r = render(&v, false);
            *r.status_mut() = status;
            Ok(r)
        }
    }
}

/// FD-008: features filtered to the selected plan's entitlements.
async fn plan_features(State(s): State<AppState>, SuperAdmin(_, _): SuperAdmin, Path(id): Path<String>) -> PageResult {
    let pid = uuid::Uuid::parse_str(&id).map_err(|_| AppError::not_found("Plan does not exist"))?;
    let plan = s.m01.deps.plans.get(pid).await?.filter(|p| p.active).ok_or_else(|| AppError::not_found("Plan does not exist"))?;
    let features = FEATURES.iter().map(|f| (f.key.to_string(), f.label.to_string(), plan.entitles(f.key))).collect();
    Ok(render(&PlanFeaturesPartial { tier: plan.tier.label().into(), features }, false))
}

async fn branding_preview(_u: WebUser, f: CsrfForm<PreviewForm>) -> impl IntoResponse {
    shared::branding_preview(f.form)
}

async fn releases(State(s): State<AppState>, SuperAdmin(p, rc): SuperAdmin, IncomingFlash(f): IncomingFlash) -> PageResult {
    let releases = s.m01.releases.releases(&actor(&p, &rc)).await?;
    let had = f.is_some();
    Ok(render(&ReleasesAdminView { page: PageCtx::for_user("Platform releases", &p, "releases", f, dev(&s)), releases }, had))
}

async fn release_create(State(s): State<AppState>, f: CsrfForm<NewReleaseForm>) -> PageResult {
    let r = s
        .m01
        .releases
        .create_release(&actor(&f.user, &f.ctx), &f.form.release_version, &f.form.title, &f.form.notes, f.form.disruptive.is_some())
        .await;
    Ok(match r {
        Ok(rel) => redirect_with("/admin/releases", "success", &format!("Release {} created (planned).", rel.release_version)),
        Err(e) => redirect_with("/admin/releases", "error", &format!("{} ({})", e.message, e.code.as_str())),
    })
}

async fn release_advance(State(s): State<AppState>, Path(id): Path<String>, f: CsrfForm<EmptyForm>) -> PageResult {
    let rid = uuid::Uuid::parse_str(&id).map_err(|_| AppError::not_found("Release does not exist"))?;
    let r = s.m01.releases.advance(&actor(&f.user, &f.ctx), rid).await;
    let _ = s.m01.releases.process_due().await;
    Ok(match r {
        Ok(rel) => redirect_with(
            "/admin/releases",
            "success",
            &format!("Release {} advanced to ring {}.", rel.release_version, rel.current_ring.map(|r| r.label()).unwrap_or("-")),
        ),
        Err(e) => redirect_with("/admin/releases", "error", &format!("{} ({})", e.message, e.code.as_str())),
    })
}

async fn analytics(State(s): State<AppState>, SuperAdmin(p, rc): SuperAdmin) -> PageResult {
    let a = s.m01.analytics.host_analytics(&actor(&p, &rc)).await?;
    Ok(render(&AnalyticsView { page: PageCtx::for_user("Aggregated analytics", &p, "analytics", None, dev(&s)), a }, false))
}

async fn entitlements(State(s): State<AppState>, SuperAdmin(p, rc): SuperAdmin, Query(q): Query<DirectoryQuery>) -> PageResult {
    let m = s
        .m01
        .directory
        .entitlement_matrix(&actor(&p, &rc), TenantFilter { cursor: q.cursor, q: q.q, limit: 50, ..Default::default() })
        .await?;
    Ok(render(&EntitlementsView { page: PageCtx::for_user("Entitlement matrix", &p, "entitlements", None, dev(&s)), m }, false))
}

async fn dr_report(State(s): State<AppState>, SuperAdmin(p, rc): SuperAdmin) -> PageResult {
    let rows = s.m01.backups.dr_report(&actor(&p, &rc)).await?;
    Ok(render(&DrView { page: PageCtx::for_user("Backup / DR report", &p, "dr", None, dev(&s)), rows }, false))
}

// ---- per-tenant tabs (Super Admin) ------------------------------------------------------------

macro_rules! sa_page {
    ($name:ident, $f:path) => {
        async fn $name(
            State(s): State<AppState>,
            SuperAdmin(p, rc): SuperAdmin,
            Path(id): Path<String>,
            IncomingFlash(f): IncomingFlash,
        ) -> PageResult {
            let c = Ctx::sa(&s, &p, &rc, &id, f)?;
            $f(&c).await
        }
    };
}

macro_rules! sa_action {
    ($name:ident, $f:path) => {
        async fn $name(State(s): State<AppState>, Path(id): Path<String>, f: CsrfForm<EmptyForm>) -> PageResult {
            let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
            $f(&c).await
        }
    };
    ($name:ident, $f:path, $form:ty) => {
        async fn $name(State(s): State<AppState>, Path(id): Path<String>, f: CsrfForm<$form>) -> PageResult {
            let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
            $f(&c, f.form).await
        }
    };
}

sa_page!(overview, shared::overview_page);
sa_page!(storage, shared::storage_page);
sa_page!(release, shared::release_page);
sa_page!(keys, shared::keys_page);
sa_page!(offboarding, shared::offboarding_page);
sa_page!(backups, shared::backups_page);
sa_action!(status, shared::status_change, StatusForm);
sa_action!(details, shared::details_save, DetailsForm);
sa_action!(config_save, shared::config_save, Vec<(String, String)>);
sa_action!(quota_limits, shared::quota_limits_save, Vec<(String, String)>);
sa_action!(metering, shared::metering_simulate, MeteringForm);
sa_action!(consume, shared::quota_consume, ConsumeForm);
sa_action!(branding_save, shared::branding_save, BrandingForm);
sa_action!(branding_verify, shared::branding_verify);
sa_action!(sender_add, shared::sender_add, DomainForm);
sa_action!(sender_verify, shared::sender_verify, DomainForm);
sa_action!(support_request, shared::support_request, GrantRequestForm);
sa_action!(support_open, shared::support_open);
sa_action!(sandbox_create, shared::sandbox_create);
sa_action!(baseline_export, shared::baseline_export, BaselineExportForm);
sa_action!(baseline_import, shared::baseline_import, BaselineImportForm);
sa_action!(release_save, shared::release_save, ReleasePrefForm);
sa_action!(key_rotate, shared::key_rotate);
sa_action!(key_byok, shared::key_byok, ByokForm);
sa_action!(export_generate, shared::export_generate);
sa_action!(backup_create, shared::backup_create);

async fn config(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path(id): Path<String>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, f)?;
    shared::config_page(&c, BTreeMap::new(), None, StatusCode::OK).await
}

async fn branding(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path(id): Path<String>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, f)?;
    shared::branding_page(&c, None, BTreeMap::new(), None, StatusCode::OK).await
}

#[derive(Deserialize, Default)]
struct MonthQuery {
    month: Option<String>,
}

async fn quotas(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path(id): Path<String>,
    Query(q): Query<MonthQuery>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, f)?;
    shared::quotas_page(&c, q.month).await
}

async fn statement(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path(id): Path<String>,
    Query(q): Query<MonthQuery>,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, None)?;
    shared::statement_csv(&c, q.month).await
}

async fn support(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path(id): Path<String>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, f)?;
    shared::support_page(&c, None).await
}

async fn sandboxes(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path(id): Path<String>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, f)?;
    shared::sandboxes_page(&c, None).await
}

#[derive(Deserialize, Default)]
struct BeforeQuery {
    before: Option<i64>,
}

async fn audit_page(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path(id): Path<String>,
    Query(q): Query<BeforeQuery>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, f)?;
    shared::audit_page(&c, q.before).await
}

async fn baselines(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path(id): Path<String>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, f)?;
    shared::baselines_page(&c, None, String::new(), String::new(), None).await
}

async fn baseline_download(
    State(s): State<AppState>,
    SuperAdmin(p, rc): SuperAdmin,
    Path((id, bid)): Path<(String, String)>,
) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, None)?;
    shared::baseline_download(&c, &bid).await
}

async fn baseline_rollback(
    State(s): State<AppState>,
    Path((id, bid)): Path<(String, String)>,
    f: CsrfForm<ChangeRecordForm>,
) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    shared::baseline_rollback(&c, &bid, f.form).await
}

async fn feature_toggle(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path((id, key)): Path<(String, String)>,
    f: CsrfForm<FeatureToggleForm>,
) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    shared::feature_toggle(&c, &headers, &key, f.form).await
}

async fn support_revoke(State(s): State<AppState>, Path((id, gid)): Path<(String, String)>, f: CsrfForm<EmptyForm>) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    shared::support_revoke(&c, &gid).await
}

async fn sandbox_promote(State(s): State<AppState>, Path((id, sid)): Path<(String, String)>, f: CsrfForm<ChangeRecordForm>) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    shared::sandbox_promote(&c, &sid, f.form).await
}

async fn export_download(State(s): State<AppState>, SuperAdmin(p, rc): SuperAdmin, Path((id, eid)): Path<(String, String)>) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, None)?;
    shared::export_download(&c, &eid).await
}

async fn backup_restore(State(s): State<AppState>, Path((id, bid)): Path<(String, String)>, f: CsrfForm<EmptyForm>) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    shared::backup_restore(&c, &bid).await
}

async fn retry(State(s): State<AppState>, Path(id): Path<String>, f: CsrfForm<EmptyForm>) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    Ok(match s.m01.provisioning.retry(&c.actor(), c.tenant).await {
        Ok(o) => redirect_with(
            &format!("/admin/tenants/{id}"),
            if o.completed { "success" } else { "error" },
            if o.completed { "Provisioning completed." } else { "Provisioning still failing — see the steps." },
        ),
        Err(e) => redirect_with(&format!("/admin/tenants/{id}"), "error", &format!("{} ({})", e.message, e.code.as_str())),
    })
}

async fn discard(State(s): State<AppState>, Path(id): Path<String>, f: CsrfForm<EmptyForm>) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    Ok(match s.m01.provisioning.discard_draft(&c.actor(), c.tenant).await {
        Ok(()) => redirect_with("/admin/tenants", "success", "Draft discarded (provisioning rolled back)."),
        Err(e) => redirect_with(&format!("/admin/tenants/{id}"), "error", &format!("{} ({})", e.message, e.code.as_str())),
    })
}

async fn isolation_check(State(s): State<AppState>, Path(id): Path<String>, f: CsrfForm<EmptyForm>) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    Ok(match s.m01.isolation.run(&c.actor(), c.tenant).await {
        Ok(r) if r.passed => redirect_with(&format!("/admin/tenants/{id}/storage"), "success", "Isolation smoke test passed."),
        Ok(r) => redirect_with(
            &format!("/admin/tenants/{id}/storage"),
            "error",
            &format!("Isolation smoke test FAILED: {}", r.failures().join(", ")),
        ),
        Err(e) => redirect_with(&format!("/admin/tenants/{id}/storage"), "error", &format!("{} ({})", e.message, e.code.as_str())),
    })
}

async fn storage_check(State(s): State<AppState>, Path(id): Path<String>, f: CsrfForm<EmptyForm>) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    Ok(match s.m01.directory.check_connectivity(&c.actor(), c.tenant).await {
        Ok(p) => redirect_with(
            &format!("/admin/tenants/{id}/storage"),
            if p.last_check_ok == Some(true) { "success" } else { "error" },
            p.last_check_message.as_deref().unwrap_or("Checked."),
        ),
        Err(e) => redirect_with(&format!("/admin/tenants/{id}/storage"), "error", &format!("{} ({})", e.message, e.code.as_str())),
    })
}

async fn legal_hold(State(s): State<AppState>, Path(id): Path<String>, f: CsrfForm<LegalHoldForm>) -> PageResult {
    let c = Ctx::sa(&s, &f.user, &f.ctx, &id, None)?;
    let hold = f.form.hold == "true";
    Ok(match s.m01.lifecycle.set_legal_hold(&c.actor(), c.tenant, hold, Some(f.form.reason.as_str()).filter(|r| !r.is_empty())).await {
        Ok(()) => redirect_with(
            &format!("/admin/tenants/{id}/offboarding"),
            "success",
            if hold { "Legal hold placed — purge blocked." } else { "Legal hold released." },
        ),
        Err(e) => redirect_with(&format!("/admin/tenants/{id}/offboarding"), "error", &format!("{} ({})", e.message, e.code.as_str())),
    })
}

/// Logo upload (multipart). CSRF token is a form field of the multipart body.
async fn branding_logo(State(s): State<AppState>, WebUser(p, rc): WebUser, Path(id): Path<String>, mp: Multipart) -> PageResult {
    let c = Ctx::sa(&s, &p, &rc, &id, None)?;
    let bytes = read_logo_multipart(&p, mp).await?;
    shared::branding_logo(&c, bytes).await
}

pub async fn read_logo_multipart(p: &crate::bootstrap_auth::service::Principal, mut mp: Multipart) -> Result<Vec<u8>, PageError> {
    let mut csrf = None;
    let mut logo = None;
    while let Some(field) = mp.next_field().await.map_err(|_| AppError::validation("logo", "Logo must be PNG/SVG under 2MB"))? {
        match field.name() {
            Some("_csrf") => csrf = Some(field.text().await.map_err(|_| AppError::validation("_csrf", "Invalid CSRF field"))?),
            Some("logo") => {
                let b = field.bytes().await.map_err(|_| AppError::validation("logo", "Logo must be PNG/SVG under 2MB"))?;
                logo = Some(b.to_vec());
            }
            _ => {}
        }
    }
    if !csrf_matches(p, csrf.as_deref()) {
        return Err(AppError::forbidden("Invalid or missing CSRF token; reload the page and try again").into());
    }
    logo.filter(|b| !b.is_empty()).ok_or_else(|| AppError::validation("logo", "Choose a PNG or SVG file").into())
}
