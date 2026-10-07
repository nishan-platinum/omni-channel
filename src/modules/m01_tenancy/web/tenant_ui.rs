//! Tenant Admin self-service routes (`/tenant/...`). No route takes a tenant id: the tenant is
//! always the one bound to the authenticated session (M01-F02).

use std::collections::BTreeMap;

use axum::extract::{Multipart, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use serde::Deserialize;

use crate::app::AppState;
use crate::bootstrap_auth::extract::{CsrfForm, TenantAdmin};
use crate::web_support::{IncomingFlash, PageResult};

use super::forms::*;
use super::shared::{self, Ctx};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/tenant", get(overview))
        .route("/tenant/details", post(details))
        .route("/tenant/config", get(config).post(config_save))
        .route("/tenant/features/{key}", post(feature_toggle))
        .route("/tenant/quotas", get(quotas))
        .route("/tenant/quotas/consume", post(consume))
        .route("/tenant/quotas/statement.csv", get(statement))
        .route("/tenant/branding", get(branding).post(branding_save))
        .route("/tenant/branding/logo", post(branding_logo))
        .route("/tenant/branding/verify", post(branding_verify))
        .route("/tenant/branding/senders", post(sender_add))
        .route("/tenant/branding/senders/verify", post(sender_verify))
        .route("/tenant/storage", get(storage))
        .route("/tenant/support", get(support))
        .route("/tenant/support/{gid}/approve", post(support_approve))
        .route("/tenant/support/{gid}/reject", post(support_reject))
        .route("/tenant/support/{gid}/revoke", post(support_revoke))
        .route("/tenant/sandboxes", get(sandboxes).post(sandbox_create))
        .route("/tenant/sandboxes/{sid}/promote", post(sandbox_promote))
        .route("/tenant/baselines", get(baselines))
        .route("/tenant/baselines/export", post(baseline_export))
        .route("/tenant/baselines/import", post(baseline_import))
        .route("/tenant/baselines/{bid}/rollback", post(baseline_rollback))
        .route("/tenant/baselines/{bid}/download", get(baseline_download))
        .route("/tenant/release", get(release).post(release_save))
        .route("/tenant/keys", get(keys))
        .route("/tenant/offboarding", get(offboarding))
        .route("/tenant/offboarding/export", post(export_generate))
        .route("/tenant/offboarding/exports/{eid}", get(export_download))
        .route("/tenant/audit", get(audit_page))
}

macro_rules! ta_page {
    ($name:ident, $f:path) => {
        async fn $name(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, IncomingFlash(f): IncomingFlash) -> PageResult {
            let c = Ctx::ta(&s, &p, &rc, f)?;
            $f(&c).await
        }
    };
}

macro_rules! ta_action {
    ($name:ident, $f:path) => {
        async fn $name(State(s): State<AppState>, f: CsrfForm<EmptyForm>) -> PageResult {
            let c = Ctx::ta(&s, &f.user, &f.ctx, None)?;
            $f(&c).await
        }
    };
    ($name:ident, $f:path, $form:ty) => {
        async fn $name(State(s): State<AppState>, f: CsrfForm<$form>) -> PageResult {
            let c = Ctx::ta(&s, &f.user, &f.ctx, None)?;
            $f(&c, f.form).await
        }
    };
}

ta_page!(overview, shared::overview_page);
ta_page!(storage, shared::storage_page);
ta_page!(release, shared::release_page);
ta_page!(keys, shared::keys_page);
ta_page!(offboarding, shared::offboarding_page);
ta_action!(details, shared::details_save, DetailsForm);
ta_action!(config_save, shared::config_save, Vec<(String, String)>);
ta_action!(consume, shared::quota_consume, ConsumeForm);
ta_action!(branding_save, shared::branding_save, BrandingForm);
ta_action!(branding_verify, shared::branding_verify);
ta_action!(sender_add, shared::sender_add, DomainForm);
ta_action!(sender_verify, shared::sender_verify, DomainForm);
ta_action!(sandbox_create, shared::sandbox_create);
ta_action!(baseline_export, shared::baseline_export, BaselineExportForm);
ta_action!(baseline_import, shared::baseline_import, BaselineImportForm);
ta_action!(release_save, shared::release_save, ReleasePrefForm);
ta_action!(export_generate, shared::export_generate);

async fn config(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, IncomingFlash(f): IncomingFlash) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, f)?;
    shared::config_page(&c, BTreeMap::new(), None, StatusCode::OK).await
}

async fn branding(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, IncomingFlash(f): IncomingFlash) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, f)?;
    shared::branding_page(&c, None, BTreeMap::new(), None, StatusCode::OK).await
}

#[derive(Deserialize, Default)]
struct MonthQuery {
    month: Option<String>,
}

async fn quotas(
    State(s): State<AppState>,
    TenantAdmin(p, rc): TenantAdmin,
    Query(q): Query<MonthQuery>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, f)?;
    shared::quotas_page(&c, q.month).await
}

async fn statement(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, Query(q): Query<MonthQuery>) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, None)?;
    shared::statement_csv(&c, q.month).await
}

async fn support(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, IncomingFlash(f): IncomingFlash) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, f)?;
    shared::support_page(&c, None).await
}

async fn sandboxes(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, IncomingFlash(f): IncomingFlash) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, f)?;
    shared::sandboxes_page(&c, None).await
}

async fn baselines(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, IncomingFlash(f): IncomingFlash) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, f)?;
    shared::baselines_page(&c, None, String::new(), String::new(), None).await
}

#[derive(Deserialize, Default)]
struct BeforeQuery {
    before: Option<i64>,
}

async fn audit_page(
    State(s): State<AppState>,
    TenantAdmin(p, rc): TenantAdmin,
    Query(q): Query<BeforeQuery>,
    IncomingFlash(f): IncomingFlash,
) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, f)?;
    shared::audit_page(&c, q.before).await
}

async fn feature_toggle(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(key): Path<String>,
    f: CsrfForm<FeatureToggleForm>,
) -> PageResult {
    let c = Ctx::ta(&s, &f.user, &f.ctx, None)?;
    shared::feature_toggle(&c, &headers, &key, f.form).await
}

async fn support_approve(State(s): State<AppState>, Path(gid): Path<String>, f: CsrfForm<DecisionForm>) -> PageResult {
    let c = Ctx::ta(&s, &f.user, &f.ctx, None)?;
    shared::support_decide(&c, &gid, true, f.form).await
}

async fn support_reject(State(s): State<AppState>, Path(gid): Path<String>, f: CsrfForm<DecisionForm>) -> PageResult {
    let c = Ctx::ta(&s, &f.user, &f.ctx, None)?;
    shared::support_decide(&c, &gid, false, f.form).await
}

async fn support_revoke(State(s): State<AppState>, Path(gid): Path<String>, f: CsrfForm<EmptyForm>) -> PageResult {
    let c = Ctx::ta(&s, &f.user, &f.ctx, None)?;
    shared::support_revoke(&c, &gid).await
}

async fn sandbox_promote(State(s): State<AppState>, Path(sid): Path<String>, f: CsrfForm<ChangeRecordForm>) -> PageResult {
    let c = Ctx::ta(&s, &f.user, &f.ctx, None)?;
    shared::sandbox_promote(&c, &sid, f.form).await
}

async fn baseline_rollback(State(s): State<AppState>, Path(bid): Path<String>, f: CsrfForm<ChangeRecordForm>) -> PageResult {
    let c = Ctx::ta(&s, &f.user, &f.ctx, None)?;
    shared::baseline_rollback(&c, &bid, f.form).await
}

async fn baseline_download(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, Path(bid): Path<String>) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, None)?;
    shared::baseline_download(&c, &bid).await
}

async fn export_download(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, Path(eid): Path<String>) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, None)?;
    shared::export_download(&c, &eid).await
}

async fn branding_logo(State(s): State<AppState>, TenantAdmin(p, rc): TenantAdmin, mp: Multipart) -> PageResult {
    let c = Ctx::ta(&s, &p, &rc, None)?;
    let bytes = super::admin::read_logo_multipart(&p, mp).await?;
    shared::branding_logo(&c, bytes).await
}
