//! `/v1` JSON API. Spec endpoints (§10.6 / Part D §61): POST /v1/tenants, GET /v1/tenants/{id},
//! PATCH /v1/tenants/{id}/status, GET|PATCH /v1/tenants/{id}/config, GET /v1/tenants/{id}/quota,
//! PATCH /v1/tenants/{id}/branding. Documented implementation extensions are marked EXT.
//! Envelope per API-007 / ADR-0006.

use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::NaiveDate;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::app::AppState;
use crate::bootstrap_auth::extract::ApiUser;
use crate::platform::errors::{AppError, AppResult};
use crate::platform::idempotency::{self, Lookup};
use crate::platform::observability::current_correlation_id;
use crate::platform::security::sha256_hex;

use super::super::application::branding::BrandingPatch;
use super::super::application::ports::TenantFilter;
use super::super::application::provisioning::CreateTenantCommand;
use super::super::application::quotas::month_of;
use super::super::application::Actor;
use super::super::domain::quota::QuotaMetric;
use super::super::domain::storage::Region;
use super::super::domain::{Tenant, TenantId, TenantStatus};
use super::{actor, parse_tenant_id};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/tenants", post(create_tenant).get(list_tenants))
        .route("/v1/tenants/{id}", get(get_tenant))
        .route("/v1/tenants/{id}/status", axum::routing::patch(patch_status))
        .route("/v1/tenants/{id}/config", get(get_config).patch(patch_config))
        .route("/v1/tenants/{id}/quota", get(get_quota))
        .route("/v1/tenants/{id}/branding", axum::routing::patch(patch_branding))
        // EXT (implementation-specific, documented in DESIGN.md §API):
        .route("/v1/tenants/{id}/quota/consume", post(consume_quota))
        .route("/v1/tenants/{id}/usage/statement", get(usage_statement))
        .route("/v1/tenants/{id}/isolation-check", post(isolation_check))
        .route("/v1/reference/metering/{id}", post(reference_metering))
}

fn ok(status: StatusCode, data: Value) -> Response {
    (status, Json(json!({ "data": data, "meta": { "correlation_id": current_correlation_id() } }))).into_response()
}

fn parse_body<T: DeserializeOwned>(bytes: &Bytes) -> AppResult<T> {
    if bytes.is_empty() {
        return Err(AppError::validation("body", "JSON body is required"));
    }
    serde_json::from_slice(bytes).map_err(|e| AppError::validation("body", format!("Invalid JSON body: {e}")))
}

/// Scope check + per-tenant API rate limit (R012 noisy-neighbour, API-006) + actor mapping.
async fn guard(state: &AppState, u: &ApiUser, scope: &str) -> AppResult<Actor> {
    if !u.0.has_scope(scope) {
        return Err(AppError::forbidden(format!("Missing scope {scope}")));
    }
    if let Some(t) = u.0.tenant_id {
        let d = state.m01.quotas.check_api_rate(TenantId(t)).await?;
        u.1.set_rate(d.limit, d.remaining, d.reset_secs);
        if !d.allowed {
            return Err(AppError::rate_limited("Rate limit or quota exceeded", d.reset_secs));
        }
    }
    Ok(actor(&u.0, &u.1))
}

pub fn tenant_json(t: &Tenant) -> Value {
    json!({
        "tenant_id": t.id,
        "tenant_code": t.code,
        "name": t.name,
        "legal_name": t.legal_name,
        "region": t.region.as_str(),
        "plan_id": t.plan_id,
        "status": t.status.as_str(),
        "parent_tenant_id": t.parent_tenant_id,
        "primary_admin_email": t.primary_admin_email,
        "isolation_mode": t.isolation_mode.as_str(),
        "storage_strategy": t.storage_strategy.as_str(),
        "is_sandbox": t.is_sandbox,
        "sandbox_of_tenant_id": t.sandbox_of_tenant_id,
        "provisioning_status": t.provisioning_status.as_str(),
        "isolation_check_status": t.isolation_check_status.as_str(),
        "suspended_reason": t.suspended_reason,
        "created_at": t.created_at,
        "activated_at": t.activated_at,
        "grace_until": t.grace_until,
        "terminated_at": t.terminated_at,
        "purge_after": t.purge_after,
        "purged_at": t.purged_at,
        "legal_hold": t.legal_hold,
        "platform_version": t.platform_version,
        "updated_at": t.updated_at,
        "version": t.version,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTenantRequest {
    name: Option<String>,
    legal_name: Option<String>,
    region: Option<String>,
    plan_id: Option<String>,
    primary_admin_email: Option<String>,
    parent_tenant_id: Option<String>,
    /// EXT: explicit code (otherwise suggested from the name).
    tenant_code: Option<String>,
    /// EXT: provisioning template (R013).
    template_code: Option<String>,
    /// EXT: dedicated database target (Regulated tier, R008).
    db_target: Option<String>,
}

/// POST /v1/tenants → 201 {tenant_id, tenant_code, status:'draft', …}. Honours Idempotency-Key.
async fn create_tenant(State(state): State<AppState>, u: ApiUser, headers: HeaderMap, body: Bytes) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:write").await?;
    let key = match headers.get("idempotency-key").and_then(|v| v.to_str().ok()) {
        Some(k) => Some(idempotency::validate_key(k)?.to_string()),
        None => None,
    };
    let hash = sha256_hex(&body);
    if let Some(k) = &key {
        if let Lookup::Replay(status, body) = idempotency::lookup(&state.db.app, u.0.user_id, k, &hash).await? {
            return Ok((StatusCode::from_u16(status).unwrap_or(StatusCode::OK), Json(body)).into_response());
        }
    }
    let r: CreateTenantRequest = parse_body(&body)?;
    let cmd = CreateTenantCommand {
        name: r.name.unwrap_or_default(),
        legal_name: r.legal_name,
        region: r.region,
        plan_id: r.plan_id,
        primary_admin_email: r.primary_admin_email.unwrap_or_default(),
        parent_tenant_id: r.parent_tenant_id,
        tenant_code: r.tenant_code,
        template_code: r.template_code,
        db_target: r.db_target,
        ..Default::default()
    };
    let out = state.m01.provisioning.create(&a, cmd).await?;
    let mut data = tenant_json(&out.tenant);
    data["provisioning"] = json!({ "run_id": out.run_id, "completed": out.completed, "duration_ms": out.duration_ms, "notes": out.notes });
    let body = json!({ "data": data, "meta": { "correlation_id": current_correlation_id() } });
    if let Some(k) = &key {
        idempotency::store(&state.db.app, None, u.0.user_id, k, &hash, 201, &body).await?;
    }
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

#[derive(Deserialize, Default)]
struct ListQuery {
    q: Option<String>,
    status: Option<String>,
    region: Option<String>,
    cursor: Option<String>,
    limit: Option<i64>,
}

/// EXT: GET /v1/tenants (Super Admin directory, cursor pagination per API-004).
async fn list_tenants(State(state): State<AppState>, u: ApiUser, Query(q): Query<ListQuery>) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:read").await?;
    let filter = TenantFilter {
        q: q.q,
        status: q.status.as_deref().filter(|s| !s.is_empty()).map(TenantStatus::parse).transpose()?,
        region: q.region.as_deref().filter(|s| !s.is_empty()).map(Region::parse).transpose()?,
        cursor: q.cursor,
        limit: q.limit.unwrap_or(25),
    };
    let page = state.m01.directory.list(&a, filter).await?;
    Ok((
        StatusCode::OK,
        Json(json!({ "data": page.items, "meta": { "next_cursor": page.next_cursor, "correlation_id": current_correlation_id() } })),
    )
        .into_response())
}

async fn get_tenant(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:read").await?;
    let t = state.m01.directory.get(&a, parse_tenant_id(&id)?).await?;
    Ok(ok(StatusCode::OK, tenant_json(&t)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusRequest {
    status: Option<String>,
    reason: Option<String>,
}

async fn patch_status(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>, body: Bytes) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:write").await?;
    let id = parse_tenant_id(&id)?;
    let r: StatusRequest = parse_body(&body)?;
    let status = r.status.ok_or_else(|| AppError::validation("status", "status is required"))?;
    let t = state.m01.lifecycle.change_status(&a, id, &status, r.reason.as_deref()).await?;
    Ok(ok(StatusCode::OK, json!({ "tenant_id": t.id, "status": t.status.as_str(), "updated_at": t.updated_at })))
}

fn settings_json(v: &super::super::application::configuration::TenantSettingsView) -> Value {
    json!({ "config": v.config, "feature_flags": v.feature_flags })
}

async fn get_config(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:read").await?;
    let v = state.m01.configuration.get(&a, parse_tenant_id(&id)?).await?;
    Ok(ok(StatusCode::OK, settings_json(&v)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigRequest {
    config: Option<BTreeMap<String, Value>>,
    feature_flags: Option<BTreeMap<String, bool>>,
}

async fn patch_config(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>, body: Bytes) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:write").await?;
    let r: ConfigRequest = parse_body(&body)?;
    let v = state
        .m01
        .configuration
        .update(&a, parse_tenant_id(&id)?, r.config.unwrap_or_default(), r.feature_flags.unwrap_or_default())
        .await?;
    Ok(ok(StatusCode::OK, settings_json(&v)))
}

async fn get_quota(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:read").await?;
    let v = state.m01.quotas.view(&a, parse_tenant_id(&id)?).await?;
    let mut limits = serde_json::Map::new();
    let mut usage = serde_json::Map::new();
    let mut thresholds = serde_json::Map::new();
    let mut levels = serde_json::Map::new();
    for l in &v.lines {
        limits.insert(l.metric.as_str().into(), json!(l.limit));
        usage.insert(l.metric.as_str().into(), json!(l.usage));
        thresholds.insert(l.metric.as_str().into(), json!(l.soft_threshold));
        levels.insert(l.metric.as_str().into(), json!(l.level));
    }
    Ok(ok(StatusCode::OK, json!({ "limits": limits, "usage": usage, "thresholds": thresholds, "levels": levels })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrandingRequest {
    logo_url: Option<String>,
    primary_color: Option<String>,
    secondary_color: Option<String>,
    custom_domain: Option<String>,
    email_from: Option<String>,
    /// EXT: activate (serve) a verified custom domain (BR-M01-005 → 409 when unverified).
    custom_domain_active: Option<bool>,
    /// EXT (R019): email footer, login page message, PDF letterhead.
    email_footer: Option<String>,
    login_message: Option<String>,
    pdf_letterhead: Option<String>,
}

async fn patch_branding(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>, body: Bytes) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:write").await?;
    let r: BrandingRequest = parse_body(&body)?;
    let v = state
        .m01
        .branding
        .update(
            &a,
            parse_tenant_id(&id)?,
            BrandingPatch {
                logo_url: r.logo_url,
                primary_color: r.primary_color,
                secondary_color: r.secondary_color,
                custom_domain: r.custom_domain,
                custom_domain_active: r.custom_domain_active,
                email_from: r.email_from,
                email_footer: r.email_footer,
                login_message: r.login_message,
                pdf_letterhead: r.pdf_letterhead,
            },
        )
        .await?;
    let b = &v.branding;
    Ok(ok(
        StatusCode::OK,
        json!({
            "branding": {
                "logo_url": v.logo_url, "primary_color": b.primary_color, "secondary_color": b.secondary_color,
                "custom_domain": b.custom_domain, "email_from": b.email_from, "email_footer": b.email_footer,
                "login_message": b.login_message, "pdf_letterhead": b.pdf_letterhead
            },
            "verification_status": v.verification_status,
            "dns_records": v.dns_records,
        }),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumeRequest {
    metric: String,
    amount: Option<i64>,
}

/// EXT: quota-bound action check (BR-M01-004: warn at 80%, 429 above 100%).
async fn consume_quota(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>, body: Bytes) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:write").await?;
    let r: ConsumeRequest = parse_body(&body)?;
    let metric = QuotaMetric::parse(&r.metric)?;
    let out = state.m01.quotas.check_and_consume(&a, parse_tenant_id(&id)?, metric, r.amount.unwrap_or(1)).await?;
    Ok(ok(StatusCode::OK, json!(out)))
}

#[derive(Deserialize, Default)]
struct StatementQuery {
    month: Option<String>,
}

/// EXT: monthly usage statement (R025).
async fn usage_statement(
    State(state): State<AppState>,
    u: ApiUser,
    Path(id): Path<String>,
    Query(q): Query<StatementQuery>,
) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:read").await?;
    let month = match q.month.as_deref() {
        Some(m) => {
            NaiveDate::parse_from_str(&format!("{m}-01"), "%Y-%m-%d").map_err(|_| AppError::validation("month", "Month must be YYYY-MM"))?
        }
        None => month_of(state.clock.now()),
    };
    let rows = state.m01.quotas.statement(&a, parse_tenant_id(&id)?, month).await?;
    let meters: BTreeMap<String, i64> = rows.into_iter().collect();
    Ok(ok(StatusCode::OK, json!({ "month": month.format("%Y-%m").to_string(), "meters": meters })))
}

/// EXT: run the isolation smoke test (Super Admin).
async fn isolation_check(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>) -> AppResult<Response> {
    let a = guard(&state, &u, "tenants:write").await?;
    let r = state.m01.isolation.run(&a, parse_tenant_id(&id)?).await?;
    Ok(ok(StatusCode::OK, json!(r)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MeteringRequest {
    meter: String,
    amount: i64,
}

/// EXT: REFERENCE metering ingestion (stand-in for M21 feeds). Super Admin only.
async fn reference_metering(State(state): State<AppState>, u: ApiUser, Path(id): Path<String>, body: Bytes) -> AppResult<Response> {
    let a = guard(&state, &u, "platform:elevated").await?;
    let r: MeteringRequest = parse_body(&body)?;
    let out = state.m01.quotas.record_metering(&a, parse_tenant_id(&id)?, &r.meter, r.amount).await?;
    Ok(ok(StatusCode::OK, json!({ "recorded": true, "quota": out })))
}
