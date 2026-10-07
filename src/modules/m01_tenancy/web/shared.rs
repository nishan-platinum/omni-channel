//! Page builders and actions shared by the Super Admin (`/admin/tenants/{id}/…`) and Tenant Admin
//! (`/tenant/…`) UIs. The caller resolves the tenant: from the path for the Super Admin (role-checked)
//! or from the session for the Tenant Admin. Authorization is re-checked in every service call.

use std::collections::BTreeMap;

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::NaiveDate;
use serde_json::Value;
use uuid::Uuid;

use crate::app::AppState;
use crate::bootstrap_auth::service::{Principal, Role};
use crate::platform::audit;
use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, ErrorCode};
use crate::platform::observability::RequestContext;
use crate::web_support::{redirect_with, render, Flash, PageCtx, PageError, PageResult};

use super::super::application::branding::BrandingPatch;
use super::super::application::quotas::{month_of, METERS};
use super::super::application::Actor;
use super::super::domain::quota::QuotaMetric;
use super::super::domain::storage::Tier;
use super::super::domain::support::GrantRequest;
use super::super::domain::TenantId;
use super::actor;
use super::forms::*;
use super::views::*;

/// Resolved request context for one tenant page.
pub struct Ctx<'a> {
    pub state: &'a AppState,
    pub user: &'a Principal,
    pub rctx: &'a RequestContext,
    pub tenant: TenantId,
    pub base: String,
    pub flash: Option<Flash>,
}

impl<'a> Ctx<'a> {
    /// Super Admin: tenant from the path, role checked here.
    pub fn sa(
        state: &'a AppState,
        user: &'a Principal,
        rctx: &'a RequestContext,
        id: &str,
        flash: Option<Flash>,
    ) -> Result<Self, PageError> {
        if user.role != Role::SuperAdmin {
            return Err(AppError::forbidden("Platform Super Admin role required").into());
        }
        let tenant = super::parse_tenant_id(id)?;
        Ok(Self { state, user, rctx, tenant, base: format!("/admin/tenants/{tenant}"), flash })
    }

    /// Tenant Admin: tenant from the server-side session only.
    pub fn ta(state: &'a AppState, user: &'a Principal, rctx: &'a RequestContext, flash: Option<Flash>) -> Result<Self, PageError> {
        match (user.role, user.tenant_id) {
            (Role::TenantAdmin, Some(t)) => Ok(Self { state, user, rctx, tenant: TenantId(t), base: "/tenant".into(), flash }),
            _ => Err(AppError::forbidden("Tenant Admin role required").into()),
        }
    }

    pub fn actor(&self) -> Actor {
        actor(self.user, self.rctx)
    }

    pub fn is_sa(&self) -> bool {
        self.user.role == Role::SuperAdmin
    }

    fn page(&self, title: &str, nav: &'static str) -> PageCtx {
        PageCtx::for_user(title, self.user, nav, self.flash.clone(), self.state.config.app_env.is_development())
    }

    fn had_flash(&self) -> bool {
        self.flash.is_some()
    }

    /// Loads the tab header. Super Admin page views are audited as elevated access (M01-F02).
    pub async fn nav(&self, active: &'static str) -> Result<TenantNav, PageError> {
        let a = self.actor();
        let scope = self.state.m01.deps.authorize(&a, self.tenant, super::super::application::Access::Read, false).await?;
        if self.is_sa() {
            let mut e = a.tenant_audit(self.tenant, "platform.elevated_access", None, Some(serde_json::json!({ "page": active })));
            e.entity_type = "tenant".into();
            self.state.m01.deps.audit.record(&AccessScope::Platform, vec![e]).await?;
        }
        let t = self.state.m01.deps.load_tenant(&scope, self.tenant).await?;
        Ok(TenantNav {
            base: self.base.clone(),
            id: t.id.to_string(),
            code: t.code.to_string(),
            name: t.name.clone(),
            status: t.status.as_str().into(),
            status_term: t.display_status().into(),
            active,
            is_sa: self.is_sa(),
            read_only: !t.status.allows_config_writes(),
            writable: t.status.allows_config_writes(),
            is_sandbox: t.is_sandbox,
        })
    }

    fn back(&self, suffix: &str) -> String {
        format!("{}{}", self.base, suffix)
    }
}

fn errors_map(e: &AppError) -> BTreeMap<String, String> {
    e.details.iter().map(|d| (d.field.clone(), d.message.clone())).collect()
}

fn flash_err(e: &AppError) -> String {
    format!("{} ({})", e.message, e.code.as_str())
}

fn is_htmx(h: &HeaderMap) -> bool {
    h.get("hx-request").is_some()
}

// ------------------------------------------------------------------------------------------------
// Overview + lifecycle
// ------------------------------------------------------------------------------------------------

pub async fn overview_page(c: &Ctx<'_>) -> PageResult {
    let a = c.actor();
    let nav = c.nav("overview").await?;
    let d = c.state.m01.directory.detail(&a, c.tenant).await?;
    let admins = c.state.m01.support.approvers(&a, c.tenant).await.unwrap_or_default();
    let notes = c.state.m01.releases.notes_for(&a, c.tenant).await.unwrap_or_default();
    let quota_lines = match c.state.m01.quotas.view(&a, c.tenant).await {
        Ok(q) => {
            q.lines.iter().filter(|l| l.limit > 0).map(|l| (l.label.to_string(), l.utilisation_pct, l.level.as_str().to_string())).collect()
        }
        Err(_) => Vec::new(),
    };
    let actions = if c.is_sa() { transition_actions(&d.next_statuses) } else { Vec::new() };
    let v = OverviewView {
        page: c.page(&format!("{} — overview", nav.name), "tenants"),
        nav,
        d,
        actions,
        admins,
        notes,
        quota_lines,
        retention_hours: c.state.config.retention_hours,
    };
    Ok(render(&v, c.had_flash()))
}

pub async fn status_change(c: &Ctx<'_>, f: StatusForm) -> PageResult {
    let reason = Some(f.reason.as_str()).filter(|r| !r.trim().is_empty());
    match c.state.m01.lifecycle.change_status(&c.actor(), c.tenant, &f.status, reason).await {
        Ok(t) => Ok(redirect_with(&c.back(""), "success", &format!("Status updated: {}", t.status.label()))),
        Err(e) => Ok(redirect_with(&c.back(""), "error", &flash_err(&e))),
    }
}

pub async fn details_save(c: &Ctx<'_>, f: DetailsForm) -> PageResult {
    let parent = match (&f.parent_tenant_id, c.is_sa()) {
        (Some(p), true) if p.trim().is_empty() => Some(None),
        (Some(p), true) => Some(Some(super::parse_tenant_id(p.trim())?)),
        _ => None,
    };
    let legal = Some(f.legal_name.as_str());
    match c.state.m01.lifecycle.update_details(&c.actor(), c.tenant, &f.name, legal, parent).await {
        Ok(_) => Ok(redirect_with(&c.back(""), "success", "Changes saved.")),
        Err(e) => Ok(redirect_with(&c.back(""), "error", &flash_err(&e))),
    }
}

// ------------------------------------------------------------------------------------------------
// Configuration & feature flags
// ------------------------------------------------------------------------------------------------

pub async fn config_page(c: &Ctx<'_>, errors: BTreeMap<String, String>, general_error: Option<String>, status: StatusCode) -> PageResult {
    let nav = c.nav("config").await?;
    let v = c.state.m01.configuration.get(&c.actor(), c.tenant).await?;
    let view = ConfigView { page: c.page(&format!("{} — configuration", nav.name), "tenants"), nav, v, errors, general_error };
    let mut r = render(&view, c.had_flash());
    *r.status_mut() = status;
    Ok(r)
}

pub async fn config_save(c: &Ctx<'_>, pairs: Vec<(String, String)>) -> PageResult {
    let mut changes: BTreeMap<String, Value> = BTreeMap::new();
    for (k, v) in pairs {
        if let Some(key) = k.strip_prefix("cfg:") {
            let val = match v.as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => Value::String(v),
            };
            changes.insert(key.to_string(), val);
        }
    }
    match c.state.m01.configuration.update(&c.actor(), c.tenant, changes, BTreeMap::new()).await {
        Ok(_) => Ok(redirect_with(&c.back("/config"), "success", "Changes saved.")),
        Err(e) if e.code == ErrorCode::ValidationFailed => {
            config_page(c, errors_map(&e), Some(flash_err(&e)), StatusCode::BAD_REQUEST).await
        }
        Err(e) => config_page(c, BTreeMap::new(), Some(flash_err(&e)), e.status()).await,
    }
}

pub async fn feature_toggle(c: &Ctx<'_>, headers: &HeaderMap, key: &str, f: FeatureToggleForm) -> PageResult {
    let enable = f.enabled == "true";
    let flags: BTreeMap<String, bool> = [(key.to_string(), enable)].into_iter().collect();
    let result = c.state.m01.configuration.update(&c.actor(), c.tenant, BTreeMap::new(), flags).await;
    if is_htmx(headers) {
        let a = c.actor();
        let v = c.state.m01.configuration.get(&a, c.tenant).await?;
        let f = v.features.iter().find(|x| x.key == key).cloned().ok_or_else(|| AppError::not_found("Unknown feature"))?;
        let partial = FeatureRowPartial {
            page: c.page("", "tenants"),
            base: c.base.clone(),
            f,
            writable: v.tenant.status.allows_config_writes(),
            error: result.err().map(|e| flash_err(&e)),
        };
        return Ok(render(&partial, false));
    }
    match result {
        Ok(_) => {
            Ok(redirect_with(&c.back("/config"), "success", &format!("Feature {key} {}.", if enable { "enabled" } else { "disabled" })))
        }
        Err(e) => Ok(redirect_with(&c.back("/config"), "error", &flash_err(&e))),
    }
}

// ------------------------------------------------------------------------------------------------
// Quotas & usage
// ------------------------------------------------------------------------------------------------

pub async fn quotas_page(c: &Ctx<'_>, month: Option<String>) -> PageResult {
    let nav = c.nav("quotas").await?;
    let a = c.actor();
    let q = c.state.m01.quotas.view(&a, c.tenant).await?;
    let m = month
        .as_deref()
        .and_then(|m| NaiveDate::parse_from_str(&format!("{m}-01"), "%Y-%m-%d").ok())
        .unwrap_or_else(|| month_of(c.state.clock.now()));
    let statement = c.state.m01.quotas.statement(&a, c.tenant, m).await?;
    let v = QuotasView {
        page: c.page(&format!("{} — quotas", nav.name), "tenants"),
        nav,
        q,
        statement,
        month: m.format("%Y-%m").to_string(),
        meters: METERS.iter().map(|(m, _)| *m).collect(),
    };
    Ok(render(&v, c.had_flash()))
}

pub async fn quota_limits_save(c: &Ctx<'_>, pairs: Vec<(String, String)>) -> PageResult {
    let mut limits: BTreeMap<String, (Option<i64>, Option<f64>)> = BTreeMap::new();
    for (k, v) in pairs {
        if let Some(m) = k.strip_prefix("limit:") {
            limits.entry(m.to_string()).or_default().0 = v.trim().parse().ok();
        } else if let Some(m) = k.strip_prefix("threshold:") {
            limits.entry(m.to_string()).or_default().1 = v.trim().parse().ok();
        }
    }
    let mut changes = Vec::new();
    for (m, (l, t)) in limits {
        let metric = QuotaMetric::parse(&m).map_err(AppError::from)?;
        match (l, t) {
            (Some(l), Some(t)) => changes.push((metric, l, t)),
            _ => return Ok(redirect_with(&c.back("/quotas"), "error", &format!("Invalid limit or threshold for {m}"))),
        }
    }
    match c.state.m01.quotas.set_limits(&c.actor(), c.tenant, changes).await {
        Ok(_) => Ok(redirect_with(&c.back("/quotas"), "success", "Quota limits saved.")),
        Err(e) => Ok(redirect_with(&c.back("/quotas"), "error", &flash_err(&e))),
    }
}

pub async fn metering_simulate(c: &Ctx<'_>, f: MeteringForm) -> PageResult {
    match c.state.m01.quotas.record_metering(&c.actor(), c.tenant, &f.meter, f.amount).await {
        Ok(Some(o)) => Ok(redirect_with(
            &c.back("/quotas"),
            if o.warning { "error" } else { "success" },
            &format!(
                "Recorded {} {}. Usage {}/{}{}",
                f.amount,
                f.meter,
                o.usage,
                o.limit,
                if o.warning { " — soft threshold reached (warning raised)" } else { "" }
            ),
        )),
        Ok(None) => Ok(redirect_with(&c.back("/quotas"), "success", &format!("Recorded {} {} (meter only).", f.amount, f.meter))),
        Err(e) => Ok(redirect_with(&c.back("/quotas"), "error", &flash_err(&e))),
    }
}

pub async fn quota_consume(c: &Ctx<'_>, f: ConsumeForm) -> PageResult {
    let metric = QuotaMetric::parse(&f.metric).map_err(AppError::from)?;
    match c.state.m01.quotas.check_and_consume(&c.actor(), c.tenant, metric, f.amount).await {
        Ok(o) => Ok(redirect_with(
            &c.back("/quotas"),
            if o.warning { "error" } else { "success" },
            &format!("Allowed. Usage {}/{}{}", o.usage, o.limit, if o.warning { " — warning threshold reached" } else { "" }),
        )),
        Err(e) => Ok(redirect_with(&c.back("/quotas"), "error", &flash_err(&e))),
    }
}

pub async fn statement_csv(c: &Ctx<'_>, month: Option<String>) -> PageResult {
    let m = month
        .as_deref()
        .and_then(|m| NaiveDate::parse_from_str(&format!("{m}-01"), "%Y-%m-%d").ok())
        .unwrap_or_else(|| month_of(c.state.clock.now()));
    let rows = c.state.m01.quotas.statement(&c.actor(), c.tenant, m).await?;
    let mut csv = String::from("month,meter,value\n");
    for (meter, v) in rows {
        csv.push_str(&format!("{},{},{}\n", m.format("%Y-%m"), meter, v));
    }
    let mut r = csv.into_response();
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/csv; charset=utf-8"));
    r.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"usage-{}-{}.csv\"", c.tenant, m.format("%Y-%m")))
            .unwrap_or(HeaderValue::from_static("attachment")),
    );
    Ok(r)
}

// ------------------------------------------------------------------------------------------------
// Branding
// ------------------------------------------------------------------------------------------------

pub async fn branding_page(
    c: &Ctx<'_>,
    form: Option<BrandingForm>,
    errors: BTreeMap<String, String>,
    general_error: Option<String>,
    status: StatusCode,
) -> PageResult {
    let nav = c.nav("branding").await?;
    let v = c.state.m01.branding.get(&c.actor(), c.tenant).await?;
    let form = form.unwrap_or_else(|| BrandingForm {
        primary_color: v.branding.primary_color.clone(),
        secondary_color: v.branding.secondary_color.clone(),
        custom_domain: v.branding.custom_domain.clone().unwrap_or_default(),
        custom_domain_active: v.branding.custom_domain_active.then(|| "true".to_string()),
        email_from: v.branding.email_from.clone().unwrap_or_default(),
        email_footer: v.branding.email_footer.clone().unwrap_or_default(),
        login_message: v.branding.login_message.clone().unwrap_or_default(),
        pdf_letterhead: v.branding.pdf_letterhead.clone().unwrap_or_default(),
        remove_logo: None,
    });
    let p = BrandingPage { page: c.page(&format!("{} — branding", nav.name), "tenants"), nav, v, form, errors, general_error };
    let mut r = render(&p, c.had_flash());
    *r.status_mut() = status;
    Ok(r)
}

pub async fn branding_save(c: &Ctx<'_>, f: BrandingForm) -> PageResult {
    let a = c.actor();
    let current = c.state.m01.branding.get(&a, c.tenant).await?;
    let b = &current.branding;
    let differs = |new: &str, old: &Option<String>| new.trim() != old.as_deref().unwrap_or("");
    let patch = BrandingPatch {
        logo_url: f.remove_logo.as_ref().map(|_| String::new()),
        primary_color: (!f.primary_color.trim().eq_ignore_ascii_case(&b.primary_color)).then(|| f.primary_color.clone()),
        secondary_color: (!f.secondary_color.trim().eq_ignore_ascii_case(&b.secondary_color)).then(|| f.secondary_color.clone()),
        custom_domain: differs(&f.custom_domain, &b.custom_domain).then(|| f.custom_domain.clone()),
        custom_domain_active: Some(f.custom_domain_active.is_some()).filter(|v| *v != b.custom_domain_active),
        email_from: differs(&f.email_from, &b.email_from).then(|| f.email_from.clone()),
        email_footer: differs(&f.email_footer, &b.email_footer).then(|| f.email_footer.clone()),
        login_message: differs(&f.login_message, &b.login_message).then(|| f.login_message.clone()),
        pdf_letterhead: differs(&f.pdf_letterhead, &b.pdf_letterhead).then(|| f.pdf_letterhead.clone()),
    };
    match c.state.m01.branding.update(&a, c.tenant, patch).await {
        Ok(_) => Ok(redirect_with(&c.back("/branding"), "success", "Changes saved.")),
        Err(e) => {
            let st = e.status();
            branding_page(c, Some(f), errors_map(&e), Some(flash_err(&e)), st).await
        }
    }
}

pub async fn branding_logo(c: &Ctx<'_>, bytes: Vec<u8>) -> PageResult {
    match c.state.m01.branding.upload_logo(&c.actor(), c.tenant, &bytes).await {
        Ok(_) => Ok(redirect_with(&c.back("/branding"), "success", "Logo uploaded.")),
        Err(e) => Ok(redirect_with(&c.back("/branding"), "error", &flash_err(&e))),
    }
}

pub async fn branding_verify(c: &Ctx<'_>) -> PageResult {
    match c.state.m01.branding.verify_domain(&c.actor(), c.tenant).await {
        Ok(v) => {
            let ok = v.verification_status.custom_domain_status == "verified";
            Ok(redirect_with(
                &c.back("/branding"),
                if ok { "success" } else { "error" },
                v.branding.custom_domain_message.as_deref().unwrap_or("Verification finished."),
            ))
        }
        Err(e) => Ok(redirect_with(&c.back("/branding"), "error", &flash_err(&e))),
    }
}

pub async fn sender_add(c: &Ctx<'_>, f: DomainForm) -> PageResult {
    match c.state.m01.branding.add_sender_domain(&c.actor(), c.tenant, &f.domain).await {
        Ok(_) => {
            Ok(redirect_with(&c.back("/branding"), "success", "Sender domain registered — publish the SPF/DKIM records, then verify."))
        }
        Err(e) => Ok(redirect_with(&c.back("/branding"), "error", &flash_err(&e))),
    }
}

pub async fn sender_verify(c: &Ctx<'_>, f: DomainForm) -> PageResult {
    match c.state.m01.branding.verify_sender_domain(&c.actor(), c.tenant, &f.domain).await {
        Ok(v) => {
            let ok = v.sender_domains.iter().any(|s| s.domain == f.domain.trim().to_ascii_lowercase() && s.status == "verified");
            Ok(redirect_with(
                &c.back("/branding"),
                if ok { "success" } else { "error" },
                if ok { "Sender domain verified." } else { "Sender domain could not be verified (simulated SPF/DKIM)." },
            ))
        }
        Err(e) => Ok(redirect_with(&c.back("/branding"), "error", &flash_err(&e))),
    }
}

pub fn branding_preview(f: PreviewForm) -> Response {
    let p = match super::super::application::branding::BrandingService::preview(&f.primary_color, &f.secondary_color) {
        Ok((p, s)) => PreviewPartial { primary: p, secondary: s, error: None },
        Err(e) => PreviewPartial { primary: "#0B2130".into(), secondary: "#F26A21".into(), error: Some(e.message) },
    };
    render(&p, false)
}

// ------------------------------------------------------------------------------------------------
// Storage & isolation
// ------------------------------------------------------------------------------------------------

pub async fn storage_page(c: &Ctx<'_>) -> PageResult {
    let nav = c.nav("storage").await?;
    let a = c.actor();
    let conn = c.state.m01.directory.connection(&a, c.tenant).await?;
    let scope = c.state.m01.deps.authorize(&a, c.tenant, super::super::application::Access::Read, false).await?;
    let checks = c.state.m01.deps.tenants.list_isolation_checks(&scope, c.tenant, 10).await?;
    let run = c.state.m01.deps.provisioning.latest_run(&scope, c.tenant).await?;
    let v = StorageView { page: c.page(&format!("{} — storage", nav.name), "tenants"), nav, conn, checks, run };
    Ok(render(&v, c.had_flash()))
}

// ------------------------------------------------------------------------------------------------
// Support access
// ------------------------------------------------------------------------------------------------

pub async fn support_page(c: &Ctx<'_>, view: Option<super::super::application::support::SupportView>) -> PageResult {
    let nav = c.nav("support").await?;
    let a = c.actor();
    let now = c.state.clock.now();
    let grants = c
        .state
        .m01
        .support
        .list(&a, c.tenant)
        .await?
        .into_iter()
        .map(|g| {
            let s = g.state_at(now).as_str();
            (g, s)
        })
        .collect();
    let approvers = c.state.m01.support.approvers(&a, c.tenant).await?;
    let scope = c.state.m01.deps.authorize(&a, c.tenant, super::super::application::Access::Read, false).await?;
    let t = c.state.m01.deps.load_tenant(&scope, c.tenant).await?;
    let regulated = c.state.m01.deps.plans.get(t.plan_id).await?.is_some_and(|p| p.tier == Tier::Regulated);
    let v = SupportPage {
        page: c.page(&format!("{} — support access", nav.name), "tenants"),
        nav,
        grants,
        approvers,
        regulated,
        view,
        me: c.user.user_id.to_string(),
    };
    Ok(render(&v, c.had_flash()))
}

pub async fn support_request(c: &Ctx<'_>, f: GrantRequestForm) -> PageResult {
    let req = GrantRequest {
        reason: f.reason,
        incident_ref: Some(f.incident_ref).filter(|s| !s.trim().is_empty()),
        duration_minutes: f.duration_minutes.trim().parse().ok(),
        named_approver_id: Uuid::parse_str(f.named_approver_id.trim()).ok(),
    };
    match c.state.m01.support.request(&c.actor(), c.tenant, req).await {
        Ok(_) => Ok(redirect_with(&c.back("/support"), "success", "Support access requested; waiting for tenant approval.")),
        Err(e) => Ok(redirect_with(&c.back("/support"), "error", &flash_err(&e))),
    }
}

pub async fn support_decide(c: &Ctx<'_>, grant: &str, approve: bool, f: DecisionForm) -> PageResult {
    let gid = Uuid::parse_str(grant).map_err(|_| AppError::not_found("Grant does not exist"))?;
    let note = Some(f.note.as_str()).filter(|n| !n.trim().is_empty());
    let r = if approve {
        c.state.m01.support.approve(&c.actor(), gid, note).await
    } else {
        c.state.m01.support.reject(&c.actor(), gid, note).await
    };
    match r {
        Ok(g) => Ok(redirect_with(&c.back("/support"), "success", &format!("Grant {}.", g.status.as_str()))),
        Err(e) => Ok(redirect_with(&c.back("/support"), "error", &flash_err(&e))),
    }
}

pub async fn support_revoke(c: &Ctx<'_>, grant: &str) -> PageResult {
    let gid = Uuid::parse_str(grant).map_err(|_| AppError::not_found("Grant does not exist"))?;
    match c.state.m01.support.revoke(&c.actor(), c.tenant, gid).await {
        Ok(_) => Ok(redirect_with(&c.back("/support"), "success", "Grant revoked.")),
        Err(e) => Ok(redirect_with(&c.back("/support"), "error", &flash_err(&e))),
    }
}

pub async fn support_open(c: &Ctx<'_>) -> PageResult {
    match c.state.m01.support.open_support_view(&c.actor(), c.tenant).await {
        Ok(v) => support_page(c, Some(v)).await,
        Err(e) => Ok(redirect_with(&c.back("/support"), "error", &flash_err(&e))),
    }
}

// ------------------------------------------------------------------------------------------------
// Sandboxes & baselines
// ------------------------------------------------------------------------------------------------

pub async fn sandboxes_page(c: &Ctx<'_>, preview: Option<super::super::application::baselines::ImportPreview>) -> PageResult {
    let nav = c.nav("sandboxes").await?;
    let a = c.actor();
    let sandboxes = if nav.is_sandbox { Vec::new() } else { c.state.m01.sandboxes.list(&a, c.tenant).await? };
    let production = if nav.is_sandbox {
        c.state.m01.deps.tenants.get(&AccessScope::Platform, c.tenant).await?.and_then(|t| t.sandbox_of_tenant_id.map(|p| p.to_string()))
    } else {
        None
    };
    let v = SandboxesView { page: c.page(&format!("{} — sandboxes", nav.name), "tenants"), nav, sandboxes, production, preview };
    Ok(render(&v, c.had_flash()))
}

pub async fn sandbox_create(c: &Ctx<'_>) -> PageResult {
    match c.state.m01.sandboxes.create(&c.actor(), c.tenant).await {
        Ok(s) => Ok(redirect_with(
            &c.back("/sandboxes"),
            if s.activated { "success" } else { "error" },
            &format!(
                "Sandbox {} provisioned{}.",
                s.outcome.tenant.code,
                if s.activated { " and activated" } else { " but provisioning reported failures" }
            ),
        )),
        Err(e) => Ok(redirect_with(&c.back("/sandboxes"), "error", &flash_err(&e))),
    }
}

pub async fn sandbox_promote(c: &Ctx<'_>, sandbox: &str, f: ChangeRecordForm) -> PageResult {
    let sid = super::parse_tenant_id(sandbox)?;
    if f.action == "apply" {
        match c.state.m01.baselines.promote(&c.actor(), sid, &f.change_record).await {
            Ok(b) => Ok(redirect_with(
                &c.back("/sandboxes"),
                "success",
                &format!("Sandbox configuration promoted (baseline v{}).", b.version_no),
            )),
            Err(e) => Ok(redirect_with(&c.back("/sandboxes"), "error", &flash_err(&e))),
        }
    } else {
        match c.state.m01.baselines.promotion_preview(&c.actor(), sid).await {
            Ok((_, p)) => sandboxes_page(c, Some(p)).await,
            Err(e) => Ok(redirect_with(&c.back("/sandboxes"), "error", &flash_err(&e))),
        }
    }
}

pub async fn baselines_page(
    c: &Ctx<'_>,
    preview: Option<super::super::application::baselines::ImportPreview>,
    document: String,
    change_record: String,
    error: Option<String>,
) -> PageResult {
    let nav = c.nav("baselines").await?;
    let a = c.actor();
    let baselines = c.state.m01.baselines.list(&a, c.tenant).await?;
    let current_json = baselines.first().map(|b| serde_json::to_string_pretty(&b.content).unwrap_or_default()).unwrap_or_default();
    let v = BaselinesView {
        page: c.page(&format!("{} — baselines", nav.name), "tenants"),
        nav,
        baselines,
        preview,
        document,
        change_record,
        current_json,
        error,
    };
    Ok(render(&v, c.had_flash()))
}

pub async fn baseline_export(c: &Ctx<'_>, f: BaselineExportForm) -> PageResult {
    match c.state.m01.baselines.export(&c.actor(), c.tenant, &f.label).await {
        Ok(b) => Ok(redirect_with(
            &c.back("/baselines"),
            "success",
            &format!("Baseline v{} exported (sha256 {}…).", b.version_no, &b.sha256[..12]),
        )),
        Err(e) => Ok(redirect_with(&c.back("/baselines"), "error", &flash_err(&e))),
    }
}

pub async fn baseline_import(c: &Ctx<'_>, f: BaselineImportForm) -> PageResult {
    let a = c.actor();
    if f.action == "apply" {
        match c.state.m01.baselines.import(&a, c.tenant, &f.document, &f.change_record, "imported").await {
            Ok(b) => Ok(redirect_with(
                &c.back("/baselines"),
                "success",
                &format!("Baseline imported (v{}); previous state archived for rollback.", b.version_no),
            )),
            Err(e) => baselines_page(c, None, f.document, f.change_record, Some(flash_err(&e))).await,
        }
    } else {
        match c.state.m01.baselines.preview_import(&a, c.tenant, &f.document).await {
            Ok(p) => baselines_page(c, Some(p), f.document, f.change_record, None).await,
            Err(e) => baselines_page(c, None, f.document, f.change_record, Some(flash_err(&e))).await,
        }
    }
}

pub async fn baseline_rollback(c: &Ctx<'_>, id: &str, f: ChangeRecordForm) -> PageResult {
    let bid = Uuid::parse_str(id).map_err(|_| AppError::not_found("Baseline does not exist"))?;
    match c.state.m01.baselines.rollback(&c.actor(), c.tenant, bid, &f.change_record).await {
        Ok(_) => Ok(redirect_with(&c.back("/baselines"), "success", "Configuration rolled back to the selected baseline.")),
        Err(e) => Ok(redirect_with(&c.back("/baselines"), "error", &flash_err(&e))),
    }
}

pub async fn baseline_download(c: &Ctx<'_>, id: &str) -> PageResult {
    let bid = Uuid::parse_str(id.trim_end_matches(".json")).map_err(|_| AppError::not_found("Baseline does not exist"))?;
    let b = c.state.m01.baselines.get(&c.actor(), c.tenant, bid).await?;
    let body = serde_json::to_string_pretty(&b.content).map_err(AppError::internal)?;
    let mut r = body.into_response();
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    r.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"baseline-{}-v{}.json\"", b.content.source_tenant_code, b.version_no))
            .unwrap_or(HeaderValue::from_static("attachment")),
    );
    Ok(r)
}

// ------------------------------------------------------------------------------------------------
// Releases, keys, offboarding, audit, backups
// ------------------------------------------------------------------------------------------------

pub async fn release_page(c: &Ctx<'_>) -> PageResult {
    let nav = c.nav("release").await?;
    let a = c.actor();
    let pref = c.state.m01.releases.preference(&a, c.tenant).await?;
    let notes = c.state.m01.releases.notes_for(&a, c.tenant).await?;
    let v = ReleaseView { page: c.page(&format!("{} — releases", nav.name), "tenants"), nav, pref, notes, days: days() };
    Ok(render(&v, c.had_flash()))
}

pub async fn release_save(c: &Ctx<'_>, f: ReleasePrefForm) -> PageResult {
    let ring = if c.is_sa() { f.ring.as_deref() } else { None };
    match c
        .state
        .m01
        .releases
        .set_preference(&c.actor(), c.tenant, ring, f.maintenance_day, f.maintenance_start_hour_utc, f.maintenance_duration_min)
        .await
    {
        Ok(_) => Ok(redirect_with(&c.back("/release"), "success", "Changes saved.")),
        Err(e) => Ok(redirect_with(&c.back("/release"), "error", &flash_err(&e))),
    }
}

pub async fn keys_page(c: &Ctx<'_>) -> PageResult {
    let nav = c.nav("keys").await?;
    let a = c.actor();
    let keys = c.state.m01.keys.list(&a, c.tenant).await?;
    let scope = c.state.m01.deps.authorize(&a, c.tenant, super::super::application::Access::Read, false).await?;
    let t = c.state.m01.deps.load_tenant(&scope, c.tenant).await?;
    let regulated = c.state.m01.deps.plans.get(t.plan_id).await?.is_some_and(|p| p.tier == Tier::Regulated);
    let v = KeysView { page: c.page(&format!("{} — keys", nav.name), "tenants"), nav, keys, regulated, now: c.state.clock.now() };
    Ok(render(&v, c.had_flash()))
}

pub async fn key_rotate(c: &Ctx<'_>) -> PageResult {
    match c.state.m01.keys.rotate(&c.actor(), c.tenant).await {
        Ok(k) => Ok(redirect_with(&c.back("/keys"), "success", &format!("Data key rotated to version {}.", k.key_version))),
        Err(e) => Ok(redirect_with(&c.back("/keys"), "error", &flash_err(&e))),
    }
}

pub async fn key_byok(c: &Ctx<'_>, f: ByokForm) -> PageResult {
    match c.state.m01.keys.register_byok(&c.actor(), c.tenant, &f.key_ref).await {
        Ok(k) => Ok(redirect_with(&c.back("/keys"), "success", &format!("Customer-supplied key registered (version {}).", k.key_version))),
        Err(e) => Ok(redirect_with(&c.back("/keys"), "error", &flash_err(&e))),
    }
}

pub async fn offboarding_page(c: &Ctx<'_>) -> PageResult {
    let nav = c.nav("offboarding").await?;
    let a = c.actor();
    let exports = c.state.m01.offboarding.list_exports(&a, c.tenant).await?;
    let certificate = if c.is_sa() { c.state.m01.offboarding.certificate(&a, c.tenant).await? } else { None };
    let scope = c.state.m01.deps.authorize(&a, c.tenant, super::super::application::Access::Read, false).await?;
    let t = c.state.m01.deps.load_tenant(&scope, c.tenant).await?;
    let v = OffboardingView {
        page: c.page(&format!("{} — offboarding", nav.name), "tenants"),
        nav,
        exports,
        certificate,
        legal_hold: t.legal_hold,
        grace_until: t.grace_until,
        purge_after: t.purge_after,
    };
    Ok(render(&v, c.had_flash()))
}

pub async fn export_generate(c: &Ctx<'_>) -> PageResult {
    match c.state.m01.offboarding.generate_export(&c.actor(), c.tenant, "manual").await {
        Ok(e) => Ok(redirect_with(&c.back("/offboarding"), "success", &format!("Encrypted export generated ({} bytes).", e.size_bytes))),
        Err(e) => Ok(redirect_with(&c.back("/offboarding"), "error", &flash_err(&e))),
    }
}

pub async fn export_download(c: &Ctx<'_>, id: &str) -> PageResult {
    let eid = Uuid::parse_str(id).map_err(|_| AppError::not_found("Export does not exist"))?;
    match c.state.m01.offboarding.download_export(&c.actor(), c.tenant, eid).await {
        Ok((name, bytes)) => {
            let mut r = bytes.into_response();
            r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            r.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(&format!("attachment; filename=\"{name}\"")).unwrap_or(HeaderValue::from_static("attachment")),
            );
            Ok(r)
        }
        Err(e) => Ok(redirect_with(&c.back("/offboarding"), "error", &flash_err(&e))),
    }
}

pub async fn audit_page(c: &Ctx<'_>, before: Option<i64>) -> PageResult {
    let nav = c.nav("audit").await?;
    let a = c.actor();
    let scope = c.state.m01.deps.authorize(&a, c.tenant, super::super::application::Access::Read, false).await?;
    let rows = audit::list_for_tenant(&c.state.db.app, &scope, c.tenant.0, before, 50).await?;
    let next_before = (rows.len() == 50).then(|| rows.last().map(|r| r.id)).flatten();
    let v = AuditView { page: c.page(&format!("{} — audit", nav.name), "tenants"), nav, rows, next_before };
    Ok(render(&v, c.had_flash()))
}

pub async fn backups_page(c: &Ctx<'_>) -> PageResult {
    let nav = c.nav("backups").await?;
    let backups = c.state.m01.backups.list(&c.actor(), c.tenant).await?;
    let v = BackupsView { page: c.page(&format!("{} — backups", nav.name), "tenants"), nav, backups };
    Ok(render(&v, c.had_flash()))
}

pub async fn backup_create(c: &Ctx<'_>) -> PageResult {
    match c.state.m01.backups.backup(&c.actor(), c.tenant).await {
        Ok(b) => Ok(redirect_with(&c.back("/backups"), "success", &format!("Backup created ({} bytes, encrypted).", b.size_bytes))),
        Err(e) => Ok(redirect_with(&c.back("/backups"), "error", &flash_err(&e))),
    }
}

pub async fn backup_restore(c: &Ctx<'_>, id: &str) -> PageResult {
    let bid = Uuid::parse_str(id).map_err(|_| AppError::not_found("Backup does not exist"))?;
    match c.state.m01.backups.restore(&c.actor(), c.tenant, bid).await {
        Ok(ms) => Ok(redirect_with(
            &c.back("/backups"),
            "success",
            &format!("Tenant configuration restored in {ms} ms (other tenants unaffected)."),
        )),
        Err(e) => Ok(redirect_with(&c.back("/backups"), "error", &flash_err(&e))),
    }
}

pub fn not_permitted() -> PageError {
    AppError::forbidden("Not permitted").into()
}
