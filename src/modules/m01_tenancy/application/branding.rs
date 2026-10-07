//! White-label branding (M01-F06; OCC-M01-R006, R019; BR-M01-005; FD-010/011; DOMAIN_NOT_VERIFIED).

use std::sync::Arc;

use serde::Serialize;
use serde_json::json;

use crate::platform::errors::{AppError, AppResult};
use crate::platform::security::random_token;

use super::super::domain::branding::{
    custom_domain_records, email_domain, ensure_domain_can_activate, parse_email, parse_fqdn, sender_domain_records, validate_logo,
    validate_text, DnsRecord, DomainStatus, HexColor,
};
use super::super::domain::errors::Violations;
use super::super::domain::events::TenantEvent;
use super::super::domain::TenantId;
use super::context::{Access, Actor, M01Deps};
use super::ports::{Branding, BrandingWrite, ChangeSet, SenderDomain};

/// Partial update (API `PATCH /v1/tenants/{id}/branding`; empty strings clear optional fields).
#[derive(Debug, Clone, Default)]
pub struct BrandingPatch {
    pub logo_url: Option<String>,
    pub primary_color: Option<String>,
    pub secondary_color: Option<String>,
    pub custom_domain: Option<String>,
    pub custom_domain_active: Option<bool>,
    pub email_from: Option<String>,
    pub email_footer: Option<String>,
    pub login_message: Option<String>,
    pub pdf_letterhead: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationStatus {
    pub custom_domain: Option<String>,
    pub custom_domain_status: &'static str,
    pub custom_domain_active: bool,
    pub email_from: Option<String>,
    pub email_from_status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct BrandingView {
    pub branding: Branding,
    pub logo_url: Option<String>,
    pub dns_records: Vec<DnsRecord>,
    pub sender_domains: Vec<SenderDomain>,
    pub verification_status: VerificationStatus,
}

pub struct BrandingService {
    deps: Arc<M01Deps>,
}

pub fn logo_path(tenant: TenantId) -> String {
    format!("/assets/tenants/{tenant}/logo")
}

impl BrandingService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps }
    }

    pub async fn get(&self, actor: &Actor, id: TenantId) -> AppResult<BrandingView> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        let b = self.deps.branding.get(&scope, id).await?;
        let senders = self.deps.branding.sender_domains(&scope, id).await?;
        let dns = match (&b.custom_domain, &b.custom_domain_token) {
            (Some(d), Some(t)) => custom_domain_records(d, tenant.code.as_str(), &self.deps.settings.platform_domain, t),
            _ => Vec::new(),
        };
        let email_status = match b.email_from.as_deref().and_then(email_domain) {
            Some(d) if senders.iter().any(|s| s.domain == d && s.status == "verified") => "verified",
            Some(_) => "unverified",
            None => "none",
        };
        Ok(BrandingView {
            logo_url: b.logo_object_key.as_ref().map(|_| logo_path(id)),
            verification_status: VerificationStatus {
                custom_domain: b.custom_domain.clone(),
                custom_domain_status: b.custom_domain_status.as_str(),
                custom_domain_active: b.custom_domain_active,
                email_from: b.email_from.clone(),
                email_from_status: email_status,
            },
            dns_records: dns,
            sender_domains: senders,
            branding: b,
        })
    }

    pub async fn update(&self, actor: &Actor, id: TenantId, patch: BrandingPatch) -> AppResult<BrandingView> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        tenant.ensure_config_writable()?;
        let current = self.deps.branding.get(&scope, id).await?;
        let mut v = Violations::default();
        let mut w = BrandingWrite::default();
        let mut changed = Vec::new();

        if let Some(url) = patch.logo_url.as_deref().map(str::trim) {
            if url.is_empty() {
                w.logo = Some(None);
                changed.push("logo_url");
            } else if url != logo_path(id) || current.logo_object_key.is_none() {
                // Logos must be uploaded to object storage (validated PNG/SVG ≤ 2MB).
                v.push("logo_url", "Logo must be PNG/SVG under 2MB (upload it first)");
            }
        }
        if let Some(c) = &patch.primary_color {
            if let Some(c) = v.capture(HexColor::parse("primary_color", c)) {
                w.primary_color = Some(c.as_str().to_string());
                changed.push("primary_color");
            }
        }
        if let Some(c) = &patch.secondary_color {
            if let Some(c) = v.capture(HexColor::parse("secondary_color", c)) {
                w.secondary_color = Some(c.as_str().to_string());
                changed.push("secondary_color");
            }
        }
        let mut new_domain_pending = false;
        if let Some(d) = patch.custom_domain.as_deref().map(str::trim) {
            if d.is_empty() {
                if current.custom_domain.is_some() {
                    w.custom_domain = Some(None);
                    changed.push("custom_domain");
                }
            } else if let Some(fqdn) = v.capture(parse_fqdn("custom_domain", d)) {
                if current.custom_domain.as_deref() != Some(fqdn.as_str()) {
                    w.custom_domain = Some(Some((fqdn, random_token(18))));
                    new_domain_pending = true;
                    changed.push("custom_domain");
                }
            }
        }
        for (field, val, max, slot) in [
            ("email_footer", &patch.email_footer, 1000usize, 0u8),
            ("login_message", &patch.login_message, 500, 1),
            ("pdf_letterhead", &patch.pdf_letterhead, 500, 2),
        ] {
            if let Some(raw) = val {
                if let Some(t) = v.capture(validate_text(field, Some(raw), max)) {
                    match slot {
                        0 => w.email_footer = Some(t),
                        1 => w.login_message = Some(t),
                        _ => w.pdf_letterhead = Some(t),
                    }
                    changed.push(field);
                }
            }
        }
        let mut email_from = None;
        if let Some(e) = patch.email_from.as_deref().map(str::trim) {
            if e.is_empty() {
                w.email_from = Some(None);
                changed.push("email_from");
            } else {
                email_from = v.capture(parse_email("email_from", e, "Valid sender email required"));
            }
        }
        v.into_result()?;

        // FD-011 / DOMAIN_NOT_VERIFIED: sender domain must be SPF/DKIM verified.
        if let Some(e) = email_from {
            let domain = email_domain(&e).unwrap_or_default().to_string();
            let verified =
                self.deps.branding.sender_domains(&scope, id).await?.iter().any(|s| s.domain == domain && s.status == "verified");
            if !verified {
                return Err(AppError::domain_not_verified("Sender domain not verified"));
            }
            w.email_from = Some(Some(e));
            changed.push("email_from");
        }

        // BR-M01-005: activation requires a verified domain (a newly entered domain is pending).
        if let Some(active) = patch.custom_domain_active {
            if active {
                let status = if new_domain_pending || (w.custom_domain == Some(None)) {
                    DomainStatus::Pending
                } else {
                    current.custom_domain_status
                };
                ensure_domain_can_activate(status)?;
            }
            if active != current.custom_domain_active || new_domain_pending {
                w.custom_domain_active = Some(active && !new_domain_pending);
                changed.push("custom_domain_active");
            }
        }

        if changed.is_empty() {
            return self.get(actor, id).await;
        }
        let mut a = actor.audit(Some(id), "tenant_branding", Some(id.to_string()), "tenant.branding_changed");
        a.before = Some(json!({
            "primary_color": current.primary_color, "secondary_color": current.secondary_color,
            "custom_domain": current.custom_domain, "custom_domain_active": current.custom_domain_active,
            "email_from": current.email_from
        }));
        a.after = Some(json!({ "changed": changed }));
        let changes = ChangeSet::new()
            .with_audit(a)
            .with_event(actor.event(id, &TenantEvent::BrandingChanged { fields: changed.iter().map(|s| s.to_string()).collect() }));
        self.deps.branding.update(&scope, id, &w, actor.user_id, changes).await?;
        self.get(actor, id).await
    }

    /// Validated logo upload into object storage (PNG/SVG ≤ 2MB).
    pub async fn upload_logo(&self, actor: &Actor, id: TenantId, bytes: &[u8]) -> AppResult<BrandingView> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        self.deps.load_tenant(&scope, id).await?.ensure_config_writable()?;
        let kind = validate_logo(bytes)?;
        let key = format!("tenants/{id}/branding/logo.{}", kind.extension());
        self.deps.objects.put(&key, bytes).await?;
        let w = BrandingWrite { logo: Some(Some((key, kind.content_type().to_string(), bytes.len() as i64))), ..Default::default() };
        let mut a = actor.audit(Some(id), "tenant_branding", Some(id.to_string()), "tenant.branding_changed");
        a.after = Some(json!({ "changed": ["logo"], "content_type": kind.content_type(), "size": bytes.len() }));
        let changes =
            ChangeSet::new().with_audit(a).with_event(actor.event(id, &TenantEvent::BrandingChanged { fields: vec!["logo".into()] }));
        self.deps.branding.update(&scope, id, &w, actor.user_id, changes).await?;
        self.get(actor, id).await
    }

    /// Logo bytes for serving (public asset: logos are shown on branded login pages).
    pub async fn logo(&self, id: TenantId) -> AppResult<Option<(String, Vec<u8>)>> {
        let b = self.deps.branding.get(&crate::platform::db::AccessScope::System, id).await?;
        match (b.logo_object_key, b.logo_content_type) {
            (Some(k), Some(ct)) => Ok(self.deps.objects.get(&k).await?.map(|bytes| (ct, bytes))),
            _ => Ok(None),
        }
    }

    /// FD-010 Verify button: checks CNAME/TXT through the DomainVerificationPort.
    pub async fn verify_domain(&self, actor: &Actor, id: TenantId) -> AppResult<BrandingView> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        let b = self.deps.branding.get(&scope, id).await?;
        let (Some(domain), Some(token)) = (b.custom_domain.clone(), b.custom_domain_token.clone()) else {
            return Err(AppError::conflict("No custom domain to verify"));
        };
        let expected = format!("{}.{}", tenant.code, self.deps.settings.platform_domain);
        let r = self.deps.domain_verifier.verify(&domain, &expected, &token).await?;
        let status = if r.verified { DomainStatus::Verified } else { DomainStatus::Failed };
        let mut a = actor.audit(Some(id), "tenant_branding", Some(id.to_string()), "tenant.domain_verification");
        a.after = Some(json!({ "domain": domain, "verified": r.verified, "simulated": r.simulated, "message": r.message }));
        let mut changes = ChangeSet::new().with_audit(a);
        if r.verified {
            changes.push_event(actor.event(id, &TenantEvent::CustomDomainVerified { domain: domain.clone() }));
        }
        self.deps.branding.set_domain_status(&scope, id, status, &r.message, self.deps.clock.now(), changes).await?;
        self.get(actor, id).await
    }

    /// FD-011: register a sender domain and show the SPF/DKIM records to publish.
    pub async fn add_sender_domain(&self, actor: &Actor, id: TenantId, domain: &str) -> AppResult<BrandingView> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        self.deps.load_tenant(&scope, id).await?.ensure_config_writable()?;
        let d = parse_fqdn("sender_domain", domain)?;
        let selector = "omni1";
        let (spf, dkim) = sender_domain_records(&d, &self.deps.settings.platform_domain, selector);
        let mut a = actor.audit(Some(id), "sender_domain", Some(d.clone()), "tenant.sender_domain_added");
        a.after = Some(json!({ "domain": d }));
        self.deps.branding.upsert_sender_domain(&scope, id, &d, &spf, selector, &dkim, ChangeSet::new().with_audit(a)).await?;
        self.get(actor, id).await
    }

    pub async fn verify_sender_domain(&self, actor: &Actor, id: TenantId, domain: &str) -> AppResult<BrandingView> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let d = parse_fqdn("sender_domain", domain)?;
        let known = self.deps.branding.sender_domains(&scope, id).await?;
        let rec = known.iter().find(|s| s.domain == d).ok_or_else(|| AppError::not_found("Sender domain not registered"))?;
        let r = self.deps.sender_verifier.verify(&d, &rec.dkim_selector).await?;
        let mut a = actor.audit(Some(id), "sender_domain", Some(d.clone()), "tenant.sender_domain_verification");
        a.after = Some(json!({ "verified": r.verified, "simulated": r.simulated, "message": r.message }));
        self.deps
            .branding
            .set_sender_status(&scope, id, &d, r.verified, &r.message, self.deps.clock.now(), ChangeSet::new().with_audit(a))
            .await?;
        self.get(actor, id).await
    }

    /// Host-based serving decision (BR-M01-005: unverified/inactive domains are not served).
    pub async fn resolve_host(&self, host: &str) -> AppResult<Option<(TenantId, bool)>> {
        let h = host.split(':').next().unwrap_or(host).trim().to_ascii_lowercase();
        if h.is_empty() || !h.contains('.') {
            return Ok(None);
        }
        self.deps.branding.find_by_custom_domain(&h).await
    }

    /// Live preview validation (no persistence).
    pub fn preview(primary: &str, secondary: &str) -> Result<(String, String), AppError> {
        let mut v = Violations::default();
        let p = v.capture(HexColor::parse("primary_color", primary));
        let s = v.capture(HexColor::parse("secondary_color", secondary));
        v.into_result()?;
        Ok((p.map(|c| c.as_str().to_string()).unwrap_or_default(), s.map(|c| c.as_str().to_string()).unwrap_or_default()))
    }
}
