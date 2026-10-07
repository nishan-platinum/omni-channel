//! BrandingRepository.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::*;
use super::super::super::domain::branding::DomainStatus;
use super::super::super::domain::TenantId;
use super::{map_unique, write_changes, PgStore};

const BRANDING_COLS: &str = "tenant_id, logo_object_key, logo_content_type, primary_color::text AS primary_color, \
    secondary_color::text AS secondary_color, custom_domain::text AS custom_domain, custom_domain_status, custom_domain_active, \
    custom_domain_token, custom_domain_checked_at, custom_domain_message, email_from::text AS email_from, email_footer, \
    login_message, pdf_letterhead, updated_at, version";

fn branding_from_row(r: &sqlx::postgres::PgRow) -> Result<Branding, sqlx::Error> {
    Ok(Branding {
        tenant_id: TenantId(r.try_get("tenant_id")?),
        logo_object_key: r.try_get("logo_object_key")?,
        logo_content_type: r.try_get("logo_content_type")?,
        primary_color: r.try_get("primary_color")?,
        secondary_color: r.try_get("secondary_color")?,
        custom_domain: r.try_get("custom_domain")?,
        custom_domain_status: DomainStatus::parse(&r.try_get::<String, _>("custom_domain_status")?),
        custom_domain_active: r.try_get("custom_domain_active")?,
        custom_domain_token: r.try_get("custom_domain_token")?,
        custom_domain_checked_at: r.try_get("custom_domain_checked_at")?,
        custom_domain_message: r.try_get("custom_domain_message")?,
        email_from: r.try_get("email_from")?,
        email_footer: r.try_get("email_footer")?,
        login_message: r.try_get("login_message")?,
        pdf_letterhead: r.try_get("pdf_letterhead")?,
        updated_at: r.try_get("updated_at")?,
        version: r.try_get("version")?,
    })
}

#[async_trait]
impl BrandingRepository for PgStore {
    async fn get(&self, scope: &AccessScope, id: TenantId) -> AppResult<Branding> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query(&format!("SELECT {BRANDING_COLS} FROM tenantadm.tenant_branding WHERE tenant_id = $1"))
            .bind(id.0)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        match row {
            Some(r) => Ok(branding_from_row(&r)?),
            None => Err(AppError::not_found("Branding does not exist")),
        }
    }

    async fn update(
        &self,
        scope: &AccessScope,
        id: TenantId,
        w: &BrandingWrite,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<Branding> {
        let (logo_set, logo) = match &w.logo {
            Some(v) => (true, v.clone()),
            None => (false, None),
        };
        let (domain_set, domain) = match &w.custom_domain {
            Some(v) => (true, v.clone()),
            None => (false, None),
        };
        let opt = |o: &Option<Option<String>>| (o.is_some(), o.clone().flatten());
        let (email_set, email) = opt(&w.email_from);
        let (footer_set, footer) = opt(&w.email_footer);
        let (login_set, login) = opt(&w.login_message);
        let (pdf_set, pdf) = opt(&w.pdf_letterhead);
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query(&format!(
            "UPDATE tenantadm.tenant_branding b SET
                logo_object_key   = CASE WHEN $2 THEN $3 ELSE b.logo_object_key END,
                logo_content_type = CASE WHEN $2 THEN $4 ELSE b.logo_content_type END,
                logo_size_bytes   = CASE WHEN $2 THEN $5 ELSE b.logo_size_bytes END,
                primary_color     = coalesce($6, b.primary_color),
                secondary_color   = coalesce($7, b.secondary_color),
                custom_domain     = CASE WHEN $8 THEN $9::citext ELSE b.custom_domain END,
                custom_domain_token = CASE WHEN $8 THEN $10 ELSE b.custom_domain_token END,
                custom_domain_status = CASE WHEN $8 THEN (CASE WHEN $9 IS NULL THEN 'none' ELSE 'pending' END) ELSE b.custom_domain_status END,
                custom_domain_checked_at = CASE WHEN $8 THEN NULL ELSE b.custom_domain_checked_at END,
                custom_domain_message = CASE WHEN $8 THEN NULL ELSE b.custom_domain_message END,
                custom_domain_active = CASE WHEN $8 THEN false WHEN $11::boolean IS NOT NULL THEN $11 ELSE b.custom_domain_active END,
                email_from        = CASE WHEN $12 THEN $13::citext ELSE b.email_from END,
                email_footer      = CASE WHEN $14 THEN $15 ELSE b.email_footer END,
                login_message     = CASE WHEN $16 THEN $17 ELSE b.login_message END,
                pdf_letterhead    = CASE WHEN $18 THEN $19 ELSE b.pdf_letterhead END,
                updated_by = $20, updated_at = now(), version = b.version + 1
             WHERE b.tenant_id = $1
             RETURNING {BRANDING_COLS}"
        ))
        .bind(id.0)
        .bind(logo_set)
        .bind(logo.as_ref().map(|l| l.0.clone()))
        .bind(logo.as_ref().map(|l| l.1.clone()))
        .bind(logo.as_ref().map(|l| l.2))
        .bind(&w.primary_color)
        .bind(&w.secondary_color)
        .bind(domain_set)
        .bind(domain.as_ref().map(|d| d.0.clone()))
        .bind(domain.as_ref().map(|d| d.1.clone()))
        .bind(if domain_set { None } else { w.custom_domain_active })
        .bind(email_set)
        .bind(email)
        .bind(footer_set)
        .bind(footer)
        .bind(login_set)
        .bind(login)
        .bind(pdf_set)
        .bind(pdf)
        .bind(actor)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_unique(e, "uq_tenant_branding_custom_domain", "Domain already claimed"))?;
        let Some(row) = row else {
            return Err(AppError::not_found("Branding does not exist"));
        };
        let b = branding_from_row(&row)?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(b)
    }

    async fn set_domain_status(
        &self,
        scope: &AccessScope,
        id: TenantId,
        status: DomainStatus,
        message: &str,
        at: DateTime<Utc>,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "UPDATE tenantadm.tenant_branding SET custom_domain_status = $2, custom_domain_message = $3, custom_domain_checked_at = $4,
                 custom_domain_active = CASE WHEN $2 = 'verified' THEN custom_domain_active ELSE false END,
                 updated_at = now(), version = version + 1
             WHERE tenant_id = $1 AND custom_domain IS NOT NULL",
        )
        .bind(id.0)
        .bind(status.as_str())
        .bind(message)
        .bind(at)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn sender_domains(&self, scope: &AccessScope, id: TenantId) -> AppResult<Vec<SenderDomain>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(
            "SELECT id, domain::text AS domain, status, spf_record, dkim_selector, dkim_record, verified_at, message
               FROM tenantadm.sender_domains WHERE tenant_id = $1 ORDER BY domain",
        )
        .bind(id.0)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| {
                Ok(SenderDomain {
                    id: r.try_get("id")?,
                    domain: r.try_get("domain")?,
                    status: r.try_get("status")?,
                    spf_record: r.try_get("spf_record")?,
                    dkim_selector: r.try_get("dkim_selector")?,
                    dkim_record: r.try_get("dkim_record")?,
                    verified_at: r.try_get("verified_at")?,
                    message: r.try_get("message")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn upsert_sender_domain(
        &self,
        scope: &AccessScope,
        id: TenantId,
        domain: &str,
        spf: &str,
        selector: &str,
        dkim: &str,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.sender_domains (id, tenant_id, domain, spf_record, dkim_selector, dkim_record)
             VALUES ($1,$2,$3::citext,$4,$5,$6)
             ON CONFLICT (tenant_id, domain) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(id.0)
        .bind(domain)
        .bind(spf)
        .bind(selector)
        .bind(dkim)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_sender_status(
        &self,
        scope: &AccessScope,
        id: TenantId,
        domain: &str,
        verified: bool,
        message: &str,
        at: DateTime<Utc>,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "UPDATE tenantadm.sender_domains SET status = $3, message = $4, checked_at = $5,
                 verified_at = CASE WHEN $3 = 'verified' THEN $5 ELSE verified_at END, updated_at = now()
             WHERE tenant_id = $1 AND domain = $2::citext",
        )
        .bind(id.0)
        .bind(domain)
        .bind(if verified { "verified" } else { "failed" })
        .bind(message)
        .bind(at)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn find_by_custom_domain(&self, domain: &str) -> AppResult<Option<(TenantId, bool)>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let row = sqlx::query("SELECT tenant_id, custom_domain_active FROM tenantadm.tenant_branding WHERE custom_domain = $1::citext")
            .bind(domain)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(match row {
            Some(r) => Some((TenantId(r.try_get("tenant_id")?), r.try_get("custom_domain_active")?)),
            None => None,
        })
    }
}
