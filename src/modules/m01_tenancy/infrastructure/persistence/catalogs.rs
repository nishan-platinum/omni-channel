//! REFERENCE ADAPTERS backed by seeded tables: PlanCatalog (stand-in for M19) and
//! TemplateCatalog (OCC-M01-R013 templates). Also the AuditSink (stand-in for M30).

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use crate::platform::audit::{self, AuditEntry};
use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::AppResult;

use super::super::super::application::context::AuditSink;
use super::super::super::application::ports::*;
use super::super::super::domain::plan::Plan;
use super::super::super::domain::quota::QuotaMetric;
use super::super::super::domain::storage::Tier;
use super::PgStore;

fn plan_from_row(r: &PgRow) -> Result<Plan, sqlx::Error> {
    let ents: Value = r.try_get("entitlements")?;
    let quotas: Value = r.try_get("quotas")?;
    let entitlements: BTreeSet<String> =
        ents.as_array().map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();
    let quotas: BTreeMap<QuotaMetric, i64> = quotas
        .as_object()
        .map(|o| o.iter().filter_map(|(k, v)| Some((QuotaMetric::parse(k).ok()?, v.as_i64()?))).collect())
        .unwrap_or_default();
    Ok(Plan {
        id: r.try_get("id")?,
        code: r.try_get("code")?,
        name: r.try_get("name")?,
        tier: Tier::parse(&r.try_get::<String, _>("tier")?).map_err(|e| sqlx::Error::Decode(format!("{e:?}").into()))?,
        entitlements,
        quotas,
        soft_threshold: r.try_get("soft_threshold")?,
        active: r.try_get::<String, _>("status")? == "active",
        version: r.try_get("version")?,
    })
}

const PLAN_COLS: &str = "id, code, name, tier, entitlements, quotas, soft_threshold::float8 AS soft_threshold, status, version";

#[async_trait]
impl PlanCatalog for PgStore {
    async fn list_active(&self) -> AppResult<Vec<Plan>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let rows = sqlx::query(&format!("SELECT {PLAN_COLS} FROM tenantadm.plans WHERE status = 'active' ORDER BY code"))
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(plan_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn get(&self, id: Uuid) -> AppResult<Option<Plan>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let row = sqlx::query(&format!("SELECT {PLAN_COLS} FROM tenantadm.plans WHERE id = $1")).bind(id).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok(row.as_ref().map(plan_from_row).transpose()?)
    }
}

fn template_from_row(r: &PgRow) -> Result<ProvisioningTemplate, sqlx::Error> {
    let features: Value = r.try_get("features")?;
    let cfg: Value = r.try_get("config_defaults")?;
    let packs: Value = r.try_get("packs")?;
    Ok(ProvisioningTemplate {
        id: r.try_get("id")?,
        code: r.try_get("code")?,
        name: r.try_get("name")?,
        description: r.try_get("description")?,
        features: features.as_array().map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default(),
        config_defaults: cfg.as_object().map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default(),
        packs: packs.as_array().map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default(),
    })
}

#[async_trait]
impl TemplateCatalog for PgStore {
    async fn list(&self) -> AppResult<Vec<ProvisioningTemplate>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let rows = sqlx::query("SELECT id, code, name, description, features, config_defaults, packs FROM tenantadm.provisioning_templates WHERE status = 'active' ORDER BY code")
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(template_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn get(&self, code: &str) -> AppResult<Option<ProvisioningTemplate>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let row = sqlx::query("SELECT id, code, name, description, features, config_defaults, packs FROM tenantadm.provisioning_templates WHERE code = $1 AND status = 'active'")
            .bind(code)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(row.as_ref().map(template_from_row).transpose()?)
    }
}

#[async_trait]
impl AuditSink for PgStore {
    async fn record(&self, scope: &AccessScope, entries: Vec<AuditEntry>) -> AppResult<()> {
        audit::append_standalone(&self.pool, scope, &entries).await?;
        Ok(())
    }
}
