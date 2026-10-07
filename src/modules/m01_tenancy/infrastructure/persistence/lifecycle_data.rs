//! BaselineRepository, ReleaseRepository, KeyRepository, ExportRepository, PurgeRepository,
//! AnalyticsRepository.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{json, Value};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};
use crate::platform::security::sha256_hex;

use super::super::super::application::ports::*;
use super::super::super::domain::baseline::BaselineDocument;
use super::super::super::domain::keys::{KeyKind, KeyState};
use super::super::super::domain::release::Ring;
use super::super::super::domain::tenant::TransitionPlan;
use super::super::super::domain::TenantId;
use super::tenants::{tenant_from_row, TENANT_COLS};
use super::{is_unique_violation, write_changes, PgStore};

fn dec(e: impl std::fmt::Debug) -> sqlx::Error {
    sqlx::Error::Decode(format!("{e:?}").into())
}

fn baseline_from_row(r: &PgRow) -> Result<StoredBaseline, sqlx::Error> {
    let content: Value = r.try_get("content")?;
    Ok(StoredBaseline {
        id: r.try_get("id")?,
        version_no: r.try_get("version_no")?,
        label: r.try_get("label")?,
        source: r.try_get("source")?,
        sha256: r.try_get("sha256")?,
        change_record: r.try_get("change_record")?,
        created_at: r.try_get("created_at")?,
        content: serde_json::from_value(content).map_err(dec)?,
    })
}

#[async_trait]
impl BaselineRepository for PgStore {
    async fn insert(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        label: &str,
        source: &str,
        doc: &BaselineDocument,
        change_record: Option<&str>,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<StoredBaseline> {
        let json = doc.canonical_json();
        let sha = sha256_hex(json.as_bytes());
        let content: Value = serde_json::from_str(&json).map_err(AppError::internal)?;
        let mut tx = scoped_tx(&self.pool, scope).await?;
        // Serialise version numbering per tenant.
        sqlx::query("SELECT 1 FROM tenantadm.tenants WHERE id = $1 FOR UPDATE").bind(tenant.0).execute(&mut *tx).await?;
        let row = sqlx::query(
            "INSERT INTO tenantadm.tenant_baselines (id, tenant_id, version_no, label, source, content, sha256, change_record, created_by)
             VALUES ($1, $2, (SELECT coalesce(max(version_no), 0) + 1 FROM tenantadm.tenant_baselines WHERE tenant_id = $2), $3, $4, $5, $6, $7, $8)
             RETURNING id, version_no, label, source, sha256, change_record, created_at, content",
        )
        .bind(Uuid::now_v7())
        .bind(tenant.0)
        .bind(label)
        .bind(source)
        .bind(&content)
        .bind(&sha)
        .bind(change_record)
        .bind(actor)
        .fetch_one(&mut *tx)
        .await?;
        let b = baseline_from_row(&row)?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(b)
    }

    async fn list(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<StoredBaseline>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(
            "SELECT id, version_no, label, source, sha256, change_record, created_at, content
               FROM tenantadm.tenant_baselines WHERE tenant_id = $1 ORDER BY version_no DESC LIMIT 50",
        )
        .bind(tenant.0)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows.iter().map(baseline_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn get(&self, scope: &AccessScope, tenant: TenantId, id: Uuid) -> AppResult<Option<StoredBaseline>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query(
            "SELECT id, version_no, label, source, sha256, change_record, created_at, content
               FROM tenantadm.tenant_baselines WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant.0)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(row.as_ref().map(baseline_from_row).transpose()?)
    }

    async fn apply(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        doc: &BaselineDocument,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        for (k, v) in &doc.config {
            sqlx::query(
                "INSERT INTO tenantadm.tenant_configs (id, tenant_id, config_key, config_value, updated_by) VALUES ($1,$2,$3,$4,$5)
                 ON CONFLICT (tenant_id, config_key) DO UPDATE SET config_value = EXCLUDED.config_value, updated_by = EXCLUDED.updated_by,
                   updated_at = now(), version = tenantadm.tenant_configs.version + 1",
            )
            .bind(Uuid::now_v7())
            .bind(tenant.0)
            .bind(k)
            .bind(v)
            .bind(actor)
            .execute(&mut *tx)
            .await?;
        }
        // Flags absent from the baseline are switched off (the baseline is the full desired state).
        sqlx::query("UPDATE tenantadm.tenant_feature_flags SET enabled = false, updated_at = now() WHERE tenant_id = $1 AND NOT (feature_key = ANY($2))")
            .bind(tenant.0)
            .bind(doc.feature_flags.keys().cloned().collect::<Vec<_>>())
            .execute(&mut *tx)
            .await?;
        for (k, on) in &doc.feature_flags {
            sqlx::query(
                "INSERT INTO tenantadm.tenant_feature_flags (id, tenant_id, feature_key, enabled, updated_by) VALUES ($1,$2,$3,$4,$5)
                 ON CONFLICT (tenant_id, feature_key) DO UPDATE SET enabled = EXCLUDED.enabled, updated_by = EXCLUDED.updated_by,
                   updated_at = now(), version = tenantadm.tenant_feature_flags.version + 1",
            )
            .bind(Uuid::now_v7())
            .bind(tenant.0)
            .bind(k)
            .bind(on)
            .bind(actor)
            .execute(&mut *tx)
            .await?;
        }
        let b = &doc.branding;
        sqlx::query(
            "UPDATE tenantadm.tenant_branding SET primary_color = $2, secondary_color = $3, email_footer = $4, login_message = $5,
                 pdf_letterhead = $6, updated_by = $7, updated_at = now(), version = version + 1 WHERE tenant_id = $1",
        )
        .bind(tenant.0)
        .bind(&b.primary_color)
        .bind(&b.secondary_color)
        .bind(&b.email_footer)
        .bind(&b.login_message)
        .bind(&b.pdf_letterhead)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
        let r = &doc.release;
        sqlx::query(
            "UPDATE tenantadm.tenant_release_preferences SET ring = $2, maintenance_day = $3, maintenance_start_hour_utc = $4,
                 maintenance_duration_min = $5, updated_by = $6, updated_at = now(), version = version + 1 WHERE tenant_id = $1",
        )
        .bind(tenant.0)
        .bind(&r.ring)
        .bind(r.maintenance_day)
        .bind(r.maintenance_start_hour_utc)
        .bind(r.maintenance_duration_min)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }
}

fn pref_from_row(r: &PgRow) -> Result<ReleasePreference, sqlx::Error> {
    Ok(ReleasePreference {
        ring: Ring::parse(&r.try_get::<String, _>("ring")?).map_err(dec)?,
        maintenance_day: r.try_get("maintenance_day")?,
        maintenance_start_hour_utc: r.try_get("maintenance_start_hour_utc")?,
        maintenance_duration_min: r.try_get("maintenance_duration_min")?,
    })
}

fn release_from_row(r: &PgRow) -> Result<PlatformRelease, sqlx::Error> {
    Ok(PlatformRelease {
        id: r.try_get("id")?,
        release_version: r.try_get("release_version")?,
        title: r.try_get("title")?,
        notes: r.try_get("notes")?,
        disruptive: r.try_get("disruptive")?,
        status: r.try_get("status")?,
        current_ring: r.try_get::<Option<String>, _>("current_ring")?.map(|s| Ring::parse(&s)).transpose().map_err(dec)?,
        created_at: r.try_get("created_at")?,
    })
}

fn rollout_from_row(r: &PgRow) -> Result<Rollout, sqlx::Error> {
    Ok(Rollout {
        release_id: r.try_get("release_id")?,
        tenant_id: TenantId(r.try_get("tenant_id")?),
        release_version: r.try_get("release_version")?,
        title: r.try_get("title")?,
        notes: r.try_get("notes")?,
        disruptive: r.try_get("disruptive")?,
        ring: Ring::parse(&r.try_get::<String, _>("ring")?).map_err(dec)?,
        scheduled_for: r.try_get("scheduled_for")?,
        status: r.try_get("status")?,
        completed_at: r.try_get("completed_at")?,
    })
}

const ROLLOUT_SQL: &str = "SELECT ro.release_id, ro.tenant_id, r.release_version, r.title, r.notes, r.disruptive, ro.ring, ro.scheduled_for, ro.status, ro.completed_at
    FROM tenantadm.release_rollouts ro JOIN tenantadm.platform_releases r ON r.id = ro.release_id";

#[async_trait]
impl ReleaseRepository for PgStore {
    async fn preference(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<ReleasePreference> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query("SELECT ring, maintenance_day, maintenance_start_hour_utc, maintenance_duration_min FROM tenantadm.tenant_release_preferences WHERE tenant_id = $1")
            .bind(tenant.0)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        match row {
            Some(r) => Ok(pref_from_row(&r)?),
            None => Err(AppError::not_found("Release preference does not exist")),
        }
    }

    async fn set_preference(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        p: &ReleasePreference,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "UPDATE tenantadm.tenant_release_preferences SET ring = $2, maintenance_day = $3, maintenance_start_hour_utc = $4,
                 maintenance_duration_min = $5, updated_by = $6, updated_at = now(), version = version + 1 WHERE tenant_id = $1",
        )
        .bind(tenant.0)
        .bind(p.ring.as_str())
        .bind(p.maintenance_day)
        .bind(p.maintenance_start_hour_utc)
        .bind(p.maintenance_duration_min)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn create_release(&self, scope: &AccessScope, r: &PlatformRelease, actor: Option<Uuid>, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("INSERT INTO tenantadm.platform_releases (id, release_version, title, notes, disruptive, status, created_by) VALUES ($1,$2,$3,$4,$5,'planned',$6)")
            .bind(r.id)
            .bind(&r.release_version)
            .bind(&r.title)
            .bind(&r.notes)
            .bind(r.disruptive)
            .bind(actor)
            .execute(&mut *tx)
            .await
            .map_err(|e| if is_unique_violation(&e) { AppError::conflict("Release version already exists") } else { AppError::internal(e) })?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn releases(&self, scope: &AccessScope) -> AppResult<Vec<PlatformRelease>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT id, release_version, title, notes, disruptive, status, current_ring, created_at FROM tenantadm.platform_releases ORDER BY created_at DESC LIMIT 50")
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(release_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn release(&self, scope: &AccessScope, id: Uuid) -> AppResult<Option<PlatformRelease>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query("SELECT id, release_version, title, notes, disruptive, status, current_ring, created_at FROM tenantadm.platform_releases WHERE id = $1")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(row.as_ref().map(release_from_row).transpose()?)
    }

    async fn set_release_ring(&self, scope: &AccessScope, id: Uuid, ring: Ring, status: &str, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("UPDATE tenantadm.platform_releases SET current_ring = $2, status = $3, updated_at = now(), version = version + 1 WHERE id = $1")
            .bind(id)
            .bind(ring.as_str())
            .bind(status)
            .execute(&mut *tx)
            .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn tenants_in_rings(&self, scope: &AccessScope, rings: &[Ring]) -> AppResult<Vec<(TenantId, ReleasePreference)>> {
        let rings: Vec<&str> = rings.iter().map(|r| r.as_str()).collect();
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(
            "SELECT p.tenant_id, p.ring, p.maintenance_day, p.maintenance_start_hour_utc, p.maintenance_duration_min
               FROM tenantadm.tenant_release_preferences p JOIN tenantadm.tenants t ON t.id = p.tenant_id
              WHERE p.ring = ANY($1) AND t.status IN ('draft', 'active', 'suspended', 'grace')",
        )
        .bind(&rings)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok((TenantId(r.try_get("tenant_id")?), pref_from_row(r)?)))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn schedule_rollout(
        &self,
        scope: &AccessScope,
        release: Uuid,
        tenant: TenantId,
        ring: Ring,
        at: DateTime<Utc>,
    ) -> AppResult<bool> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let n = sqlx::query(
            "INSERT INTO tenantadm.release_rollouts (id, release_id, tenant_id, ring, scheduled_for) VALUES ($1,$2,$3,$4,$5)
             ON CONFLICT (release_id, tenant_id) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(release)
        .bind(tenant.0)
        .bind(ring.as_str())
        .bind(at)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(n > 0)
    }

    async fn due_rollouts(&self, now: DateTime<Utc>) -> AppResult<Vec<Rollout>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let rows = sqlx::query(&format!(
            "{ROLLOUT_SQL} WHERE ro.status = 'scheduled' AND ro.scheduled_for <= $1 ORDER BY ro.scheduled_for LIMIT 200"
        ))
        .bind(now)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows.iter().map(rollout_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn complete_rollout(&self, scope: &AccessScope, release: Uuid, tenant: TenantId, at: DateTime<Utc>) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "UPDATE tenantadm.release_rollouts SET status = 'completed', completed_at = $3 WHERE release_id = $1 AND tenant_id = $2",
        )
        .bind(release)
        .bind(tenant.0)
        .bind(at)
        .execute(&mut *tx)
        .await?;
        // A release is completed once it reached all tenants and every rollout finished.
        sqlx::query(
            "UPDATE tenantadm.platform_releases r SET status = 'completed', updated_at = now()
              WHERE r.id = $1 AND r.current_ring = 'general'
                AND NOT EXISTS (SELECT 1 FROM tenantadm.release_rollouts ro WHERE ro.release_id = r.id AND ro.status = 'scheduled')",
        )
        .bind(release)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn rollouts_for_tenant(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<Rollout>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(&format!("{ROLLOUT_SQL} WHERE ro.tenant_id = $1 ORDER BY ro.scheduled_for DESC LIMIT 50"))
            .bind(tenant.0)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(rollout_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn rollout_counts(&self, scope: &AccessScope, release: Uuid) -> AppResult<(i64, i64)> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query("SELECT count(*) FILTER (WHERE status = 'scheduled') AS s, count(*) FILTER (WHERE status = 'completed') AS c FROM tenantadm.release_rollouts WHERE release_id = $1")
            .bind(release)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((row.try_get("s")?, row.try_get("c")?))
    }
}

fn key_from_row(r: &PgRow) -> Result<TenantKey, sqlx::Error> {
    Ok(TenantKey {
        id: r.try_get("id")?,
        kind: KeyKind::parse(&r.try_get::<String, _>("key_kind")?).map_err(dec)?,
        key_ref: r.try_get("key_ref")?,
        key_version: r.try_get("key_version")?,
        state: KeyState::parse(&r.try_get::<String, _>("state")?).map_err(dec)?,
        wrapped_dek: r.try_get("wrapped_dek")?,
        created_at: r.try_get("created_at")?,
        rotate_after: r.try_get("rotate_after")?,
    })
}

#[async_trait]
impl KeyRepository for PgStore {
    async fn list(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<TenantKey>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT id, key_kind, key_ref, key_version, state, wrapped_dek, created_at, rotate_after FROM tenantadm.tenant_keys WHERE tenant_id = $1 ORDER BY key_version DESC")
            .bind(tenant.0)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(key_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn by_version(&self, scope: &AccessScope, tenant: TenantId, version: i32) -> AppResult<Option<TenantKey>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query("SELECT id, key_kind, key_ref, key_version, state, wrapped_dek, created_at, rotate_after FROM tenantadm.tenant_keys WHERE tenant_id = $1 AND key_version = $2")
            .bind(tenant.0)
            .bind(version)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(row.as_ref().map(key_from_row).transpose()?)
    }

    async fn rotate(&self, scope: &AccessScope, tenant: TenantId, k: &NewKey, actor: Option<Uuid>, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query("UPDATE tenantadm.tenant_keys SET state = 'retired', retired_at = now() WHERE tenant_id = $1 AND state = 'active'")
            .bind(tenant.0)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO tenantadm.tenant_keys (id, tenant_id, key_kind, key_ref, key_version, state, wrapped_dek, activated_at, rotate_after, created_by)
             VALUES ($1,$2,$3,$4,$5,$6,$7,now(),$8,$9)",
        )
        .bind(k.id)
        .bind(tenant.0)
        .bind(k.kind.as_str())
        .bind(&k.key_ref)
        .bind(k.key_version)
        .bind(k.state.as_str())
        .bind(&k.wrapped_dek)
        .bind(k.rotate_after)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }
}

fn export_from_row(r: &PgRow) -> Result<ExportRecord, sqlx::Error> {
    Ok(ExportRecord {
        id: r.try_get("id")?,
        tenant_id: TenantId(r.try_get("tenant_id")?),
        reason: r.try_get("reason")?,
        object_key: r.try_get("object_key")?,
        sha256: r.try_get("sha256")?,
        size_bytes: r.try_get("size_bytes")?,
        key_version: r.try_get("key_version")?,
        created_at: r.try_get("created_at")?,
    })
}

fn backup_from_row(r: &PgRow) -> Result<BackupRecord, sqlx::Error> {
    Ok(BackupRecord {
        id: r.try_get("id")?,
        tenant_id: TenantId(r.try_get("tenant_id")?),
        object_key: r.try_get("object_key")?,
        sha256: r.try_get("sha256")?,
        size_bytes: r.try_get("size_bytes")?,
        key_version: r.try_get("key_version")?,
        created_at: r.try_get("created_at")?,
    })
}

async fn json_rows(tx: &mut sqlx::PgConnection, sql: &str, tenant: Uuid) -> Result<Value, sqlx::Error> {
    let v: Option<Value> = sqlx::query_scalar(&format!("SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb) FROM ({sql}) x"))
        .bind(tenant)
        .fetch_one(&mut *tx)
        .await?;
    Ok(v.unwrap_or(Value::Array(vec![])))
}

#[async_trait]
impl ExportRepository for PgStore {
    async fn snapshot(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Value> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let t = sqlx::query(&format!("SELECT {TENANT_COLS} FROM tenantadm.tenants t WHERE t.id = $1"))
            .bind(tenant.0)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| AppError::not_found("Tenant does not exist"))?;
        let tenant_json = serde_json::to_value(tenant_from_row(&t)?).map_err(AppError::internal)?;
        let id = tenant.0;
        let mut out = serde_json::Map::new();
        out.insert("tenant".into(), tenant_json);
        for (name, sql) in [
            ("config", "SELECT config_key, config_value, updated_at FROM tenantadm.tenant_configs WHERE tenant_id = $1 ORDER BY config_key"),
            ("feature_flags", "SELECT feature_key, enabled, updated_at FROM tenantadm.tenant_feature_flags WHERE tenant_id = $1 ORDER BY feature_key"),
            ("quotas", "SELECT metric, limit_value, usage_value, soft_threshold, period, cycle_start FROM tenantadm.tenant_quotas WHERE tenant_id = $1 ORDER BY metric"),
            ("usage_meters", "SELECT metric, period_month, value FROM tenantadm.usage_meters WHERE tenant_id = $1 ORDER BY period_month, metric"),
            ("branding", "SELECT logo_object_key, logo_content_type, primary_color, secondary_color, custom_domain::text, custom_domain_status, custom_domain_active, email_from::text, email_footer, login_message, pdf_letterhead FROM tenantadm.tenant_branding WHERE tenant_id = $1"),
            ("sender_domains", "SELECT domain::text, status, verified_at FROM tenantadm.sender_domains WHERE tenant_id = $1"),
            ("release_preferences", "SELECT ring, maintenance_day, maintenance_start_hour_utc, maintenance_duration_min FROM tenantadm.tenant_release_preferences WHERE tenant_id = $1"),
            ("config_baselines", "SELECT version_no, label, source, sha256, change_record, created_at, content FROM tenantadm.tenant_baselines WHERE tenant_id = $1 ORDER BY version_no"),
            ("support_grants", "SELECT id, reason, incident_ref, status, starts_at, expires_at, use_count, created_at FROM tenantadm.support_grants WHERE tenant_id = $1"),
            ("keys", "SELECT key_kind, key_ref, key_version, state, created_at, rotate_after FROM tenantadm.tenant_keys WHERE tenant_id = $1 ORDER BY key_version"),
            ("audit_log", "SELECT id, actor_id, actor_role, entity_type, entity_id, action, before, after, reason, security_event, correlation_id, created_at, encode(hash, 'hex') AS hash FROM shared.audit_log WHERE tenant_id = $1 ORDER BY id LIMIT 10000"),
        ] {
            out.insert(name.into(), json_rows(&mut tx, sql, id).await?);
        }
        tx.commit().await?;
        Ok(Value::Object(out))
    }

    async fn insert_export(&self, scope: &AccessScope, r: &ExportRecord, actor: Option<Uuid>, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.tenant_exports (id, tenant_id, reason, object_key, sha256, size_bytes, key_version, created_by, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        )
        .bind(r.id)
        .bind(r.tenant_id.0)
        .bind(&r.reason)
        .bind(&r.object_key)
        .bind(&r.sha256)
        .bind(r.size_bytes)
        .bind(r.key_version)
        .bind(actor)
        .bind(r.created_at)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn exports(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<ExportRecord>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT id, tenant_id, reason, object_key, sha256, size_bytes, key_version, created_at FROM tenantadm.tenant_exports WHERE tenant_id = $1 ORDER BY created_at DESC")
            .bind(tenant.0)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(export_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn export(&self, scope: &AccessScope, id: Uuid) -> AppResult<Option<ExportRecord>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query("SELECT id, tenant_id, reason, object_key, sha256, size_bytes, key_version, created_at FROM tenantadm.tenant_exports WHERE id = $1")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(row.as_ref().map(export_from_row).transpose()?)
    }

    async fn insert_backup(&self, scope: &AccessScope, r: &BackupRecord, actor: Option<Uuid>, changes: ChangeSet) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.tenant_backups (id, tenant_id, object_key, sha256, size_bytes, key_version, created_by, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(r.id)
        .bind(r.tenant_id.0)
        .bind(&r.object_key)
        .bind(&r.sha256)
        .bind(r.size_bytes)
        .bind(r.key_version)
        .bind(actor)
        .bind(r.created_at)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn backups(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Vec<BackupRecord>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT id, tenant_id, object_key, sha256, size_bytes, key_version, created_at FROM tenantadm.tenant_backups WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 50")
            .bind(tenant.0)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rows.iter().map(backup_from_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn record_restore(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        backup: Uuid,
        started: DateTime<Utc>,
        completed: DateTime<Utc>,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        sqlx::query(
            "INSERT INTO tenantadm.tenant_restores (id, tenant_id, backup_id, started_at, completed_at, duration_ms, created_by)
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(Uuid::now_v7())
        .bind(tenant.0)
        .bind(backup)
        .bind(started)
        .bind(completed)
        .bind((completed - started).num_milliseconds())
        .bind(actor)
        .execute(&mut *tx)
        .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn dr_report(&self, scope: &AccessScope) -> AppResult<Vec<DrReportRow>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let rows = sqlx::query(
            "SELECT t.id, t.tenant_code, p.tier,
                    (SELECT max(b.created_at) FROM tenantadm.tenant_backups b WHERE b.tenant_id = t.id) AS last_backup_at,
                    (SELECT r.duration_ms FROM tenantadm.tenant_restores r WHERE r.tenant_id = t.id ORDER BY r.completed_at DESC LIMIT 1) AS last_restore_ms
               FROM tenantadm.tenants t JOIN tenantadm.plans p ON p.id = t.plan_id
              WHERE t.status IN ('active', 'suspended', 'grace')
              ORDER BY t.tenant_code LIMIT 500",
        )
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| {
                Ok(DrReportRow {
                    tenant_id: TenantId(r.try_get("id")?),
                    tenant_code: r.try_get("tenant_code")?,
                    tier: r.try_get("tier")?,
                    last_backup_at: r.try_get("last_backup_at")?,
                    last_restore_ms: r.try_get("last_restore_ms")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    async fn certificate(&self, scope: &AccessScope, tenant: TenantId) -> AppResult<Option<DestructionCertificate>> {
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let row = sqlx::query("SELECT id, tenant_id, tenant_code, manifest, manifest_sha256, issued_at FROM tenantadm.destruction_certificates WHERE tenant_id = $1 ORDER BY issued_at DESC LIMIT 1")
            .bind(tenant.0)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(match row {
            Some(r) => Some(DestructionCertificate {
                id: r.try_get("id")?,
                tenant_id: TenantId(r.try_get("tenant_id")?),
                tenant_code: r.try_get("tenant_code")?,
                manifest: r.try_get("manifest")?,
                manifest_sha256: r.try_get("manifest_sha256")?,
                issued_at: r.try_get("issued_at")?,
            }),
            None => None,
        })
    }
}

#[async_trait]
impl PurgeRepository for PgStore {
    async fn purge(
        &self,
        scope: &AccessScope,
        tenant: TenantId,
        plan: &TransitionPlan,
        manifest_extra: Value,
        actor: Option<Uuid>,
        changes: ChangeSet,
    ) -> AppResult<DestructionCertificate> {
        let id = tenant.0;
        let mut tx = scoped_tx(&self.pool, scope).await?;
        let code: String =
            sqlx::query_scalar("SELECT tenant_code FROM tenantadm.tenants WHERE id = $1 AND status = 'terminated' FOR UPDATE")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| AppError::conflict("Only terminated tenants can be purged"))?;
        let mut counts: BTreeMap<String, i64> = BTreeMap::new();
        for table in [
            "tenantadm.tenant_configs",
            "tenantadm.tenant_feature_flags",
            "tenantadm.tenant_quotas",
            "tenantadm.usage_meters",
            "tenantadm.tenant_branding",
            "tenantadm.sender_domains",
            "tenantadm.support_grants",
            "tenantadm.tenant_baselines",
            "tenantadm.tenant_exports",
            "tenantadm.tenant_restores",
            "tenantadm.tenant_backups",
            "tenantadm.tenant_release_preferences",
            "tenantadm.release_rollouts",
            "tenantadm.provisioning_steps",
            "tenantadm.provisioning_runs",
            "tenantadm.isolation_checks",
            "shared.notification_outbox",
            "shared.idempotency_keys",
        ] {
            // Table names are compile-time constants (never user input).
            let n = sqlx::query(&format!("DELETE FROM {table} WHERE tenant_id = $1")).bind(id).execute(&mut *tx).await?.rows_affected();
            counts.insert(table.to_string(), n as i64);
        }
        let n = sqlx::query("DELETE FROM shared.event_outbox WHERE tenant_id = $1 AND published_at IS NOT NULL")
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        counts.insert("shared.event_outbox (delivered)".into(), n as i64);
        // Crypto-shredding: wrapped data keys are destroyed, so any surviving ciphertext is unreadable.
        let n = sqlx::query("UPDATE tenantadm.tenant_keys SET state = 'destroyed', wrapped_dek = NULL, destroyed_at = now() WHERE tenant_id = $1 AND state <> 'destroyed'")
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        counts.insert("tenantadm.tenant_keys (crypto-shredded)".into(), n as i64);
        sqlx::query("UPDATE tenantadm.tenant_database_connections SET status = 'decommissioned', updated_at = now() WHERE tenant_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        // Tombstone: code stays reserved (BR-M01-001), personal data removed.
        sqlx::query(
            "UPDATE tenantadm.tenants SET status = 'purged', purged_at = $2, name = '[purged]', legal_name = NULL,
                 primary_admin_email = ('purged+' || id::text || '@invalid.invalid')::citext, inheritance_flags = '{}'::jsonb,
                 suspended_reason = NULL, status_reason = $3, updated_at = now(), version = version + 1
             WHERE id = $1",
        )
        .bind(id)
        .bind(plan.purged_at)
        .bind(&plan.reason)
        .execute(&mut *tx)
        .await?;
        let issued_at = plan.purged_at.unwrap_or_else(Utc::now);
        let manifest = json!({
            "tenant_id": id,
            "tenant_code": code,
            "purged_at": issued_at,
            "issued_by": actor,
            "control_plane_rows_deleted": counts,
            "data_plane_and_objects": manifest_extra,
            "retained": ["tombstone tenant row (code reserved)", "shared.audit_log (retention >= 7 years)", "this certificate"],
        });
        let manifest_sha = sha256_hex(serde_json::to_string(&manifest).map_err(AppError::internal)?.as_bytes());
        let cert_id = Uuid::now_v7();
        sqlx::query("INSERT INTO tenantadm.destruction_certificates (id, tenant_id, tenant_code, manifest, manifest_sha256, issued_by, issued_at) VALUES ($1,$2,$3,$4,$5,$6,$7)")
            .bind(cert_id)
            .bind(id)
            .bind(&code)
            .bind(&manifest)
            .bind(&manifest_sha)
            .bind(actor)
            .bind(issued_at)
            .execute(&mut *tx)
            .await?;
        write_changes(&mut tx, &changes).await?;
        tx.commit().await?;
        Ok(DestructionCertificate { id: cert_id, tenant_id: tenant, tenant_code: code, manifest, manifest_sha256: manifest_sha, issued_at })
    }
}

#[async_trait]
impl AnalyticsRepository for PgStore {
    async fn aggregate(&self, month: NaiveDate, _now: DateTime<Utc>) -> AppResult<AnalyticsRaw> {
        // Aggregates over M01 metadata only; opted-out tenants are excluded before aggregation.
        const PARTICIPANTS: &str = "WITH participants AS (
            SELECT t.id, t.status, t.region, p.tier, p.entitlements FROM tenantadm.tenants t JOIN tenantadm.plans p ON p.id = t.plan_id
             WHERE t.status IN ('active', 'suspended', 'grace') AND NOT t.is_sandbox
               AND NOT EXISTS (SELECT 1 FROM tenantadm.tenant_configs c WHERE c.tenant_id = t.id
                               AND c.config_key = 'analytics.cross_tenant_opt_out' AND c.config_value = 'true'::jsonb))";
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let pairs = |rows: Vec<PgRow>| -> Result<Vec<(String, i64)>, sqlx::Error> {
            rows.iter().map(|r| Ok((r.try_get::<String, _>(0)?, r.try_get::<i64, _>(1)?))).collect()
        };
        let participants: i64 =
            sqlx::query_scalar(&format!("{PARTICIPANTS} SELECT count(*) FROM participants")).fetch_one(&mut *tx).await?;
        let opted_out: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM tenantadm.tenant_configs WHERE config_key = 'analytics.cross_tenant_opt_out' AND config_value = 'true'::jsonb",
        )
        .fetch_one(&mut *tx)
        .await?;
        let by_status = pairs(
            sqlx::query(&format!("{PARTICIPANTS} SELECT status, count(*) FROM participants GROUP BY status ORDER BY status"))
                .fetch_all(&mut *tx)
                .await?,
        )?;
        let by_tier = pairs(
            sqlx::query(&format!("{PARTICIPANTS} SELECT tier, count(*) FROM participants GROUP BY tier ORDER BY tier"))
                .fetch_all(&mut *tx)
                .await?,
        )?;
        let by_region = pairs(
            sqlx::query(&format!("{PARTICIPANTS} SELECT region, count(*) FROM participants GROUP BY region ORDER BY region"))
                .fetch_all(&mut *tx)
                .await?,
        )?;
        let feature_enabled = pairs(
            sqlx::query(&format!(
                "{PARTICIPANTS} SELECT f.feature_key, count(*) FROM tenantadm.tenant_feature_flags f JOIN participants pt ON pt.id = f.tenant_id
                 WHERE f.enabled GROUP BY f.feature_key ORDER BY f.feature_key"
            ))
            .fetch_all(&mut *tx)
            .await?,
        )?;
        let feature_entitled = pairs(
            sqlx::query(&format!(
                "{PARTICIPANTS} SELECT e.feature, count(*) FROM participants pt CROSS JOIN LATERAL jsonb_array_elements_text(pt.entitlements) AS e(feature)
                 GROUP BY e.feature ORDER BY e.feature"
            ))
            .fetch_all(&mut *tx)
            .await?,
        )?;
        let utilisation_buckets = pairs(
            sqlx::query(&format!(
                "{PARTICIPANTS}, util AS (
                    SELECT pt.id, coalesce(max(q.usage_value::float8 / q.limit_value), 0) AS u
                      FROM participants pt LEFT JOIN tenantadm.tenant_quotas q ON q.tenant_id = pt.id AND q.limit_value > 0 AND q.period <> 'per_minute'
                     GROUP BY pt.id)
                 SELECT CASE WHEN u < 0.5 THEN '< 50%' WHEN u < 0.8 THEN '50-80%' WHEN u < 1 THEN '80-100%' ELSE '100%' END AS bucket, count(*)
                   FROM util GROUP BY 1 ORDER BY 1"
            ))
            .fetch_all(&mut *tx)
            .await?,
        )?;
        let api_calls: Option<i64> = sqlx::query_scalar(&format!(
            "{PARTICIPANTS} SELECT sum(m.value)::bigint FROM tenantadm.usage_meters m JOIN participants pt ON pt.id = m.tenant_id
             WHERE m.metric = 'api_calls' AND m.period_month = $1"
        ))
        .bind(month)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(AnalyticsRaw {
            participants,
            opted_out,
            by_status,
            by_tier,
            by_region,
            feature_enabled,
            feature_entitled,
            utilisation_buckets,
            api_calls_this_month: api_calls.unwrap_or(0),
        })
    }
}
