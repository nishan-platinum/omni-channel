//! M01's implementation of the bootstrap-auth `TenantGate` port: login/API gating by tenant
//! status (BR-M01-002) and the tenant's session/password policy (R021 boundary).

use async_trait::async_trait;
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::bootstrap_auth::service::{TenantAccessInfo, TenantGate};
use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::AppResult;

use super::super::domain::TenantStatus;

pub struct M01TenantGate {
    pub pool: PgPool,
    pub default_idle_minutes: i64,
}

#[async_trait]
impl TenantGate for M01TenantGate {
    async fn access_info(&self, tenant_id: Uuid) -> AppResult<Option<TenantAccessInfo>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let row = sqlx::query(
            "SELECT t.tenant_code, t.name, t.status,
                    (SELECT c.config_value FROM tenantadm.tenant_configs c WHERE c.tenant_id = t.id AND c.config_key = 'security.session_idle_timeout_minutes') AS idle,
                    (SELECT c.config_value FROM tenantadm.tenant_configs c WHERE c.tenant_id = t.id AND c.config_key = 'security.password_min_length') AS pwmin,
                    b.primary_color::text AS primary_color, b.secondary_color::text AS secondary_color
               FROM tenantadm.tenants t LEFT JOIN tenantadm.tenant_branding b ON b.tenant_id = t.id
              WHERE t.id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        let Some(r) = row else {
            return Ok(None);
        };
        let status_s: String = r.try_get("status")?;
        let status = TenantStatus::parse(&status_s)?;
        let idle: Option<Value> = r.try_get("idle")?;
        let pwmin: Option<Value> = r.try_get("pwmin")?;
        Ok(Some(TenantAccessInfo {
            code: r.try_get("tenant_code")?,
            name: r.try_get("name")?,
            status: status_s,
            allows_access: status.allows_access(),
            read_only: status == TenantStatus::Grace,
            idle_timeout_minutes: idle.and_then(|v| v.as_i64()).unwrap_or(self.default_idle_minutes),
            password_min_length: pwmin.and_then(|v| v.as_u64()).unwrap_or(12) as usize,
            primary_color: r.try_get("primary_color")?,
            secondary_color: r.try_get("secondary_color")?,
        }))
    }

    async fn tenant_by_code(&self, code: &str) -> AppResult<Option<Uuid>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let id: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM tenantadm.tenants WHERE tenant_code = $1").bind(code).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok(id)
    }
}
