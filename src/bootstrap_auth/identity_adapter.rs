//! REFERENCE ADAPTER: M01's `IdentityPort` (owned by M02) implemented with bootstrap auth.

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::modules::m01_tenancy::application::ports::{IdentityPort, TenantUser};
use crate::modules::m01_tenancy::domain::TenantId;
use crate::platform::errors::AppResult;

use super::service::AuthService;

pub struct BootstrapIdentityAdapter {
    pub auth: Arc<AuthService>,
}

#[async_trait]
impl IdentityPort for BootstrapIdentityAdapter {
    async fn invite_tenant_admin(&self, tenant: TenantId, email: &str, display_name: &str) -> AppResult<(Uuid, Option<String>)> {
        self.auth.invite_tenant_admin(tenant.0, email, display_name).await
    }

    async fn revoke_sessions(&self, tenant: TenantId) -> AppResult<u64> {
        self.auth.revoke_tenant_sessions(tenant.0).await
    }

    async fn count_users(&self, tenant: TenantId) -> AppResult<i64> {
        self.auth.count_users(tenant.0).await
    }

    async fn tenant_admins(&self, tenant: TenantId) -> AppResult<Vec<TenantUser>> {
        Ok(self
            .auth
            .tenant_admins(tenant.0)
            .await?
            .into_iter()
            .map(|(id, email, display_name, status)| TenantUser { id, email, display_name, status })
            .collect())
    }

    async fn purge_tenant(&self, tenant: TenantId) -> AppResult<u64> {
        self.auth.purge_tenant(tenant.0).await
    }
}
