//! Actor model, authorization of tenant access (M01-F02), domain→application error mapping,
//! and the dependency container shared by the M01 services.

use std::sync::Arc;

use chrono::Duration;
use serde_json::Value;
use uuid::Uuid;

use crate::platform::audit::AuditEntry;
use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult, FieldError};
use crate::platform::events::EventEnvelope;
use crate::platform::time::Clock;

use super::super::domain::events::TenantEvent;
use super::super::domain::tenant::LifecyclePolicy;
use super::super::domain::{DomainError, TenantId};
use super::ports::*;

impl From<DomainError> for AppError {
    fn from(e: DomainError) -> Self {
        match e {
            DomainError::Validation(v) => AppError::validation_many(v.into_iter().map(|x| FieldError::new(x.field, x.message)).collect()),
            DomainError::Conflict(m) => AppError::conflict(m),
            DomainError::Forbidden(m) => AppError::forbidden(m),
            DomainError::NotFound(m) => AppError::not_found(m),
            DomainError::QuotaExceeded { metric, message, retry_after_secs } => {
                if metric == "users" || metric == "api_requests_per_minute" {
                    AppError::rate_limited(message, retry_after_secs)
                } else {
                    AppError::quota_exceeded(message, retry_after_secs)
                }
            }
            DomainError::DomainNotVerified(m) => AppError::domain_not_verified(m),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorRole {
    SuperAdmin,
    TenantAdmin,
    /// Internal jobs (scheduler, billing-driven transitions via service scope).
    System,
}

impl ActorRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SuperAdmin => "super_admin",
            Self::TenantAdmin => "tenant_admin",
            Self::System => "system",
        }
    }
}

/// The authenticated caller, built by the web layer from the server-side session/token.
#[derive(Debug, Clone)]
pub struct Actor {
    pub user_id: Option<Uuid>,
    pub role: ActorRole,
    /// Home tenant of a Tenant Admin (from the session — never from request input).
    pub tenant_id: Option<TenantId>,
    pub email: Option<String>,
    pub correlation_id: Option<String>,
    pub ip: Option<String>,
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
}

impl Actor {
    pub fn system(reason: &str) -> Self {
        Self {
            user_id: None,
            role: ActorRole::System,
            tenant_id: None,
            email: None,
            correlation_id: Some(format!("system:{reason}")),
            ip: None,
            user_agent: None,
        }
    }

    pub fn is_super_admin(&self) -> bool {
        self.role == ActorRole::SuperAdmin
    }

    pub fn is_tenant_admin(&self) -> bool {
        self.role == ActorRole::TenantAdmin
    }

    pub fn require_super_admin(&self) -> AppResult<()> {
        if matches!(self.role, ActorRole::SuperAdmin | ActorRole::System) {
            Ok(())
        } else {
            Err(AppError::forbidden("Platform Super Admin role required"))
        }
    }

    /// Scope for operations that are platform-wide (no specific tenant).
    pub fn platform_scope(&self) -> AppResult<AccessScope> {
        match self.role {
            ActorRole::SuperAdmin => Ok(AccessScope::Platform),
            ActorRole::System => Ok(AccessScope::System),
            ActorRole::TenantAdmin => Err(AppError::forbidden("Platform Super Admin role required")),
        }
    }

    pub fn audit(&self, tenant: Option<TenantId>, entity_type: &str, entity_id: Option<String>, action: &str) -> AuditEntry {
        AuditEntry {
            tenant_id: tenant.map(|t| t.0),
            actor_id: self.user_id,
            actor_role: self.role.as_str().to_string(),
            entity_type: entity_type.to_string(),
            entity_id,
            action: action.to_string(),
            before: None,
            after: None,
            reason: None,
            security_event: false,
            ip: self.ip.clone(),
            user_agent: self.user_agent.clone(),
            correlation_id: self.correlation_id.clone(),
        }
    }

    pub fn tenant_audit(&self, tenant: TenantId, action: &str, before: Option<Value>, after: Option<Value>) -> AuditEntry {
        let mut e = self.audit(Some(tenant), "tenant", Some(tenant.to_string()), action);
        e.before = before;
        e.after = after;
        e
    }

    pub fn event(&self, tenant: TenantId, ev: &TenantEvent) -> EventEnvelope {
        EventEnvelope {
            id: Uuid::now_v7(),
            event_type: ev.event_type().to_string(),
            tenant_id: Some(tenant.0),
            aggregate_id: tenant.to_string(),
            payload: ev.payload(tenant),
            correlation_id: self.correlation_id.clone(),
            occurred_at: chrono::Utc::now(),
        }
    }
}

/// Settings of the M01 module (from deployment configuration).
#[derive(Debug, Clone)]
pub struct M01Settings {
    pub lifecycle: LifecyclePolicy,
    pub platform_domain: String,
    pub public_base_url: String,
    pub ops_alert_email: String,
    pub auto_purge: bool,
    pub development: bool,
}

impl M01Settings {
    pub fn lifecycle_from_hours(grace_hours: i64, retention_hours: i64) -> LifecyclePolicy {
        LifecyclePolicy { grace_period: Duration::hours(grace_hours), retention: Duration::hours(retention_hours) }
    }
}

/// Dependency container. Services hold an `Arc<M01Deps>` and use only what they need.
pub struct M01Deps {
    pub tenants: Arc<dyn TenantRepository>,
    pub configs: Arc<dyn ConfigRepository>,
    pub quotas: Arc<dyn QuotaRepository>,
    pub branding: Arc<dyn BrandingRepository>,
    pub connections: Arc<dyn ConnectionRepository>,
    pub provisioning: Arc<dyn ProvisioningRepository>,
    pub grants: Arc<dyn SupportGrantRepository>,
    pub baselines: Arc<dyn BaselineRepository>,
    pub releases: Arc<dyn ReleaseRepository>,
    pub keys: Arc<dyn KeyRepository>,
    pub exports: Arc<dyn ExportRepository>,
    pub purge: Arc<dyn PurgeRepository>,
    pub analytics: Arc<dyn AnalyticsRepository>,
    pub audit: Arc<dyn AuditSink>,
    pub plans: Arc<dyn PlanCatalog>,
    pub templates: Arc<dyn TemplateCatalog>,
    pub identity: Arc<dyn IdentityPort>,
    pub notifications: Arc<dyn NotificationPort>,
    pub downstream: Arc<dyn DownstreamProvisioningPort>,
    pub domain_verifier: Arc<dyn DomainVerificationPort>,
    pub sender_verifier: Arc<dyn EmailSenderVerificationPort>,
    pub kms: Arc<dyn KeyManagementPort>,
    pub objects: Arc<dyn ObjectStoragePort>,
    pub release_port: Arc<dyn ReleaseManagementPort>,
    pub anonymiser: Arc<dyn AnonymisedDataCopyPort>,
    pub export_participants: Vec<Arc<dyn ExportParticipant>>,
    pub data_router: Arc<dyn TenantDataRouter>,
    pub clock: Arc<dyn Clock>,
    pub settings: M01Settings,
}

/// Standalone audit writes (security events that must persist even when the request fails).
#[async_trait::async_trait]
pub trait AuditSink: Send + Sync {
    async fn record(&self, scope: &AccessScope, entries: Vec<AuditEntry>) -> AppResult<()>;
}

impl M01Deps {
    /// Authorises access to one tenant and returns the DB scope to use.
    ///
    /// * Super Admin → platform scope (cross-tenant, explicit). Reads of a specific tenant are
    ///   recorded as `platform.elevated_read` when `audit_elevated` is set (M01-F02 step 5).
    /// * Tenant Admin → only their own tenant; any other id is refused with 403 and a security
    ///   audit event (M01-F02 step 4). Tenant identity comes from the session, never the request.
    /// * System → system scope.
    pub async fn authorize(&self, actor: &Actor, tenant: TenantId, access: Access, audit_elevated: bool) -> AppResult<AccessScope> {
        match actor.role {
            ActorRole::SuperAdmin => {
                if audit_elevated {
                    let mut e = actor.audit(Some(tenant), "tenant", Some(tenant.to_string()), "platform.elevated_access");
                    e.after = Some(serde_json::json!({ "access": if access == Access::Read { "read" } else { "write" } }));
                    self.audit.record(&AccessScope::Platform, vec![e]).await?;
                }
                Ok(AccessScope::Platform)
            }
            ActorRole::System => Ok(AccessScope::System),
            ActorRole::TenantAdmin => match actor.tenant_id {
                Some(own) if own == tenant => Ok(AccessScope::Tenant(own.0)),
                Some(own) => {
                    let mut e = actor.audit(Some(own), "tenant", Some(tenant.to_string()), "security.cross_tenant_attempt");
                    e.security_event = true;
                    e.after = Some(serde_json::json!({ "requested_tenant_id": tenant, "access": format!("{access:?}") }));
                    self.audit.record(&AccessScope::Tenant(own.0), vec![e]).await?;
                    tracing::warn!(actor_tenant = %own, requested_tenant = %tenant, "cross-tenant access attempt denied");
                    Err(AppError::forbidden("Cross-tenant access is not permitted"))
                }
                None => Err(AppError::forbidden("No tenant context")),
            },
        }
    }

    pub async fn load_tenant(&self, scope: &AccessScope, id: TenantId) -> AppResult<super::super::domain::Tenant> {
        self.tenants.get(scope, id).await?.ok_or_else(|| AppError::not_found("Tenant does not exist"))
    }
}
