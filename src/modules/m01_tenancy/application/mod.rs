//! M01 application layer: use cases orchestrating the domain and the ports.

pub mod analytics;
pub mod backup;
pub mod baselines;
pub mod branding;
pub mod configuration;
pub mod context;
pub mod directory;
pub mod handlers;
pub mod isolation;
pub mod keys;
pub mod lifecycle;
pub mod offboarding;
pub mod ports;
pub mod provisioning;
pub mod quotas;
pub mod releases;
pub mod sandbox;
pub mod support;

use std::sync::Arc;

use crate::platform::observability::TenantMetrics;

pub use context::{Access, Actor, ActorRole, M01Deps, M01Settings};

/// All M01 use-case services, wired once at startup.
pub struct M01Services {
    pub deps: Arc<M01Deps>,
    pub provisioning: Arc<provisioning::ProvisioningService>,
    pub lifecycle: Arc<lifecycle::LifecycleService>,
    pub directory: Arc<directory::DirectoryService>,
    pub configuration: Arc<configuration::ConfigurationService>,
    pub quotas: Arc<quotas::QuotaService>,
    pub branding: Arc<branding::BrandingService>,
    pub support: Arc<support::SupportAccessService>,
    pub sandboxes: Arc<sandbox::SandboxService>,
    pub baselines: Arc<baselines::BaselineService>,
    pub releases: Arc<releases::ReleaseService>,
    pub keys: Arc<keys::KeyService>,
    pub offboarding: Arc<offboarding::OffboardingService>,
    pub analytics: Arc<analytics::AnalyticsService>,
    pub isolation: Arc<isolation::IsolationService>,
    pub backups: Arc<backup::BackupService>,
}

impl M01Services {
    pub fn new(deps: Arc<M01Deps>, metrics: Arc<TenantMetrics>, pool: sqlx::PgPool) -> Self {
        let isolation = Arc::new(isolation::IsolationService::new(deps.clone()));
        let support = Arc::new(support::SupportAccessService::new(deps.clone()));
        let offboarding = Arc::new(offboarding::OffboardingService::new(deps.clone(), support.clone()));
        let lifecycle = Arc::new(lifecycle::LifecycleService::new(deps.clone(), offboarding.clone()));
        let provisioning = Arc::new(provisioning::ProvisioningService::new(deps.clone(), isolation.clone()));
        let baselines = Arc::new(baselines::BaselineService::new(deps.clone()));
        Self {
            directory: Arc::new(directory::DirectoryService::new(deps.clone(), metrics, pool)),
            configuration: Arc::new(configuration::ConfigurationService::new(deps.clone())),
            quotas: Arc::new(quotas::QuotaService::new(deps.clone())),
            branding: Arc::new(branding::BrandingService::new(deps.clone())),
            sandboxes: Arc::new(sandbox::SandboxService::new(deps.clone(), provisioning.clone(), lifecycle.clone())),
            releases: Arc::new(releases::ReleaseService::new(deps.clone())),
            keys: Arc::new(keys::KeyService::new(deps.clone())),
            analytics: Arc::new(analytics::AnalyticsService::new(deps.clone())),
            backups: Arc::new(backup::BackupService::new(deps.clone(), baselines.clone())),
            baselines,
            provisioning,
            lifecycle,
            support,
            offboarding,
            isolation,
            deps,
        }
    }
}
