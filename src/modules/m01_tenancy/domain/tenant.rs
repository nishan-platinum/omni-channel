//! The tenant aggregate and its lifecycle state machine (M01-F03, spec §10.4 / §62, ADR-0002).
//! This module is the ONLY place that decides whether a status transition is legal.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::errors::DomainError;
use super::ids::{TenantCode, TenantId};
use super::storage::{IsolationMode, Region, StorageStrategy};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantStatus {
    Draft,
    Active,
    Suspended,
    Grace,
    Terminated,
    Purged,
}

pub const ALL_STATUSES: [TenantStatus; 6] = [
    TenantStatus::Draft,
    TenantStatus::Active,
    TenantStatus::Suspended,
    TenantStatus::Grace,
    TenantStatus::Terminated,
    TenantStatus::Purged,
];

/// The register's allowed transitions, verbatim (State Machine Register §62).
pub const ALLOWED_TRANSITIONS: [(TenantStatus, TenantStatus); 7] = [
    (TenantStatus::Draft, TenantStatus::Active),
    (TenantStatus::Active, TenantStatus::Suspended),
    (TenantStatus::Suspended, TenantStatus::Active),
    (TenantStatus::Active, TenantStatus::Grace),
    (TenantStatus::Grace, TenantStatus::Terminated),
    (TenantStatus::Grace, TenantStatus::Active),
    (TenantStatus::Terminated, TenantStatus::Purged),
];

pub const ILLEGAL_TRANSITION: &str = "Illegal status transition";

impl TenantStatus {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s.trim() {
            "draft" => Ok(Self::Draft),
            "active" => Ok(Self::Active),
            "suspended" => Ok(Self::Suspended),
            "grace" => Ok(Self::Grace),
            "terminated" => Ok(Self::Terminated),
            "purged" => Ok(Self::Purged),
            _ => Err(DomainError::field("status", "Status must be one of: draft, active, suspended, grace, terminated, purged")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Suspended => "suspended",
            Self::Grace => "grace",
            Self::Terminated => "terminated",
            Self::Purged => "purged",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Draft => "Draft",
            Self::Active => "Active",
            Self::Suspended => "Suspended",
            Self::Grace => "Grace (read-only)",
            Self::Terminated => "Terminated",
            Self::Purged => "Purged",
        }
    }

    /// Conceptual R014 vocabulary (ADR-0002) for display.
    pub fn r014_term(self, provisioning: ProvisioningStatus) -> &'static str {
        match (self, provisioning) {
            (Self::Draft, ProvisioningStatus::Completed) => "Review",
            (Self::Draft, _) => "Provisioning",
            (Self::Active, _) => "Active",
            (Self::Suspended, _) => "Suspended",
            (Self::Grace, _) => "Offboarding",
            (Self::Terminated, _) => "Archived",
            (Self::Purged, _) => "Purged",
        }
    }

    pub fn can_transition_to(self, to: TenantStatus) -> bool {
        ALLOWED_TRANSITIONS.contains(&(self, to))
    }

    /// Targets reachable from this status (UI shows only permitted transitions, spec §10.5).
    pub fn next_statuses(self) -> Vec<TenantStatus> {
        ALLOWED_TRANSITIONS.iter().filter(|(f, _)| *f == self).map(|(_, t)| *t).collect()
    }

    /// Whether tenant users may log in / call the API (BR-M01-002). Grace is read-only access.
    pub fn allows_access(self) -> bool {
        matches!(self, Self::Active | Self::Grace)
    }

    /// Whether tenant configuration/branding may be changed. Grace is read-only (spec F03 step 3);
    /// terminated/purged tenants are frozen.
    pub fn allows_config_writes(self) -> bool {
        matches!(self, Self::Draft | Self::Active | Self::Suspended)
    }

    /// Destructive transitions need a confirmation in the UI (STD-005, spec §10.5).
    pub fn is_destructive_target(self) -> bool {
        matches!(self, Self::Suspended | Self::Grace | Self::Terminated | Self::Purged)
    }

    pub fn requires_reason(self) -> bool {
        matches!(self, Self::Suspended | Self::Grace)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisioningStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl ProvisioningStatus {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "pending" => Ok(Self::Pending),
            "in_progress" => Ok(Self::InProgress),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(DomainError::field("provisioning_status", "Unknown provisioning status")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationCheckStatus {
    NotRun,
    Passed,
    Failed,
}

impl IsolationCheckStatus {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "not_run" => Ok(Self::NotRun),
            "passed" => Ok(Self::Passed),
            "failed" => Ok(Self::Failed),
            _ => Err(DomainError::field("isolation_check_status", "Unknown isolation status")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotRun => "not_run",
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Tenant {
    pub id: TenantId,
    pub code: TenantCode,
    pub name: String,
    pub legal_name: Option<String>,
    pub region: Region,
    pub plan_id: Uuid,
    pub status: TenantStatus,
    pub parent_tenant_id: Option<TenantId>,
    pub primary_admin_email: String,
    pub storage_strategy: StorageStrategy,
    pub isolation_mode: IsolationMode,
    pub is_sandbox: bool,
    pub sandbox_of_tenant_id: Option<TenantId>,
    pub template_code: Option<String>,
    pub inheritance_flags: Value,
    pub provisioning_status: ProvisioningStatus,
    pub isolation_check_status: IsolationCheckStatus,
    pub isolation_checked_at: Option<DateTime<Utc>>,
    pub suspended_reason: Option<String>,
    pub status_reason: Option<String>,
    pub activated_at: Option<DateTime<Utc>>,
    pub grace_until: Option<DateTime<Utc>>,
    pub terminated_at: Option<DateTime<Utc>>,
    pub purge_after: Option<DateTime<Utc>>,
    pub purged_at: Option<DateTime<Utc>>,
    pub legal_hold: bool,
    pub platform_version: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i32,
}

/// Lifecycle policy parameters (configurable per deployment).
#[derive(Debug, Clone, Copy)]
pub struct LifecyclePolicy {
    pub grace_period: Duration,
    pub retention: Duration,
}

/// Side effects the application layer must carry out after a successful transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionEffect {
    /// Opens login/API (draft→active, recovery, reinstate).
    OpenAccess,
    /// Blocks new sessions; existing sessions revoked (policy "revoke") or left to drain.
    BlockNewSessions,
    /// NT-001 notification to Tenant Admin.
    NotifySuspended,
    /// Read-only mode and purge timer start.
    StartGraceTimer,
    /// Offer the offboarding export (generated on entering grace and termination).
    GenerateExport,
    /// Revoke all sessions (termination).
    RevokeAllSessions,
    /// Retention countdown: purge scheduled at `purge_after`.
    SchedulePurge,
    /// Irreversible purge with destruction certificate.
    PurgeData,
}

/// The full, validated outcome of a transition. Persisted atomically with audit + event.
#[derive(Debug, Clone, PartialEq)]
pub struct TransitionPlan {
    pub from: TenantStatus,
    pub to: TenantStatus,
    pub reason: Option<String>,
    pub at: DateTime<Utc>,
    pub set_activated_at: Option<DateTime<Utc>>,
    pub suspended_reason: Option<String>,
    pub grace_until: Option<DateTime<Utc>>,
    pub terminated_at: Option<DateTime<Utc>>,
    pub purge_after: Option<DateTime<Utc>>,
    pub purged_at: Option<DateTime<Utc>>,
    pub effects: Vec<TransitionEffect>,
    /// Domain event name for the transition.
    pub event_type: &'static str,
}

fn normalized_reason(reason: Option<&str>) -> Option<String> {
    reason.map(str::trim).filter(|r| !r.is_empty()).map(|r| r.chars().take(500).collect())
}

impl Tenant {
    /// Decides a transition. Pure: no I/O; `now` is injected.
    pub fn plan_transition(
        &self,
        to: TenantStatus,
        reason: Option<&str>,
        now: DateTime<Utc>,
        policy: LifecyclePolicy,
    ) -> Result<TransitionPlan, DomainError> {
        let from = self.status;
        if !from.can_transition_to(to) {
            return Err(DomainError::conflict(ILLEGAL_TRANSITION));
        }
        let reason = normalized_reason(reason);
        let mut plan = TransitionPlan {
            from,
            to,
            reason: reason.clone(),
            at: now,
            set_activated_at: None,
            suspended_reason: None,
            grace_until: self.grace_until,
            terminated_at: self.terminated_at,
            purge_after: self.purge_after,
            purged_at: None,
            effects: Vec::new(),
            event_type: "tenant.status_changed",
        };
        match (from, to) {
            (TenantStatus::Draft, TenantStatus::Active) => {
                if self.provisioning_status != ProvisioningStatus::Completed {
                    return Err(DomainError::conflict("Tenant cannot be activated before provisioning has completed"));
                }
                // UJ-19 E1: cannot go Active until the isolation smoke test is green.
                if self.isolation_check_status != IsolationCheckStatus::Passed {
                    return Err(DomainError::conflict("Tenant cannot be activated until the isolation smoke test has passed"));
                }
                if self.activated_at.is_none() {
                    plan.set_activated_at = Some(now);
                }
                plan.effects.push(TransitionEffect::OpenAccess);
                plan.event_type = "tenant.activated";
            }
            (TenantStatus::Active, TenantStatus::Suspended) => {
                let Some(r) = reason else {
                    return Err(DomainError::field("reason", "Suspension reason is required"));
                };
                plan.suspended_reason = Some(r);
                plan.effects.push(TransitionEffect::BlockNewSessions);
                plan.effects.push(TransitionEffect::NotifySuspended);
                plan.event_type = "tenant.suspended";
            }
            (TenantStatus::Suspended, TenantStatus::Active) => {
                plan.effects.push(TransitionEffect::OpenAccess);
                plan.event_type = "tenant.reinstated";
            }
            (TenantStatus::Active, TenantStatus::Grace) => {
                if reason.is_none() {
                    return Err(DomainError::field(
                        "reason",
                        "A reason (offboarding or non-payment) is required to start the grace period",
                    ));
                }
                plan.grace_until = Some(now + policy.grace_period);
                plan.effects.push(TransitionEffect::StartGraceTimer);
                plan.effects.push(TransitionEffect::GenerateExport);
                plan.event_type = "tenant.grace_started";
            }
            (TenantStatus::Grace, TenantStatus::Active) => {
                if let Some(until) = self.grace_until {
                    if now >= until {
                        return Err(DomainError::conflict("Grace period has expired; the tenant can no longer be recovered"));
                    }
                }
                plan.grace_until = None;
                plan.effects.push(TransitionEffect::OpenAccess);
                plan.event_type = "tenant.recovered";
            }
            (TenantStatus::Grace, TenantStatus::Terminated) => {
                plan.terminated_at = Some(now);
                plan.purge_after = Some(now + policy.retention);
                plan.effects.push(TransitionEffect::RevokeAllSessions);
                plan.effects.push(TransitionEffect::GenerateExport);
                plan.effects.push(TransitionEffect::SchedulePurge);
                plan.event_type = "tenant.terminated";
            }
            (TenantStatus::Terminated, TenantStatus::Purged) => {
                if self.legal_hold {
                    return Err(DomainError::conflict("Tenant is under legal hold; purge is blocked"));
                }
                match self.purge_after {
                    Some(after) if now >= after => {}
                    _ => return Err(DomainError::conflict("Retention window has not elapsed; purge is not yet permitted")),
                }
                plan.purged_at = Some(now);
                plan.effects.push(TransitionEffect::PurgeData);
                plan.event_type = "tenant.purged";
            }
            _ => return Err(DomainError::conflict(ILLEGAL_TRANSITION)),
        }
        Ok(plan)
    }

    pub fn ensure_config_writable(&self) -> Result<(), DomainError> {
        if self.status.allows_config_writes() {
            Ok(())
        } else {
            Err(DomainError::conflict(format!("Tenant is {}; configuration is read-only", self.status.as_str())))
        }
    }

    pub fn display_status(&self) -> &'static str {
        self.status.r014_term(self.provisioning_status)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn sample_tenant(status: TenantStatus) -> Tenant {
        let now = Utc::now();
        Tenant {
            id: TenantId::new(),
            code: TenantCode::parse("acme-retail").unwrap(),
            name: "Acme Retail".into(),
            legal_name: None,
            region: Region::MyCentral,
            plan_id: Uuid::now_v7(),
            status,
            parent_tenant_id: None,
            primary_admin_email: "admin@acme.example".into(),
            storage_strategy: StorageStrategy::SharedRowLevel,
            isolation_mode: IsolationMode::RowLevel,
            is_sandbox: false,
            sandbox_of_tenant_id: None,
            template_code: None,
            inheritance_flags: Value::Object(Default::default()),
            provisioning_status: ProvisioningStatus::Completed,
            isolation_check_status: IsolationCheckStatus::Passed,
            isolation_checked_at: Some(now),
            suspended_reason: None,
            status_reason: None,
            activated_at: None,
            grace_until: None,
            terminated_at: None,
            purge_after: None,
            purged_at: None,
            legal_hold: false,
            platform_version: "1.0.0".into(),
            created_at: now,
            updated_at: now,
            version: 1,
        }
    }

    fn policy() -> LifecyclePolicy {
        LifecyclePolicy { grace_period: Duration::days(30), retention: Duration::days(90) }
    }

    #[test]
    fn register_transitions_are_exactly_the_allowed_set() {
        let mut allowed = 0;
        for from in ALL_STATUSES {
            for to in ALL_STATUSES {
                if from.can_transition_to(to) {
                    allowed += 1;
                }
            }
        }
        assert_eq!(allowed, 7);
        assert!(!TenantStatus::Suspended.can_transition_to(TenantStatus::Grace));
        assert!(!TenantStatus::Draft.can_transition_to(TenantStatus::Terminated));
        assert!(!TenantStatus::Purged.can_transition_to(TenantStatus::Active));
        assert!(!TenantStatus::Terminated.can_transition_to(TenantStatus::Active));
    }

    #[test]
    fn illegal_transition_is_conflict() {
        let t = sample_tenant(TenantStatus::Draft);
        let err = t.plan_transition(TenantStatus::Suspended, Some("x"), Utc::now(), policy()).unwrap_err();
        assert_eq!(err, DomainError::Conflict(ILLEGAL_TRANSITION.into()));
    }

    #[test]
    fn activation_requires_passed_isolation_check() {
        let mut t = sample_tenant(TenantStatus::Draft);
        t.isolation_check_status = IsolationCheckStatus::Failed;
        assert!(matches!(t.plan_transition(TenantStatus::Active, None, Utc::now(), policy()), Err(DomainError::Conflict(_))));
        t.isolation_check_status = IsolationCheckStatus::Passed;
        t.provisioning_status = ProvisioningStatus::Failed;
        assert!(t.plan_transition(TenantStatus::Active, None, Utc::now(), policy()).is_err());
        t.provisioning_status = ProvisioningStatus::Completed;
        let now = Utc::now();
        let plan = t.plan_transition(TenantStatus::Active, None, now, policy()).unwrap();
        assert_eq!(plan.set_activated_at, Some(now));
        assert_eq!(plan.event_type, "tenant.activated");
    }

    #[test]
    fn activated_at_is_set_only_once() {
        let mut t = sample_tenant(TenantStatus::Suspended);
        let first = Utc::now() - Duration::days(3);
        t.activated_at = Some(first);
        let plan = t.plan_transition(TenantStatus::Active, None, Utc::now(), policy()).unwrap();
        assert_eq!(plan.set_activated_at, None);
        assert_eq!(plan.event_type, "tenant.reinstated");
    }

    #[test]
    fn suspension_requires_reason() {
        let t = sample_tenant(TenantStatus::Active);
        assert!(matches!(t.plan_transition(TenantStatus::Suspended, Some("   "), Utc::now(), policy()), Err(DomainError::Validation(_))));
        let plan = t.plan_transition(TenantStatus::Suspended, Some("Non-payment INV-1"), Utc::now(), policy()).unwrap();
        assert_eq!(plan.suspended_reason.as_deref(), Some("Non-payment INV-1"));
        assert!(plan.effects.contains(&TransitionEffect::NotifySuspended));
    }

    #[test]
    fn grace_sets_timer_and_recovery_only_before_expiry() {
        let t = sample_tenant(TenantStatus::Active);
        let now = Utc::now();
        let plan = t.plan_transition(TenantStatus::Grace, Some("offboarding"), now, policy()).unwrap();
        assert_eq!(plan.grace_until, Some(now + Duration::days(30)));
        assert!(plan.effects.contains(&TransitionEffect::GenerateExport));

        let mut g = sample_tenant(TenantStatus::Grace);
        g.grace_until = Some(now + Duration::hours(1));
        assert!(g.plan_transition(TenantStatus::Active, None, now, policy()).is_ok());
        assert!(g.plan_transition(TenantStatus::Active, None, now + Duration::hours(2), policy()).is_err());
    }

    #[test]
    fn termination_schedules_purge_after_retention() {
        let mut g = sample_tenant(TenantStatus::Grace);
        let now = Utc::now();
        g.grace_until = Some(now);
        let plan = g.plan_transition(TenantStatus::Terminated, None, now, policy()).unwrap();
        assert_eq!(plan.terminated_at, Some(now));
        assert_eq!(plan.purge_after, Some(now + Duration::days(90)));
        assert!(plan.effects.contains(&TransitionEffect::SchedulePurge));
    }

    #[test]
    fn purge_respects_retention_and_legal_hold() {
        let now = Utc::now();
        let mut t = sample_tenant(TenantStatus::Terminated);
        t.purge_after = Some(now + Duration::days(1));
        assert!(t.plan_transition(TenantStatus::Purged, None, now, policy()).is_err());
        t.purge_after = Some(now - Duration::seconds(1));
        t.legal_hold = true;
        assert!(t.plan_transition(TenantStatus::Purged, None, now, policy()).is_err());
        t.legal_hold = false;
        let p = t.plan_transition(TenantStatus::Purged, None, now, policy()).unwrap();
        assert_eq!(p.purged_at, Some(now));
        assert_eq!(p.event_type, "tenant.purged");
    }

    #[test]
    fn access_and_write_rules() {
        assert!(TenantStatus::Active.allows_access());
        assert!(TenantStatus::Grace.allows_access());
        assert!(!TenantStatus::Suspended.allows_access());
        assert!(!TenantStatus::Draft.allows_access());
        assert!(!TenantStatus::Grace.allows_config_writes());
        assert!(sample_tenant(TenantStatus::Grace).ensure_config_writable().is_err());
    }

    #[test]
    fn r014_vocabulary_mapping() {
        assert_eq!(TenantStatus::Draft.r014_term(ProvisioningStatus::InProgress), "Provisioning");
        assert_eq!(TenantStatus::Draft.r014_term(ProvisioningStatus::Completed), "Review");
        assert_eq!(TenantStatus::Grace.r014_term(ProvisioningStatus::Completed), "Offboarding");
        assert_eq!(TenantStatus::Terminated.r014_term(ProvisioningStatus::Completed), "Archived");
    }
}
