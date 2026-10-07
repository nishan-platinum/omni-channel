//! Break-glass support access (OCC-M01-R024, ADR-0009).

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::errors::{DomainError, Violations};
use super::ids::TenantId;
use super::storage::Tier;

pub const DEFAULT_DURATION_MINUTES: i32 = 240;
pub const MIN_DURATION_MINUTES: i32 = 15;
pub const MAX_DURATION_MINUTES: i32 = 480;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GrantStatus {
    Requested,
    Approved,
    Rejected,
    Revoked,
}

impl GrantStatus {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "requested" => Ok(Self::Requested),
            "approved" => Ok(Self::Approved),
            "rejected" => Ok(Self::Rejected),
            "revoked" => Ok(Self::Revoked),
            _ => Err(DomainError::field("status", "Unknown grant status")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Revoked => "revoked",
        }
    }
}

/// Effective state including time (an approved grant past its window is expired).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GrantState {
    Pending,
    Active,
    Expired,
    Rejected,
    Revoked,
}

impl GrantState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending approval",
            Self::Active => "active",
            Self::Expired => "expired",
            Self::Rejected => "rejected",
            Self::Revoked => "revoked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SupportGrant {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub requested_by: Uuid,
    pub reason: String,
    pub incident_ref: Option<String>,
    pub duration_minutes: i32,
    pub named_approver_id: Option<Uuid>,
    pub status: GrantStatus,
    pub decided_by: Option<Uuid>,
    pub decided_at: Option<DateTime<Utc>>,
    pub decision_note: Option<String>,
    pub starts_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub use_count: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct GrantRequest {
    pub reason: String,
    pub incident_ref: Option<String>,
    pub duration_minutes: Option<i32>,
    pub named_approver_id: Option<Uuid>,
}

/// Validates a request. Regulated tier: incident reference AND named approver are mandatory.
pub fn validate_request(req: &GrantRequest, tier: Tier) -> Result<(String, Option<String>, i32), DomainError> {
    let mut v = Violations::default();
    let reason = req.reason.trim().to_string();
    if reason.chars().count() < 5 || reason.chars().count() > 500 {
        v.push("reason", "Reason is required (5-500 characters)");
    }
    let incident = req.incident_ref.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string);
    if incident.as_ref().is_some_and(|i| i.len() > 100) {
        v.push("incident_ref", "Incident reference must be at most 100 characters");
    }
    let duration = req.duration_minutes.unwrap_or(DEFAULT_DURATION_MINUTES);
    if !(MIN_DURATION_MINUTES..=MAX_DURATION_MINUTES).contains(&duration) {
        v.push("duration_minutes", format!("Duration must be between {MIN_DURATION_MINUTES} and {MAX_DURATION_MINUTES} minutes"));
    }
    if tier == Tier::Regulated {
        if incident.is_none() {
            v.push("incident_ref", "Regulated tenants require an incident reference per grant");
        }
        if req.named_approver_id.is_none() {
            v.push("named_approver_id", "Regulated tenants require a named approver");
        }
    }
    v.into_result()?;
    Ok((reason, incident, duration))
}

impl SupportGrant {
    pub fn state_at(&self, now: DateTime<Utc>) -> GrantState {
        match self.status {
            GrantStatus::Requested => GrantState::Pending,
            GrantStatus::Rejected => GrantState::Rejected,
            GrantStatus::Revoked => GrantState::Revoked,
            GrantStatus::Approved => match (self.starts_at, self.expires_at) {
                (Some(s), Some(e)) if now >= s && now < e => GrantState::Active,
                _ => GrantState::Expired,
            },
        }
    }

    /// Approval by a Tenant Admin of the same tenant. For grants with a named approver only that
    /// user may approve. The window starts at approval.
    pub fn approve(&self, approver_id: Uuid, now: DateTime<Utc>) -> Result<(DateTime<Utc>, DateTime<Utc>), DomainError> {
        if self.status != GrantStatus::Requested {
            return Err(DomainError::conflict("Only pending grants can be approved"));
        }
        if let Some(named) = self.named_approver_id {
            if named != approver_id {
                return Err(DomainError::forbidden("Only the named approver may approve this grant"));
            }
        }
        if approver_id == self.requested_by {
            return Err(DomainError::forbidden("A grant cannot be self-approved"));
        }
        Ok((now, now + Duration::minutes(i64::from(self.duration_minutes))))
    }

    pub fn reject(&self) -> Result<(), DomainError> {
        if self.status != GrantStatus::Requested {
            return Err(DomainError::conflict("Only pending grants can be rejected"));
        }
        Ok(())
    }

    pub fn revoke(&self, now: DateTime<Utc>) -> Result<(), DomainError> {
        match self.state_at(now) {
            GrantState::Pending | GrantState::Active => Ok(()),
            _ => Err(DomainError::conflict("Grant is no longer active")),
        }
    }

    pub fn ensure_usable(&self, now: DateTime<Utc>) -> Result<(), DomainError> {
        if self.state_at(now) == GrantState::Active {
            Ok(())
        } else {
            Err(DomainError::forbidden("Support access requires an active, tenant-approved grant"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(named: Option<Uuid>) -> SupportGrant {
        SupportGrant {
            id: Uuid::now_v7(),
            tenant_id: TenantId::new(),
            requested_by: Uuid::now_v7(),
            reason: "Investigate incident".into(),
            incident_ref: Some("INC-1".into()),
            duration_minutes: 240,
            named_approver_id: named,
            status: GrantStatus::Requested,
            decided_by: None,
            decided_at: None,
            decision_note: None,
            starts_at: None,
            expires_at: None,
            revoked_at: None,
            use_count: 0,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn default_window_is_four_hours() {
        let g = grant(None);
        let now = Utc::now();
        let (s, e) = g.approve(Uuid::now_v7(), now).unwrap();
        assert_eq!(s, now);
        assert_eq!(e - s, Duration::hours(4));
    }

    #[test]
    fn time_box_expires() {
        let mut g = grant(None);
        let now = Utc::now();
        let (s, e) = g.approve(Uuid::now_v7(), now).unwrap();
        g.status = GrantStatus::Approved;
        g.starts_at = Some(s);
        g.expires_at = Some(e);
        assert_eq!(g.state_at(now + Duration::hours(1)), GrantState::Active);
        assert!(g.ensure_usable(now + Duration::hours(1)).is_ok());
        assert_eq!(g.state_at(now + Duration::hours(4)), GrantState::Expired);
        assert!(g.ensure_usable(now + Duration::hours(5)).is_err());
    }

    #[test]
    fn pending_grant_is_not_usable() {
        assert!(grant(None).ensure_usable(Utc::now()).is_err());
    }

    #[test]
    fn named_approver_enforced() {
        let named = Uuid::now_v7();
        let g = grant(Some(named));
        assert!(g.approve(Uuid::now_v7(), Utc::now()).is_err());
        assert!(g.approve(named, Utc::now()).is_ok());
    }

    #[test]
    fn regulated_requires_incident_and_named_approver() {
        let req =
            GrantRequest { reason: "Investigate incident".into(), incident_ref: None, duration_minutes: None, named_approver_id: None };
        assert!(validate_request(&req, Tier::Standard).is_ok());
        match validate_request(&req, Tier::Regulated) {
            Err(DomainError::Validation(v)) => assert_eq!(v.len(), 2),
            other => panic!("{other:?}"),
        }
        let bad = GrantRequest { duration_minutes: Some(600), ..req };
        assert!(validate_request(&bad, Tier::Standard).is_err());
    }
}
