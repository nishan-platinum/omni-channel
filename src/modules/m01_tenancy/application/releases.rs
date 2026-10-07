//! Ring-based release management, maintenance windows and in-app release notes
//! (OCC-M01-R026, P2; FR-OPS-121; M18-R010 AT "disruptive change honours tenant window").

use std::sync::Arc;

use serde_json::json;
use uuid::Uuid;

use crate::platform::db::AccessScope;
use crate::platform::errors::{AppError, AppResult};

use super::super::domain::events::TenantEvent;
use super::super::domain::release::{schedule_for, validate_version, MaintenanceWindow, Ring, RINGS};
use super::super::domain::TenantId;
use super::context::{Access, Actor, M01Deps};
use super::ports::{ChangeSet, PlatformRelease, ReleasePreference, Rollout};

pub struct ReleaseService {
    deps: Arc<M01Deps>,
}

impl ReleaseService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps }
    }

    pub async fn preference(&self, actor: &Actor, id: TenantId) -> AppResult<ReleasePreference> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        self.deps.releases.preference(&scope, id).await
    }

    /// Tenant Admins set the maintenance window; the ring is assigned by the Super Admin.
    pub async fn set_preference(
        &self,
        actor: &Actor,
        id: TenantId,
        ring: Option<&str>,
        day: i16,
        hour: i16,
        duration: i32,
    ) -> AppResult<ReleasePreference> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        self.deps.load_tenant(&scope, id).await?.ensure_config_writable()?;
        let current = self.deps.releases.preference(&scope, id).await?;
        let window = MaintenanceWindow::new(day, hour, duration)?;
        let ring = match ring {
            Some(r) => {
                let r = Ring::parse(r)?;
                if r != current.ring && !actor.is_super_admin() {
                    return Err(AppError::forbidden("Release ring is assigned by the platform operator"));
                }
                r
            }
            None => current.ring,
        };
        let pref = ReleasePreference {
            ring,
            maintenance_day: window.day,
            maintenance_start_hour_utc: window.start_hour_utc,
            maintenance_duration_min: window.duration_min,
        };
        let a = actor.tenant_audit(id, "tenant.release_preference_changed", Some(json!(current)), Some(json!(pref)));
        self.deps.releases.set_preference(&scope, id, &pref, actor.user_id, ChangeSet::new().with_audit(a)).await?;
        Ok(pref)
    }

    pub async fn releases(&self, actor: &Actor) -> AppResult<Vec<(PlatformRelease, i64, i64)>> {
        let scope = actor.platform_scope()?;
        let mut out = Vec::new();
        for r in self.deps.releases.releases(&scope).await? {
            let (scheduled, done) = self.deps.releases.rollout_counts(&scope, r.id).await?;
            out.push((r, scheduled, done));
        }
        Ok(out)
    }

    pub async fn create_release(
        &self,
        actor: &Actor,
        version: &str,
        title: &str,
        notes: &str,
        disruptive: bool,
    ) -> AppResult<PlatformRelease> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let version = validate_version(version)?;
        let title = title.trim();
        if title.is_empty() || title.chars().count() > 200 {
            return Err(AppError::validation("title", "Title is required (max 200 chars)"));
        }
        let notes = notes.trim();
        if notes.is_empty() || notes.len() > 10_000 {
            return Err(AppError::validation("notes", "Release notes are required (max 10000 chars)"));
        }
        let rel = PlatformRelease {
            id: Uuid::now_v7(),
            release_version: version,
            title: title.into(),
            notes: notes.into(),
            disruptive,
            status: "planned".into(),
            current_ring: None,
            created_at: self.deps.clock.now(),
        };
        let mut a = actor.audit(None, "platform_release", Some(rel.id.to_string()), "release.created");
        a.after = Some(json!({ "version": rel.release_version, "disruptive": disruptive }));
        self.deps.releases.create_release(&scope, &rel, actor.user_id, ChangeSet::new().with_audit(a)).await.map_err(|e| {
            if e.code == crate::platform::errors::ErrorCode::Conflict {
                AppError::conflict("Release version already exists")
            } else {
                e
            }
        })?;
        Ok(rel)
    }

    /// Advances a release to the next ring (host sandbox → early adopters → all) and schedules
    /// rollouts; disruptive releases wait for each tenant's maintenance window.
    pub async fn advance(&self, actor: &Actor, release: Uuid) -> AppResult<PlatformRelease> {
        actor.require_super_admin()?;
        let scope = actor.platform_scope()?;
        let mut rel = self.deps.releases.release(&scope, release).await?.ok_or_else(|| AppError::not_found("Release does not exist"))?;
        let next = match rel.current_ring {
            None => Ring::HostSandbox,
            Some(r) => r.next().ok_or_else(|| AppError::conflict("Release already reached all tenants"))?,
        };
        let rings: Vec<Ring> = RINGS.iter().copied().filter(|r| *r <= next).collect();
        let now = self.deps.clock.now();
        let mut scheduled = 0;
        for (tenant, pref) in self.deps.releases.tenants_in_rings(&scope, &rings).await? {
            let w = MaintenanceWindow {
                day: pref.maintenance_day,
                start_hour_utc: pref.maintenance_start_hour_utc,
                duration_min: pref.maintenance_duration_min,
            };
            let at = schedule_for(&w, rel.disruptive, now);
            if self.deps.releases.schedule_rollout(&scope, rel.id, tenant, pref.ring, at).await? {
                scheduled += 1;
            }
        }
        let mut a = actor.audit(None, "platform_release", Some(rel.id.to_string()), "release.ring_advanced");
        a.after = Some(json!({ "ring": next.as_str(), "scheduled": scheduled }));
        self.deps.releases.set_release_ring(&scope, rel.id, next, "rolling_out", ChangeSet::new().with_audit(a)).await?;
        rel.current_ring = Some(next);
        rel.status = "rolling_out".into();
        Ok(rel)
    }

    /// Scheduler: executes due rollouts through the ReleaseManagementPort.
    pub async fn process_due(&self) -> AppResult<usize> {
        let now = self.deps.clock.now();
        let actor = Actor::system("release-scheduler");
        let mut n = 0;
        for r in self.deps.releases.due_rollouts(now).await? {
            match self.deps.release_port.deploy(r.tenant_id, &r.release_version).await {
                Ok(_) => {
                    self.deps.releases.complete_rollout(&AccessScope::System, r.release_id, r.tenant_id, now).await?;
                    self.deps.tenants.set_platform_version(&AccessScope::System, r.tenant_id, &r.release_version).await?;
                    let e = actor.event(
                        r.tenant_id,
                        &TenantEvent::ReleaseScheduled {
                            release_version: r.release_version.clone(),
                            scheduled_for: r.scheduled_for.to_rfc3339(),
                        },
                    );
                    let mut a =
                        actor.tenant_audit(r.tenant_id, "tenant.release_applied", None, Some(json!({ "version": r.release_version })));
                    a.entity_type = "release_rollout".into();
                    self.deps.audit.record(&AccessScope::System, vec![a]).await?;
                    let _ = e;
                    n += 1;
                }
                Err(e) => e.log(),
            }
        }
        Ok(n)
    }

    /// In-app release notes for a tenant (releases scheduled/applied to it).
    pub async fn notes_for(&self, actor: &Actor, id: TenantId) -> AppResult<Vec<Rollout>> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        self.deps.releases.rollouts_for_tenant(&scope, id).await
    }
}
