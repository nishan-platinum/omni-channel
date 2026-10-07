//! In-process consumers of M01 domain events (outbox dispatcher). Each consumer is idempotent:
//! the dispatcher records `(consumer, event_id)` and never re-delivers a processed event.

use std::sync::Arc;

use async_trait::async_trait;

use crate::platform::events::{EventEnvelope, EventHandler};

use super::super::domain::TenantId;
use super::context::M01Deps;
use super::ports::Notification;

/// Translates events into the spec notifications NT-001/NT-002/NT-003 (+NT-019 purge, isolation
/// alerts) through the NotificationPort (M25 reference adapter).
pub struct NotificationConsumer {
    pub deps: Arc<M01Deps>,
}

#[async_trait]
impl EventHandler for NotificationConsumer {
    fn name(&self) -> &'static str {
        "m01.notifications"
    }

    fn handles(&self, t: &str) -> bool {
        matches!(
            t,
            "tenant.suspended" | "tenant.quota_warning" | "tenant.quota_exhausted" | "tenant.purged" | "tenant.isolation_check_failed"
        )
    }

    async fn handle(&self, ev: &EventEnvelope) -> anyhow::Result<()> {
        let Some(tid) = ev.tenant_id.map(TenantId) else {
            return Ok(());
        };
        let data = &ev.payload["data"];
        let (id, template, priority, subject, body): (&str, &str, &str, String, String) = match ev.event_type.as_str() {
            "tenant.suspended" => (
                "NT-001",
                "NT-TENANT-SUSP",
                "High",
                "Your tenant has been suspended".into(),
                format!("Reason: {}. Logins and API calls are blocked; your data is retained.", data["reason"].as_str().unwrap_or("-")),
            ),
            "tenant.quota_warning" => (
                "NT-002",
                "NT-QUOTA-WARN",
                "Normal",
                format!("Quota warning: {}", data["metric"].as_str().unwrap_or("")),
                format!(
                    "Usage {} of {} reached the warning threshold. Upgrade the plan or request a limit increase.",
                    data["usage"], data["limit"]
                ),
            ),
            "tenant.quota_exhausted" => (
                "NT-003",
                "NT-QUOTA-FULL",
                "High",
                format!("Quota exhausted: {}", data["metric"].as_str().unwrap_or("")),
                format!("Usage {} of {} — further actions are blocked until reset or upgrade.", data["usage"], data["limit"]),
            ),
            "tenant.purged" => (
                "NT-019",
                "NT-PURGE",
                "Normal",
                "Tenant data purged".into(),
                "Your tenant data has been irreversibly purged after the retention window. A destruction certificate was issued.".into(),
            ),
            "tenant.isolation_check_failed" => (
                "NT-M01-ISOLATION",
                "NT-SEC-ALERT",
                "High",
                "SECURITY: tenant isolation smoke test failed".into(),
                format!("Tenant {tid} is held in draft. Failed checks: {}", data["failures"]),
            ),
            _ => return Ok(()),
        };
        let recipients: Vec<String> = match ev.event_type.as_str() {
            "tenant.isolation_check_failed" => vec![self.deps.settings.ops_alert_email.clone()],
            "tenant.purged" => data["notify"].as_str().map(|s| vec![s.to_string()]).unwrap_or_default(),
            _ => {
                let mut r: Vec<String> = self
                    .deps
                    .identity
                    .tenant_admins(tid)
                    .await
                    .map_err(|e| anyhow::anyhow!(e.message))?
                    .into_iter()
                    .filter(|u| u.status != "disabled")
                    .map(|u| u.email)
                    .collect();
                if r.is_empty() {
                    if let Ok(Some(t)) = self.deps.tenants.get(&crate::platform::db::AccessScope::System, tid).await {
                        r.push(t.primary_admin_email);
                    }
                }
                r
            }
        };
        for recipient in recipients {
            self.deps
                .notifications
                .send(Notification {
                    tenant_id: Some(tid),
                    notification_id: id.into(),
                    template_key: template.into(),
                    recipient,
                    channels: "Email + In-app".into(),
                    priority: priority.into(),
                    subject: subject.clone(),
                    body: body.clone(),
                })
                .await
                .map_err(|e| anyhow::anyhow!(e.message))?;
        }
        Ok(())
    }
}

/// REFERENCE consumer standing in for M23 (provisioning) and M25 subscribers of `tenant.created`
/// (interlink M01-F01 → M23/M25). It only logs; real modules would reserve numbers/channels.
pub struct DownstreamCreatedConsumer;

#[async_trait]
impl EventHandler for DownstreamCreatedConsumer {
    fn name(&self) -> &'static str {
        "m23.reference.tenant_created"
    }
    fn handles(&self, t: &str) -> bool {
        t == "tenant.created"
    }
    async fn handle(&self, ev: &EventEnvelope) -> anyhow::Result<()> {
        tracing::info!(event_id = %ev.id, tenant_id = ?ev.tenant_id, "REFERENCE M23 consumer: tenant.created received (no numbers reserved in M01 prototype)");
        Ok(())
    }
}
