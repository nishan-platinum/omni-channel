//! Per-tenant configuration & feature flags (M01-F04; OCC-M01-R004, R015, R018, R020, R021;
//! BR-M01-003; FD-008).

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use serde_json::{json, Value};

use crate::platform::errors::{AppError, AppResult};

use super::super::domain::config::{self as cfgdom, ConfigActor, ConfigKeyDef, Editor};
use super::super::domain::events::TenantEvent;
use super::super::domain::features::{self, is_channel, FeatureDef};
use super::super::domain::plan::Plan;
use super::super::domain::quota::QuotaMetric;
use super::super::domain::{Tenant, TenantId};
use super::context::{Access, Actor, M01Deps};
use super::ports::ChangeSet;

#[derive(Debug, Clone, Serialize)]
pub struct ConfigEntry {
    pub key: &'static str,
    pub label: &'static str,
    pub help: &'static str,
    pub value: Value,
    /// Display helpers for the server-rendered form.
    pub display: String,
    pub checked: bool,
    pub kind: &'static str,
    pub options: Vec<&'static str>,
    pub editable: bool,
    pub enforced_by: &'static str,
    #[serde(skip)]
    pub def: &'static ConfigKeyDef,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureEntry {
    pub key: &'static str,
    pub dom_id: String,
    pub label: &'static str,
    pub category: &'static str,
    pub owner: &'static str,
    pub entitled: bool,
    pub enabled: bool,
    pub requirement: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TenantSettingsView {
    pub tenant: Tenant,
    pub plan: Plan,
    /// Effective configuration (defaults merged with stored values) — API `config`.
    pub config: BTreeMap<String, Value>,
    /// Effective feature flags — API `feature_flags`.
    pub feature_flags: BTreeMap<String, bool>,
    pub entries: Vec<ConfigEntry>,
    pub features: Vec<FeatureEntry>,
}

pub struct ConfigurationService {
    deps: Arc<M01Deps>,
}

impl ConfigurationService {
    pub fn new(deps: Arc<M01Deps>) -> Self {
        Self { deps }
    }

    pub async fn get(&self, actor: &Actor, id: TenantId) -> AppResult<TenantSettingsView> {
        let scope = self.deps.authorize(actor, id, Access::Read, true).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        let plan = self.deps.plans.get(tenant.plan_id).await?.ok_or_else(|| AppError::not_found("Plan does not exist"))?;
        let stored = self.deps.configs.load(&scope, id).await?;
        Ok(build_view(actor, tenant, plan, stored.config, stored.flags))
    }

    /// `PATCH /v1/tenants/{id}/config` — config and/or feature flags, validated as one unit.
    pub async fn update(
        &self,
        actor: &Actor,
        id: TenantId,
        config: BTreeMap<String, Value>,
        flags: BTreeMap<String, bool>,
    ) -> AppResult<TenantSettingsView> {
        let scope = self.deps.authorize(actor, id, Access::Write, false).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        tenant.ensure_config_writable()?;
        let plan = self.deps.plans.get(tenant.plan_id).await?.ok_or_else(|| AppError::not_found("Plan does not exist"))?;
        let stored = self.deps.configs.load(&scope, id).await?;
        let mut current_cfg = cfgdom::defaults();
        current_cfg.extend(stored.config.clone());

        let who = if actor.is_tenant_admin() { ConfigActor::TenantAdmin } else { ConfigActor::SuperAdmin };
        let cfg_changes = cfgdom::validate_changes(&config, &current_cfg, who, plan.tier)?;
        let mut merged_cfg = current_cfg.clone();
        merged_cfg.extend(cfg_changes.clone());

        // Feature flags: entitlement (BR-M01-003 → 403) and dependencies (F04 step 3).
        let mut flag_changes = BTreeMap::new();
        for (key, enable) in &flags {
            features::validate_feature_change(key, *enable, &plan.entitlements, &merged_cfg)?;
            if stored.flags.get(key) != Some(enable) {
                flag_changes.insert(key.clone(), *enable);
            }
        }
        // Disabling a prerequisite must not leave a dependent feature enabled.
        let mut merged_flags = stored.flags.clone();
        merged_flags.extend(flag_changes.clone());
        for def in features::FEATURES {
            if let (Some((cfg_key, msg)), Some(true)) = (def.requires_config, merged_flags.get(def.key)) {
                if merged_cfg.get(cfg_key) != Some(&Value::Bool(true)) {
                    return Err(AppError::validation(format!("feature_flags.{}", def.key), msg));
                }
            }
        }
        // Channel count is a static quota (R005 "channels").
        let channels = merged_flags.iter().filter(|(k, v)| **v && is_channel(k)).count() as i64;
        let channel_limit = plan.quota(QuotaMetric::Channels);
        let quotas = self.deps.quotas.list(&scope, id).await?;
        let limit = quotas.iter().find(|q| q.metric == QuotaMetric::Channels).map(|q| q.limit).unwrap_or(channel_limit);
        let enabling_channel = flag_changes.iter().any(|(k, v)| *v && is_channel(k));
        if enabling_channel && channels > limit {
            return Err(AppError::quota_exceeded("Plan quota reached; upgrade required", 3600));
        }

        let cfg_changes: BTreeMap<String, Value> =
            cfg_changes.into_iter().filter(|(k, v)| current_cfg.get(k) != Some(v) || !stored.config.contains_key(k)).collect();
        if cfg_changes.is_empty() && flag_changes.is_empty() {
            return Ok(build_view(actor, tenant, plan, stored.config, stored.flags));
        }

        let mut changes = ChangeSet::new();
        if !cfg_changes.is_empty() {
            let before: BTreeMap<_, _> = cfg_changes.keys().map(|k| (k.clone(), current_cfg.get(k).cloned())).collect();
            changes.push_audit(actor.audit(Some(id), "tenant_config", Some(id.to_string()), "tenant.config_changed").tap(|a| {
                a.before = Some(json!(before));
                a.after = Some(json!(cfg_changes));
            }));
            changes.push_event(actor.event(id, &TenantEvent::ConfigChanged { keys: cfg_changes.keys().cloned().collect() }));
        }
        for (k, v) in &flag_changes {
            changes.push_audit(actor.audit(Some(id), "tenant_feature_flag", Some(k.clone()), "tenant.feature_changed").tap(|a| {
                a.before = Some(json!({ "enabled": stored.flags.get(k) }));
                a.after = Some(json!({ "enabled": v }));
            }));
            changes.push_event(actor.event(id, &TenantEvent::FeatureChanged { feature: k.clone(), enabled: *v }));
        }
        self.deps.configs.apply(&scope, id, &cfg_changes, &flag_changes, actor.user_id, changes).await?;
        self.deps.quotas.set_static_usage(&scope, id, QuotaMetric::Channels, channels).await?;
        self.get_unaudited(actor, id).await
    }

    async fn get_unaudited(&self, actor: &Actor, id: TenantId) -> AppResult<TenantSettingsView> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        let tenant = self.deps.load_tenant(&scope, id).await?;
        let plan = self.deps.plans.get(tenant.plan_id).await?.ok_or_else(|| AppError::not_found("Plan does not exist"))?;
        let stored = self.deps.configs.load(&scope, id).await?;
        Ok(build_view(actor, tenant, plan, stored.config, stored.flags))
    }

    /// Runtime feature check for other modules (F04 step 2: "a feature flag is checked at runtime").
    pub async fn is_enabled(&self, actor: &Actor, id: TenantId, feature: &str) -> AppResult<bool> {
        let scope = self.deps.authorize(actor, id, Access::Read, false).await?;
        Ok(self.deps.configs.load(&scope, id).await?.flags.get(feature).copied().unwrap_or(false))
    }
}

fn display_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(a) => a.iter().map(|x| x.as_str().map(str::to_string).unwrap_or_else(|| x.to_string())).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

trait Tap: Sized {
    fn tap(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }
}
impl<T> Tap for T {}

fn build_view(
    actor: &Actor,
    tenant: Tenant,
    plan: Plan,
    stored_cfg: BTreeMap<String, Value>,
    stored_flags: BTreeMap<String, bool>,
) -> TenantSettingsView {
    let mut config = cfgdom::defaults();
    config.extend(stored_cfg);
    let writable = tenant.status.allows_config_writes();
    let entries = cfgdom::CONFIG_KEYS
        .iter()
        .map(|d| {
            let value = config.get(d.key).cloned().unwrap_or_else(|| (d.default)());
            ConfigEntry {
                key: d.key,
                label: d.label,
                help: d.help,
                display: display_value(&value),
                checked: value == Value::Bool(true),
                kind: match d.ty {
                    cfgdom::ConfigType::Bool => "bool",
                    cfgdom::ConfigType::Int { .. } => "int",
                    cfgdom::ConfigType::Enum(_) => "enum",
                    _ => "list",
                },
                options: match d.ty {
                    cfgdom::ConfigType::Enum(a) => a.to_vec(),
                    _ => Vec::new(),
                },
                value,
                editable: writable
                    && (actor.is_super_admin() || d.editor == Editor::TenantAdmin)
                    && d.only_tier.is_none_or(|t| t == plan.tier),
                enforced_by: d.enforced_by,
                def: d,
            }
        })
        .collect();
    let mut feature_flags = BTreeMap::new();
    let features = features::FEATURES
        .iter()
        .map(|f: &FeatureDef| {
            let enabled = stored_flags.get(f.key).copied().unwrap_or(false);
            feature_flags.insert(f.key.to_string(), enabled);
            FeatureEntry {
                key: f.key,
                dom_id: format!("feature-{}", f.key.replace('.', "-")),
                label: f.label,
                category: f.category.label(),
                owner: f.owner,
                entitled: plan.entitles(f.key),
                enabled,
                requirement: f.requires_config.map(|(_, m)| m),
            }
        })
        .collect();
    TenantSettingsView { tenant, plan, config, feature_flags, entries, features }
}
