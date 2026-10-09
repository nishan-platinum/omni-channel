//! Application configuration from environment variables (12-factor). Secrets are read here only
//! for bootstrap purposes and are never logged (`Debug` is implemented manually).

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{anyhow, Context};
use base64::Engine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppEnv {
    Development,
    Test,
    Production,
}

impl AppEnv {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "development" | "dev" => Ok(Self::Development),
            "test" => Ok(Self::Test),
            "production" | "prod" => Ok(Self::Production),
            other => Err(anyhow!("APP_ENV must be development|test|production, got {other}")),
        }
    }

    pub fn is_development(self) -> bool {
        matches!(self, Self::Development)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Test => "test",
            Self::Production => "production",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Clone)]
pub struct AppConfig {
    pub app_env: AppEnv,
    pub bind_addr: SocketAddr,
    pub public_base_url: String,
    /// Runtime connection (role crm_app, NOBYPASSRLS, not owner).
    pub database_url: String,
    /// Owner connection used for migrations and tenant schema DDL only.
    pub migration_database_url: String,
    pub db_max_connections: u32,
    pub run_migrations: bool,
    pub bootstrap_superadmin_email: Option<String>,
    pub bootstrap_superadmin_password: Option<String>,
    pub cookie_secure: bool,
    pub session_absolute_hours: i64,
    pub default_session_idle_minutes: i64,
    pub tenant_db_targets_file: PathBuf,
    pub data_dir: PathBuf,
    pub local_kms_master_key: Vec<u8>,
    pub grace_period_hours: i64,
    pub retention_hours: i64,
    pub auto_purge: bool,
    pub scheduler_enabled: bool,
    pub scheduler_interval_secs: u64,
    pub simulated_verified_suffixes: Vec<String>,
    pub platform_domain: String,
    pub log_format: LogFormat,
    pub access_log: bool,
    pub api_token_ttl_minutes: i64,
    /// M10 hub: Redis for cross-node real-time fan-out (FR-ARC-003). None → single-node local bus.
    pub redis_url: Option<String>,
    /// M10 hub: this node's id (presence ownership, logs, /ready).
    pub node_id: String,
    /// WhatsApp Cloud API (ADR-0013): `fake` → the fake-meta server, `meta` → graph.facebook.com.
    pub whatsapp: WhatsAppSettings,
    /// SIMULATED SBC event-feed signing secret.
    pub hub_sim_sip_secret: String,
    /// Development only: seed the `demo` tenant, agents and simulated channels at start-up.
    pub hub_demo_seed: bool,
    pub hub_demo_password: Option<String>,
}

impl fmt::Debug for AppConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppConfig")
            .field("app_env", &self.app_env)
            .field("bind_addr", &self.bind_addr)
            .field("public_base_url", &self.public_base_url)
            .field("database_url", &redact_url(&self.database_url))
            .field("migration_database_url", &redact_url(&self.migration_database_url))
            .field("db_max_connections", &self.db_max_connections)
            .field("bootstrap_superadmin_email", &self.bootstrap_superadmin_email)
            .field("bootstrap_superadmin_password", &"<redacted>")
            .field("cookie_secure", &self.cookie_secure)
            .field("tenant_db_targets_file", &self.tenant_db_targets_file)
            .field("data_dir", &self.data_dir)
            .field("local_kms_master_key", &"<redacted>")
            .field("grace_period_hours", &self.grace_period_hours)
            .field("retention_hours", &self.retention_hours)
            .field("auto_purge", &self.auto_purge)
            .field("scheduler_enabled", &self.scheduler_enabled)
            .field("platform_domain", &self.platform_domain)
            .field("redis_url", &self.redis_url.as_deref().map(redact_url))
            .field("node_id", &self.node_id)
            .field("whatsapp_provider", &self.whatsapp.provider)
            .field("whatsapp_base_url", &self.whatsapp.base_url)
            .field("hub_secrets", &"<redacted>")
            .field("hub_demo_seed", &self.hub_demo_seed)
            .finish_non_exhaustive()
    }
}

/// Removes the password from a database URL for logging.
pub fn redact_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut u) => {
            if u.password().is_some() {
                let _ = u.set_password(Some("****"));
            }
            u.to_string()
        }
        Err(_) => "<unparseable url>".to_string(),
    }
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn var_or(name: &str, default: &str) -> String {
    var(name).unwrap_or_else(|| default.to_string())
}

fn parse_var<T: std::str::FromStr>(name: &str, default: T) -> anyhow::Result<T>
where
    T::Err: fmt::Display,
{
    match var(name) {
        Some(v) => v.parse::<T>().map_err(|e| anyhow!("invalid {name}: {e}")),
        None => Ok(default),
    }
}

fn parse_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match var(name) {
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => Err(anyhow!("invalid boolean for {name}: {other}")),
        },
        None => Ok(default),
    }
}

/// WhatsApp channel settings. Each provider has its own credentials so real Meta secrets are never
/// sent to the fake server (and vice versa).
#[derive(Clone)]
pub struct WhatsAppSettings {
    /// `fake` (default) or `meta`.
    pub provider: String,
    /// Graph API base URL: `FAKE_META_URL` for fake, `WHATSAPP_GRAPH_BASE_URL` (default
    /// https://graph.facebook.com) for meta.
    pub base_url: String,
    pub api_version: String,
    pub access_token: String,
    pub app_secret: String,
    /// Webhook subscription handshake token (`hub.verify_token`), configured in Meta's dashboard.
    pub verify_token: String,
    /// Business phone number id (real Meta: from WhatsApp → API Setup). The development demo
    /// tenant gets a channel endpoint for it.
    pub phone_number_id: Option<String>,
    pub display_number: Option<String>,
    /// fake-meta control API (simulator console, load tests); only with the fake provider.
    pub fake_control_url: Option<String>,
    /// Meta App ID and WhatsApp Business Account ID: with both set (provider `meta`) the hub
    /// registers its own webhook in Meta at startup — no manual webhook settings (ADR-0013).
    pub app_id: Option<String>,
    pub business_account_id: Option<String>,
    /// Public https base URL Meta should call. Explicit value, else `https://NGROK_DOMAIN`, else
    /// discovered from the cloudflared quick tunnel (`TUNNEL_METRICS_URL`/quicktunnel).
    pub public_base_url: Option<String>,
    pub tunnel_metrics_url: Option<String>,
    /// Outbound messages per second per sending number (Meta tiers: 80 base, up to 1000).
    pub send_rate_per_sec: u32,
}

impl WhatsAppSettings {
    pub fn is_fake(&self) -> bool {
        self.provider == "fake"
    }

    fn from_env() -> anyhow::Result<Self> {
        let provider = var_or("WHATSAPP_PROVIDER", "fake").to_ascii_lowercase();
        let api_version = var_or("WHATSAPP_GRAPH_API_VERSION", "v25.0");
        let verify_token = var_or("WHATSAPP_VERIFY_TOKEN", "dev-verify-token");
        let phone_number_id = var("WHATSAPP_PHONE_NUMBER_ID");
        let display_number = var("WHATSAPP_DISPLAY_NUMBER");
        let app_id = var("WHATSAPP_APP_ID");
        let business_account_id = var("WHATSAPP_BUSINESS_ACCOUNT_ID");
        let public_base_url = var("WHATSAPP_PUBLIC_BASE_URL")
            .or_else(|| var("NGROK_DOMAIN").map(|d| format!("https://{}", d.trim_start_matches("https://"))));
        let tunnel_metrics_url = var("TUNNEL_METRICS_URL");
        let send_rate_per_sec = parse_var("WHATSAPP_SEND_RATE_PER_SEC", 80u32)?.clamp(1, 10_000);
        match provider.as_str() {
            "fake" => {
                let url = var_or("FAKE_META_URL", "http://localhost:58090");
                Ok(Self {
                    provider,
                    base_url: url.clone(),
                    api_version,
                    access_token: var_or("FAKE_META_ACCESS_TOKEN", "fake-meta-dev-token"),
                    app_secret: var_or("FAKE_META_APP_SECRET", "fake-meta-dev-app-secret"),
                    verify_token,
                    phone_number_id,
                    display_number,
                    fake_control_url: Some(url),
                    app_id,
                    business_account_id,
                    public_base_url,
                    tunnel_metrics_url,
                    send_rate_per_sec,
                })
            }
            "meta" => Ok(Self {
                provider,
                base_url: var_or("WHATSAPP_GRAPH_BASE_URL", "https://graph.facebook.com"),
                api_version,
                access_token: var("WHATSAPP_ACCESS_TOKEN").context("WHATSAPP_PROVIDER=meta needs WHATSAPP_ACCESS_TOKEN")?,
                app_secret: var("WHATSAPP_APP_SECRET").context("WHATSAPP_PROVIDER=meta needs WHATSAPP_APP_SECRET")?,
                verify_token,
                phone_number_id: Some(phone_number_id.context("WHATSAPP_PROVIDER=meta needs WHATSAPP_PHONE_NUMBER_ID")?),
                display_number,
                fake_control_url: None,
                app_id,
                business_account_id,
                public_base_url,
                tunnel_metrics_url,
                send_rate_per_sec,
            }),
            other => Err(anyhow!("WHATSAPP_PROVIDER must be fake or meta, got {other}")),
        }
    }
}

impl AppConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let app_env = AppEnv::parse(&var_or("APP_ENV", "development"))?;
        let bind_addr: SocketAddr = var_or("APP_BIND_ADDR", "0.0.0.0:3000").parse().context("APP_BIND_ADDR")?;
        let database_url = var("DATABASE_URL").context("DATABASE_URL is required")?;
        let migration_database_url = var("MIGRATION_DATABASE_URL").context("MIGRATION_DATABASE_URL is required")?;

        let kms_key_b64 =
            var("LOCAL_KMS_MASTER_KEY").context("LOCAL_KMS_MASTER_KEY is required (base64, 32 bytes; development reference KMS)")?;
        let local_kms_master_key =
            base64::engine::general_purpose::STANDARD.decode(kms_key_b64.trim()).context("LOCAL_KMS_MASTER_KEY must be base64")?;
        if local_kms_master_key.len() != 32 {
            return Err(anyhow!("LOCAL_KMS_MASTER_KEY must decode to exactly 32 bytes"));
        }

        let cookie_secure = parse_bool("COOKIE_SECURE", !app_env.is_development())?;
        let log_format = match var_or("LOG_FORMAT", "pretty").as_str() {
            "json" => LogFormat::Json,
            _ => LogFormat::Pretty,
        };

        let cfg = Self {
            app_env,
            bind_addr,
            public_base_url: var_or("PUBLIC_BASE_URL", "http://localhost:3000"),
            database_url,
            migration_database_url,
            db_max_connections: parse_var("DB_MAX_CONNECTIONS", 20u32)?,
            run_migrations: parse_bool("RUN_MIGRATIONS", true)?,
            bootstrap_superadmin_email: var("BOOTSTRAP_SUPERADMIN_EMAIL"),
            bootstrap_superadmin_password: var("BOOTSTRAP_SUPERADMIN_PASSWORD"),
            cookie_secure,
            session_absolute_hours: parse_var("SESSION_ABSOLUTE_HOURS", 12i64)?,
            default_session_idle_minutes: parse_var("SESSION_IDLE_MINUTES", 30i64)?,
            tenant_db_targets_file: PathBuf::from(var_or("TENANT_DB_TARGETS_FILE", "config/tenant-db-targets.local.toml")),
            data_dir: PathBuf::from(var_or("DATA_DIR", "./data")),
            local_kms_master_key,
            grace_period_hours: parse_var("LIFECYCLE_GRACE_PERIOD_HOURS", 720i64)?,
            retention_hours: parse_var("LIFECYCLE_RETENTION_HOURS", 2160i64)?,
            auto_purge: parse_bool("LIFECYCLE_AUTO_PURGE", !app_env.is_development())?,
            scheduler_enabled: parse_bool("LIFECYCLE_SCHEDULER_ENABLED", true)?,
            scheduler_interval_secs: parse_var("LIFECYCLE_SCHEDULER_INTERVAL_SECS", 30u64)?,
            simulated_verified_suffixes: var_or("SIMULATED_VERIFIED_DOMAIN_SUFFIXES", ".verified.test")
                .split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect(),
            platform_domain: var_or("PLATFORM_DOMAIN", "tenants.omni.local"),
            log_format,
            access_log: parse_bool("ACCESS_LOG", true)?,
            api_token_ttl_minutes: parse_var("API_TOKEN_TTL_MINUTES", 60i64)?,
            redis_url: var("REDIS_URL"),
            node_id: var("NODE_ID")
                .or_else(|| var("HOSTNAME"))
                .unwrap_or_else(|| format!("node-{}", &uuid::Uuid::new_v4().simple().to_string()[..8])),
            whatsapp: WhatsAppSettings::from_env()?,
            hub_sim_sip_secret: var_or("HUB_SIM_SIP_SECRET", "dev-sim-sip-secret-change-me"),
            hub_demo_seed: parse_bool("HUB_DEMO_SEED", false)? && app_env.is_development(),
            hub_demo_password: var("HUB_DEMO_PASSWORD"),
        };

        if cfg.grace_period_hours < 0 || cfg.retention_hours < 0 {
            return Err(anyhow!("lifecycle periods must be >= 0"));
        }
        if !cfg.app_env.is_development() && cfg.bootstrap_superadmin_password.is_some() {
            tracing::warn!("BOOTSTRAP_SUPERADMIN_PASSWORD is set outside development; rotate it after first login");
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_password() {
        let r = redact_url("postgres://crm_app:secret@localhost:5432/crm");
        assert!(!r.contains("secret"));
        assert!(r.contains("****"));
    }

    #[test]
    fn parses_env_names() {
        assert_eq!(AppEnv::parse("production").unwrap(), AppEnv::Production);
        assert!(AppEnv::parse("staging").is_err());
    }
}
