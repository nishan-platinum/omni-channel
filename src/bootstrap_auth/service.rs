//! Bootstrap authentication service (temporary M02 stand-in, ADR-0007): Argon2id passwords,
//! lockout, server-side sessions (browser + API bearer), invitations, Super Admin seeding.
//! Tenant status gating is delegated to the `TenantGate` port implemented by M01.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::platform::db::{scoped_tx, AccessScope};
use crate::platform::errors::{AppError, AppResult};
use crate::platform::security::{dummy_verify_async, hash_password_async, random_token, sha256, verify_password_async};
use crate::platform::time::Clock;

pub const MAX_FAILED_ATTEMPTS: i32 = 5;
pub const LOCKOUT_MINUTES: i64 = 15;
pub const INVITATION_HOURS: i64 = 72;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    SuperAdmin,
    TenantAdmin,
    /// Contact-centre agent of one tenant (M10 hub). Has no M01 administration scopes.
    Agent,
}

impl Role {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "super_admin" => Some(Self::SuperAdmin),
            "tenant_admin" => Some(Self::TenantAdmin),
            "agent" => Some(Self::Agent),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SuperAdmin => "super_admin",
            Self::TenantAdmin => "tenant_admin",
            Self::Agent => "agent",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::SuperAdmin => "Platform Super Admin",
            Self::TenantAdmin => "Tenant Admin",
            Self::Agent => "Agent",
        }
    }
    /// Bootstrap scopes (stand-in for OAuth scopes `<module>.<action>`, API-002).
    pub fn scopes(self) -> Vec<String> {
        match self {
            Self::SuperAdmin => vec!["tenants:read".into(), "tenants:write".into(), "platform:elevated".into()],
            Self::TenantAdmin => vec!["tenants:read".into(), "tenants:write".into(), "hub:admin".into()],
            Self::Agent => vec!["hub:agent".into()],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SessionKind {
    Browser,
    Api,
}

impl SessionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Browser => "browser",
            Self::Api => "api",
        }
    }
}

/// Result of the tenant gate: whether a tenant's users may authenticate right now.
#[derive(Debug, Clone)]
pub struct TenantAccessInfo {
    pub code: String,
    pub name: String,
    pub status: String,
    pub allows_access: bool,
    pub read_only: bool,
    pub idle_timeout_minutes: i64,
    pub password_min_length: usize,
    pub primary_color: Option<String>,
    pub secondary_color: Option<String>,
}

/// Implemented by M01 (dependency inversion: auth never reads M01 tables itself).
#[async_trait]
pub trait TenantGate: Send + Sync {
    async fn access_info(&self, tenant_id: Uuid) -> AppResult<Option<TenantAccessInfo>>;
    async fn tenant_by_code(&self, code: &str) -> AppResult<Option<Uuid>>;
}

/// The authenticated principal. Tenant context originates here (server side) — never from input.
#[derive(Debug, Clone, Serialize)]
pub struct Principal {
    pub user_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub role: Role,
    pub tenant_id: Option<Uuid>,
    pub tenant_code: Option<String>,
    pub tenant_name: Option<String>,
    pub tenant_status: Option<String>,
    pub read_only: bool,
    pub primary_color: Option<String>,
    pub secondary_color: Option<String>,
    pub session_id: Uuid,
    #[serde(skip)]
    pub csrf_token: String,
    pub scopes: Vec<String>,
    pub kind: SessionKind,
}

impl Principal {
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }
}

#[derive(Debug, Clone)]
pub struct ClientMeta {
    pub ip: Option<String>,
    pub user_agent: Option<String>,
}

pub struct AuthService {
    pool: PgPool,
    gate: Arc<dyn TenantGate>,
    clock: Arc<dyn Clock>,
    absolute_hours: i64,
    default_idle_minutes: i64,
    api_ttl_minutes: i64,
}

struct UserRow {
    id: Uuid,
    tenant_id: Option<Uuid>,
    email: String,
    display_name: String,
    role: Role,
    status: String,
    password_hash: Option<String>,
    failed_attempts: i32,
    locked_until: Option<DateTime<Utc>>,
}

fn user_from_row(r: &sqlx::postgres::PgRow) -> Result<UserRow, sqlx::Error> {
    Ok(UserRow {
        id: r.try_get("id")?,
        tenant_id: r.try_get("tenant_id")?,
        email: r.try_get("email")?,
        display_name: r.try_get("display_name")?,
        role: Role::parse(&r.try_get::<String, _>("role")?).ok_or_else(|| sqlx::Error::Decode("role".into()))?,
        status: r.try_get("status")?,
        password_hash: r.try_get("password_hash")?,
        failed_attempts: r.try_get("failed_attempts")?,
        locked_until: r.try_get("locked_until")?,
    })
}

const USER_COLS: &str = "id, tenant_id, email::text AS email, display_name, role, status, password_hash, failed_attempts, locked_until";
const INVALID_CREDENTIALS: &str = "Invalid email or password";

impl AuthService {
    pub fn new(
        pool: PgPool,
        gate: Arc<dyn TenantGate>,
        clock: Arc<dyn Clock>,
        absolute_hours: i64,
        default_idle_minutes: i64,
        api_ttl_minutes: i64,
    ) -> Self {
        Self { pool, gate, clock, absolute_hours, default_idle_minutes, api_ttl_minutes }
    }

    /// Seeds the development Super Admin when absent (credentials come from the environment).
    pub async fn seed_super_admin(&self, email: &str, password: &str) -> AppResult<bool> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM identity.users WHERE tenant_id IS NULL AND email = $1::citext)")
                .bind(email)
                .fetch_one(&mut *tx)
                .await?;
        if exists {
            tx.commit().await?;
            return Ok(false);
        }
        if password.chars().count() < 12 {
            return Err(AppError::validation("password", "BOOTSTRAP_SUPERADMIN_PASSWORD must have at least 12 characters"));
        }
        let hash = hash_password_async(password).await?;
        // Atomic: several instances may start concurrently against an empty database.
        let inserted = sqlx::query(
            "INSERT INTO identity.users (id, tenant_id, email, display_name, role, status, password_hash)
             VALUES ($1, NULL, $2::citext, 'Platform Super Admin', 'super_admin', 'active', $3)
             ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(email)
        .bind(hash)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(inserted > 0)
    }

    /// Email + password (+ optional organisation code) login. Credentials are verified before any
    /// tenant-state information is revealed.
    pub async fn login(
        &self,
        email: &str,
        password: &str,
        tenant_code: Option<&str>,
        kind: SessionKind,
        meta: &ClientMeta,
    ) -> AppResult<(String, Principal)> {
        let email = email.trim();
        if email.is_empty() || password.is_empty() || email.len() > 320 || password.len() > 1024 {
            return Err(AppError::unauthenticated(INVALID_CREDENTIALS));
        }
        let code = tenant_code.map(str::trim).filter(|s| !s.is_empty());
        let tenant_filter = match code {
            Some(c) => match self.gate.tenant_by_code(c).await? {
                Some(t) => Some(t),
                None => {
                    dummy_verify_async(password).await;
                    return Err(AppError::unauthenticated(INVALID_CREDENTIALS));
                }
            },
            None => None,
        };
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let rows = sqlx::query(&format!(
            "SELECT {USER_COLS} FROM identity.users WHERE email = $1::citext AND ($2::uuid IS NULL OR tenant_id = $2) ORDER BY role DESC LIMIT 5"
        ))
        .bind(email)
        .bind(tenant_filter)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let users = rows.iter().map(user_from_row).collect::<Result<Vec<_>, _>>()?;
        let user = match users.len() {
            0 => {
                dummy_verify_async(password).await;
                return Err(AppError::unauthenticated(INVALID_CREDENTIALS));
            }
            1 => &users[0],
            _ => {
                // Same email in several organisations (e.g. production + sandbox): need the code.
                dummy_verify_async(password).await;
                return Err(AppError::validation(
                    "tenant_code",
                    "This email belongs to several organisations — enter the organisation code",
                ));
            }
        };
        let now = self.clock.now();
        if user.locked_until.is_some_and(|l| l > now) {
            dummy_verify_async(password).await;
            return Err(AppError::unauthenticated("Account temporarily locked after repeated failures; try again later"));
        }
        let ok = match (user.status.as_str(), user.password_hash.as_deref()) {
            ("active", Some(h)) => verify_password_async(password, h).await,
            _ => {
                dummy_verify_async(password).await;
                false
            }
        };
        if !ok {
            self.record_failure(user.id, user.failed_attempts, now).await?;
            return Err(AppError::unauthenticated(INVALID_CREDENTIALS));
        }
        // BR-M01-002: tenant users of a non-accessible tenant are refused after authentication.
        let info = match user.tenant_id {
            Some(t) => Some(self.check_tenant(t).await?),
            None => None,
        };
        let idle = info.as_ref().map(|i| i.idle_timeout_minutes).unwrap_or(self.default_idle_minutes);
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        sqlx::query(
            "UPDATE identity.users SET failed_attempts = 0, locked_until = NULL, last_login_at = $2, updated_at = now() WHERE id = $1",
        )
        .bind(user.id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let (token, session_id, csrf) = self.create_session(user.id, user.tenant_id, user.role, kind, idle, meta).await?;
        let p = Principal {
            user_id: user.id,
            email: user.email.clone(),
            display_name: user.display_name.clone(),
            role: user.role,
            tenant_id: user.tenant_id,
            tenant_code: info.as_ref().map(|i| i.code.clone()),
            tenant_name: info.as_ref().map(|i| i.name.clone()),
            tenant_status: info.as_ref().map(|i| i.status.clone()),
            read_only: info.as_ref().is_some_and(|i| i.read_only),
            primary_color: info.as_ref().and_then(|i| i.primary_color.clone()),
            secondary_color: info.as_ref().and_then(|i| i.secondary_color.clone()),
            session_id,
            csrf_token: csrf,
            scopes: user.role.scopes(),
            kind,
        };
        Ok((token, p))
    }

    async fn record_failure(&self, user: Uuid, failed: i32, now: DateTime<Utc>) -> AppResult<()> {
        let attempts = failed + 1;
        let locked = (attempts >= MAX_FAILED_ATTEMPTS).then(|| now + Duration::minutes(LOCKOUT_MINUTES));
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        sqlx::query("UPDATE identity.users SET failed_attempts = $2, locked_until = $3, updated_at = now() WHERE id = $1")
            .bind(user)
            .bind(if locked.is_some() { 0 } else { attempts })
            .bind(locked)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn check_tenant(&self, tenant: Uuid) -> AppResult<TenantAccessInfo> {
        let info = self.gate.access_info(tenant).await?.ok_or_else(|| AppError::forbidden("Tenant does not exist"))?;
        if info.allows_access {
            return Ok(info);
        }
        Err(match info.status.as_str() {
            "suspended" => AppError::tenant_suspended(),
            "draft" => AppError::forbidden("Tenant is not active yet; the platform operator must activate it"),
            _ => AppError::forbidden("Tenant is no longer active"),
        })
    }

    async fn create_session(
        &self,
        user: Uuid,
        tenant: Option<Uuid>,
        role: Role,
        kind: SessionKind,
        idle_minutes: i64,
        meta: &ClientMeta,
    ) -> AppResult<(String, Uuid, String)> {
        let token = random_token(32);
        let csrf = random_token(24);
        let id = Uuid::now_v7();
        let now = self.clock.now();
        let expires = match kind {
            SessionKind::Browser => now + Duration::hours(self.absolute_hours),
            SessionKind::Api => now + Duration::minutes(self.api_ttl_minutes),
        };
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        sqlx::query(
            "INSERT INTO identity.sessions (id, token_hash, user_id, tenant_id, kind, csrf_token, scopes, created_at, last_seen_at, expires_at, idle_timeout_secs, ip, user_agent)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,$10,$11,$12)",
        )
        .bind(id)
        .bind(sha256(token.as_bytes()))
        .bind(user)
        .bind(tenant)
        .bind(kind.as_str())
        .bind(&csrf)
        .bind(role.scopes())
        .bind(now)
        .bind(expires)
        .bind((idle_minutes.clamp(5, 480) * 60) as i32)
        .bind(&meta.ip)
        .bind(&meta.user_agent)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok((token, id, csrf))
    }

    /// Validates a session/bearer token and re-checks the tenant gate on every request
    /// (suspension takes effect immediately even for existing sessions).
    pub async fn authenticate(&self, token: &str, kind: SessionKind) -> AppResult<Principal> {
        if token.is_empty() || token.len() > 200 {
            return Err(AppError::unauthenticated("Missing or invalid credentials"));
        }
        let now = self.clock.now();
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let row = sqlx::query(
            "SELECT s.id AS session_id, s.csrf_token, s.scopes, s.last_seen_at, s.idle_timeout_secs, s.expires_at, s.revoked_at,
                    u.id, u.tenant_id, u.email::text AS email, u.display_name, u.role, u.status
               FROM identity.sessions s JOIN identity.users u ON u.id = s.user_id
              WHERE s.token_hash = $1 AND s.kind = $2",
        )
        .bind(sha256(token.as_bytes()))
        .bind(kind.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(r) = row else {
            tx.commit().await?;
            return Err(AppError::unauthenticated("Missing or invalid credentials"));
        };
        let revoked: Option<DateTime<Utc>> = r.try_get("revoked_at")?;
        let expires: DateTime<Utc> = r.try_get("expires_at")?;
        let last_seen: DateTime<Utc> = r.try_get("last_seen_at")?;
        let idle: i32 = r.try_get("idle_timeout_secs")?;
        let status: String = r.try_get("status")?;
        if revoked.is_some() || expires <= now || last_seen + Duration::seconds(i64::from(idle)) <= now || status != "active" {
            tx.commit().await?;
            return Err(AppError::unauthenticated("Session expired; please sign in again"));
        }
        let session_id: Uuid = r.try_get("session_id")?;
        if now - last_seen > Duration::seconds(60) {
            sqlx::query("UPDATE identity.sessions SET last_seen_at = $2 WHERE id = $1")
                .bind(session_id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        let tenant_id: Option<Uuid> = r.try_get("tenant_id")?;
        let info = match tenant_id {
            Some(t) => Some(self.check_tenant(t).await?),
            None => None,
        };
        let role = Role::parse(&r.try_get::<String, _>("role")?).ok_or_else(|| AppError::unauthenticated("Invalid role"))?;
        Ok(Principal {
            user_id: r.try_get("id")?,
            email: r.try_get("email")?,
            display_name: r.try_get("display_name")?,
            role,
            tenant_id,
            tenant_code: info.as_ref().map(|i| i.code.clone()),
            tenant_name: info.as_ref().map(|i| i.name.clone()),
            tenant_status: info.as_ref().map(|i| i.status.clone()),
            read_only: info.as_ref().is_some_and(|i| i.read_only),
            primary_color: info.as_ref().and_then(|i| i.primary_color.clone()),
            secondary_color: info.as_ref().and_then(|i| i.secondary_color.clone()),
            session_id,
            csrf_token: r.try_get("csrf_token")?,
            scopes: r.try_get("scopes")?,
            kind,
        })
    }

    pub async fn logout(&self, session_id: Uuid) -> AppResult<()> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        sqlx::query("UPDATE identity.sessions SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL")
            .bind(session_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn revoke_tenant_sessions(&self, tenant: Uuid) -> AppResult<u64> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let n = sqlx::query("UPDATE identity.sessions SET revoked_at = now() WHERE tenant_id = $1 AND revoked_at IS NULL")
            .bind(tenant)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(n)
    }

    /// Creates or re-invites a Tenant Admin. Returns None as token when the user is already active.
    pub async fn invite_tenant_admin(&self, tenant: Uuid, email: &str, display_name: &str) -> AppResult<(Uuid, Option<String>)> {
        let scope = AccessScope::Tenant(tenant);
        let mut tx = scoped_tx(&self.pool, &scope).await?;
        let existing = sqlx::query("SELECT id, status FROM identity.users WHERE tenant_id = $1 AND email = $2::citext")
            .bind(tenant)
            .bind(email)
            .fetch_optional(&mut *tx)
            .await?;
        let user_id = match existing {
            Some(r) => {
                let status: String = r.try_get("status")?;
                let id: Uuid = r.try_get("id")?;
                if status == "active" {
                    tx.commit().await?;
                    return Ok((id, None));
                }
                id
            }
            None => {
                let id = Uuid::now_v7();
                sqlx::query("INSERT INTO identity.users (id, tenant_id, email, display_name, role, status) VALUES ($1,$2,$3::citext,$4,'tenant_admin','invited')")
                    .bind(id)
                    .bind(tenant)
                    .bind(email)
                    .bind(display_name)
                    .execute(&mut *tx)
                    .await?;
                id
            }
        };
        // Single active invitation per user: older ones are invalidated.
        sqlx::query("UPDATE identity.invitations SET expires_at = now() WHERE user_id = $1 AND accepted_at IS NULL")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        let token = random_token(32);
        sqlx::query("INSERT INTO identity.invitations (id, user_id, tenant_id, token_hash, expires_at) VALUES ($1,$2,$3,$4,$5)")
            .bind(Uuid::now_v7())
            .bind(user_id)
            .bind(tenant)
            .bind(sha256(token.as_bytes()))
            .bind(self.clock.now() + Duration::hours(INVITATION_HOURS))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((user_id, Some(token)))
    }

    /// Looks up a pending invitation (for rendering the accept page).
    pub async fn invitation(&self, token: &str) -> AppResult<(String, Option<TenantAccessInfo>, Uuid)> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let row = sqlx::query(
            "SELECT i.tenant_id, u.email::text AS email FROM identity.invitations i JOIN identity.users u ON u.id = i.user_id
              WHERE i.token_hash = $1 AND i.accepted_at IS NULL AND i.expires_at > $2",
        )
        .bind(sha256(token.as_bytes()))
        .bind(self.clock.now())
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        let r = row.ok_or_else(|| AppError::not_found("Invitation is invalid or has expired"))?;
        let tenant: Uuid = r.try_get("tenant_id")?;
        Ok((r.try_get("email")?, self.gate.access_info(tenant).await?, tenant))
    }

    pub async fn accept_invitation(&self, token: &str, password: &str, confirm: &str) -> AppResult<String> {
        let (email, info, _tenant) = self.invitation(token).await?;
        let min = info.as_ref().map(|i| i.password_min_length).unwrap_or(12).max(12);
        if password != confirm {
            return Err(AppError::validation("confirm", "Passwords do not match"));
        }
        if password.chars().count() < min || password.len() > 128 {
            return Err(AppError::validation("password", format!("Password must have {min}-128 characters")));
        }
        if password.eq_ignore_ascii_case(&email) {
            return Err(AppError::validation("password", "Password must not equal your email"));
        }
        let hash = hash_password_async(password).await?;
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let user: Option<Uuid> = sqlx::query_scalar(
            "UPDATE identity.invitations SET accepted_at = now() WHERE token_hash = $1 AND accepted_at IS NULL AND expires_at > now() RETURNING user_id",
        )
        .bind(sha256(token.as_bytes()))
        .fetch_optional(&mut *tx)
        .await?;
        let user = user.ok_or_else(|| AppError::not_found("Invitation is invalid or has expired"))?;
        sqlx::query("UPDATE identity.users SET password_hash = $2, status = 'active', failed_attempts = 0, locked_until = NULL, updated_at = now(), version = version + 1 WHERE id = $1")
            .bind(user)
            .bind(hash)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(email)
    }

    pub async fn count_users(&self, tenant: Uuid) -> AppResult<i64> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM identity.users WHERE tenant_id = $1 AND status <> 'disabled'")
            .bind(tenant)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(n)
    }

    pub async fn tenant_admins(&self, tenant: Uuid) -> AppResult<Vec<(Uuid, String, String, String)>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let rows = sqlx::query("SELECT id, email::text AS email, display_name, status FROM identity.users WHERE tenant_id = $1 AND role = 'tenant_admin' ORDER BY email")
            .bind(tenant)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        rows.iter()
            .map(|r| Ok((r.try_get("id")?, r.try_get("email")?, r.try_get("display_name")?, r.try_get("status")?)))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(AppError::from)
    }

    /// Creates an active agent user for a tenant (M10 hub; the Tenant Admin sets the initial
    /// password). Returns the user id.
    pub async fn create_agent_user(&self, tenant: Uuid, email: &str, display_name: &str, password: &str) -> AppResult<Uuid> {
        let email = email.trim();
        if email.is_empty() || email.len() > 320 || !email.contains('@') {
            return Err(AppError::validation("email", "A valid email address is required"));
        }
        let name = display_name.trim();
        if name.is_empty() || name.chars().count() > 200 {
            return Err(AppError::validation("display_name", "Display name is required (max 200 characters)"));
        }
        let info = self.check_tenant(tenant).await?;
        if password.chars().count() < info.password_min_length.max(12) || password.len() > 1024 {
            return Err(AppError::validation(
                "password",
                format!("Password must have at least {} characters", info.password_min_length.max(12)),
            ));
        }
        let hash = hash_password_async(password).await?;
        let id = Uuid::now_v7();
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        sqlx::query(
            "INSERT INTO identity.users (id, tenant_id, email, display_name, role, status, password_hash)
             VALUES ($1, $2, $3::citext, $4, 'agent', 'active', $5)",
        )
        .bind(id)
        .bind(tenant)
        .bind(email)
        .bind(name)
        .bind(hash)
        .execute(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(d) if d.is_unique_violation() => {
                AppError::conflict("A user with this email already exists in this tenant")
            }
            e => e.into(),
        })?;
        tx.commit().await?;
        Ok(id)
    }

    /// Finds a tenant user id by email (used by the development demo seed).
    pub async fn tenant_user_id(&self, tenant: Uuid, email: &str) -> AppResult<Option<Uuid>> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::Tenant(tenant)).await?;
        let id = sqlx::query_scalar("SELECT id FROM identity.users WHERE tenant_id = $1 AND email = $2::citext")
            .bind(tenant)
            .bind(email)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn purge_tenant(&self, tenant: Uuid) -> AppResult<u64> {
        let mut tx = scoped_tx(&self.pool, &AccessScope::System).await?;
        let n = sqlx::query("DELETE FROM identity.users WHERE tenant_id = $1").bind(tenant).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(n)
    }
}
