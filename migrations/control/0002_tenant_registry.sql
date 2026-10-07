-- Tenant registry and M01-owned configuration (OCC-M01-R001..R027). Conventions per DBS-001..012.

-- ---------------------------------------------------------------------------------------------
-- Reference plans (REFERENCE ADAPTER for M19 PlanCatalog). Prototype fixtures, not commercial plans.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE tenantadm.plans (
    id              uuid PRIMARY KEY,
    code            text NOT NULL,
    name            text NOT NULL,
    tier            text NOT NULL,
    entitlements    jsonb NOT NULL,               -- array of feature keys
    quotas          jsonb NOT NULL,               -- object: metric -> limit
    soft_threshold  numeric(5,4) NOT NULL DEFAULT 0.8000,
    status          text NOT NULL DEFAULT 'active',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    version         integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_plans_code UNIQUE (code),
    CONSTRAINT ck_plans_tier CHECK (tier IN ('standard', 'premium', 'regulated')),
    CONSTRAINT ck_plans_status CHECK (status IN ('active', 'retired')),
    CONSTRAINT ck_plans_soft_threshold CHECK (soft_threshold >= 0 AND soft_threshold <= 1)
);
GRANT SELECT ON tenantadm.plans TO crm_app;

-- Provisioning templates (OCC-M01-R013). Packs are handed to downstream module ports.
CREATE TABLE tenantadm.provisioning_templates (
    id              uuid PRIMARY KEY,
    code            text NOT NULL,
    name            text NOT NULL,
    description     text NOT NULL,
    features        jsonb NOT NULL,               -- feature keys enabled initially (intersected with plan)
    config_defaults jsonb NOT NULL,               -- config key -> value
    packs           jsonb NOT NULL,               -- downstream packs (roles/teams, BPM, reports, SLA, dropdowns)
    status          text NOT NULL DEFAULT 'active',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_provisioning_templates_code UNIQUE (code),
    CONSTRAINT ck_provisioning_templates_status CHECK (status IN ('active', 'retired'))
);
GRANT SELECT ON tenantadm.provisioning_templates TO crm_app;

-- ---------------------------------------------------------------------------------------------
-- Tenants (root record). Physical PK id, exposed as tenant_id (ADR-0003).
-- ---------------------------------------------------------------------------------------------
CREATE TABLE tenantadm.tenants (
    id                      uuid PRIMARY KEY,
    tenant_code             text NOT NULL,
    name                    varchar(120) NOT NULL,
    legal_name              varchar(200),
    region                  text NOT NULL DEFAULT 'my-central',
    plan_id                 uuid NOT NULL,
    status                  text NOT NULL DEFAULT 'draft',
    parent_tenant_id        uuid,
    primary_admin_email     citext NOT NULL,
    storage_strategy        text NOT NULL,
    isolation_mode          text NOT NULL DEFAULT 'row_level',
    is_sandbox              boolean NOT NULL DEFAULT false,
    sandbox_of_tenant_id    uuid,
    template_code           text,
    inheritance_flags       jsonb NOT NULL DEFAULT '{}'::jsonb,
    provisioning_status     text NOT NULL DEFAULT 'pending',
    isolation_check_status  text NOT NULL DEFAULT 'not_run',
    isolation_checked_at    timestamptz,
    suspended_reason        varchar(500),
    status_reason           varchar(500),
    activated_at            timestamptz,
    grace_until             timestamptz,
    terminated_at           timestamptz,
    purge_after             timestamptz,
    purged_at               timestamptz,
    legal_hold              boolean NOT NULL DEFAULT false,
    platform_version        text NOT NULL DEFAULT '1.0.0',
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    created_by              uuid,
    updated_by              uuid,
    deleted_at              timestamptz,
    version                 integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_tenants_tenant_code UNIQUE (tenant_code),
    CONSTRAINT ck_tenants_tenant_code CHECK (tenant_code ~ '^[a-z0-9-]{3,32}$'),
    CONSTRAINT ck_tenants_name CHECK (char_length(btrim(name)) BETWEEN 1 AND 120),
    CONSTRAINT ck_tenants_region CHECK (region IN ('my-central', 'sg', 'apac')),
    CONSTRAINT ck_tenants_status CHECK (status IN ('draft', 'active', 'suspended', 'grace', 'terminated', 'purged')),
    CONSTRAINT ck_tenants_storage_strategy CHECK (storage_strategy IN ('shared_row_level', 'schema_per_tenant', 'dedicated_database')),
    CONSTRAINT ck_tenants_isolation_mode CHECK (isolation_mode IN ('row_level', 'schema_per_tenant', 'database_per_tenant')),
    CONSTRAINT ck_tenants_provisioning_status CHECK (provisioning_status IN ('pending', 'in_progress', 'completed', 'failed')),
    CONSTRAINT ck_tenants_isolation_check_status CHECK (isolation_check_status IN ('not_run', 'passed', 'failed')),
    CONSTRAINT ck_tenants_suspended_reason CHECK (status <> 'suspended' OR suspended_reason IS NOT NULL),
    CONSTRAINT ck_tenants_not_own_parent CHECK (parent_tenant_id IS NULL OR parent_tenant_id <> id),
    CONSTRAINT fk_tenants_plan FOREIGN KEY (plan_id) REFERENCES tenantadm.plans (id) ON DELETE RESTRICT,
    CONSTRAINT fk_tenants_parent FOREIGN KEY (parent_tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE RESTRICT,
    CONSTRAINT fk_tenants_sandbox_of FOREIGN KEY (sandbox_of_tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE RESTRICT
);
CREATE INDEX ix_tenants_status_created_at ON tenantadm.tenants (status, created_at DESC);
CREATE INDEX ix_tenants_created_at_id ON tenantadm.tenants (created_at DESC, id DESC);
CREATE INDEX ix_tenants_plan_id ON tenantadm.tenants (plan_id);
CREATE INDEX ix_tenants_parent_tenant_id ON tenantadm.tenants (parent_tenant_id);
CREATE INDEX ix_tenants_sandbox_of_tenant_id ON tenantadm.tenants (sandbox_of_tenant_id);
CREATE INDEX ix_tenants_region ON tenantadm.tenants (region);
CREATE INDEX ix_tenants_lower_name ON tenantadm.tenants (lower(name));

ALTER TABLE tenantadm.tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenantadm.tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenants_isolation ON tenantadm.tenants
    USING (shared.rls_tenant_visible(id))
    WITH CHECK (shared.rls_tenant_visible(id));
GRANT SELECT, INSERT, UPDATE, DELETE ON tenantadm.tenants TO crm_app;

-- ---------------------------------------------------------------------------------------------
-- Helper macro pattern: every tenant-scoped child table gets ENABLE+FORCE RLS and one policy.
-- ---------------------------------------------------------------------------------------------

-- Typed configuration (OCC-M01-R004/R020/R021, config_key/config_value)
CREATE TABLE tenantadm.tenant_configs (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    config_key      text NOT NULL,
    config_value    jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    updated_by      uuid,
    version         integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_tenant_configs_tenant_key UNIQUE (tenant_id, config_key),
    CONSTRAINT ck_tenant_configs_key CHECK (config_key ~ '^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$'),
    CONSTRAINT fk_tenant_configs_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);

-- Feature flags / module entitlements (OCC-M01-R004/R015, BR-M01-003)
CREATE TABLE tenantadm.tenant_feature_flags (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    feature_key     text NOT NULL,
    enabled         boolean NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    updated_by      uuid,
    version         integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_tenant_feature_flags_tenant_feature UNIQUE (tenant_id, feature_key),
    CONSTRAINT fk_tenant_feature_flags_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);

-- Quotas & guardrails (OCC-M01-R005/R012, BR-M01-004)
CREATE TABLE tenantadm.tenant_quotas (
    id                      uuid PRIMARY KEY,
    tenant_id               uuid NOT NULL,
    metric                  text NOT NULL,
    limit_value             bigint NOT NULL,
    usage_value             bigint NOT NULL DEFAULT 0,
    soft_threshold          numeric(5,4) NOT NULL DEFAULT 0.8000,
    period                  text NOT NULL,
    cycle_start             timestamptz NOT NULL DEFAULT now(),
    warned_cycle_start      timestamptz,
    exhausted_cycle_start   timestamptz,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    updated_by              uuid,
    version                 integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_tenant_quotas_tenant_metric UNIQUE (tenant_id, metric),
    CONSTRAINT ck_tenant_quotas_limit CHECK (limit_value >= 0),
    CONSTRAINT ck_tenant_quotas_usage CHECK (usage_value >= 0),
    CONSTRAINT ck_tenant_quotas_soft_threshold CHECK (soft_threshold >= 0 AND soft_threshold <= 1),
    CONSTRAINT ck_tenant_quotas_period CHECK (period IN ('static', 'monthly', 'hourly', 'per_minute')),
    CONSTRAINT fk_tenant_quotas_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);

-- Usage metering for billing (OCC-M01-R025)
CREATE TABLE tenantadm.usage_meters (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    metric          text NOT NULL,
    period_month    date NOT NULL,
    value           bigint NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_usage_meters_tenant_metric_period UNIQUE (tenant_id, metric, period_month),
    CONSTRAINT ck_usage_meters_value CHECK (value >= 0),
    CONSTRAINT ck_usage_meters_metric CHECK (metric IN ('active_users', 'storage_bytes', 'api_calls', 'emails_sent', 'ai_tokens', 'channel_sessions', 'volume')),
    CONSTRAINT fk_usage_meters_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);

-- Branding (OCC-M01-R006/R019, BR-M01-005, FD-010/011)
CREATE TABLE tenantadm.tenant_branding (
    id                          uuid PRIMARY KEY,
    tenant_id                   uuid NOT NULL,
    logo_object_key             text,
    logo_content_type           text,
    logo_size_bytes             bigint,
    primary_color               char(7) NOT NULL DEFAULT '#0B2130',
    secondary_color             char(7) NOT NULL DEFAULT '#F26A21',
    custom_domain               citext,
    custom_domain_status        text NOT NULL DEFAULT 'none',
    custom_domain_active        boolean NOT NULL DEFAULT false,
    custom_domain_token         text,
    custom_domain_checked_at    timestamptz,
    custom_domain_message       text,
    email_from                  citext,
    email_footer                varchar(1000),
    login_message               varchar(500),
    pdf_letterhead              varchar(500),
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now(),
    updated_by                  uuid,
    version                     integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_tenant_branding_tenant UNIQUE (tenant_id),
    CONSTRAINT ck_tenant_branding_primary_color CHECK (primary_color ~ '^#[0-9A-Fa-f]{6}$'),
    CONSTRAINT ck_tenant_branding_secondary_color CHECK (secondary_color ~ '^#[0-9A-Fa-f]{6}$'),
    CONSTRAINT ck_tenant_branding_domain_status CHECK (custom_domain_status IN ('none', 'pending', 'verified', 'failed')),
    CONSTRAINT ck_tenant_branding_domain_active CHECK (NOT custom_domain_active OR custom_domain_status = 'verified'),
    CONSTRAINT ck_tenant_branding_logo_size CHECK (logo_size_bytes IS NULL OR logo_size_bytes <= 2097152),
    CONSTRAINT fk_tenant_branding_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
-- "Domain already claimed": a custom domain belongs to at most one tenant platform-wide.
CREATE UNIQUE INDEX uq_tenant_branding_custom_domain ON tenantadm.tenant_branding (custom_domain) WHERE custom_domain IS NOT NULL;

-- Sender domains for email_from (FD-011, DOMAIN_NOT_VERIFIED)
CREATE TABLE tenantadm.sender_domains (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    domain          citext NOT NULL,
    status          text NOT NULL DEFAULT 'pending',
    spf_record      text NOT NULL,
    dkim_selector   text NOT NULL,
    dkim_record     text NOT NULL,
    checked_at      timestamptz,
    verified_at     timestamptz,
    message         text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_sender_domains_tenant_domain UNIQUE (tenant_id, domain),
    CONSTRAINT ck_sender_domains_status CHECK (status IN ('pending', 'verified', 'failed')),
    CONSTRAINT fk_sender_domains_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);

-- Tenant database connection profiles (OCC-M01-R008, ADR-0004). NEVER stores passwords.
CREATE TABLE tenantadm.tenant_database_connections (
    id                  uuid PRIMARY KEY,
    tenant_id           uuid NOT NULL,
    engine              text NOT NULL,
    storage_strategy    text NOT NULL,
    target_name         text NOT NULL,
    host                text,
    port                integer,
    database_name       text NOT NULL,
    schema_name         text,
    region              text NOT NULL,
    secret_ref          text,
    status              text NOT NULL DEFAULT 'pending',
    last_checked_at     timestamptz,
    last_check_ok       boolean,
    last_check_message  text,
    last_latency_ms     integer,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    version             integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_tenant_database_connections_tenant UNIQUE (tenant_id),
    CONSTRAINT ck_tenant_database_connections_engine CHECK (engine IN ('postgres', 'mysql')),
    CONSTRAINT ck_tenant_database_connections_strategy CHECK (storage_strategy IN ('shared_row_level', 'schema_per_tenant', 'dedicated_database')),
    CONSTRAINT ck_tenant_database_connections_status CHECK (status IN ('pending', 'ready', 'failed', 'decommissioned')),
    CONSTRAINT ck_tenant_database_connections_secret_ref CHECK (secret_ref IS NULL OR secret_ref ~ '^env:TENANT_DB_[A-Z0-9_]+$'),
    CONSTRAINT ck_tenant_database_connections_schema CHECK (schema_name IS NULL OR schema_name ~ '^[a-z][a-z0-9_]{0,62}$'),
    CONSTRAINT ck_tenant_database_connections_database CHECK (database_name ~ '^[a-z][a-z0-9_]{0,62}$'),
    CONSTRAINT fk_tenant_database_connections_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);

-- Provisioning saga (OCC-M01-R001/R013, §58 rollback)
CREATE TABLE tenantadm.provisioning_runs (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    status          text NOT NULL,
    template_code   text,
    started_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz,
    duration_ms     bigint,
    error_summary   text,
    created_by      uuid,
    CONSTRAINT ck_provisioning_runs_status CHECK (status IN ('in_progress', 'completed', 'failed')),
    CONSTRAINT fk_provisioning_runs_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_provisioning_runs_tenant_started ON tenantadm.provisioning_runs (tenant_id, started_at DESC);

CREATE TABLE tenantadm.provisioning_steps (
    id              uuid PRIMARY KEY,
    run_id          uuid NOT NULL,
    tenant_id       uuid NOT NULL,
    step            text NOT NULL,
    status          text NOT NULL,
    detail          text,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    CONSTRAINT ck_provisioning_steps_status CHECK (status IN ('completed', 'failed', 'skipped')),
    CONSTRAINT fk_provisioning_steps_run FOREIGN KEY (run_id) REFERENCES tenantadm.provisioning_runs (id) ON DELETE CASCADE,
    CONSTRAINT fk_provisioning_steps_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_provisioning_steps_run ON tenantadm.provisioning_steps (run_id, started_at);
CREATE INDEX ix_provisioning_steps_tenant ON tenantadm.provisioning_steps (tenant_id);

-- Isolation smoke-test results (OCC-M01-R009, UJ-19 E1)
CREATE TABLE tenantadm.isolation_checks (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    passed          boolean NOT NULL,
    results         jsonb NOT NULL,
    actor_id        uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_isolation_checks_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_isolation_checks_tenant_created ON tenantadm.isolation_checks (tenant_id, created_at DESC);

-- Break-glass support grants (OCC-M01-R024, ADR-0009)
CREATE TABLE tenantadm.support_grants (
    id                  uuid PRIMARY KEY,
    tenant_id           uuid NOT NULL,
    requested_by        uuid NOT NULL,
    reason              varchar(500) NOT NULL,
    incident_ref        varchar(100),
    duration_minutes    integer NOT NULL DEFAULT 240,
    named_approver_id   uuid,
    status              text NOT NULL DEFAULT 'requested',
    decided_by          uuid,
    decided_at          timestamptz,
    decision_note       varchar(500),
    starts_at           timestamptz,
    expires_at          timestamptz,
    revoked_at          timestamptz,
    use_count           integer NOT NULL DEFAULT 0,
    last_used_at        timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    version             integer NOT NULL DEFAULT 1,
    CONSTRAINT ck_support_grants_status CHECK (status IN ('requested', 'approved', 'rejected', 'revoked')),
    CONSTRAINT ck_support_grants_duration CHECK (duration_minutes BETWEEN 15 AND 480),
    CONSTRAINT ck_support_grants_window CHECK (status <> 'approved' OR (starts_at IS NOT NULL AND expires_at IS NOT NULL AND expires_at > starts_at)),
    CONSTRAINT fk_support_grants_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_support_grants_tenant_created ON tenantadm.support_grants (tenant_id, created_at DESC);
-- Regulated tier: one grant per incident per tenant.
CREATE UNIQUE INDEX uq_support_grants_tenant_incident ON tenantadm.support_grants (tenant_id, incident_ref)
    WHERE incident_ref IS NOT NULL AND status IN ('requested', 'approved');

-- Configuration baselines (OCC-M01-R022)
CREATE TABLE tenantadm.tenant_baselines (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    version_no      integer NOT NULL,
    label           varchar(200) NOT NULL,
    source          text NOT NULL,
    content         jsonb NOT NULL,
    sha256          char(64) NOT NULL,
    change_record   varchar(200),
    created_by      uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_tenant_baselines_tenant_version UNIQUE (tenant_id, version_no),
    CONSTRAINT ck_tenant_baselines_source CHECK (source IN ('export', 'pre_import_snapshot', 'imported', 'promotion')),
    CONSTRAINT fk_tenant_baselines_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);

-- Per-tenant encryption keys (OCC-M01-R010). Only wrapped DEKs; master keys never stored.
CREATE TABLE tenantadm.tenant_keys (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    key_kind        text NOT NULL,
    key_ref         text NOT NULL,
    key_version     integer NOT NULL,
    state           text NOT NULL,
    wrapped_dek     bytea,
    created_at      timestamptz NOT NULL DEFAULT now(),
    activated_at    timestamptz,
    retired_at      timestamptz,
    destroyed_at    timestamptz,
    rotate_after    timestamptz NOT NULL,
    created_by      uuid,
    CONSTRAINT uq_tenant_keys_tenant_version UNIQUE (tenant_id, key_version),
    CONSTRAINT ck_tenant_keys_kind CHECK (key_kind IN ('platform_managed', 'customer_supplied')),
    CONSTRAINT ck_tenant_keys_state CHECK (state IN ('pending_validation', 'active', 'retired', 'destroyed', 'failed')),
    CONSTRAINT fk_tenant_keys_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX uq_tenant_keys_one_active ON tenantadm.tenant_keys (tenant_id) WHERE state = 'active';

-- Offboarding exports (OCC-M01-R016)
CREATE TABLE tenantadm.tenant_exports (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    reason          text NOT NULL,
    object_key      text NOT NULL,
    sha256          char(64) NOT NULL,
    size_bytes      bigint NOT NULL,
    key_version     integer NOT NULL,
    created_by      uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_tenant_exports_reason CHECK (reason IN ('grace', 'terminated', 'manual')),
    CONSTRAINT fk_tenant_exports_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_tenant_exports_tenant_created ON tenantadm.tenant_exports (tenant_id, created_at DESC);

-- Per-tenant backups and restores (OCC-M01-R011)
CREATE TABLE tenantadm.tenant_backups (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    object_key      text NOT NULL,
    sha256          char(64) NOT NULL,
    size_bytes      bigint NOT NULL,
    key_version     integer NOT NULL,
    created_by      uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_tenant_backups_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_tenant_backups_tenant_created ON tenantadm.tenant_backups (tenant_id, created_at DESC);

CREATE TABLE tenantadm.tenant_restores (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    backup_id       uuid NOT NULL,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz NOT NULL,
    duration_ms     bigint NOT NULL,
    created_by      uuid,
    CONSTRAINT fk_tenant_restores_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE,
    CONSTRAINT fk_tenant_restores_backup FOREIGN KEY (backup_id) REFERENCES tenantadm.tenant_backups (id) ON DELETE CASCADE
);
CREATE INDEX ix_tenant_restores_tenant ON tenantadm.tenant_restores (tenant_id, completed_at DESC);

-- Destruction certificates survive the purge (tombstone evidence).
CREATE TABLE tenantadm.destruction_certificates (
    id                  uuid PRIMARY KEY,
    tenant_id           uuid NOT NULL,
    tenant_code         text NOT NULL,
    manifest            jsonb NOT NULL,
    manifest_sha256     char(64) NOT NULL,
    issued_by           uuid,
    issued_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_destruction_certificates_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE RESTRICT
);
CREATE INDEX ix_destruction_certificates_tenant ON tenantadm.destruction_certificates (tenant_id);

-- Release management (OCC-M01-R026)
CREATE TABLE tenantadm.tenant_release_preferences (
    id                          uuid PRIMARY KEY,
    tenant_id                   uuid NOT NULL,
    ring                        text NOT NULL DEFAULT 'general',
    maintenance_day             smallint NOT NULL DEFAULT 7,
    maintenance_start_hour_utc  smallint NOT NULL DEFAULT 18,
    maintenance_duration_min    integer NOT NULL DEFAULT 120,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now(),
    updated_by                  uuid,
    version                     integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_tenant_release_preferences_tenant UNIQUE (tenant_id),
    CONSTRAINT ck_tenant_release_preferences_ring CHECK (ring IN ('host_sandbox', 'early_adopter', 'general')),
    CONSTRAINT ck_tenant_release_preferences_day CHECK (maintenance_day BETWEEN 1 AND 7),
    CONSTRAINT ck_tenant_release_preferences_hour CHECK (maintenance_start_hour_utc BETWEEN 0 AND 23),
    CONSTRAINT ck_tenant_release_preferences_duration CHECK (maintenance_duration_min BETWEEN 30 AND 480),
    CONSTRAINT fk_tenant_release_preferences_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);

CREATE TABLE tenantadm.platform_releases (
    id              uuid PRIMARY KEY,
    release_version text NOT NULL,
    title           varchar(200) NOT NULL,
    notes           text NOT NULL,
    disruptive      boolean NOT NULL DEFAULT false,
    status          text NOT NULL DEFAULT 'planned',
    current_ring    text,
    created_by      uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    version         integer NOT NULL DEFAULT 1,
    CONSTRAINT uq_platform_releases_version UNIQUE (release_version),
    CONSTRAINT ck_platform_releases_status CHECK (status IN ('planned', 'rolling_out', 'completed')),
    CONSTRAINT ck_platform_releases_ring CHECK (current_ring IS NULL OR current_ring IN ('host_sandbox', 'early_adopter', 'general'))
);
GRANT SELECT, INSERT, UPDATE ON tenantadm.platform_releases TO crm_app;

CREATE TABLE tenantadm.release_rollouts (
    id              uuid PRIMARY KEY,
    release_id      uuid NOT NULL,
    tenant_id       uuid NOT NULL,
    ring            text NOT NULL,
    scheduled_for   timestamptz NOT NULL,
    status          text NOT NULL DEFAULT 'scheduled',
    completed_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_release_rollouts_release_tenant UNIQUE (release_id, tenant_id),
    CONSTRAINT ck_release_rollouts_status CHECK (status IN ('scheduled', 'completed')),
    CONSTRAINT fk_release_rollouts_release FOREIGN KEY (release_id) REFERENCES tenantadm.platform_releases (id) ON DELETE CASCADE,
    CONSTRAINT fk_release_rollouts_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_release_rollouts_tenant ON tenantadm.release_rollouts (tenant_id, scheduled_for DESC);
CREATE INDEX ix_release_rollouts_due ON tenantadm.release_rollouts (status, scheduled_for);

-- RLS for every tenant-scoped control-plane table.
DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'tenant_configs', 'tenant_feature_flags', 'tenant_quotas', 'usage_meters', 'tenant_branding',
        'sender_domains', 'tenant_database_connections', 'provisioning_runs', 'provisioning_steps',
        'isolation_checks', 'support_grants', 'tenant_baselines', 'tenant_keys', 'tenant_exports',
        'tenant_backups', 'tenant_restores', 'destruction_certificates', 'tenant_release_preferences',
        'release_rollouts'
    ]
    LOOP
        EXECUTE format('ALTER TABLE tenantadm.%I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('ALTER TABLE tenantadm.%I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format('CREATE POLICY %I ON tenantadm.%I USING (shared.rls_tenant_visible(tenant_id)) WITH CHECK (shared.rls_tenant_visible(tenant_id))',
                       t || '_isolation', t);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON tenantadm.%I TO crm_app', t);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON tenantadm.%I (tenant_id)', 'ix_' || t || '_tenant_id', t);
    END LOOP;
END
$$;
