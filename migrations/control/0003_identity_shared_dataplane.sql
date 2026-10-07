-- Bootstrap identity (ADR-0007), shared platform tables, and the shared tenant data plane.

-- ---------------------------------------------------------------------------------------------
-- identity.* — temporary minimal identity, replaceable by M02.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE identity.users (
    id                  uuid PRIMARY KEY,
    tenant_id           uuid,
    email               citext NOT NULL,
    display_name        varchar(200) NOT NULL,
    role                text NOT NULL,
    status              text NOT NULL DEFAULT 'invited',
    password_hash       text,
    failed_attempts     integer NOT NULL DEFAULT 0,
    locked_until        timestamptz,
    last_login_at       timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    version             integer NOT NULL DEFAULT 1,
    CONSTRAINT ck_users_role CHECK (role IN ('super_admin', 'tenant_admin')),
    CONSTRAINT ck_users_status CHECK (status IN ('invited', 'active', 'disabled')),
    CONSTRAINT ck_users_role_tenant CHECK ((role = 'super_admin' AND tenant_id IS NULL) OR (role = 'tenant_admin' AND tenant_id IS NOT NULL)),
    CONSTRAINT fk_users_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX uq_users_tenant_email ON identity.users (coalesce(tenant_id, '00000000-0000-0000-0000-000000000000'::uuid), email);
CREATE INDEX ix_users_email ON identity.users (email);
CREATE INDEX ix_users_tenant_id ON identity.users (tenant_id);

CREATE TABLE identity.sessions (
    id                  uuid PRIMARY KEY,
    token_hash          bytea NOT NULL,
    user_id             uuid NOT NULL,
    tenant_id           uuid,
    kind                text NOT NULL,
    csrf_token          text NOT NULL,
    scopes              text[] NOT NULL DEFAULT '{}',
    created_at          timestamptz NOT NULL DEFAULT now(),
    last_seen_at        timestamptz NOT NULL DEFAULT now(),
    expires_at          timestamptz NOT NULL,
    idle_timeout_secs   integer NOT NULL,
    revoked_at          timestamptz,
    ip                  text,
    user_agent          text,
    CONSTRAINT uq_sessions_token_hash UNIQUE (token_hash),
    CONSTRAINT ck_sessions_kind CHECK (kind IN ('browser', 'api')),
    CONSTRAINT fk_sessions_user FOREIGN KEY (user_id) REFERENCES identity.users (id) ON DELETE CASCADE,
    CONSTRAINT fk_sessions_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_sessions_user_id ON identity.sessions (user_id);
CREATE INDEX ix_sessions_tenant_id ON identity.sessions (tenant_id) WHERE revoked_at IS NULL;

CREATE TABLE identity.invitations (
    id              uuid PRIMARY KEY,
    user_id         uuid NOT NULL,
    tenant_id       uuid NOT NULL,
    token_hash      bytea NOT NULL,
    expires_at      timestamptz NOT NULL,
    accepted_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_invitations_token_hash UNIQUE (token_hash),
    CONSTRAINT fk_invitations_user FOREIGN KEY (user_id) REFERENCES identity.users (id) ON DELETE CASCADE,
    CONSTRAINT fk_invitations_tenant FOREIGN KEY (tenant_id) REFERENCES tenantadm.tenants (id) ON DELETE CASCADE
);
CREATE INDEX ix_invitations_user_id ON identity.invitations (user_id);
CREATE INDEX ix_invitations_tenant_id ON identity.invitations (tenant_id);

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['users', 'sessions', 'invitations']
    LOOP
        EXECUTE format('ALTER TABLE identity.%I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('ALTER TABLE identity.%I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format('CREATE POLICY %I ON identity.%I USING (shared.rls_tenant_visible(tenant_id)) WITH CHECK (shared.rls_tenant_visible(tenant_id))',
                       t || '_isolation', t);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON identity.%I TO crm_app', t);
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------------------------
-- shared.audit_log — append-only, tamper-evident hash chain (SEC-140/141, §45.4.7).
-- BIGSERIAL per DBS-002 exception. Not partitioned in the prototype (DBS-008 deferred, see DESIGN.md).
-- ---------------------------------------------------------------------------------------------
CREATE TABLE shared.audit_log (
    id              bigserial PRIMARY KEY,
    tenant_id       uuid,
    actor_id        uuid,
    actor_role      text NOT NULL,
    entity_type     text NOT NULL,
    entity_id       text,
    action          text NOT NULL,
    before          jsonb,
    after           jsonb,
    reason          text,
    security_event  boolean NOT NULL DEFAULT false,
    ip              text,
    user_agent      text,
    correlation_id  text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    prev_hash       bytea,
    hash            bytea
);
CREATE INDEX ix_audit_log_tenant_created ON shared.audit_log (tenant_id, created_at DESC);
CREATE INDEX ix_audit_log_entity ON shared.audit_log (entity_type, entity_id);
CREATE INDEX ix_audit_log_correlation ON shared.audit_log (correlation_id);
CREATE INDEX ix_audit_log_created_brin ON shared.audit_log USING brin (created_at);

CREATE OR REPLACE FUNCTION shared.audit_log_chain() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = shared, pg_temp AS $$
DECLARE
    last_hash bytea;
BEGIN
    -- Serialise chain extension so each row links to exactly one predecessor.
    PERFORM pg_advisory_xact_lock(724519001);
    SELECT a.hash INTO last_hash FROM shared.audit_log a ORDER BY a.id DESC LIMIT 1;
    NEW.prev_hash := last_hash;
    NEW.created_at := now();
    NEW.hash := sha256(
        coalesce(last_hash, '\x'::bytea) ||
        convert_to(
            coalesce(NEW.tenant_id::text, '') || '|' || coalesce(NEW.actor_id::text, '') || '|' ||
            NEW.actor_role || '|' || NEW.entity_type || '|' || coalesce(NEW.entity_id, '') || '|' ||
            NEW.action || '|' || coalesce(NEW.before::text, '') || '|' || coalesce(NEW.after::text, '') || '|' ||
            coalesce(NEW.reason, '') || '|' || coalesce(NEW.correlation_id, '') || '|' || NEW.created_at::text,
            'UTF8'));
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION shared.audit_log_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'shared.audit_log is append-only';
END
$$;

CREATE TRIGGER trg_audit_log_chain BEFORE INSERT ON shared.audit_log
    FOR EACH ROW EXECUTE FUNCTION shared.audit_log_chain();
CREATE TRIGGER trg_audit_log_immutable BEFORE UPDATE OR DELETE ON shared.audit_log
    FOR EACH ROW EXECUTE FUNCTION shared.audit_log_immutable();

-- ENABLE (not FORCE): the chain trigger runs as the owner and must see the latest row of the whole
-- chain. crm_app is not the owner, so RLS still applies to every runtime query.
ALTER TABLE shared.audit_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_log_select ON shared.audit_log FOR SELECT
    USING (shared.rls_tenant_visible(tenant_id));
CREATE POLICY audit_log_insert ON shared.audit_log FOR INSERT
    WITH CHECK (shared.rls_tenant_visible(tenant_id)
                OR (tenant_id IS NULL AND current_setting('app.scope', true) IN ('platform', 'system')));
GRANT SELECT, INSERT ON shared.audit_log TO crm_app;
GRANT USAGE ON SEQUENCE shared.audit_log_id_seq TO crm_app;

-- ---------------------------------------------------------------------------------------------
-- Transactional outbox for domain events + idempotent consumers.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE shared.event_outbox (
    id              uuid PRIMARY KEY,
    tenant_id       uuid,
    event_type      text NOT NULL,
    aggregate_id    text NOT NULL,
    payload         jsonb NOT NULL,
    correlation_id  text,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz,
    attempts        integer NOT NULL DEFAULT 0,
    last_error      text,
    CONSTRAINT ck_event_outbox_type CHECK (event_type ~ '^[a-z_]+\.[a-z_]+$')
);
CREATE INDEX ix_event_outbox_pending ON shared.event_outbox (occurred_at) WHERE published_at IS NULL;
CREATE INDEX ix_event_outbox_tenant ON shared.event_outbox (tenant_id, occurred_at DESC);

CREATE TABLE shared.event_consumptions (
    consumer        text NOT NULL,
    event_id        uuid NOT NULL,
    tenant_id       uuid,
    processed_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_event_consumptions PRIMARY KEY (consumer, event_id)
);

-- Notification outbox (REFERENCE ADAPTER for M25).
CREATE TABLE shared.notification_outbox (
    id              uuid PRIMARY KEY,
    tenant_id       uuid,
    notification_id text NOT NULL,
    template_key    text NOT NULL,
    recipient       citext NOT NULL,
    channels        text NOT NULL,
    priority        text NOT NULL,
    subject         text NOT NULL,
    body            text,
    status          text NOT NULL DEFAULT 'delivered_local',
    max_retries     integer NOT NULL DEFAULT 3,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_notification_outbox_priority CHECK (priority IN ('High', 'Normal', 'Low')),
    CONSTRAINT ck_notification_outbox_status CHECK (status IN ('delivered_local', 'failed'))
);
CREATE INDEX ix_notification_outbox_created ON shared.notification_outbox (created_at DESC);
CREATE INDEX ix_notification_outbox_tenant ON shared.notification_outbox (tenant_id, created_at DESC);

-- Idempotency keys (API-003): 24h replay of the original response.
CREATE TABLE shared.idempotency_keys (
    id                  uuid PRIMARY KEY,
    tenant_id           uuid,
    principal_id        uuid NOT NULL,
    idem_key            varchar(200) NOT NULL,
    request_hash        char(64) NOT NULL,
    response_status     integer NOT NULL,
    response_body       jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    expires_at          timestamptz NOT NULL,
    CONSTRAINT uq_idempotency_keys_principal_key UNIQUE (principal_id, idem_key)
);

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['event_outbox', 'event_consumptions', 'notification_outbox', 'idempotency_keys']
    LOOP
        EXECUTE format('ALTER TABLE shared.%I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('ALTER TABLE shared.%I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format($p$CREATE POLICY %I ON shared.%I
                          USING (shared.rls_tenant_visible(tenant_id)
                                 OR (tenant_id IS NULL AND current_setting('app.scope', true) IN ('platform', 'system')))
                          WITH CHECK (shared.rls_tenant_visible(tenant_id)
                                 OR (tenant_id IS NULL AND current_setting('app.scope', true) IN ('platform', 'system')))$p$,
                       t || '_isolation', t);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON shared.%I TO crm_app', t);
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------------------------
-- tenant_data.* — shared (Standard tier) tenant data plane. M01 owns only the isolation canary
-- used by the post-provision isolation smoke test (R009, UJ-19 E1). Platform scope cannot read it.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE tenant_data.isolation_canaries (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    marker          text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_isolation_canaries_tenant_id ON tenant_data.isolation_canaries (tenant_id);
ALTER TABLE tenant_data.isolation_canaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_data.isolation_canaries FORCE ROW LEVEL SECURITY;
CREATE POLICY isolation_canaries_tenant_only ON tenant_data.isolation_canaries
    USING (shared.rls_tenant_only(tenant_id))
    WITH CHECK (shared.rls_tenant_only(tenant_id));
GRANT SELECT, INSERT, DELETE ON tenant_data.isolation_canaries TO crm_app;
