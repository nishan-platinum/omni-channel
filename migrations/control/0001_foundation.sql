-- M01 control plane foundation: schemas, runtime role, RLS helper functions.
-- Runs as the owner role (crm_owner). The runtime role crm_app is NOT the owner and has NOBYPASSRLS
-- (ADR-0005). Docker/CI init scripts create crm_app with LOGIN and a password; this block only makes
-- the migration self-sufficient when the role is missing.

CREATE EXTENSION IF NOT EXISTS citext;

CREATE SCHEMA IF NOT EXISTS tenantadm;   -- tenant registry & M01 configuration (DBS-009)
CREATE SCHEMA IF NOT EXISTS identity;    -- bootstrap auth (temporary M02 stand-in)
CREATE SCHEMA IF NOT EXISTS shared;      -- audit log, outbox, notifications, idempotency
CREATE SCHEMA IF NOT EXISTS tenant_data; -- shared (Standard tier) tenant data plane

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'crm_app') THEN
        CREATE ROLE crm_app NOLOGIN NOBYPASSRLS;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA tenantadm, identity, shared, tenant_data TO crm_app;

-- Tenant context helpers. Context is set per transaction by platform::db::scoped_tx using
-- set_config(..., true). Missing context => false => fail closed.
CREATE OR REPLACE FUNCTION shared.ctx_tenant_id() RETURNS uuid
LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN current_setting('app.scope', true) = 'tenant'
                THEN nullif(current_setting('app.tenant_id', true), '')::uuid
           END
$$;

-- Control-plane visibility: tenant scope sees its own rows; platform (Super Admin, audited) and
-- system (internal jobs) scopes see all M01 metadata.
CREATE OR REPLACE FUNCTION shared.rls_tenant_visible(t uuid) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT CASE coalesce(current_setting('app.scope', true), '')
               WHEN 'platform' THEN true
               WHEN 'system'   THEN true
               WHEN 'tenant'   THEN t IS NOT NULL AND t = shared.ctx_tenant_id()
               ELSE false
           END
$$;

-- Data-plane visibility: ONLY the matching tenant scope. Platform scope never reads tenant business
-- data (break-glass opens a tenant scope after validating an approved grant, ADR-0009).
CREATE OR REPLACE FUNCTION shared.rls_tenant_only(t uuid) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT coalesce(current_setting('app.scope', true), '') = 'tenant'
           AND t IS NOT NULL
           AND t = shared.ctx_tenant_id()
$$;

GRANT EXECUTE ON FUNCTION shared.ctx_tenant_id(), shared.rls_tenant_visible(uuid), shared.rls_tenant_only(uuid) TO crm_app;
