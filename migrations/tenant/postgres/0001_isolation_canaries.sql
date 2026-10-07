-- Tenant data-plane schema for PostgreSQL schema-per-tenant (Premium) and dedicated databases
-- (Regulated). {{schema}} is replaced with a server-generated, regex-validated identifier
-- (tn_<uuid-hex>) — never with user input. {{runtime_role}} likewise comes from server config.
CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.tenant_schema_migrations (
    version     integer PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {{schema}}.isolation_canaries (
    id          uuid PRIMARY KEY,
    tenant_id   uuid NOT NULL,
    marker      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_isolation_canaries_tenant_id ON {{schema}}.isolation_canaries (tenant_id);

-- Defense in depth: the same tenant-only policy as the shared data plane. The context function is
-- created locally so dedicated databases do not depend on the control-plane database.
CREATE OR REPLACE FUNCTION {{schema}}.ctx_allows(t uuid) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT coalesce(current_setting('app.scope', true), '') = 'tenant'
           AND t = nullif(current_setting('app.tenant_id', true), '')::uuid
$$;

ALTER TABLE {{schema}}.isolation_canaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{schema}}.isolation_canaries FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS isolation_canaries_tenant_only ON {{schema}}.isolation_canaries;
CREATE POLICY isolation_canaries_tenant_only ON {{schema}}.isolation_canaries
    USING ({{schema}}.ctx_allows(tenant_id))
    WITH CHECK ({{schema}}.ctx_allows(tenant_id));

GRANT USAGE ON SCHEMA {{schema}} TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION {{schema}}.ctx_allows(uuid) TO {{runtime_role}};
GRANT SELECT, INSERT, DELETE ON {{schema}}.isolation_canaries TO {{runtime_role}};
GRANT SELECT ON {{schema}}.tenant_schema_migrations TO {{runtime_role}};

INSERT INTO {{schema}}.tenant_schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING;
