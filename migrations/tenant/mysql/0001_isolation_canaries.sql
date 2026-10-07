-- Tenant data-plane schema for a dedicated MySQL database (Regulated tier).
-- MySQL has NO row-level security: isolation comes from the dedicated database boundary, the
-- per-database grants of the runtime user, and repository-level tenant_id predicates.
-- Statements are separated by ';' and executed one by one by the MySQL adapter.
CREATE TABLE IF NOT EXISTS tenant_schema_migrations (
    version     INT PRIMARY KEY,
    applied_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
CREATE TABLE IF NOT EXISTS isolation_canaries (
    id          CHAR(36) PRIMARY KEY,
    tenant_id   CHAR(36) NOT NULL,
    marker      VARCHAR(200) NOT NULL,
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_isolation_canaries_tenant_id (tenant_id)
);
INSERT IGNORE INTO tenant_schema_migrations (version) VALUES (1);
