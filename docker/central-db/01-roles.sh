#!/bin/sh
# Creates the runtime role used by the application. It is NOT the owner of any table and has
# NOBYPASSRLS, so PostgreSQL row-level security applies to every query it runs (ADR-0005).
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'crm_app') THEN
    CREATE ROLE crm_app LOGIN NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '${CRM_APP_PASSWORD}';
  ELSE
    ALTER ROLE crm_app LOGIN NOBYPASSRLS NOSUPERUSER PASSWORD '${CRM_APP_PASSWORD}';
  END IF;
END
\$\$;
GRANT CONNECT ON DATABASE "${POSTGRES_DB}" TO crm_app;
EOSQL
