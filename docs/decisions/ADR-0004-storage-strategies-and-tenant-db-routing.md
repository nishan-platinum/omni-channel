# ADR-0004 — Storage strategies, tenant database routing and `isolation_mode`

* Status: Accepted · Date: 2026-10-06

## Context
OCC-M01-R008: Standard = shared schema with tenant_id; Premium = schema-per-tenant; Regulated =
database-per-tenant pinned to a region. The `isolation_mode` field lists only `row_level |
schema_per_tenant`. The programme additionally requires PostgreSQL **and** MySQL for dedicated tenant
databases.

## Decision
* `StorageStrategy` = `shared_row_level` | `schema_per_tenant` | `dedicated_database`, derived from the
  plan tier (Standard / Premium / Regulated). `isolation_mode` is exposed with the extra value
  `database_per_tenant` (superset of the field rule, required by R008).
* `DatabaseEngine` = `postgres` | `mysql` (only for `dedicated_database`; shared/schema are PostgreSQL).
* The central DB stores a **connection profile** per tenant (`tenantadm.tenant_database_connections`):
  engine, strategy, target name, host, port, database, schema, region, `secret_ref`, status, last check.
  No passwords are stored; `secret_ref` is resolved by `SecretResolver` (env adapter restricted to
  `TENANT_DB_*` variables).
* Dedicated targets come from a server-side catalogue (`config/tenant-db-targets.toml`). The SA picks a
  target by name; the target's region must equal the tenant region (residency pinning, else
  VALIDATION_FAILED). HTTP input never carries host/port/credentials.
* Provisioning creates the tenant's store: shared → canary row in `tenant_data` (RLS); premium → schema
  `tn_<uuid-hex>` in the central cluster (owner pool for DDL); dedicated → database `tn_<uuid-hex>` on
  the target server + tenant schema migrations.
* `TenantConnectionRouter` maps a tenant to an `Arc<dyn TenantDataStore>`; pools for dedicated targets
  are created lazily with short connect timeouts, so one unavailable tenant DB never affects readiness.
* M01 owns no CRM business tables. The data plane holds only M01's **isolation canary** table, which is
  what the post-provision isolation smoke test (UJ-19 E1, R009) and the export/purge use. Future
  modules plug their own data through the same router.

## Consequences
Engine-specific SQL lives only in `infrastructure/tenant_data/{shared_pg,schema_pg,dedicated_pg,
dedicated_mysql}.rs`. MySQL isolation relies on the dedicated database boundary plus `tenant_id` checks
— there is no RLS in MySQL, and the docs say so.
