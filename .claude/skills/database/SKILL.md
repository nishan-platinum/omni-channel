---
name: database
description: Procedure for schema changes, migrations, RLS policies, repositories and tenant data-store adapters (PostgreSQL control plane, shared/schema/dedicated PostgreSQL, dedicated MySQL).
---

# Database change procedure

## Where things live
* `migrations/control/` — central PostgreSQL (control plane + shared tenant data plane). Run
  automatically at startup with the **owner** role (`MIGRATION_DATABASE_URL`).
* `migrations/tenant/postgres/`, `migrations/tenant/mysql/` — tenant data-plane schema applied by the
  provisioning adapters to schema-per-tenant schemas and dedicated databases.
* Repositories: `src/modules/m01_tenancy/infrastructure/persistence/`.
* Tenant data stores: `src/modules/m01_tenancy/infrastructure/tenant_data/`.

## Steps
1. **Write a new forward-only migration** `NNNN_description.sql` (never edit an applied one — SQLx
   checksums will fail). Follow DBS-001…012: plural snake_case tables in schemas `tenantadm`,
   `identity`, `shared`, `tenant_data`; PK `id uuid` (UUID v7 generated in Rust); `tenant_id uuid NOT NULL`
   as first FK on tenant-scoped tables; `created_at/updated_at timestamptz NOT NULL DEFAULT now()`,
   `version integer NOT NULL DEFAULT 1` where rows are edited; names `ix_`, `uq_`, `fk_`, `ck_`;
   CHECK-constrained text, never PG enums; no `ON UPDATE CASCADE`.
2. **RLS for every tenant-scoped table**: `ENABLE` + `FORCE ROW LEVEL SECURITY` and a policy using
   `shared.rls_tenant_visible(tenant_id)` (control plane, platform scope allowed) or
   `shared.rls_tenant_only(tenant_id)` (data plane, platform scope NOT allowed). Grant the needed DML to
   `crm_app` only.
3. **Repository code** must obtain connections through `platform::db::scoped_tx(&pool, &scope)`.
   Use `sqlx::query`/`query_as` with bind parameters. Dynamic identifiers only via
   `platform::db::safe_ident` (validated `^[a-z][a-z0-9_]{0,62}$`, server-generated).
4. **Engine differences** (Postgres vs MySQL) stay inside the `TenantDataStore` implementations. Never
   branch on engine in application or domain code. MySQL has no RLS — filter by `tenant_id` and rely on
   the dedicated-database boundary.
5. **Secrets**: connection profiles store `secret_ref` (`env:TENANT_DB_*`), never passwords.
6. **Test**: add/extend `tests/postgres` (RLS with the non-bypass `crm_app` role), `tests/mysql`, and
   `tests/isolation` when tenant-scoped data changes. Run `cargo test --all-features` with the Docker
   databases up.
