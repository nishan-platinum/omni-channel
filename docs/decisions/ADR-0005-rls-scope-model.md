# ADR-0005 — Tenant context, RLS scope model and database roles

* Status: Accepted · Date: 2026-10-06

## Decision
* Two PostgreSQL roles: `crm_owner` (owns objects, runs migrations) and `crm_app` (runtime, `LOGIN
  NOBYPASSRLS`, not owner). Every tenant-scoped table has `ENABLE` and `FORCE ROW LEVEL SECURITY`.
* Every repository call runs in a transaction opened by `platform::db::scoped_tx`, which executes
  `set_config('app.scope', …, true)` and `set_config('app.tenant_id', …, true)` — transaction-local, so
  commit/rollback clears it and pooled connections never carry a stale tenant.
* `AccessScope` (Rust): `Tenant(TenantId)`, `Platform { actor }` (Super Admin, audited), `System`
  (internal jobs: login lookup, scheduler, outbox), each mapped to `app.scope` = `tenant|platform|system`.
* Policy helpers:
  * `shared.rls_tenant_visible(t)` — control-plane tables: tenant scope sees only its rows; platform and
    system scopes see all rows (host operations on M01 metadata).
  * `shared.rls_tenant_only(t)` — data-plane tables: **only** a matching tenant scope. Platform scope
    sees nothing (host cannot read tenant business data without break-glass, R024).
  * No `app.scope` → false → no rows, inserts rejected (fail closed).
* Application checks remain the first line (TenantContext comparison, 403 + security audit); RLS is
  defense in depth and independently tested with the `crm_app` role.
