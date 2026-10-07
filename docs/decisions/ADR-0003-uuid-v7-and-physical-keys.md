# ADR-0003 — UUID v7 primary keys and physical PK naming

* Status: Accepted · Date: 2026-10-06

## Context
The M01 field dictionary and Part A §7.1 say "UUID v4"; the database standard DBS-002 (Part G) says
UUID **v7** generated application-side, BIGSERIAL for append-only logs. DBS-001 says the PK column is
`id`, while the M01 business model calls it `tenant_id`.

## Decision
* All persisted primary keys are UUID v7 generated in Rust (`uuid::Uuid::now_v7`). DBS-002 is the newer
  cross-cutting physical standard and ranks above the carried-over field-dictionary text.
* `shared.audit_log` uses `bigserial` (DBS-002 exception for append-only logs).
* Physical PK is `tenantadm.tenants.id`; every tenant-scoped child table carries `tenant_id` as FK.
  The domain/API exposes it as `tenant_id` (e.g. `POST /v1/tenants` → `{"tenant_id": …}`).
* Reference plan fixtures use fixed v7-shaped UUIDs so they are stable across environments.
