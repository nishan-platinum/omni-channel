---
name: testing
description: How to write and run the test layers (domain unit, application/API integration, tenant isolation, PostgreSQL RLS, MySQL adapter, smoke, benchmark) for the M01 Rust project.
---

# Testing procedure

## Prerequisites
`docker compose up -d central-db tenant-pg tenant-mysql` (ports 55432, 55433, 53306). The test
harness (`tests/common/mod.rs`) reads `TEST_*` variables with defaults matching docker-compose.

## Layers
| Layer | Location | Notes |
|---|---|---|
| Domain unit | `#[cfg(test)]` in `src/modules/m01_tenancy/domain/*` | pure, fast, `cargo test --lib` |
| Application + HTTP/API | `tests/m01/` | builds the real router with `tower::ServiceExt::oneshot` |
| Isolation (release blocker) | `tests/isolation/` | cross-tenant read/update/delete, id tampering, body/query tenant_id, missing context, suspended/terminated, SA elevation + audit |
| PostgreSQL | `tests/postgres/` | RLS via the non-bypass `crm_app` role, optimistic locking, uniqueness, schema-per-tenant, dedicated PG |
| MySQL | `tests/mysql/` | dedicated MySQL connectivity, migrations, tenant routing/boundary |
| Smoke | `scripts/smoke_test.sh` | real HTTP against the running stack |
| Benchmark | `scripts/benchmark.sh` | see `docs/architecture/performance-testing.md` |

## Rules
1. Each test creates its own tenants with unique codes (`common::unique_code`) so tests run in parallel
   without resetting the database.
2. Name tests after the rule they prove and reference the ID in a comment (e.g. `// BR-M01-001`).
3. Every requirement test is listed in `docs/requirements/m01-traceability.md`.
4. Never weaken an isolation assertion to make a test pass; fix the code.
5. Run before reporting:
   ```bash
   cargo fmt --check
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test --all-features
   ```
   Report exact pass/fail counts. Skipped or ignored tests are not passes.
