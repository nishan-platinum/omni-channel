---
name: m01-tenancy
description: Workflow for implementing or changing any M01 Multi-Tenancy & Tenant Management requirement (OCC-M01-Rnnn, BR-M01-nnn, FD-nnn, NT-nnn) in this Rust/Axum codebase, end to end from spec to traceability.
---

# M01 requirement workflow

Use this every time you touch tenant provisioning, isolation, lifecycle, configuration, feature
flags, quotas, branding, support access, sandboxes, releases, keys or analytics.

1. **Identify the requirement ID(s).** Look them up in `docs/requirements/m01-requirements.md` and the
   current row in `docs/requirements/m01-traceability.md`.
2. **Inspect the source specification** (`docs/specification/*.pdf`, confidential, not in git). Extract
   text with `pdftotext -layout` into a scratch directory and read the M01 chapter (printed pages 45–56)
   plus the row for the ID. Do not copy large spec text into the repo.
3. **Inspect related cross-cutting registers**: Part D §53 field dependencies, §54 business rules,
   §57 notifications, §59 CRUD, §60 errors, §61 API register, §62 state machines; Part F (SEC/NFR/STD),
   Part G (DBS/API standards). Check for conflicts — if found, follow the precedence in `AGENTS.md`
   and write an ADR.
4. **Identify the domain behaviour**: invariants, value objects, transitions, errors.
5. **Implement the domain change** in `src/modules/m01_tenancy/domain/` (pure Rust, no Axum/SQLx).
   Add unit tests in the same file.
6. **Implement the application use case** in `src/modules/m01_tenancy/application/`. Orchestrate ports;
   decide audit entries and domain events; never write SQL here.
7. **Implement infrastructure** if persistence or an external port changes:
   `infrastructure/persistence/` (SQLx, scoped transactions) or `infrastructure/adapters.rs` (reference
   adapters, labelled `REFERENCE ADAPTER`). Add migrations under `migrations/control` (see the
   `database` skill).
8. **Implement API/UI** in `src/modules/m01_tenancy/web/` + `templates/` (see the `frontend` skill).
   Spec endpoints under `/v1/tenants…` keep the register contract; extensions are documented in
   `DESIGN.md` §API.
9. **Create tests**: unit (domain), integration/API (`tests/m01`), isolation (`tests/isolation`) when the
   change touches tenant-scoped data (see the `testing` skill).
10. **Update traceability** (`docs/requirements/m01-traceability.md`): implementation file, API/UI, tests,
    status (honest vocabulary), notes, reference adapter.
11. **Run verification**: `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
    `cargo test --all-features`, and `./scripts/smoke_test.sh` against a running stack.

Never: accept tenant ids from tenant users' input, bypass `scoped_tx`, add lifecycle transitions not in
the spec state machine, or mark a requirement "implemented" when only a TODO exists.
