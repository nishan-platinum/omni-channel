# CLAUDE.md — TM CPaaS Omni Channel CRM · M01 (Rust)

**Read `DESIGN.md` before changing code. Follow `AGENTS.md` (the full operating contract).**

## What this is
A **Rust** implementation of **M01 — Multi-Tenancy & Tenant Management** only, built for a
cross-language performance/correctness comparison. Functional source of truth: the confidential PDF in
`docs/specification/` (git-ignored). Extracted requirements: `docs/requirements/m01-requirements.md`.

## Stack (non-negotiable)
Rust · Axum · Tokio · Askama (server-rendered) · HTMX (small, optional) · SQLx · Serde · Tower ·
tracing · PostgreSQL (control plane + shared/schema tenant data) · PostgreSQL & MySQL (dedicated tenant DBs).

**Forbidden:** PHP, Laravel, Angular, React, Vue, Next.js, NestJS, Node backends, SPA frameworks,
npm build pipelines, `composer.json`, `artisan`, `*.php`. Laravel/Angular mentions in the spec are
historical — translate them (see `docs/decisions/ADR-0001-rust-stack-and-ssr.md`).

## Scope
M01 only (P1 + P2). Other modules (M02, M19, M21, M23, M25, M29, M30 …) are **ports + clearly-labelled
reference adapters** in `src/modules/m01_tenancy/infrastructure/adapters.rs` (+ `tenant_data/targets.rs`, `gate.rs`). Never claim a stub is a
production integration.

## Rules that must never be broken
1. **Tenant isolation.** Tenant context comes only from the authenticated server-side session/token.
   Never from query/form/JSON/route input for tenant users. All tenant-scoped DB access goes through
   `platform::db::scoped_tx` (sets `app.scope`/`app.tenant_id` per transaction; RLS fails closed).
   Super Admin cross-tenant access is explicit (`AccessScope::Platform`) and audited.
2. **No SQL in web handlers.** SQL lives in `infrastructure/` only. Domain has no Axum/SQLx/Askama deps.
3. **Traceability.** Any requirement work cites its ID (OCC-M01-Rnnn, BR-M01-nnn, FD-nnn, NT-nnn) and
   updates `docs/requirements/m01-traceability.md`.
4. **No secrets** in code, logs, DB rows or API responses (DB passwords via `secret_ref` only).

## Commands
```bash
cp .env.example .env
docker compose up --build                 # full stack → http://localhost:3000
docker compose up -d central-db tenant-pg tenant-mysql   # DBs only, then:
cargo run                                 # local app against Docker DBs
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features                 # needs the Docker DBs (unit tests: cargo test --lib)
./scripts/smoke_test.sh                   # end-to-end HTTP smoke against a running app
./scripts/benchmark.sh                    # see docs/architecture/performance-testing.md
```

## Definition of done
Never claim completion on `cargo build` alone. fmt + clippy (-D warnings) + all tests (including
isolation tests) + Docker startup + smoke test must pass. Failed or skipped tests are not "done".
