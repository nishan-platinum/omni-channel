# CLAUDE.md — TM CPaaS Omni Channel CRM · M01 + M10 hub (Rust)

**Read `DESIGN.md` before changing code. Follow `AGENTS.md` (the full operating contract).**

## What this is
A **Rust** implementation of **M01 — Multi-Tenancy & Tenant Management** plus the **M10 omnichannel hub
gateway slice** (channel adapters → canonical messages → durable ordered store → skill routing → agent and
customer WebSockets, multi-node). Functional source of truth: the confidential PDF in `docs/specification/`
(git-ignored). Extracted requirements: `docs/requirements/m01-requirements.md`,
`docs/requirements/m10-hub-requirements.md`.

## Stack (non-negotiable)
Rust · Axum · Tokio · Askama (server-rendered) · HTMX (small, optional) · SQLx · Serde · Tower ·
tracing · PostgreSQL (control plane + shared/schema tenant data) · PostgreSQL & MySQL (dedicated tenant DBs).

**Forbidden:** PHP, Laravel, Angular, React, Vue, Next.js, NestJS, Node backends, SPA frameworks,
npm build pipelines, `composer.json`, `artisan`, `*.php`. Laravel/Angular mentions in the spec are
historical — translate them (see `docs/decisions/ADR-0001-rust-stack-and-ssr.md`).

## Scope
M01 (P1 + P2) and the M10 hub gateway slice (`src/modules/m10_hub/`, ADR-0011). WhatsApp: real Cloud API
adapter; `WHATSAPP_PROVIDER=meta` (real Meta) or `fake` (fake-meta server, NOT WhatsApp; bulk tests) —
ADR-0013. Voice: **SIMULATED** SBC feed (ADR-0012). Never claim a fake/simulator is real.
Other modules (M02, M13, M19, M21, M23, M25, M29, M30 …) are **ports + clearly-labelled reference
adapters** in `src/modules/m01_tenancy/infrastructure/adapters.rs` (+ `tenant_data/targets.rs`, `gate.rs`).
Never claim a stub or simulator is a production integration.
**Bake-off gateway** (`src/modules/gateway/`, binary `gateway`, ADR-0014): the ScicomCX bake-off contract
(`/ingress/*`, `/conversations`, `/presence`, `/config/reload`, `/healthz`, `/metrics`, `/ws/customer|agent`),
single tenant + shared bearer token by contract, own DB (`migrations/gateway/`), Compose profile `gateway`
(gw1, gw2, HAProxy :8088). Black-box suite `conformance/` (C01–C51), harness `gw_load` + `scripts/gateway_eval.sh`.
It is a reference build, NOT an official candidate (not built from a fresh scaffold under the build protocol).

## Rules that must never be broken
1. **Tenant isolation.** Tenant context comes only from the authenticated server-side session/token.
   Never from query/form/JSON/route input for tenant users. All tenant-scoped DB access goes through
   `platform::db::scoped_tx` (sets `app.scope`/`app.tenant_id` per transaction; RLS fails closed).
   Super Admin cross-tenant access is explicit (`AccessScope::Platform`) and audited.
2. **No SQL in web handlers.** SQL lives in `infrastructure/` only. Domain has no Axum/SQLx/Askama deps.
3. **Traceability.** Any requirement work cites its ID (OCC-M01-Rnnn, OCC-M10-Rnnn, BR-M01-nnn, FD-nnn,
   NT-nnn, FR-ARC-nnn) and updates `docs/requirements/m01-traceability.md` or `m10-hub-traceability.md`.
4. **No secrets** in code, logs, DB rows or API responses (DB passwords via `secret_ref` only).
5. **Hub durability.** Acknowledge a message only after its transaction commits; keep per-conversation
   sequence numbers gap-free; inbound tenant comes only from `hub.channel_endpoints` (never the payload).

## Commands
```bash
cp .env.example .env
docker compose up --build                 # full stack → http://localhost:3000
docker compose up -d central-db tenant-pg tenant-mysql redis   # DBs + Redis only, then:
cargo run                                 # local app against Docker DBs
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features                 # needs the Docker DBs (unit tests: cargo test --lib)
./scripts/smoke_test.sh                   # end-to-end HTTP smoke (incl. hub flow via hub_load) against a running app
./scripts/hub_cluster_test.sh             # 2 hub nodes + nginx: node kill + rolling restart, zero acked loss
./scripts/benchmark.sh                    # M01 API; hub: target/release/hub_load idle|latency (performance-testing.md)
docker compose --profile gateway up -d    # bake-off gateway: gw1, gw2, HAProxy :8088, gateway-db
RUNS=3 ./conformance/run.sh               # bake-off conformance C01–C51 (3 runs in a row)
./scripts/gateway_eval.sh e1|e2|e3|e5|e6|e7|e9   # bake-off eval scenarios (scaled to this machine)
```

## Definition of done
Never claim completion on `cargo build` alone. fmt + clippy (-D warnings) + all tests (including
isolation tests) + Docker startup + smoke test (+ cluster test for hub changes; + `RUNS=3 ./conformance/run.sh`
for gateway changes) must pass. Failed or skipped tests are not "done".
