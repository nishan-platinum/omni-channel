# AGENTS.md — Implementation-Agent Operating Contract

This contract binds every human or AI agent changing this repository. `CLAUDE.md` is the short form;
`DESIGN.md` is the architecture; `docs/decisions/` holds the ADRs.

## 1. Sources of truth
* The PDF in `docs/specification/` (*TM CPaaS Omni Channel CRM — Unified Functional & Design
  Specification v2.0*) is the **functional** source of truth. It is confidential: never commit it,
  never paste large portions of it into the repo, never expose its text through the application.
* `docs/requirements/m01-requirements.md` and `docs/requirements/m10-hub-requirements.md` are the
  extracted, paraphrased requirement sets.
* Precedence when material conflicts: (1) unified requirements + cross-cutting registers/standards,
  (2) unified module chapter, (3) field/API/state registers, (4) carried-over narrative,
  (5) historical technology details. Unresolvable conflicts → document in `DESIGN.md`
  (§Reconciliations) **and** an ADR, pick one, keep it easy to change.

## 2. Technology override
Rust + Axum + Tokio + Askama + HTMX + SQLx + Serde + Tower + tracing. PostgreSQL for the control
plane; PostgreSQL/MySQL adapters for dedicated tenant databases. Laravel/PHP/Angular/React/Vue/Node
are forbidden; translate historical references (Laravel middleware → Tower/Axum middleware, Eloquent →
SQLx repositories, Laravel events → typed Rust domain events, Angular → Askama SSR + HTMX).

## 3. Scope
* M01 (all P1 and P2 requirements) and the **M10 hub gateway slice** (ADR-0011): channel adapters,
  canonical messages, durable ordered store, skill routing, presence, agent/customer WebSockets,
  multi-node. Do not build the rest of M10 or M02–M40 without an ADR.
* WhatsApp uses the real Cloud API adapter against real Meta or the **fake-meta** server (ADR-0013);
  label fake-meta "not WhatsApp" everywhere. Voice is a **simulated** SBC feed (ADR-0012). Never log or
  show WhatsApp tokens/secrets (Meta echoes tokens in errors — redact).
* Dependencies on other modules are **ports** (`application/ports.rs`) with **reference adapters**
  (`infrastructure/adapters.rs`). Label every reference adapter as such in code, UI and docs.
* Do not invent business requirements. If something is needed only to make the prototype operable
  (e.g. bootstrap auth, reference plans, the hub demo seed), mark it as an implementation aid in the
  traceability notes.
* Do not change the architecture silently: ADR first.

## 4. Architecture rules
| Layer | May depend on | Must not |
|---|---|---|
| `modules/m01_tenancy/domain`, `modules/m10_hub/domain` | std, serde, chrono, uuid, regex | Axum, Askama, SQLx, HTTP types |
| `modules/m01_tenancy/application` | domain, port traits, `platform` primitives | SQL, HTTP |
| `modules/m01_tenancy/infrastructure` | SQLx, adapters, application ports | HTTP handlers, templates |
| `modules/m01_tenancy/web` | application services, view models | SQL, business rules |
| `platform` | cross-cutting only | module business rules |
| `bootstrap_auth` | platform | M01 domain internals beyond published ports |

* **No SQL in Axum handlers.** No business rules in templates.
* Lifecycle transitions are decided only by `domain::tenant::Tenant::plan_transition` (allowed pairs in `ALLOWED_TRANSITIONS`).
* Repository methods that must be atomic (state + audit + outbox event) execute in one transaction
  inside infrastructure.

## 5. Tenant isolation (release-blocking)
* Tenant identity comes from the authenticated session or bearer token, resolved server-side.
  Tenant users can never select a tenant via query string, form field, JSON body or route parameter;
  TA routes take no tenant id at all, and `/v1/tenants/{id}` rejects any id ≠ the caller's tenant
  with 403 + security audit row.
* All tenant-scoped SQL runs inside `platform::db::scoped_tx(pool, &AccessScope)`, which sets
  transaction-local `app.scope` / `app.tenant_id` (`set_config(..., true)`), so nothing leaks through
  pooled connections. No scope → RLS returns nothing / rejects writes (fail closed).
* Super Admin cross-tenant operations use `AccessScope::Platform` explicitly and are audited.
  Platform scope does **not** grant access to tenant data-plane rows; that needs an active
  break-glass support grant.
* The runtime DB role (`crm_app`) is not the table owner and has `NOBYPASSRLS`. Never run the app
  with the owner/migration role.
* Never accept database connection details from HTTP input. Dedicated DB targets come from the
  server-side catalogue file; credentials from `secret_ref` (environment adapter, `TENANT_DB_*` only).
* MySQL has **no** RLS: isolation there is the dedicated database boundary plus repository-level
  `tenant_id` checks. Never claim otherwise.
* Hub (`hub.*`): FORCE RLS on every table; inbound provider traffic gets its tenant **only** from the
  endpoint registry (`hub.channel_endpoints`), never from payload fields; platform scope sees no hub
  rows; agents only see conversations assigned to them.

## 6. Tests and verification
* Every requirement implemented gets at least one test; trace it in
  `docs/requirements/m01-traceability.md` or `docs/requirements/m10-hub-traceability.md` (status
  vocabulary is defined there).
* Domain rules → unit tests in the domain module. Use cases, HTTP, isolation, PostgreSQL and MySQL →
  integration tests under `tests/` (they need the Docker databases).
* Isolation tests (`tests/isolation`) are release blockers. A failing isolation test is never
  "known flaky".
* Required before claiming completion:
  ```bash
  cargo fmt --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test --all-features
  docker compose up --build   # then ./scripts/smoke_test.sh
  ./scripts/hub_cluster_test.sh   # for hub changes: 2 nodes, node kill, rolling restart
  ```
* Failed or skipped tests cannot be reported as completion. Report exact results.

## 7. Code quality
* Idiomatic stable Rust, safe Rust only. No `unwrap()/expect()` on request paths (allowed in tests and
  for startup configuration with a clear message).
* Explicit error types (`DomainError`, `AppError`); keep causes internal, return safe messages.
* Enums/value types instead of stringly-typed statuses. Parameterised SQL only; identifiers (schema
  names) only from server-generated values validated against a strict regex.
* Askama auto-escaping stays on. No `|safe` on user-controlled data.
* Never log passwords, session tokens, invitation tokens, CSRF tokens, DB passwords or secret values.

## 8. Documentation duties
Any behavioural change updates, as relevant: `DESIGN.md`, `README.md`, ADRs, traceability, skills.
