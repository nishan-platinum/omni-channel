# DESIGN — M01 Multi-Tenancy & Tenant Management (Rust)

Living design document. Decisions with alternatives are recorded as ADRs in `docs/decisions/`.

## 1. Purpose

A working, measurable Rust implementation of **M01 — Multi-Tenancy & Tenant Management** from the
*TM CPaaS Omni Channel CRM — Unified Functional & Design Specification v2.0*, built so that correctness,
performance, resource usage and developer experience can be compared with implementations in other
languages. Scope: all M01 P1 and P2 requirements (27; see `docs/requirements/`). Every other module
(M02–M40) is represented only by ports and labelled reference adapters.

## 2. Stack (ADR-0001)

| Concern | Choice | Notes |
|---|---|---|
| Language / runtime | Rust stable, Tokio | `#![forbid(unsafe_code)]` |
| HTTP | Axum 0.8 + Tower / tower-http | middleware = Tower layers; auth = Axum extractors |
| HTML | Askama 0.14 (compile-time templates, auto-escaping) + HTMX 2 (vendored) | no SPA, no npm |
| Data | SQLx 0.8 (PostgreSQL + MySQL), runtime-checked queries | builds without a live DB; migrations embedded |
| Serialisation | Serde | |
| Observability | tracing + tracing-subscriber (pretty / JSON) | correlation id per request |
| Crypto | argon2 (Argon2id), aes-gcm, hkdf/hmac, sha2 | |

**Why server-side rendering:** the admin UI is form-centric; SSR keeps authorization, validation and CSRF on
the server, needs no build pipeline, keeps the binary self-contained (templates compiled in), and makes
behaviour easy to test with plain HTTP. HTMX adds partial updates (feature toggles, directory filtering,
branding preview) as progressive enhancement — every form also works without JavaScript.

## 3. Architecture: modular monolith

```
src/
  main.rs                  config → tracing → state → background workers → serve
  app.rs                   dependency injection, router, background workers
  web_support.rs           layout context, flash, HTML error pages, cookies
  platform/                cross-cutting: config, db (scoped_tx), errors, events (outbox), audit,
                           middleware, observability, ratelimit, security, time, idempotency
  bootstrap_auth/          temporary M02 stand-in: users, sessions, invitations, extractors, CSRF
  modules/m01_tenancy/
    domain/                pure rules: tenant + state machine, ids, storage, plan, features, config,
                           quota, branding, support, sandbox, baseline, release, keys, analytics, events
    application/           use cases + ports (repository and external-module traits)
    infrastructure/        persistence (SQLx), tenant_data (router + PG/MySQL stores), adapters, gate
    web/                   /v1 API, Super Admin UI, Tenant Admin UI, shared page builders, view models
templates/ static/ migrations/{control,tenant} config/ tests/ scripts/ docs/
```

Dependency rules (enforced by review and module visibility; see AGENTS.md §4): domain depends on nothing
framework-related; application depends on domain + port traits; infrastructure implements ports; web
calls application services only (no SQL in handlers); `bootstrap_auth` talks to M01 only through
`IdentityPort` (implemented by bootstrap_auth) and `TenantGate` (implemented by M01) — dependency inversion
in both directions so M02 can replace it.

## 4. Domain model

| Concept | Type | Key invariants |
|---|---|---|
| Tenant | `domain::tenant::Tenant` | lifecycle via `plan_transition` only; `ensure_config_writable` (grace/terminated/purged read-only) |
| TenantId / TenantCode | `domain::ids` | UUID v7; code `^[a-z0-9-]{3,32}$`, unique, immutable, suggestion from name |
| TenantStatus | enum | 7 register transitions; everything else CONFLICT |
| Region, Tier, StorageStrategy, IsolationMode, DatabaseEngine | `domain::storage` | tier → strategy (R008); MySQL `supports_rls() == false` |
| Plan (M19 view) | `domain::plan` | entitlements ⊇ enabled features |
| Features | `domain::features` | 22 module/channel/AI/app features, owner module, dependency (WhatsApp ⇒ BSP verified) |
| Configuration | `domain::config` | 15 typed namespaced keys, editor permission, tier restriction, cross-key rule |
| Quota | `domain::quota` | 11 metrics, static/monthly/hourly/per-minute; warn at threshold once per cycle; block above 100 % |
| Branding | `domain::branding` | hex colours, FQDN, email, content-validated PNG/SVG ≤ 2 MB, activation needs verified domain |
| SupportGrant | `domain::support` | 15–480 min (default 240), approval starts window, named approver, Regulated rules |
| Hierarchy | `domain::hierarchy` | depth ≤ 3, no cycles (incl. subtree height on re-parenting) |
| Sandbox | `domain::sandbox` | only for active production tenants, ≤ 3, no nested sandboxes |
| Baseline | `domain::baseline` | strict schema (`deny_unknown_fields`), canonical JSON + SHA-256, diff |
| Release / window | `domain::release` | ring order, next maintenance window computation |
| Keys | `domain::keys` | BYOK only Regulated, 90-day rotation |
| Analytics | `domain::analytics` | k-anonymity 3 |
| Events | `domain::events::TenantEvent` | `tenant.<action>` names |

## 5. Control plane vs tenant data plane

* **Central PostgreSQL (control plane, Scicom-owned):** schemas `tenantadm` (registry, config, flags,
  quotas, usage meters, branding, sender domains, DB connection profiles, provisioning runs/steps,
  isolation checks, support grants, baselines, keys, exports, backups/restores, destruction certificates,
  release preferences/rollouts, reference plans/templates), `identity` (bootstrap auth), `shared` (audit log,
  event outbox, consumptions, notification outbox, idempotency keys), `tenant_data` (Standard tier shared
  data plane). Normalised tables with proper columns; JSONB only for typed config values, plan entitlements
  and baseline documents.
* **Tenant data plane:** M01 owns no CRM business tables. Each tenant's store holds M01's
  `isolation_canaries` table used by the isolation smoke test, export and purge; future modules plug
  their tables into the same routing.

### Storage strategies and routing (ADR-0004)

| Tier | Strategy | Where | Isolation |
|---|---|---|---|
| Standard | `shared_row_level` | `tenant_data.*` in the central cluster | `tenant_id` + RLS (`rls_tenant_only`) |
| Premium | `schema_per_tenant` | schema `tn_<uuid-hex>` in the central cluster (owner pool creates it) | schema + RLS |
| Regulated | `dedicated_database` | database `tn_<uuid-hex>` on a catalogue target (PostgreSQL **or** MySQL), region-pinned | dedicated DB + per-tenant runtime login; RLS too on PostgreSQL; **no RLS on MySQL** |

`TenantConnectionProfile` (`tenantadm.tenant_database_connections`) stores engine, strategy, target, host,
port, database, schema, region, `secret_ref`, status and last check — never a password.
`DataPlaneRouter` (implements `TenantDataRouter`) resolves a profile into an `Arc<dyn TenantDataStore>`
(`PgTenantStore` or `MySqlTenantStore`), caches it per tenant, creates dedicated pools lazily with short
timeouts (a tenant DB outage never affects `/ready`). Engine branching exists only inside this module.
Dedicated targets come from `config/tenant-db-targets*.toml`; secrets from `env:TENANT_DB_*` references
via `EnvSecretResolver`; per-tenant runtime passwords are derived by HMAC from a seed secret (stand-in for
vault dynamic credentials) so nothing tenant-specific is stored.

## 6. Tenant context and isolation (ADR-0005)

1. **Who:** the principal comes from a server-side session (cookie, browser) or opaque bearer token
   (API), both SHA-256-hashed in `identity.sessions`. The tenant is the user's `tenant_id` — never request
   input. TA routes (`/tenant/*`) contain no tenant id; `/v1/tenants/{id}` with a foreign id → 403 +
   `security.cross_tenant_attempt` audit row; request DTOs use `deny_unknown_fields`.
2. **Authorize:** `M01Deps::authorize(actor, tenant, access, audit_elevated)` → `AccessScope`:
   `Tenant(id)` for TA, `Platform` for SA (explicit, audited as `platform.elevated_access`), `System` for jobs.
3. **Enforce in the DB:** every query runs in `scoped_tx`, which sets `app.scope` / `app.tenant_id` with
   `set_config(…, true)` (transaction-local → cleared on COMMIT/ROLLBACK, no leakage through the pool).
   Runtime role `crm_app` is `NOBYPASSRLS` and not the owner; all tenant-scoped tables have ENABLE + FORCE RLS.
   Missing context → no rows / inserts rejected.
4. **Data plane vs host:** control-plane policy `rls_tenant_visible` lets platform/system scope see M01
   metadata; data-plane policy `rls_tenant_only` never does. Host access to tenant data requires an active
   break-glass grant (ADR-0009), which opens a tenant scope for that read and audits its use.
5. **MySQL:** dedicated database + per-tenant login granted only on that database + store bound to one
   tenant (repository guard) + server-side routing.
6. **Proof:** isolation smoke test per tenant (gates activation, UJ-19 E1) and `tests/isolation` in CI.

## 7. Bootstrap authentication (ADR-0007)

Argon2id (19 MiB, t=2) on the blocking pool behind a semaphore; lockout after 5 failures (15 min);
dummy verification for unknown users; browser session cookie `occ_session` (HttpOnly, SameSite=Lax,
Secure outside development), idle timeout from tenant policy (default 30 min), 12 h absolute; synchronizer
CSRF token per session (`_csrf` field or `X-CSRF-Token` header for HTMX); login/invitation forms use a
double-submit cookie. API bearer tokens (1 h) from `POST /v1/bootstrap/token`; the API never accepts
cookies. Tenant gate on every request: draft → 403, suspended → 403 TENANT_SUSPENDED, terminated/purged → 403,
grace → read-only. The Super Admin is seeded from `BOOTSTRAP_SUPERADMIN_*`. Invitations are single-use
(72 h), delivered via `NotificationPort` (visible in the development-only `/dev/outbox`).

## 8. Lifecycle (ADR-0002) and offboarding (ADR-0010)

Register state machine with guards (activation requires completed provisioning + passed isolation test;
suspension/grace require reasons; recovery only before `grace_until`; purge only after `purge_after` and
without legal hold). Side effects: session revocation (suspend policy `revoke`/`drain`), NT-001, encrypted
export on grace and termination, scheduler (`LIFECYCLE_SCHEDULER_*`) for grace expiry and auto-purge.
Purge = data plane delete/drop + object storage prefix delete + identity purge + one control-plane
transaction (child rows deleted, keys crypto-shredded, tombstone row, destruction certificate with
SHA-256 manifest, audit, `tenant.purged`). The R014 vocabulary (Provisioning/Offboarding/Archived) is
displayed as a derived label.

## 9. Provisioning saga

`POST /v1/tenants` → validation (all field errors at once) → plan/template/parent/hierarchy/code checks →
one transaction (tenant, config defaults + template config, plan-filtered flags, plan quotas, branding,
release prefs, connection profile, data key, provisioning run, audit, `tenant.created` outbox) → recorded
steps: initial Tenant Admin + invitation, data store provisioning, template packs, isolation smoke test →
`provisioning_status` completed/failed. Failed drafts can be retried (idempotent steps) or discarded
(compensating delete incl. schema/database drop) — §58 "rollback provisioning".

## 10. Events, notifications, audit

* Domain events: typed `TenantEvent` → `EventEnvelope` written to `shared.event_outbox` in the same
  transaction as the change (transactional outbox). `OutboxDispatcher` (every 2 s) delivers to in-process
  handlers with idempotency (`shared.event_consumptions`), retry and dead-lettering after 5 attempts.
  A broker (Kafka/RabbitMQ) can replace the dispatcher without touching producers.
* Notifications: `NotificationConsumer` maps `tenant.suspended`/`quota_warning`/`quota_exhausted`/`purged`/
  `isolation_check_failed` to NT-001/NT-002/NT-003/NT-019/ops alert via `NotificationPort`.
* Audit: `shared.audit_log` (bigserial, append-only via trigger, SHA-256 hash chain, RLS); written in the
  same transaction as the change; security events flagged. Never contains secrets.

## 11. API (ADR-0006)

Spec endpoints keep the register contract inside the API-007 envelope `{data, meta}` /
`{error:{code,message,details,correlation_id}}`. Documented extensions: `GET /v1/tenants`,
`POST /v1/tenants/{id}/quota/consume`, `GET /v1/tenants/{id}/usage/statement`,
`POST /v1/tenants/{id}/isolation-check`, `POST /v1/reference/metering/{id}` (M21 stand-in),
`POST /v1/bootstrap/token` (M02 stand-in). API-003: `X-Request-Id` echoed, `X-Correlation-Id`,
`X-RateLimit-*` for tenant principals, `Idempotency-Key` on `POST /v1/tenants` (24 h replay, payload hash).
Errors: unified catalogue codes/statuses; internal causes logged with correlation id only.

## 12. UI (Askama + HTMX)

`layouts/base.html` + `tenants/_tabs.html` + one template per tab shared by Super Admin (`/admin/tenants/{id}/…`)
and Tenant Admin (`/tenant/…`) via `web/shared.rs::Ctx`. POST/redirect/GET with a 30-second flash cookie;
validation re-renders with per-field errors and preserved input (STD-001); `data-confirm` on destructive
actions (STD-005); empty states (STD-006); status badges; accessible labels, skip link, focus styles.
Tenant theme tokens are served from `/theme.css` (validated hex). Strict CSP (`script-src 'self'`),
no inline scripts; HTMX `allowEval`/`allowScriptTags` disabled.

## 13. Security summary

Argon2id, opaque hashed session tokens, HttpOnly/SameSite cookies, CSRF (synchronizer + double submit),
role extractors on every route, tenant authorization in every service, RLS with a non-bypass role,
parameterised SQL (identifiers only from validated server-generated names), Askama escaping, content-validated
uploads with sandboxed SVG serving, path-traversal-safe object keys, secrets only via `TENANT_DB_*`
references, no credentials in logs/UI/API (`AppConfig` redacts), security headers (CSP, nosniff,
frame-ancestors none, Referrer-Policy, HSTS when secure), per-tenant API rate limit, bounded password
hashing, append-only tamper-evident audit, development-only features disabled in production.

## 14. Observability

`tracing` with pretty or JSON output; per-request access log (method, path — never the query string —,
status, latency, tenant id, actor id, correlation id; switch off with `ACCESS_LOG=false`); per-tenant
rolling error-rate metrics; `/health` (liveness) and `/ready` (central DB only).

## 15. Migrations

Control-plane migrations (`migrations/control`, embedded with `sqlx::migrate!`) run at startup with the
owner role (`RUN_MIGRATIONS=true`; idempotent; SQLx advisory lock). Tenant data-plane DDL
(`migrations/tenant/{postgres,mysql}`, embedded) is applied by the router during provisioning.
Forward-only; never edit an applied migration.

## 16. Testing and benchmarking

See `.claude/skills/testing/SKILL.md` and `docs/architecture/performance-testing.md`. Layers: domain unit
tests (86 incl. platform), application/API/UI integration tests (`tests/m01`, 44), isolation suite
(`tests/isolation`, 11, release blocker), PostgreSQL (`tests/postgres`, 6), MySQL (`tests/mysql`, 2),
end-to-end smoke (`scripts/smoke_test.sh`, 47 checks), benchmark (`scripts/benchmark.sh`). Tests use a
`ManualClock` to exercise time-boxed rules (grants, grace, retention, quota cycles).

## 17. Reconciliations / deviations from the specification

| Topic | Decision | ADR |
|---|---|---|
| Laravel/Angular | Rust/Axum/Askama | 0001 |
| Two lifecycles (R014 vs register) | register states + `purged`; R014 as derived label | 0002 |
| UUID v4 vs DBS-002 UUID v7 | v7; PK `id`, API `tenant_id` | 0003 |
| `isolation_mode` values | + `database_per_tenant`; MySQL dedicated adapter | 0004 |
| VALIDATION_FAILED 400 vs VAL-1xxx 422 | unified catalogue (400) inside API-007 envelope | 0006 |
| Cross-tenant id: 403 (M01-F02) vs 404 (TC-CAS-040) | 403 + security audit for `/v1/tenants/{id}`; nested records invisible via RLS (404) | 0006 |
| OAuth2/JWT (API-002) | bootstrap opaque bearer tokens until M02 | 0007 |
| `shared.audit_log` monthly partitioning (DBS-008) | not partitioned in the prototype; BRIN index on `created_at` | — |
| Audit table RLS | ENABLE (not FORCE) so the owner-run hash-chain trigger sees the whole chain | 0005 |
| Retention/grace durations | configurable hours; development default retention 0 h / auto-purge off | 0010 |
| Email sender error | DOMAIN_NOT_VERIFIED (403) per Part D catalogue | — |
| Custom domain activation | explicit `custom_domain_active` flag; activation of unverified → 409 "Domain not verified" | — |

## 18. Known limitations

* Reference adapters only for M02/M19/M21/M23/M25/M29/M30/M06 DNS+TLS/KMS/object storage/backups/CI-CD (see
  traceability §9). They are labelled in code, UI and docs.
* In-process rate limiter and API-call buffer are per instance (multi-pod would use Redis).
* Audit inserts are serialised by the hash chain (throughput ceiling on audited paths).
* No MG/AG roles (bootstrap auth has Super Admin + Tenant Admin only).
* Tenant data plane contains only the M01 isolation canary — real module data is future work.
* Managed TLS, real DNS lookups, email delivery, PDF letterheads are not implemented.

## 19. Replacement points

`IdentityPort` + `TenantGate` + extractors (→ M02), `PlanCatalog` (→ M19), metering endpoint + `QuotaService`
callers (→ M21 and channel modules), `DownstreamProvisioningPort` (→ M23 etc.), `NotificationPort`
(→ M25), `OutboxDispatcher` (→ event bus), `AuditSink` / `platform::audit` (→ M30), `DomainVerificationPort`
& `EmailSenderVerificationPort` (→ M06/DNS/TLS), `KeyManagementPort` (→ KMS/HSM), `ObjectStoragePort`
(→ S3-class store), `ReleaseManagementPort` (→ CI/CD), `AnonymisedDataCopyPort` (→ M29/M38),
`ExportParticipant` registry (→ every data-owning module), `EnvSecretResolver` (→ vault).
