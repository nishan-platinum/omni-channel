# M01 — Multi-Tenancy & Tenant Management: Extracted Requirements

Source: *TM CPaaS Omni Channel CRM — Unified Functional & Design Specification v2.0* (confidential,
`docs/specification/`, git-ignored). This file is an **implementation-oriented extraction**, written in
our own words. It is not a copy of the specification. Spec page numbers refer to the printed footer.

Precedence used when sources disagree (see `DESIGN.md` §Reconciliations and `docs/decisions/`):

1. Unified requirements and explicit cross-cutting standards/registers (Part D, Part F, Part G).
2. Current unified module specification (Part B §10).
3. Field / API / state-machine registers.
4. Carried-over historical narrative (CX 2.0 chapter 30 "TEN").
5. Historical technology details (Laravel/Angular/Eloquent) — **always translated to Rust**.

---

## 1. Capabilities (spec §10.1)

| Feature | Name |
|---|---|
| M01-F01 | Tenant provisioning |
| M01-F02 | Tenant isolation |
| M01-F03 | Tenant lifecycle & suspension |
| M01-F04 | Per-tenant configuration & feature flags |
| M01-F05 | Tenant quotas & metering guardrails |
| M01-F06 | White-label branding |

## 2. Unified functional requirements (spec §10.2) — 27 total (23 × P1, 4 × P2)

| ID | Pri | Source | Feature | Summary (paraphrased) |
|---|---|---|---|---|
| OCC-M01-R001 | P1 | FR-M01-001 | F01 | Create tenant with unique code, region, plan and initial admin; the root record. |
| OCC-M01-R002 | P1 | FR-M01-002 | F02 | All data access scoped to exactly one tenant, enforced at the data layer. |
| OCC-M01-R003 | P1 | FR-M01-003 | F03 | Tenant state machine draft/active/suspended/grace/terminated with side-effects. |
| OCC-M01-R004 | P1 | FR-M01-004 | F04 | Per-tenant channels, features, locale, limits within plan entitlements. |
| OCC-M01-R005 | P1 | FR-M01-005 | F05 | Plan-based limits (users, numbers, channels, monthly volume) with soft warn + hard block. |
| OCC-M01-R006 | P1 | FR-M01-006 | F06 | Logo, colours, custom domain, email sender identity per tenant. |
| OCC-M01-R007 | P1 | FR-TEN-001 | F02 | Every row/file/index/message/log/cache key carries tenant_id; data layer injects scope; nothing returns cross-tenant data. |
| OCC-M01-R008 | P1 | FR-TEN-002 | F02 | Storage per tier: Standard = shared schema + tenant_id; Premium = schema-per-tenant; Regulated = database-per-tenant with residency pinning. |
| OCC-M01-R009 | P1 | FR-TEN-003 | F02 | Automated cross-tenant isolation tests in CI every release; attempts must fail and alert. |
| OCC-M01-R011 | P1 | FR-TEN-005 | F02 | Per-tenant backup/restore/DR without affecting other tenants; tenant RPO/RTO reporting (NFR-007). |
| OCC-M01-R012 | P1 | FR-TEN-006 | F05 | Noisy-neighbour protection: per-tenant quotas/rate limits (API req/min, campaign send rate, report query cost, storage GB, BPM executions/hour) with soft/hard thresholds configurable per tier. |
| OCC-M01-R013 | P1 | FR-TEN-010 | F01 | Provision from a template (module set, roles/teams, BPM, reports, SLA, dropdown packs) in ≤ 30 min, fully automated. |
| OCC-M01-R014 | P1 | FR-TEN-011 | F03 | Lifecycle Provisioning → Active → Suspended → Offboarding → Archived → Purged; every transition audited with reason and actor. (Reconciled — ADR-0002.) |
| OCC-M01-R015 | P1 | FR-TEN-012 | F04 | Module/channel/AI/app-ecosystem entitlements switchable per tenant; disabled ones disappear from navigation, API scope and Studio. |
| OCC-M01-R016 | P1 | FR-TEN-013 | F03 | Offboarding: full export (records, files, audit, config baseline) delivered encrypted; certified purge with destruction certificate after retention. |
| OCC-M01-R018 | P1 | FR-TEN-020 | F04 | Tenant-admin self-service controls bounded to the tenant (UJ-15 scope). |
| OCC-M01-R019 | P1 | FR-TEN-021 | F06 | Branding: logo, colour theme, email footer, portal domain (CNAME + managed TLS), login page, PDF letterhead. |
| OCC-M01-R020 | P1 | FR-TEN-022 | F04 | Locale defaults: timezone, currency, date/number format, UI language set (users override within the allowed set). |
| OCC-M01-R021 | P1 | FR-TEN-023 | F04 | Per-tenant security policy: password/MFA, session timeout, IP allow-list, SSO IdP, API client management (enforcement owned by M02). |
| OCC-M01-R022 | P1 | FR-TEN-024 | F04 | Configuration baseline export/import with diff view; sandbox→production promotion. |
| OCC-M01-R023 | P1 | FR-TEN-030 | F01/F05 | Host console tenant directory: tier, state, health (error rate, queue depth, quota consumption), version, entitlement matrix; host actions audited. |
| OCC-M01-R024 | P1 | FR-TEN-031 | F02 | Break-glass: host access needs tenant-admin-approved, time-boxed grant (default 4h) with full audit; Regulated tier needs named-user approval per incident. |
| OCC-M01-R025 | P1 | FR-TEN-032 | F05 | Usage metering (active users, storage, API calls, emails, AI tokens, channel sessions); monthly statement export; threshold alerts. |
| OCC-M01-R010 | P2 | FR-TEN-004 | F02 | Per-tenant encryption keys (envelope), rotation without downtime, BYOK for Regulated tier. |
| OCC-M01-R017 | P2 | FR-TEN-014 | F04 | ≥1 linked sandbox per production tenant with config copy and anonymised data subset. |
| OCC-M01-R026 | P2 | FR-TEN-033 | F04 | Ring-based releases (host sandbox → early adopters → all); per-tenant maintenance windows honoured for disruptive changes; in-app release notes. |
| OCC-M01-R027 | P2 | FR-TEN-034 | F05 | Host-level anonymised aggregated analytics; never row-level business data; Regulated tenants may opt out. |

Note: numbering skips no IDs — R001–R027 are all present (R010, R017, R026, R027 are P2).

## 3. Entity `tenant` (spec §10.3.1) — field rules

| Field | Rule | Error |
|---|---|---|
| tenant_id | system-generated, immutable PK (spec says UUID v4; DBS-002 says v7 → ADR-0003) | — |
| tenant_code | 3–32 chars `^[a-z0-9-]+$`, unique platform-wide, immutable; auto-suggested from name, editable before create | CONFLICT "Tenant code already in use"; VALIDATION_FAILED "Code must be 3-32 lowercase alphanumeric or hyphen" |
| name | 1–120 chars | VALIDATION_FAILED "Name is required (max 120 chars)" |
| legal_name | 0–200 chars | — |
| region | one of `my-central`, `sg`, `apac`; default `my-central`; cannot change after data exists | VALIDATION_FAILED "Unsupported region" |
| plan_id | must reference an active plan | NOT_FOUND "Plan does not exist" |
| status | draft/active/suspended/grace/terminated (+ purged, ADR-0002); default draft | CONFLICT "Illegal status transition" |
| parent_tenant_id | max hierarchy depth 3; no cycles | CONFLICT "Circular or too-deep hierarchy" |
| primary_admin_email | valid email; becomes first TA; triggers activation invite | VALIDATION_FAILED "Valid admin email required" |
| created_at / activated_at / terminated_at | system UTC; activated_at set on first transition to active; terminated_at starts retention countdown | — |
| isolation_mode | row_level / schema_per_tenant (extended with database_per_tenant, ADR-0004) | — |
| suspended_reason | required when status = suspended; shown to TA | — |
| grace_until | purge-timer target | — |
| config_key / config_value | namespaced (e.g. `channel.whatsapp.enabled`), typed, validated against schema | VALIDATION_FAILED "Unknown config key" |
| feature_flags | subset of plan entitlements | FORBIDDEN "Feature not included in your plan" |
| quota_users | ≥1, from plan; blocks user creation at limit | RATE_LIMITED "User quota reached; upgrade plan" |
| quota_numbers | ≥0 | — |
| quota_volume_month | ≥0, calls+messages per cycle; resets per cycle | — |
| usage_users / usage_volume_month | derived / metered | — |
| soft_threshold | 0–1, default 0.80 | — |
| logo_url | PNG/SVG ≤ 2 MB in object storage | VALIDATION_FAILED "Logo must be PNG/SVG under 2MB" |
| primary_color | `^#[0-9A-Fa-f]{6}$`, default `#0B2130` | VALIDATION_FAILED "Colour must be a 6-digit hex" |
| secondary_color | hex, default `#F26A21` | (same rule applied) |
| custom_domain | FQDN; DNS CNAME verified before served | CONFLICT "Domain not verified" / "Domain already claimed" |
| email_from | sender domain SPF/DKIM verified (M06) | FORBIDDEN / DOMAIN_NOT_VERIFIED "Sender domain not verified" |

Plan entity (owned by M19, §28.3.1): plan_id, entitlements (json), quotas (json), version.

## 4. State machine `tenant.status` (spec §10.4, §62 register)

| From | To | Trigger / side effect |
|---|---|---|
| draft | active | SA activates after review; opens login/API |
| active | suspended | SA or billing; **reason required**; blocks new sessions; data retained |
| suspended | active | reinstate |
| active | grace | non-payment or offboarding; read-only; purge timer starts |
| grace | terminated | grace expires; data scheduled for purge; export offered first; `tenant.terminated` |
| grace | active | recovered before expiry |
| terminated | (purged) | retention window elapses; data irreversibly removed |

Any other transition → CONFLICT (409). Every transition audited with actor, reason, timestamp.

## 5. Business rules (spec §10.7, Part D §54)

| ID | Rule | Testable condition |
|---|---|---|
| BR-M01-001 | Tenant code unique platform-wide and immutable | duplicate code → 409 |
| BR-M01-002 | Suspended tenant blocks new logins and API calls but retains data | login to suspended tenant → 403 |
| BR-M01-003 | Enabling a feature beyond plan entitlement is not permitted | enable non-entitled feature → 403 |
| BR-M01-004 | 80% of quota → warning; 100% → blocked | 101% send → 429 + alert |
| BR-M01-005 | Custom domain cannot be activated until DNS verified | unverified domain → not served |

## 6. Field dependencies (Part D §53)

| ID | Screen | Parent | Condition | Child | Action |
|---|---|---|---|---|---|
| FD-008 | New Tenant | plan_id | selected | feature_flags | filter to plan entitlements |
| FD-009 | New Tenant | parent_tenant_id | set | inheritance_flags | show reseller inheritance options |
| FD-010 | Branding | custom_domain | entered | dns_records | show + Verify button |
| FD-011 | Branding | email_from | entered | spf_dkim_status | show verification |

## 7. Errors (spec §7.5, §10.7, Part D §60)

VALIDATION_FAILED 400 · UNAUTHENTICATED 401 · FORBIDDEN 403 · NOT_FOUND 404 · CONFLICT 409 ·
RATE_LIMITED 429 (Retry-After) · INTERNAL 500 (correlation id) · TENANT_SUSPENDED 403 ·
QUOTA_EXCEEDED 429 · DOMAIN_NOT_VERIFIED 403. Envelope (API-007):
`{"error":{"code","message","details":[],"correlation_id"}}`.

## 8. Notifications (spec §10.7, Part D §57)

| ID | Event | Recipient | Channels | Template | Priority | Retries |
|---|---|---|---|---|---|---|
| NT-001 | Tenant suspended | Tenant Admin | Email + In-app | NT-TENANT-SUSP | High | 3 |
| NT-002 | Quota 80% reached | Tenant Admin | Email + In-app | NT-QUOTA-WARN | Normal | 3 |
| NT-003 | Quota exhausted | Tenant Admin | Email + In-app | NT-QUOTA-FULL | High | 3 |

Related: NT-019 Data purge (M29) — used for the purge notice.

## 9. API surface (spec §10.6, Part D §61, Annex C)

| Method | Path | Scope | Request | Response | Errors |
|---|---|---|---|---|---|
| POST | /v1/tenants | tenants:write | {name, legal_name?, region, plan_id, primary_admin_email, parent_tenant_id?} | 201 {tenant_id, tenant_code, status:'draft', …} | 409, 400, 404 |
| GET | /v1/tenants/{id} | tenants:read | path | 200 {tenant} | 404 |
| PATCH | /v1/tenants/{id}/status | tenants:write | {status, reason?} | 200 {tenant_id, status, updated_at} | 409, 400 |
| GET | /v1/tenants/{id}/config | tenants:read | path | 200 {config, feature_flags} | — |
| PATCH | /v1/tenants/{id}/config | tenants:write | {config?, feature_flags?} | 200 {config, feature_flags} | 403, 400 |
| GET | /v1/tenants/{id}/quota | tenants:read | path | 200 {limits, usage, thresholds} | — |
| PATCH | /v1/tenants/{id}/branding | tenants:write | {logo_url?, primary_color?, secondary_color?, custom_domain?, email_from?} | 200 {branding, verification_status} | 400, 409 |

Cross-cutting API standards: API-003 (X-Request-Id echo, X-Correlation-Id, Idempotency-Key on POST,
24h replay), API-004 (cursor pagination), API-006 (rate limits, per-tenant overlay), API-007 (envelope).

## 10. Permissions (spec §10.9, Part D §59)

| Role | Tenant |
|---|---|
| SA | create, read, update, change status, delete; elevated cross-tenant read only via audited scope |
| TA | read own tenant; update name/branding/config within plan |
| MG / AG | read own tenant (not in scope of the prototype UI — no MG/AG roles in bootstrap auth) |
| API (billing) | suspend/reinstate via service scope |

## 11. Journeys

* **UJ-19 Tenant provisioning to go-live** — provision from template with isolation smoke tests; create TA
  and hand over (break-glass explained); TA sets security policy, branding (CNAME+TLS), runs UJ-15 in a
  sandbox, promotes sandbox config via baseline import with diff; sign-off → Active, metering starts.
  E1: isolation smoke test fails → tenant held, cannot go Active. E2: residency → Regulated tier
  database-per-tenant + BYOK. E3: suspended for non-payment → login/API blocked, data retained, instant
  reactivation.
* **UJ-15 Tenant onboarding & studio configuration** — tenant-admin configuration bounded to tenant scope;
  config baseline archived; E2 rollback to previous baseline.
* **Approval workflow "Tenant Onboarding"** (Part D §58): Draft → Provisioning → Review → Active;
  approver SA; rollback provisioning on failure; fully audited.

## 12. Cross-cutting constraints that materially shape M01

* §7.1 conventions: tenant_id on every table, RLS, UTC timestamps, soft delete, audit rows, `/v1`,
  Idempotency-Key, cursor pagination, error envelope, `<entity>.<action>` events.
* DBS-001…012: PostgreSQL 15+, snake_case plural tables, PK `id`, `ix_/uq_/fk_/ck_` names, UUID v7
  app-generated PKs (BIGSERIAL for append-only logs), `tenant_id` first FK + RLS, audit columns +
  `version`, FK rules (no ON UPDATE CASCADE), CHECK-constrained text instead of PG enums, citext emails,
  schema-per-domain (`tenantadm`, `identity`, `shared`).
* SEC-004 session idle timeout (default 30 min); SEC-010/101–104 OWASP, CSRF, SameSite, headers;
  SEC-111/112 per-tenant envelope keys, rotation ≤ 90 days; SEC-113 secrets in a vault (never in DB/code);
  SEC-140/141 append-only, tamper-evident audit; SEC-144 break-glass in the security event stream.
* STD-001 server-authoritative validation with per-field errors; STD-002 correlation id on errors;
  STD-003 optimistic concurrency; STD-005 confirmation for destructive actions; STD-006 empty/loading
  states.
* ERR-001 correlation id in every error and log line; ERR-003 never expose stack traces/SQL.
* NFR-003 single-record API ≤ 500 ms P95; NFR-007 RPO ≤ 15 min / RTO ≤ 4 h; NFR-012 EN + BM.
* FR-TST-104 tenant-isolation suite every release; FR-OPS-121 ring-based tenant rollout.
* Ch 72 data lifecycle: purge with certified destruction; legal hold overrides purge; audit retained ≥ 7 years.
