# M01 Traceability Matrix

Spec: *TM CPaaS Omni Channel CRM — Unified Functional & Design Specification v2.0* (confidential).
Extraction: [`m01-requirements.md`](m01-requirements.md). Code paths are relative to `src/modules/m01_tenancy/`
unless they start with `src/`, `migrations/`, `templates/` or `tests/`.

## Status vocabulary

| Status | Meaning |
|---|---|
| **Implemented** | Behaviour fully implemented and tested in this repository. |
| **Implemented (reference adapter)** | M01 behaviour implemented and tested; a dependency on another module / infrastructure is served by a clearly-labelled local adapter (ADR-0008). |
| **M01 boundary implemented; enforcement pending (Mnn)** | M01 stores/validates/exposes the setting; the module that enforces it does not exist yet. |
| **External production integration pending** | Only an interface + reference adapter exists; real infrastructure is future work. |
| **N/A (M01-only prototype)** | Requirement scope belongs to modules not built here; only the M01-owned part applies. |

Test locations: `tests/m01/*.rs` (application + API + UI), `tests/isolation/main.rs` (release blocker),
`tests/postgres/main.rs`, `tests/mysql/main.rs`, unit tests inside `domain/*.rs` and `src/platform/*.rs`,
end-to-end `scripts/smoke_test.sh`.

## 1. Unified requirements (27)

| ID | Source | Pri | Feature | Summary | Implementation | API / UI | Tests | Status | Notes / reconciliation | Dependency / adapter |
|---|---|---|---|---|---|---|---|---|---|---|
| OCC-M01-R001 | FR-M01-001 | P1 | F01 | Create tenant: unique code, region, plan, initial admin | `application/provisioning.rs`, `domain/ids.rs`, `domain/tenant.rs`, `infrastructure/persistence/tenants.rs` (`insert_provisioned`) | `POST /v1/tenants`; `/admin/tenants/new` | `m01/provisioning.rs` (all), `m01/ui.rs::create_tenant_through_the_form_and_activate`, unit `domain::ids` | Implemented (reference adapter) | Saga with recorded steps, retry and draft discard (§58 rollback). Code auto-suggested from name. | M02 identity → bootstrap_auth (`IdentityPort`); M19 plans → seeded reference plans |
| OCC-M01-R002 | FR-M01-002 | P1 | F02 | All data access scoped to one tenant at the data layer | `src/platform/db.rs::scoped_tx`, RLS in `migrations/control/0001–0003`, `application/context.rs::authorize` | all routes | `isolation/main.rs` (all 11), `postgres::migrations_are_idempotent_and_rls_is_forced` | Implemented | Defense in depth: app-level authorization + PostgreSQL RLS (`crm_app` NOBYPASSRLS, FORCE RLS). Cross-tenant attempt → 403 + security audit. | — |
| OCC-M01-R003 | FR-M01-003 | P1 | F03 | State machine with side effects | `domain/tenant.rs::plan_transition`, `application/lifecycle.rs`, `application/offboarding.rs` | `PATCH /v1/tenants/{id}/status`; overview lifecycle panel | `m01/lifecycle.rs` (all), unit `domain::tenant` (10 tests) | Implemented | Register transitions only; `purged` explicit terminal state (ADR-0002). | M21 billing-driven transitions → System actor (no M21) |
| OCC-M01-R004 | FR-M01-004 | P1 | F04 | Per-tenant channels/features/locale/limits within plan | `domain/config.rs`, `domain/features.rs`, `application/configuration.rs` | `GET/PATCH /v1/tenants/{id}/config`; Configuration tab | `m01/config.rs` (all), unit `domain::config`, `domain::features` | Implemented (reference adapter) | Typed namespaced keys, feature dependency (WhatsApp ⇒ BSP verified), channel count quota. | M19 entitlements → reference plans |
| OCC-M01-R005 | FR-M01-005 | P1 | F05 | Plan limits with soft warning + hard block | `domain/quota.rs`, `application/quotas.rs`, `persistence/settings.rs::consume` | `GET /v1/tenants/{id}/quota`; EXT `POST …/quota/consume`; Quotas tab | `m01/quotas.rs::warn_at_80_block_above_100`, `user_quota_uses_rate_limited_message`, `config::channel_count_is_quota_bound`, unit `domain::quota` | Implemented (reference adapter) | Users/numbers/channels/storage static; volume/emails/AI monthly; campaign/report/BPM hourly. | M21 usage feed → `/v1/reference/metering/{id}` |
| OCC-M01-R006 | FR-M01-006 | P1 | F06 | Logo, colours, custom domain, email sender | `domain/branding.rs`, `application/branding.rs`, `persistence/branding.rs`, `web/mod.rs` (logo, theme, host guard) | `PATCH /v1/tenants/{id}/branding`; Branding tab; `/theme.css`; `/assets/tenants/{id}/logo` | `m01/branding.rs` (all 4), unit `domain::branding` | Implemented (reference adapter) | Logo content-validated (PNG magic / safe SVG) and served with sandbox CSP. | DNS + SPF/DKIM → `SimulatedDnsVerifier`; object storage → local FS |
| OCC-M01-R007 | FR-TEN-001 | P1 | F02 | tenant_id on every row/file/message/log/cache key; injected scope | All tenant-scoped tables carry `tenant_id` + RLS; object keys `tenants/{id}/…`; outbox/notifications/audit carry `tenant_id`; access log records `tenant_id` | — | `isolation/main.rs::postgres_rls_blocks_cross_tenant_access_with_non_bypass_role`, `direct_repository_access_is_scoped` | Implemented | Search indexes/caches/queues of other modules: N/A (not built). In-process rate-limit/API-call buffers keyed by tenant. | — |
| OCC-M01-R008 | FR-TEN-002 | P1 | F02 | Standard shared / Premium schema / Regulated dedicated DB with residency | `domain/storage.rs`, `infrastructure/tenant_data/*` (router, `pg_store.rs`, `mysql_store.rs`), `config/tenant-db-targets*.toml` | Storage tab; `db_target` on create | `m01/provisioning.rs::storage_strategy_follows_tier_and_residency`, `postgres::schema_per_tenant_store_is_created_and_dropped`, `postgres::dedicated_postgres_database_per_tenant`, `mysql/main.rs` | Implemented | Dedicated tier supports PostgreSQL **and** MySQL; per-tenant runtime login (HMAC-derived). `isolation_mode` extended with `database_per_tenant` (ADR-0004). MySQL: no RLS — dedicated DB + login boundary. | Secrets → env `SecretResolver` (vault pending) |
| OCC-M01-R009 | FR-TEN-003 | P1 | F02 | Automated cross-tenant isolation tests in CI; failures alert | `application/isolation.rs`, `TenantDataStore::isolation_probe`, `TenantRepository::control_plane_probe`, `.github/workflows/ci.yml` | EXT `POST /v1/tenants/{id}/isolation-check`; Storage tab "Run isolation smoke test" | `isolation/main.rs`, `m01/lifecycle.rs::activation_requires_passed_isolation_test` | Implemented | Post-provision smoke test gates activation (UJ-19 E1); failure → security audit + `tenant.isolation_check_failed` → ops alert notification. UI/search/report/file-store probes of other modules: N/A. | — |
| OCC-M01-R010 | FR-TEN-004 | P2 | F02 | Per-tenant envelope keys, rotation without downtime, BYOK (Regulated) | `domain/keys.rs`, `application/keys.rs`, `application/offboarding.rs` (encrypt/decrypt), `adapters.rs::LocalKms` | Encryption keys tab | `m01/p2.rs::key_rotation_keeps_old_exports_readable_and_byok_is_regulated_only`, unit `domain::keys`, `adapters::kms_wraps_and_unwraps` | External production integration pending | Keys protect M01 exports/backups; rotation retires old versions (still decrypt). Purge crypto-shreds keys. Database/file-store at-rest encryption is platform infrastructure. | KMS/HSM → `LocalKms` (reference, master key from env) |
| OCC-M01-R011 | FR-TEN-005 | P1 | F02 | Per-tenant backup/restore/DR; tenant RPO/RTO reporting | `application/backup.rs`, `persistence/lifecycle_data.rs` (backups/restores/dr_report) | Backup & restore tab; `/admin/dr` | `m01/p2.rs::per_tenant_backup_restore_does_not_touch_other_tenants` | External production integration pending | Reference: encrypted per-tenant M01 configuration snapshot + single-tenant restore + RPO/RTO report vs NFR-007. Database PITR/DR failover is infrastructure. | Backup infra → local object storage (reference) |
| OCC-M01-R012 | FR-TEN-006 | P1 | F05 | Per-tenant quotas/rate limits (API req/min, campaign rate, report cost, storage, BPM/hour), host-configurable | `src/platform/ratelimit.rs`, `application/quotas.rs::check_api_rate`, quota metrics in `domain/quota.rs` | `X-RateLimit-*` headers; Quotas tab (SA edits limits/thresholds) | `m01/quotas.rs::api_rate_limit_per_tenant_is_noisy_neighbour_safe`, unit `ratelimit` | Implemented | API limit enforced per request (in-process; Redis for multi-pod, FR-PRF-101). Other metrics enforced via `QuotaService::check_and_consume` for future modules. | — |
| OCC-M01-R013 | FR-TEN-010 | P1 | F01 | Provision from template ≤ 30 min, automated | `tenantadm.provisioning_templates` (`migrations/control/0004`), `application/provisioning.rs`, `adapters.rs::ReferenceDownstreamProvisioning` | Template select on New tenant | `m01/provisioning.rs::template_provisioning_is_fast_and_recorded` | Implemented (reference adapter) | Duration recorded per run (typically < 1 s). Packs (roles/teams, BPM, reports, SLA, dropdowns) handed to downstream ports. | M02/M09/M15/M24/M31/M23 → reference downstream adapter |
| OCC-M01-R014 | FR-TEN-011 | P1 | F03 | Provisioning→Active→Suspended→Offboarding→Archived→Purged; audited | `domain/tenant.rs::r014_term`, `application/lifecycle.rs` | R014 view label on tenant header | `m01/lifecycle.rs::full_lifecycle_with_side_effects`, unit `r014_vocabulary_mapping` | Implemented | Reconciled onto the register state machine (ADR-0002). Every transition audited with actor, reason, correlation id. | — |
| OCC-M01-R015 | FR-TEN-012 | P1 | F04 | Module/channel/AI/app entitlements per tenant; disabled ones disappear | `domain/features.rs` (22 features), flags table, `ConfigurationService::is_enabled` | Feature toggles; `/admin/entitlements` matrix | `m01/config.rs::features_cannot_exceed_plan_entitlement`, `ui::tenant_admin_invitation_login_and_self_service` | M01 boundary implemented; enforcement pending (M27 nav, M22 API scopes, Studio) | Runtime check API provided for future modules. | — |
| OCC-M01-R016 | FR-TEN-013 | P1 | F03 | Encrypted full export; certified purge with destruction certificate | `application/offboarding.rs`, `persistence/lifecycle_data.rs::purge`, `tenant_data::decommission` | Offboarding tab (export download, legal hold, certificate) | `m01/lifecycle.rs::full_lifecycle_with_side_effects`, `legal_hold_blocks_purge_and_scheduler_expires_grace`, `support::host_export_download_needs_grant`, `postgres::dedicated_postgres_database_per_tenant` | Implemented (reference adapter) | Export = M01 control plane + audit + data-plane rows + object manifest, AES-256-GCM. Records/files of M02–M40 via `ExportParticipant` (none yet). Legal hold blocks purge. | Object storage + KMS reference |
| OCC-M01-R017 | FR-TEN-014 | P2 | F04 | ≥1 linked sandbox with config copy + anonymised data subset | `domain/sandbox.rs`, `application/sandbox.rs`, `adapters.rs::ReferenceAnonymiser` | Sandboxes tab | `m01/p2.rs::sandbox_copy_and_promotion_with_diff`, unit `domain::sandbox` | Implemented (reference adapter) | Up to 3 sandboxes; auto-activated after green provisioning; no production data copied (M01 owns no business rows). | M29/M38 anonymisation → reference adapter |
| OCC-M01-R018 | FR-TEN-020 | P1 | F04 | Tenant-admin self-service controls bounded to tenant (UJ-15 scope) | `web/tenant_ui.rs` (no tenant id in any route), `application/context.rs::authorize` | `/tenant/*` | `ui::tenant_admin_invitation_login_and_self_service`, `isolation::tenant_id_in_request_never_overrides_session_tenant` | N/A (M01-only prototype) — M01 part implemented | M01-owned self-service (name, config, features, branding, locale, security policy, baselines, sandboxes, maintenance window, exports) is bounded to the session tenant. Studio, teams/roles, business centers, SLA, templates, BPM, dropdowns, portal belong to M18/M02/M09/M15/M17/M31. | — |
| OCC-M01-R019 | FR-TEN-021 | P1 | F06 | Logo, theme, email footer, portal domain (CNAME + TLS), login page, PDF letterhead | `domain/branding.rs`, `application/branding.rs`, `bootstrap_auth/web.rs` (branded login on verified host), `/theme.css` | Branding tab; login page on custom domain | `m01/branding.rs::custom_domain_requires_verification_before_serving` | Implemented (reference adapter) | Managed TLS issuance and PDF rendering are infrastructure/M32 — not implemented. | DNS/TLS → simulated verifier |
| OCC-M01-R020 | FR-TEN-022 | P1 | F04 | Locale defaults (timezone, currency, date/number formats, language set) | `domain/config.rs` (`locale.*` keys + cross-key rule) | Configuration tab | `m01/config.rs::unknown_key_and_bad_types_are_rejected`, unit `default_language_must_be_allowed` | M01 boundary implemented; enforcement pending (M27 rendering) | EN + BM per NFR-012. | — |
| OCC-M01-R021 | FR-TEN-023 | P1 | F04 | Security policy: password/MFA, session timeout, IP allow-list, SSO IdP, API clients | `domain/config.rs` (`security.*`), `infrastructure/gate.rs` (applies idle timeout + password length to bootstrap auth) | Configuration tab | `m01/config.rs::tenant_security_policy_drives_bootstrap_auth` | M01 boundary implemented; enforcement pending (M02) | Session idle timeout and password length are enforced by bootstrap auth now; MFA, IP allow-list, SSO, API clients are stored + validated only. | M02 |
| OCC-M01-R022 | FR-TEN-024 | P1 | F04 | Config baseline export/import with diff; sandbox→production promotion | `domain/baseline.rs`, `application/baselines.rs`, `persistence/lifecycle_data.rs` (BaselineRepository) | Config baselines tab (preview diff, apply, rollback, download); Sandboxes tab promote | `m01/p2.rs::baseline_export_import_rollback`, `sandbox_copy_and_promotion_with_diff`, unit `domain::baseline` | Implemented | Change record required (GOV-002); pre-import snapshot for rollback (UJ-15 E2). | — |
| OCC-M01-R023 | FR-TEN-030 | P1 | F01/F05 | Host directory: tier, state, health, version, entitlement matrix; host actions audited | `application/directory.rs`, `src/platform/observability.rs::TenantMetrics`, `src/platform/events.rs::pending_counts` | `/admin/tenants` (HTMX filter), `/admin/entitlements`, dashboard | `ui::super_admin_pages_render`, `isolation::super_admin_elevation_is_explicit_and_audited` | Implemented | Health = 1 h error rate, outbox queue depth, max quota utilisation, tenant DB check. Every SA page view/elevated read audited. | — |
| OCC-M01-R024 | FR-TEN-031 | P1 | F02 | Break-glass: tenant-approved time-boxed grant (4 h), audited; Regulated named approver per incident | `domain/support.rs`, `application/support.rs`, `persistence/ops.rs` (grants) | Support access tab (SA request/open view; TA approve/reject/revoke) | `m01/support.rs` (all 3), `isolation::manipulated_record_ids_do_not_leak`, unit `domain::support` | Implemented | Host platform scope can never read the data plane (RLS `rls_tenant_only`); the support view opens a tenant context only under an active grant (ADR-0009). | — |
| OCC-M01-R025 | FR-TEN-032 | P1 | F05 | Usage metering (users, storage, API calls, emails, AI tokens, channel sessions), monthly statement, threshold alerts | `application/quotas.rs` (`METERS`, `statement`, API-call buffer), `usage_meters` table | EXT `GET /v1/tenants/{id}/usage/statement`; Quotas tab CSV export | `m01/quotas.rs::metering_statement_and_reference_feed` | Implemented (reference adapter) | API calls metered automatically; other meters via reference M21 feed. | M21 → reference metering endpoint |
| OCC-M01-R026 | FR-TEN-033 | P2 | F04 | Ring-based releases; maintenance windows honoured; in-app release notes | `domain/release.rs`, `application/releases.rs`, `adapters.rs::ReferenceReleaseManager` | `/admin/releases`; Releases & maintenance tab; notes on overview | `m01/p2.rs::ring_based_release_honours_maintenance_window`, unit `domain::release` | External production integration pending | Scheduling + notes implemented; actual deployment is CI/CD infrastructure (reference adapter records only). | CI/CD → reference release manager |
| OCC-M01-R027 | FR-TEN-034 | P2 | F05 | Anonymised aggregated host analytics; no row-level business data; Regulated opt-out | `domain/analytics.rs` (k=3 suppression), `application/analytics.rs`, `persistence/lifecycle_data.rs::aggregate`, `analytics.cross_tenant_opt_out` | `/admin/analytics` | `m01/p2.rs::analytics_aggregate_excludes_opted_out_and_never_reads_data_plane`, `config::analytics_opt_out_only_for_regulated`, unit `domain::analytics` | Implemented | Aggregates M01 metadata only; opt-out applied before aggregation. | — |

## 2. Business rules

| Rule | Implementation | Test | Status |
|---|---|---|---|
| BR-M01-001 unique + immutable code | `uq_tenants_tenant_code`, `ck_tenants_tenant_code`, no update path for code, tombstone keeps code after purge | `provisioning::duplicate_code_is_conflict_409`, `postgres::unique_constraints_hold_at_database_level`, `lifecycle::full_lifecycle_with_side_effects` | Implemented |
| BR-M01-002 suspended blocks logins/API, retains data | `bootstrap_auth::service::check_tenant`, session revoke/drain policy | `lifecycle::full_lifecycle_with_side_effects`, `isolation::suspended_terminated_and_purged_tenants_are_denied` | Implemented |
| BR-M01-003 feature beyond plan → 403 | `domain/features.rs::validate_feature_change` | `config::features_cannot_exceed_plan_entitlement`, `p2::baseline_export_import_rollback` | Implemented |
| BR-M01-004 warn at 80 %, block at 100 % (101 % → 429 + alert) | `domain/quota.rs::evaluate`, `persistence/settings.rs::consume` | `quotas::warn_at_80_block_above_100` | Implemented |
| BR-M01-005 unverified domain not served | `ensure_domain_can_activate`, `ck_tenant_branding_domain_active`, `web::host_guard` (421) | `branding::custom_domain_requires_verification_before_serving` | Implemented (simulated DNS) |

## 3. Field dependencies

| ID | Implementation | Test | Status |
|---|---|---|---|
| FD-008 plan → feature list filtered | `/admin/plans/{id}/features` partial (HTMX) on New tenant; seeding filters to plan | `provisioning::create_tenant_returns_201_draft_and_seeds_everything` | Implemented |
| FD-009 parent → inheritance options | New tenant form (shown only with a parent; `static/js/app.js`), stored in `tenants.inheritance_flags` | `ui::create_tenant_through_the_form_and_activate` (form render) | Implemented (flags stored; inheritance behaviour belongs to M18/M27) |
| FD-010 custom domain → DNS records + Verify | `custom_domain_records`, Branding tab | `branding::custom_domain_requires_verification_before_serving` | Implemented (simulated DNS) |
| FD-011 email_from → SPF/DKIM status | `sender_domains`, Branding tab | `branding::email_sender_requires_spf_dkim_verification` | Implemented (simulated SPF/DKIM) |

## 4. Notifications

| ID | Trigger | Implementation | Test | Status |
|---|---|---|---|---|
| NT-001 Tenant suspended | `tenant.suspended` | `application/handlers.rs::NotificationConsumer` | `lifecycle::full_lifecycle_with_side_effects` | Implemented (reference M25 outbox) |
| NT-002 Quota 80 % | `tenant.quota_warning` | same | `quotas::warn_at_80_block_above_100` | Implemented (reference M25 outbox) |
| NT-003 Quota exhausted | `tenant.quota_exhausted` | same | `quotas::warn_at_80_block_above_100` | Implemented (reference M25 outbox) |
| NT-019 Data purge (M29) | `tenant.purged` | same | (event asserted in `lifecycle::full_lifecycle_with_side_effects`) | Implemented (reference M25 outbox) |
| NT-M01-INVITE / NT-M01-SUPPORT / NT-M01-ISOLATION (implementation ids) | provisioning, support request, isolation failure | provisioning/support/handlers | `provisioning::create_tenant_returns_201_draft_and_seeds_everything` | Implemented (reference) |

Retries: the outbox dispatcher retries failed consumers up to 5 times then dead-letters (ERR-004). Channel retries ×3 are M25's.

## 5. Error codes

| Code | HTTP | Where raised | Test |
|---|---|---|---|
| VALIDATION_FAILED | 400 | field rules, config schema, transitions needing reason | `provisioning::field_validation_errors`, `config::unknown_key_and_bad_types_are_rejected` |
| UNAUTHENTICATED | 401 | missing/invalid/expired/revoked token | `isolation::unauthenticated_access_is_refused` |
| FORBIDDEN | 403 | cross-tenant, role, entitlement, CSRF | `isolation::*`, `config::features_cannot_exceed_plan_entitlement`, `ui::csrf_is_required_for_state_changes` |
| NOT_FOUND | 404 | unknown plan/template/parent/tenant | `provisioning::field_validation_errors` |
| CONFLICT | 409 | duplicate code, illegal transition, hierarchy, unverified domain, claimed domain, optimistic lock, read-only grace | `lifecycle::illegal_transitions_are_conflicts`, `postgres::optimistic_locking_rejects_stale_versions` |
| RATE_LIMITED | 429 + Retry-After | API rate limit; user quota | `quotas::api_rate_limit_per_tenant_is_noisy_neighbour_safe`, `quotas::user_quota_uses_rate_limited_message` |
| QUOTA_EXCEEDED | 429 + Retry-After | plan quotas | `quotas::warn_at_80_block_above_100`, `config::channel_count_is_quota_bound` |
| TENANT_SUSPENDED | 403 | login/API of suspended tenant | `lifecycle::full_lifecycle_with_side_effects` |
| DOMAIN_NOT_VERIFIED | 403 | email sender without SPF/DKIM | `branding::email_sender_requires_spf_dkim_verification` |
| INTERNAL | 500 | unexpected; cause logged with correlation id only | unit `platform::errors::internal_hides_cause` |

## 6. State transitions (State Machine Register §62)

| From → To | Guard / side effect | Test |
|---|---|---|
| draft → active | provisioning completed + isolation passed; sets `activated_at` once | `lifecycle::activation_requires_passed_isolation_test`, unit `activation_requires_passed_isolation_check` |
| active → suspended | reason required; revoke (or drain) sessions; NT-001 | `lifecycle::full_lifecycle_with_side_effects` |
| suspended → active | reinstate | same |
| active → grace | reason required; `grace_until`; read-only; export | same |
| grace → active | only before `grace_until` | same + `legal_hold_blocks_purge_and_scheduler_expires_grace` |
| grace → terminated | scheduler at expiry or SA; sessions revoked; export; `purge_after` | same |
| terminated → purged | retention elapsed, no legal hold; certified purge | same |
| any other | CONFLICT "Illegal status transition" | `lifecycle::illegal_transitions_are_conflicts`, unit `register_transitions_are_exactly_the_allowed_set` |

## 7. API endpoints

| Endpoint | Scope | Handler | Test | Status |
|---|---|---|---|---|
| POST /v1/tenants | tenants:write | `web/api.rs::create_tenant` | `provisioning::*` | Implemented (+ Idempotency-Key) |
| GET /v1/tenants/{id} | tenants:read | `get_tenant` | `lifecycle::*`, `isolation::*` | Implemented |
| PATCH /v1/tenants/{id}/status | tenants:write | `patch_status` | `lifecycle::*` | Implemented |
| GET /v1/tenants/{id}/config | tenants:read | `get_config` | `config::*` | Implemented |
| PATCH /v1/tenants/{id}/config | tenants:write | `patch_config` | `config::*` | Implemented |
| GET /v1/tenants/{id}/quota | tenants:read | `get_quota` | `quotas::*` | Implemented |
| PATCH /v1/tenants/{id}/branding | tenants:write | `patch_branding` | `branding::*` | Implemented |
| EXT GET /v1/tenants | tenants:read (SA) | `list_tenants` | `isolation::super_admin_elevation_is_explicit_and_audited` | Implementation extension |
| EXT POST /v1/tenants/{id}/quota/consume | tenants:write | `consume_quota` | `quotas::*` | Implementation extension |
| EXT GET /v1/tenants/{id}/usage/statement | tenants:read | `usage_statement` | `quotas::metering_statement_and_reference_feed` | Implementation extension |
| EXT POST /v1/tenants/{id}/isolation-check | tenants:write (SA) | `isolation_check` | `lifecycle::activation_requires_passed_isolation_test` | Implementation extension |
| EXT POST /v1/reference/metering/{id} | platform:elevated | `reference_metering` | `quotas::metering_statement_and_reference_feed` | Reference adapter (M21) |
| EXT POST /v1/bootstrap/token | — | `bootstrap_auth/web.rs::api_token` | all API tests | Bootstrap auth (M02 stand-in) |

## 8. Journeys

| Journey | Coverage |
|---|---|
| UJ-19 Tenant provisioning to go-live | Steps 1–2, 4, 6–7 and E1–E3 covered by provisioning/lifecycle/branding/baseline tests and `scripts/smoke_test.sh`; step 3 (client IdP/MFA) and step 5 (Studio/BPM) are M02/M18/M09 scope. |
| UJ-15 Administrator onboarding & studio | M01 part (bounded tenant scope, baseline archive, E2 rollback) covered; Studio/teams/BPM N/A. |

## 9. External production integrations still pending

M02 identity/SSO/MFA/RBAC/API clients · M19 commercial plan catalogue · M21 metering/billing feeds ·
M23/M02/M09/M15/M24/M31 template-pack provisioning · M25 notification delivery · M06/DNS/TLS
verification and certificate issuance · KMS/HSM (incl. real BYOK) · object storage · database backup/PITR/DR ·
CI/CD release deployment · M29 anonymised data subsets · M30 SIEM export · M27 entitlement-driven
navigation and theming of other modules.
