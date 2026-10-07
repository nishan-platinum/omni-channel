# ADR-0006 — API envelope and error code reconciliation

* Status: Accepted · Date: 2026-10-06

## Context
The unified error catalogue (§7.5, Part D §60) uses `VALIDATION_FAILED` with HTTP **400**; the carried-over
CX 2.0 chapter 47 uses `VAL-1xxx` codes with HTTP **422**. API-007 defines the envelope. The M01 API
register shows response bodies without the envelope.

## Decision
* Error codes and HTTP statuses follow the **unified catalogue** (400 for VALIDATION_FAILED; 403
  FORBIDDEN / TENANT_SUSPENDED / DOMAIN_NOT_VERIFIED; 404; 409 CONFLICT; 429 RATE_LIMITED /
  QUOTA_EXCEEDED with `Retry-After`; 500 INTERNAL).
* Envelope per API-007: success `{"data": <register body>, "meta": {"correlation_id": …}}`, error
  `{"error": {"code", "message", "details": [{"field","code","message"}], "correlation_id"}}`.
  The register body (e.g. `{tenant_id, tenant_code, status:"draft", …}`) is the `data` member.
* `X-Request-Id` is echoed and `X-Correlation-Id` returned on every response (API-003).
* `Idempotency-Key` is honoured on `POST /v1/tenants` (24 h replay of the original response, API-003).
* Cross-tenant id access by a tenant principal returns **403 FORBIDDEN** with a security audit event
  (M01-F02 step 4). Unknown ids return 404 for Super Admin.
