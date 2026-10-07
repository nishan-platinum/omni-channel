# ADR-0009 — Break-glass support access

* Status: Accepted · Date: 2026-10-06

## Decision (OCC-M01-R024, M18-R009, SEC-121/144)
* A Super Admin requests a grant for one tenant with reason, optional incident reference and duration
  (default 240 min = 4 h, max 480).
* Regulated tier: incident reference mandatory and a **named approver** (an active Tenant Admin of that
  tenant) must be chosen; only that user may approve; one grant per incident.
* Standard/Premium: any active Tenant Admin of that tenant may approve.
* States: `requested → approved → (expired | revoked)`, `requested → rejected`. The window starts at
  approval and ends at `approved_at + duration`. Time is evaluated with the injected `Clock`.
* Only while a grant is active may the SA open the tenant **support view** (data-plane rows). Each use
  writes `support_access.used` to the audit log and a security event; a use without an active grant is
  denied (403) and logged (`support_access.denied`).
* Platform-scope M01 metadata (directory, config, quotas) remains visible to the SA without a grant,
  but every SA read of a specific tenant is audited as `platform.elevated_read` (M01-F02 step 5).
