# ADR-0010 — Offboarding export, retention and certified purge

* Status: Accepted · Date: 2026-10-06

## Decision (OCC-M01-R016, Ch 72)
* Entering `grace` or `terminated` generates an offboarding export: JSON bundle (tenant record, config
  baseline, feature flags, quotas, usage, branding metadata, audit log for the tenant, data-plane rows,
  object-storage manifest) encrypted with AES-256-GCM using the tenant's data key from
  `KeyManagementPort`; stored via `ObjectStoragePort`; SHA-256 recorded in `tenantadm.tenant_exports`.
  Exports of future modules are collected through `ExternalExportPort` participants (none yet).
* Downloads: Tenant Admin (own tenant) at any time while grace/terminated; Super Admin only with an
  active support grant (the export contains tenant data).
* `terminated → purged` only after `terminated_at + LIFECYCLE_RETENTION_HOURS` and when no legal hold.
* Purge: drop/delete data-plane rows (schema drop / dedicated database drop / shared rows delete), delete
  M01 config, flags, quotas, usage, branding, assets, baselines, sandboxes links, grants, sessions, users;
  crypto-shred tenant keys; keep a tombstone tenant row (code stays reserved) and the audit log (≥ 7 years);
  write a **destruction certificate** (counts per category, SHA-256 digest of the manifest, actor, time).
* The scheduler auto-terminates expired grace tenants; auto-purge is configurable
  (`LIFECYCLE_AUTO_PURGE`, off in development so the purge can be exercised from the UI).
