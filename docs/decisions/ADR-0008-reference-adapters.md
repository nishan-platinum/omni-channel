# ADR-0008 — Ports and reference adapters for other modules

* Status: Accepted · Date: 2026-10-06

| Port | Future owner | Reference adapter (this repo) |
|---|---|---|
| `PlanCatalog` | M19 | `tenantadm.plans` seeded with prototype plans `ref-standard`, `ref-premium`, `ref-regulated` (+ inactive `ref-legacy`). Not commercial plans. |
| `IdentityPort` | M02 | bootstrap_auth users/invitations |
| `NotificationPort` | M25 | `shared.notification_outbox` + structured log; `/dev/outbox` (development + SA only) |
| `ProvisioningPort` (downstream template packs) | M23/M02/M09/M15/M24/M31 | records template-pack steps; no external calls |
| `DomainVerificationPort` | M06 / DNS+TLS infra | **simulated**: domains ending `.verified.test` (configurable) verify |
| `EmailSenderVerificationPort` | M06 | **simulated** SPF/DKIM, same rule |
| `KeyManagementPort` | KMS/HSM (SEC-112) | local envelope encryption with a dev master key from env |
| `ObjectStoragePort` | object storage (FR-ARC-004) | local filesystem, tenant-prefixed paths |
| `BackupRestorePort` | platform ops (NFR-007/008) | per-tenant M01 snapshot (encrypted) + config restore |
| `ReleaseManagementPort` | CI/CD (FR-OPS-121) | schedules rollouts in the DB; no deployments |
| `AnalyticsPort` | M24 reporting mart | in-DB aggregation over M01 metadata with k-anonymity |
| `AnonymisedDataCopyPort` | M29/M38 | copies no business rows (none exist in M01); synthetic canary only |
| `ExternalExportPort` / `ExternalPurgePort` | each data-owning module | registry with no participants yet |
| `SecretResolver` | vault (SEC-113) | environment variables `TENANT_DB_*` |
| Metering ingestion | M21 | `POST /v1/reference/metering/{tenant_id}` (SA) + automatic API-call metering |

Every reference adapter is labelled "reference adapter" in code comments, UI and README.
