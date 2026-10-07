# ADR-0002 — Tenant lifecycle reconciliation

* Status: Accepted · Date: 2026-10-06

## Context
Two lifecycles appear in the specification:
* **Executable state machine** (M01 §10.4, State Machine Register §62, field rule for `status`):
  `draft → active ⇄ suspended`, `active → grace → terminated → (purged)`, `grace → active`.
* **OCC-M01-R014 (FR-TEN-011, CX 2.0 origin)**: Provisioning → Active → Suspended → Offboarding →
  Archived → Purged.
* Part D §58 approval workflow "Tenant Onboarding": Draft → Provisioning → Review → Active.

## Decision
`tenant.status` uses the register values `draft, active, suspended, grace, terminated, purged` with
exactly the register transitions:

| From | To | Guard |
|---|---|---|
| draft | active | provisioning completed and isolation smoke test passed (UJ-19 E1) |
| active | suspended | reason required |
| suspended | active | — |
| active | grace | reason required (offboarding / non-payment); sets `grace_until` |
| grace | active | only before `grace_until` |
| grace | terminated | — (scheduler at expiry, or SA early termination) |
| terminated | purged | retention window elapsed and no legal hold |

`purged` is modelled as an explicit terminal status (the register writes "(purged)"), because the
tombstone row must remain to keep `tenant_code` unique (BR-M01-001) and to anchor the destruction
certificate and audit trail.

Conceptual mapping of the alternative vocabularies (displayed in the UI as a hint, not stored):

| R014 / §58 term | Executable representation |
|---|---|
| Provisioning | `draft` with `provisioning_status = in_progress` (or `failed`) |
| Review | `draft` with `provisioning_status = completed` |
| Active | `active` |
| Suspended | `suspended` |
| Offboarding | `grace` |
| Archived | `terminated` (data retained read-only for the retention window, export available) |
| Purged | `purged` |

No other transitions are allowed (e.g. no `suspended → grace`, no `draft → terminated`). A failed
draft can be discarded (compensating delete of the never-activated draft) — that is a provisioning
rollback (§58 "Rollback provisioning"), not a status transition.

## Consequences
Every change is a single domain function (`Tenant::plan_transition`), and every transition writes
an audit row (actor, reason, previous/new status, correlation id) and a domain event.
