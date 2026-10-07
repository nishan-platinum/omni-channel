# ADR-0001 — Rust stack, modular monolith and server-side rendering

* Status: Accepted · Date: 2026-10-06

## Context
The specification's historical sources describe a Laravel + Angular modular monolith (Part A §4.4,
FR-ARC-001). This implementation exists to compare languages, and the programme mandates Rust.

## Decision
* Rust (stable) + Axum + Tokio + Tower; SQLx for PostgreSQL and MySQL; Askama server-rendered HTML
  with a small amount of HTMX; Serde; tracing.
* One crate, a **modular monolith**: `platform` (cross-cutting), `bootstrap_auth` (temporary M02
  stand-in), `modules::m01_tenancy::{domain, application, infrastructure, web}`.
* Translation table: Laravel middleware → Tower/Axum middleware & extractors; Eloquent → SQLx
  repositories; Laravel events/queues → typed domain events + transactional outbox + in-process
  dispatcher; Laravel validation → domain value objects; Angular desktop → Askama + HTMX.

## Consequences
* No Node/npm toolchain; the UI works without JavaScript (HTMX is progressive enhancement).
* Module boundaries are enforced by Rust module visibility and the dependency rules in `AGENTS.md`;
  the M01 module could later be extracted behind its ports.
