# Bake-off gateway — traceability (C01–C51, T1–T10)

Source: the ScicomCX gateway bake-off spec (sections 2–6: slice, contract, thresholds, conformance
suite, eval harness). Design: ADR-0014. Wire protocol: docs/architecture/gateway-protocol.md.
Measurements: docs/architecture/gateway-measurements-2026-10-08.md.
Code: `src/modules/gateway/` (domain · application · infrastructure · web), binaries `gateway`, `gw_load`.
Tests: black-box `conformance/` (pytest, run 3× in a row on two nodes) and `tests/gateway_store/`
(store invariants), unit tests in `domain.rs`, `application/{sessions,metrics}.rs`.

## Scope (spec section 2)

| Item | Implementation | Status |
|---|---|---|
| Simulated WhatsApp webhook → canonical message | `web::ingress_whatsapp`, `domain::WhatsappIngress` | Implemented |
| Simulated SIP event feed (invite/bye/dtmf) → canonical message | `web::ingress_sip`, `domain::SipIngress` | Implemented |
| Customer + agent WebSocket sessions, heartbeat, resume | `web/ws.rs` (signed session ids, per-conversation cursors, gap fill) | Implemented |
| Routing: longest-idle agent with the skill, queue otherwise | `domain::pick_longest_idle`, `store::drain_chunk`, `application/router.rs` | Implemented |
| Runtime state: presence, queue depth per skill, assignment | `gw.agents`, `gw.agent_sessions`, `gw.nodes`, `GET /presence` | Implemented |
| Persistence: durable, ordered per conversation | `gw.messages` (append-only), `seq` under row lock, ack after commit | Implemented |
| Two nodes behind one LB, correct across nodes | Compose profile `gateway`: gw1, gw2, HAProxy round-robin; Redis event stream | Implemented |
| Platform fixture, reload | `infrastructure/fixture.rs` (file or URL), `POST /config/reload` | Implemented (+ body extension) |
| Single tenant, shared bearer token | `web::require_bearer`, `?token=` for WebSockets | Implemented |

## Conformance (spec section 5)

| ID | Test (`conformance/`) | Implementation | Status |
|---|---|---|---|
| C01 | `test_c0_ingress::test_c01…` | ingress → `store::append_customer` (opens conversation, `seq` 1) | Pass |
| C02 | `test_c02…` | unique `dedup_key` → `409`, no `seq` burnt (`tests/gateway_store`) | Pass |
| C03 | `test_c03…` | SIP events → one conversation, `call_event` × 3 | Pass |
| C04 | `test_c04…` | bearer middleware (401 before any work) | Pass |
| C05 | `test_c05…` | `seq` via row lock; concurrency test in `tests/gateway_store` | Pass |
| C06 | `test_c06…` | `Fixture::skill_for` at conversation open | Pass |
| C10 | `test_c1_sessions::test_c10…` | `hello` → `welcome{session_id, resume_from}` | Pass |
| C11 | `test_c11…` | close 4401 before `welcome` | Pass |
| C12 | `test_c12…` | no `ping` for 60 s → close 4408 (checked every second) | Pass |
| C13 | `test_c13…` | customer `send` → `ack` after commit; agent gets the same `message_id` | Pass |
| C14 | `test_c14…` | agent `send` → customer; stored `direction: outbound` | Pass |
| C15 | `test_c15…` | `store::append_agent` refuses unassigned → `error{not_assigned}`, nothing stored | Pass |
| C16 | `test_c16…` | `resume` replays every message after `last_seq`, in order | Pass |
| C17 | `test_c17…` | bounded per-session queue (1,024) + gap fill; slow reader gets all 200 in order | Pass |
| C20 | `test_c2_routing::test_c20…` | longest idle wins; `assignment` message | Pass |
| C21 | `test_c21…` | queue depth in `/presence` | Pass |
| C22 | `test_c22…` | `status{available}` → router drains the skill's queue (< 1 s) | Pass |
| C23 | `test_c23…` | queue order `(queued_at, id)` | Pass |
| C24 | `test_c24…` | reaper: disconnected 30 s → re-route (28–32 s window) | Pass |
| C25 | `test_c25…` | reconnect/resume within 30 s keeps assignment and replays | Pass |
| C26 | `test_c26…` | `disposition` = last `seq`, conversation closed, open count −1 | Pass |
| C27 | `test_c27…` | message on closed conversation opens a new one (`seq` 1) | Pass |
| C28 | `test_c28…` | reload removes a skill → no new work, existing kept | Pass |
| C30 | `test_c3_multinode::test_c30…` | Redis event stream across nodes (< 1 s) | Pass |
| C31 | `test_c31…` | presence from the shared store (< 1 s on the other node) | Pass |
| C32 | `test_c32…` | queued on A, agent available on B → assigned (< 1 s) | Pass |
| C33 | `test_c33…` | 100 customers / 10 agents split over nodes, exactly once after de-dup | Pass |
| C40 | `test_c4_durability::test_c40…` | `202` after commit; SIGKILL node, read from the other | Pass |
| C41 | `test_c41…` | `ack` after commit; SIGKILL; customer gets it on resume | Pass |
| C42 | `test_c42…` | SIGKILL mid-run of C05: no acked message missing, `seq` contiguous | Pass |
| C43 | `test_c43…` | clean restart of both nodes: state kept, presence rebuilt on resume | Pass |
| C50 | `test_c5_observability::test_c50…` | `/healthz` 503 until ready (bound before startup), 200 when WebSockets accepted | Pass |
| C51 | `test_c51…` | `/metrics` Prometheus text with the four required series | Pass |

The suite passed 3 runs in a row (33/33 each) against the two-node deployment.

## Thresholds (spec section 4) — see the measurements document for numbers and caveats

| ID | Status on this machine (final runs) |
|---|---|
| T1 ≥ 100k idle sessions / node | Met (100k, 4 CPUs) |
| T2 ≤ 40 KB / session | Met (16.4 KB) |
| T3 p99 ≤ 250 ms at 1× | Met (median 13.6 ms of 3 runs, with 50k idle/node) |
| T4 p99 ≤ 1 s at 2× | Met (median 15.3 ms) |
| T5 errors ≤ 0.1 % at 2× | Met (0 %) |
| T6 0 lost on worker kill | Met |
| T7 0 lost on node kill | Met |
| T8 re-routed ≤ 30 s, 100 % | Met (max 3.5 s) |
| T9 ≥ 99 % survive rolling deploy without reconnect | **Not met** (0 %; drain + resume, ADR-0014) |
| T10 C30–C33 under load | Met (3/3 runs during 1× load) |

## Not a bake-off candidate
This is a reference build of the contract, suite and harness. It was not built from the fresh
scaffold under the build protocol (fixed prompt, token budget, logged steering), so its build cost
(section 7) is not measured and it cannot be entered as the Rust candidate (ADR-0014).
