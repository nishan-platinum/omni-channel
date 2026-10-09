# M10 Hub (gateway slice) — Traceability Matrix

Requirements: [`m10-hub-requirements.md`](m10-hub-requirements.md). Decisions: ADR-0011 (hub),
ADR-0012 (simulated voice), ADR-0013 (WhatsApp Cloud API + fake-meta). Code paths are relative to `src/modules/m10_hub/` unless they start
with `src/`, `migrations/`, `templates/`, `static/`, `scripts/` or `tests/`.

Status vocabulary as in [`m01-traceability.md`](m01-traceability.md), plus:
**Simulated** — behaviour implemented against a clearly-labelled local simulator (ADR-0012), not a
real provider. **Partial** — the listed part is implemented; the rest is named in the notes.

Tests: `tests/hub/main.rs` (12), `tests/isolation/main.rs::hub_isolation` (6, release blocker), unit
tests in `domain/mod.rs`, `application/sessions.rs`, `infrastructure/channels/*.rs`; end to end
`scripts/smoke_test.sh` (hub section → `hub_load smoke`), `scripts/hub_cluster_test.sh` (two nodes,
node kill, rolling restart), headless-browser check of `/agent` + `/chat` (see
`docs/architecture/performance-testing.md`).

| ID | Implementation | API / UI | Tests | Status | Notes |
|---|---|---|---|---|---|
| OCC-M10-R014 | `migrations/control/0005_hub.sql` (`hub.conversations`, one open thread per endpoint+customer), `infrastructure/persistence.rs::append_inbound` | `GET /v1/hub/conversations` | `hub::whatsapp_message_reaches_available_agent…`, `hub::sip_call_events_become_an_ordered_voice_conversation` | Partial | Linking to contact/case needs M13/M15 (not built). |
| OCC-M10-R015 | `hub.messages` append-only (trigger `forbid_message_rewrite`; runtime role has no DELETE), `hub.message_status_events` | — | `hub::per_conversation_order_is_strict…`, isolation RLS test | Implemented | Close is a new `system` event, not an update. |
| OCC-M10-R016 | customer identity = channel address (`customer_address`) | — | — | Not implemented (M13/M38) | No contact store in this repo; no triage queue. |
| OCC-M10-R017 | `domain::pick_agent`, `persistence.rs::route` (lock chosen agent, re-check), `drain_for_agent` (`SKIP LOCKED`), `service.rs::routing_tick` | `/hub/admin` queue table, `GET /v1/hub/queues` | `hub::conversation_waits_in_skill_queue…`, `hub::routing_respects_skills_and_capacity…`, unit `domain::tests` | Partial | Skill + capacity + least-loaded implemented; priority, business hours and SLA-risk rules not. |
| OCC-M10-R018 | — | — | — | Not implemented | SIP events do not carry agent state; no voice/chat blend. |
| OCC-M10-R021 | `persistence.rs::messages_after`; agent access trimmed to assigned conversations | `GET /v1/hub/conversations/{id}/messages?after_seq=` | `hub_isolation::agent_sees_only_conversations_assigned_to_them` | Partial | Per conversation; per contact needs M13. |
| OCC-M10-R022 / OCC-M26-R029 | `application/ports.rs::ChannelAdapter` (connect, ingest, deliver, health, backfill); `infrastructure/channels/*` | `/hub/admin` channel health | channel unit tests | Implemented | WhatsApp + SIP simulated, web chat native. |
| OCC-M10-R028 | `/hub/admin` (queues, agents, presence, load, last heartbeat, socket counts) | `/hub/admin` | `ui` smoke | Partial | Page is refreshed, not pushed live; no SLA risk, no coaching. |
| OCC-M10-R030 | `messages.delivery_status` + `message_status_events`; `domain::DeliveryStatus::can_advance_to` | WS `message.status` | `hub::agent_reply_is_acked_after_commit_and_receipts_reach_read`, unit `delivery_status_only_moves_forward` | Implemented | Receipts never move status backwards. |
| OCC-M10-R031 | WhatsApp: signed status webhooks from Meta / fake-meta (`whatsapp.rs::parse_webhook`, incl. `failed` with error code); web chat: `seen` read markers (`service.rs::customer_seen`) | WS `seen` | `hub::web_chat_customer_and_agent_talk_live_with_read_receipts` | Simulated (WhatsApp) / Implemented (web chat) | Inbound "read when surfaced to agent" not implemented. |
| OCC-M10-R032 | `domain::retry_delay_secs`, `persistence.rs::outbound_failed`, `service.rs::delivery_tick` | agent sees `failed` badge | `hub::failed_delivery_enters_retry_ladder`, unit `retry_ladder_is_1m_5m_30m_then_fail` | Partial | No follow-up task (M16); hard-bounce/opt-out suppression not applicable to the simulator. |
| OCC-M10-R033 | `hub.outbound_queue` FIFO lease (`claim_outbound`), idempotency keys, per-conversation `seq` | WS `message.send{client_msg_id}` | `hub::per_conversation_order_is_strict_and_client_retries_are_idempotent`, `scripts/hub_cluster_test.sh` | Implemented | Ack after commit; zero acknowledged loss verified across node kill + rolling restart. |
| OCC-M10-R034 | `hub.agent_presence` (+ heartbeat, reaper), `service.rs::set_presence/reaper_tick` | agent desktop status select, WS `presence.set` | `hub::stale_agent_is_reaped_and_work_is_rerouted` | Implemented | Capacity is per agent (see R037). |
| OCC-M10-R037 | `hub.agents.max_concurrent` | `/hub/admin` agent form | routing tests | Partial | Per-agent capacity; per-channel blend rules not implemented. |
| OCC-M10-R041 | per customer session 20 msg / 10 s (`web/ws.rs`) | WS error `RATE_LIMITED` | `hub_load chaos` reports `rate_limited` | Partial | Over-limit messages are refused (client re-sends), not parked in a review queue; no per-identity limit for webhooks. |
| OCC-M05-R001 | `infrastructure/channels/whatsapp_cloud.rs` (Cloud API), `whatsapp.rs` (wire format), `whatsapp_setup.rs` (auto webhook registration), `src/fake_meta` (load tests) | `POST/GET /v1/hub/channels/whatsapp/webhook`, `/hub/simulator`, `/hub/admin` (connect number, Meta status) | `whatsapp.rs` unit tests, `hub::agent_reply_goes_through_the_cloud_api…`, `…retry_ladder`, `…fails_at_once`, `…24_hour_window…`, `bulk_whatsapp_traffic…`, `meta_auto_setup…` | Implemented (real-Meta run pending credentials) | 24 h window enforced by the provider and reported (131047); templates for business-initiated messages not implemented. ADR-0013. |
| OCC-M05-R011 | non-text WhatsApp types become a placeholder text | — | `non_text_messages_degrade_gracefully_and_statuses_parse` | Partial | No media storage. |
| OCC-M03-R001 / R012 | `infrastructure/channels/sip_sim.rs` (DID → tenant via endpoint registry; unknown DID → 404) | `POST /v1/hub/channels/sip/events`, `/hub/simulator` | `hub::sip_call_events…`, `hub::webhooks_reject_bad_signatures_and_unknown_numbers` | Simulated | No IVR, media, recording or agent call control. |
| OCC-M08-R001 | public chat page `/chat/{widget_key}` | `templates/hub/chat.html`, `static/js/hub-chat.js` | browser check | Partial | Not embeddable/branded yet. |
| OCC-M08-R002 | customer WebSocket (auth frame, resume by seq, read markers, reconnect + watchdog) | `/v1/hub/ws/customer` | `hub::web_chat_customer_and_agent…`, browser check | Partial | No typing indicators. |
| FR-ARC-002 | one binary, N nodes; `docker compose --profile cluster` (2 nodes + nginx), graceful drain (`src/main.rs`) | — | `scripts/hub_cluster_test.sh` | Partial | API/worker/WebSocket roles are not split into separate pods; every node runs all of them. |
| FR-ARC-003 | Redis pub/sub bus (`infrastructure/bus.rs`), bounded per-socket queues, outbound retry ladder | `/ready` → `hub.bus_ok` | cluster test (cross-node delivery) | Partial | Redis Streams/dead-letter not used: durability is in PostgreSQL, Redis is only fan-out. |
| FR-ARC-007 | `/ready` reports node, bus health, agent/customer socket counts; `/hub/admin` channel health + queue depth | `/ready` | smoke | Partial | No metrics export/alerting. |
| BR-M01-002 (applied to hub) | ingest + agent send refuse suspended tenants; sockets re-validate every 60 s | — | `hub_isolation::suspended_tenant_stops_ingest_and_agent_access` | Implemented | Message hot path caches a positive status ≤ 2 s (ADR-0011). |
| OCC-M01-R002 (applied to hub) | FORCE RLS on all `hub.*`; tenant only from session/endpoint registry | — | `tests/isolation/main.rs::hub_isolation` (6) | Implemented | Platform scope sees no hub rows. |
| OCC-M01-R005 (applied to hub) | agent creation consumes the M01 `users` quota | `POST /v1/hub/agents`, `/hub/admin` | `hub::agent_creation_counts_against_the_m01_user_quota` | Implemented | — |
