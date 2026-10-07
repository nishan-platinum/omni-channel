# M10 Omnichannel Hub — gateway slice: extracted requirements

Paraphrased from the confidential *TM CPaaS Omni Channel CRM — Unified Functional & Design
Specification v2.0* (Chapter 41 Omnichannel Conversation Hub; M10 Cloud Contact Centre; M05, M03,
M08, M26 channel/adapter requirements; Chapter 40 architecture). Only the requirements that make up
the **gateway slice** are listed; the rest of M10 (agent desktop panels, wrap-up, WFM, QA, SLA
engine, bot handover) is out of scope here. Traceability: [`m10-hub-traceability.md`](m10-hub-traceability.md).

## Gateway slice (ScicomCX bake-off definition → spec)

| Slice part | Spec anchors |
|---|---|
| Channel adapters → one canonical message | OCC-M10-R022 (FR-HUB-009), OCC-M26-R029, OCC-M05-R001, OCC-M03-R001/R012 |
| Long-lived customer and agent WebSocket sessions, heartbeat, resume | FR-ARC-002 (WebSocket pod), OCC-M08-R002, OCC-M10-R028/R031 |
| Routing to an available agent with the skill; queue otherwise | OCC-M10-R017 (FR-HUB-004), OCC-M10-R037 |
| Runtime state: presence, queue depth per skill, assignment | OCC-M10-R034 (FR-HUB-110), FR-ARC-003 |
| Every message durable and ordered per conversation | OCC-M10-R014/R015/R033 |
| Two nodes behind one load balancer | FR-ARC-002/003 |

## Requirements (paraphrased)

| ID | Pri | Requirement |
|---|---|---|
| OCC-M10-R014 | P1 | One conversation per interaction thread regardless of channel; conversations are the engagement master record (linked to contact/case where those modules exist). |
| OCC-M10-R015 | P1 | The message/event store is append-only; corrections are new events, never overwrites. |
| OCC-M10-R016 | P1 | Identity resolver: match verified identifiers first, unmatched interactions to triage / contact stub. |
| OCC-M10-R017 | P1 | Routing/ACD by skill, priority, business hours, SLA risk and channel capacity per tenant, in a routing worker; agents receive work per presence and capacity. |
| OCC-M10-R018 | P1 | Presence synchronised with voice state so an agent on a call is not offered chats beyond blend rules. |
| OCC-M10-R021 | P1 | Timeline API returns the merged, chronologically ordered history, permission-trimmed. |
| OCC-M10-R022 | P1 | Every channel adapter implements a common contract (connect, ingest, deliver, health, backfill); channels replaceable without hub change. |
| OCC-M10-R028 | P1 | Supervisor real-time view (queues, presence, SLA risk) from WebSocket events. |
| OCC-M10-R030 | P1 | Outbound messages carry delivery status queued/sent/delivered/read/failed; transitions are append-only events. |
| OCC-M10-R031 | P1 | Per-channel receipt mapping (WhatsApp provider callbacks; web chat socket ACK + read markers; voice uses call events). |
| OCC-M10-R032 | P1 | Failed delivery retry ladder (default 1 m / 5 m / 30 m), then failed + agent-visible signal. |
| OCC-M10-R033 | P1 | Outbound FIFO per conversation; idempotency key prevents duplicate dispatch on retry. |
| OCC-M10-R034 | P1 | Agent presence (Available/Busy/Away/Wrap-up/Offline) authoritative in the hub; routing uses presence + per-channel capacity. |
| OCC-M10-R037 | P1 | Blended capacity rules, capacity model per channel configurable per tenant. |
| OCC-M10-R041 | P1 | Inbound rate limiting per channel identity with backpressure; excess parked for review, never dropped silently. |
| OCC-M05-R001 | P1 | WhatsApp as official BSP: send/receive, 24 h session window, approved templates outside it. |
| OCC-M05-R011 | P1 | Rich media per channel capability with graceful degradation. |
| OCC-M03-R001 | P1 | Inbound PSTN/SIP call via TM SBC: resolve dialled number to tenant; unknown numbers rejected (SIP 404). |
| OCC-M03-R012 | P1 | Call events normalised across platforms (ringing, answered, held/retrieved, transferred, ended, abandoned) with ids, ANI, DNIS. |
| OCC-M08-R001 | P1 | Embeddable, tenant-branded chat widget for web and mobile. |
| OCC-M08-R002 | P1 | Real-time agent–customer chat with typing indicators, read receipts and reconnection. |
| OCC-M26-R029 | P1 | Connector/adapter contract connect · ingest · deliver · health · backfill for every channel/voice/app adapter. |
| FR-ARC-002 | P1 | Workloads deploy as independently scalable pods (API, workers, WebSockets, adapters) behind an ingress. |
| FR-ARC-003 | P1 | Real-time state (presence, chat, queue status) uses Redis/streams with backpressure, retry and dead-letter handling. |
| FR-ARC-007 | P1 | Monitoring of API latency, queue backlog, adapter health and WebSocket connection counts. |
