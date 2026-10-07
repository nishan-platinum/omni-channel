# ADR-0011 — M10 omnichannel hub (gateway slice) inside this repository

* Status: Accepted · Date: 2026-10-07

## Context
A review of the competing Phoenix M01 repository (ScicomCX gateway bake-off) showed that an M01-only
codebase implements none of the gateway slice: channel adapters producing one canonical message,
long-lived customer/agent WebSocket sessions with heartbeat and resume, skill routing with queues,
runtime state (presence, queue depth, assignment), durable ordered messages, and two nodes behind one
load balancer. The owner decided to build that slice **in this repository**, on top of M01, as the
spec's *Omnichannel Conversation Hub* (Chapter 41; OCC-M10-R014…R034, FR-ARC-002/003/007). This
repository therefore no longer qualifies as a fresh-scaffold bake-off entry; it is the product path.

## Decision
* **Module.** `src/modules/m10_hub/` with the same layering as M01 (domain / application /
  infrastructure / web). Hub data lives in schema `hub` (migration `0005_hub.sql`).
* **Tenancy.** Every hub table has FORCE RLS via `shared.rls_tenant_or_system`: tenant scope sees its
  own rows, system scope serves webhooks and workers, **platform (Super Admin) scope sees nothing**
  (engagement data is tenant business data, as with `tenant_data.*`). Inbound traffic is mapped to a
  tenant only through `hub.channel_endpoints` (WhatsApp phone_number_id / DID / widget key) — never
  from payload fields. Hub data stays in the central PostgreSQL for every storage tier in this slice
  (see Consequences).
* **Durability and order.** A message is acknowledged only after the transaction storing it commits.
  Each conversation has a gap-free sequence (`last_seq`, row-locked) and messages are unique per
  `(conversation_id, seq)`; idempotency keys (`wa:<wamid>`, `sip:<event>`, `agent:<user>:<client id>`,
  `webchat:<session>:<client id>`) make retries and duplicate provider deliveries harmless. The
  inbound hot path is one upsert (find-or-open thread + next seq) plus one insert; a duplicate rolls
  back without consuming a sequence number. Message content is append-only (trigger).
* **Routing.** Pure rule in the domain (skill, Available, under capacity, least utilised).
  Routing chooses from an unlocked snapshot, then locks only the chosen agent's presence row and
  re-checks capacity (retry with the next candidate); draining for an agent locks that agent and takes
  queued conversations with `FOR UPDATE SKIP LOCKED`. A routing tick (2 s) is the safety net for races.
  This is correct across nodes without a distributed lock.
* **Presence.** Authoritative in `hub.agent_presence` (OCC-M10-R034). Live agent sockets refresh
  `heartbeat_at` every 20 s; the reaper (15 s) marks agents silent for 60 s Offline and re-queues their
  conversations (node crash, closed laptop).
* **Real-time fan-out.** Redis pub/sub (`occ:hub:events`, FR-ARC-003): every node subscribes and
  forwards envelopes to the sockets it holds (`SessionRegistry`). Redis holds no durable state; if a
  push is lost, clients catch up by sequence number. Without `REDIS_URL` a single node uses an
  in-process bus. The load balancer needs no stickiness.
* **Sessions.** One WebSocket protocol for agents (cookie or bearer; Origin check for cookies) and
  customers (token in the first frame, never in the URL). Each socket has a reader (ordered handling →
  ordered acks) and a writer (responses + pushes + 20 s pings) running concurrently; 60 s of silence
  closes the socket. Per-socket buffers are 4 KiB (memory per idle session) with bounded channels
  (customers 64, agents 1024; overflow drops the session, the client resumes). Customer handshakes go
  through admission control (semaphore ≈ half the DB pool, wait ≤ 20 s, then close 1013) so a
  reconnect storm cannot starve message traffic. Graceful shutdown sends `reconnect` + close 1012.
* **Hot-path caches.** Endpoint lookups are cached 30 s. A *positive* tenant-status check is cached
  2 s per node on the message path, so suspending a tenant stops message ingest within 2 s; logins,
  API calls and socket re-validation (every 60 s) re-check on every request.
* **Outbound.** Agent replies to WhatsApp go through a transactional queue (`hub.outbound_queue`),
  leased FIFO per conversation (a message is not due while an earlier one of the same conversation is
  queued), delivered through the channel adapter, with the retry ladder 1 m / 5 m / 30 m then
  `failed` (OCC-M10-R032/R033). Receipts move status forward only (late/out-of-order tolerant).
* **Agents** are tenant users with the new bootstrap role `agent` (scope `hub:agent`, no M01 scopes);
  creating one counts against the M01 `users` quota.

## Consequences
* M01 is unchanged in behaviour; the hub depends on M01 only through the tenant gate, quotas and auth.
* Regulated (dedicated database) tenants still keep their hub data in the central cluster in this
  slice; moving hub tables to the tenant's data plane is future work (the repository is behind a port).
* Simulated channels only (ADR-0012). Measurements: `docs/architecture/hub-measurements-2026-10-07.md`,
  procedure in `docs/architecture/performance-testing.md`.
