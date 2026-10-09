# ADR-0014 — ScicomCX bake-off gateway contract in this repository

* Status: Accepted · Date: 2026-10-08

## Context
ScicomCX is choosing the runtime for its omnichannel gateway with a bake-off: every candidate
implements the same gateway slice from one spec (external HTTP/WebSocket contract, routing,
durability, multi-node), passes one black-box conformance suite (C01–C51) and is measured by one
eval harness (E1–E9) against fixed thresholds (T1–T10). The spec's rules also say candidates start
from a fresh scaffold under a logged build protocol; existing codebases are neither candidates nor
seeds (an earlier repo was rejected for being the platform layer, issue 1).

The M10 hub (ADR-0011) is a CRM gateway with its own contract (multi-tenant, M01 auth, `/v1/hub/…`,
Meta-format webhooks). It does not speak the bake-off contract. The product owner chose to add the
bake-off contract **to this repository** rather than to a fresh repo.

## Decision
* **Separate module, binary and database.** `src/modules/gateway/` (domain / application /
  infrastructure / web), binary `gateway`, migrations `migrations/gateway/`, Compose profile
  `gateway` (2 nodes `gw1`/`gw2`, HAProxy `gw-lb` on :8088, `gateway-db`). It shares no tables,
  routes or code paths with M01/M10; the CRM is unchanged.
* **Single tenant by contract.** One tenant and one shared bearer token (spec: "Tenancy, auth …
  out of scope"). CLAUDE.md rule 1 (tenant context, RLS scopes) governs the CRM; it does not apply
  to this module because the contract has no tenants. Token compared in constant time; never logged.
* **Durability.** PostgreSQL. `202`/`ack` only after commit. `seq` allocated by bumping
  `conversations.last_seq` under the row lock in the appending transaction → gap-free, contiguous,
  across nodes and restarts. `gw.messages` is append-only (trigger). Duplicates: unique
  `dedup_key` (`wa:{external_id}`, `sip:{call_id}|{event}|{at}` with `at` normalised) → `409`;
  WebSocket `client_ref` per actor → the original `ack` is re-sent.
* **Event stream.** Every committed message is published once to Redis pub/sub (`gw:events`) and
  delivered by each node to the sessions it holds (customer of the conversation, assigned agent).
  Pub/sub is not the durability layer: sessions keep a per-conversation cursor and fill gaps from
  the store, and `resume` replays from the store, so a lost event delays but never loses a message.
* **Routing.** Longest-idle available agent with the conversation's skill (`domain::pick_longest_idle`);
  per-skill transaction advisory lock, queue served in arrival order (`queued_at, id`). Agents have no
  concurrency cap (the contract defines none). `idle_since` = last time the agent became available
  or was given a conversation.
* **Presence and failure detection.** Agent connections are rows (`gw.agent_sessions`) tied to a node;
  nodes heartbeat every second; a node silent for 6 s is dead. An agent with no live connection is
  unavailable; its conversations re-route 30 s after the disconnect (node death: 30 s after the dead
  node's last heartbeat). A `resume` within the grace period restores the previous status.
* **Configuration.** The platform fixture is read from `GATEWAY_FIXTURE` (file, or http(s) URL for
  the hybrid candidate's platform endpoint) at startup and on `POST /config/reload`, stored in
  `gw.config` and announced to all nodes. Extension: a JSON body on `POST /config/reload` is applied
  as the fixture (lets the suite change routing rules without file access); it lasts until the next
  file reload — a starting node reads the fixture source, as the spec requires.
* **Sessions.** Session ids are HMAC-signed `(role, id)` tokens: any node can resume them without
  shared session storage. Customer subscription lookups are batched (one query per ≤ 500
  customers) so reconnect storms do not become database bursts.
* **Recovery of the stream and presence.** A node that re-subscribes to the stream, or fails to
  publish an event, triggers `Resync`: sessions re-read their subscriptions and catch up from the
  store. Each node removes its own agent-connection rows that no longer match a live socket
  (a lost disconnect write would otherwise keep a ghost agent "available").
* **Routing worker.** Ingest never waits for routing: a per-node, per-skill worker drains the queue
  in short set-based chunks (one transaction and one publish per ≤ 100 conversations).

## Interpretations where the spec is silent (documented in docs/architecture/gateway-protocol.md)
* WebSocket frames are JSON objects with a `type`; `message` frames carry the canonical message
  under `message`; `ack` also carries `conversation_id`; errors are `error{code, message, ref}`.
* A customer `send` without `conversation_id` goes to the customer's open WhatsApp conversation
  (or opens one); on a closed conversation it opens a new one (C27).
* `welcome.resume_from` = the server's last `seq` per subscribed conversation.
* `resume` may be the first frame (no `hello` needed). Missing entries in
  `last_seq_by_conversation` replay from `seq` 1.
* `assignment` and `disposition` are canonical messages (`direction: outbound`, actors `system` /
  `agent`). An agent being assigned receives the conversation's full history.
* Close codes: 4401 bad token, 4400 bad handshake, 4408 no ping for 60 s, 1013 slow consumer,
  1012 node draining (preceded by a `reconnect` frame).
* `/healthz` and `/metrics` also require the bearer token ("every request carries" it); HAProxy's
  health check sends it.

## Consequences
* The suite (`conformance/`, Python, black-box) and the harness (`gw_load`) are reusable against any
  candidate that implements the contract (base URLs and token are parameters).
* **This build is not a valid bake-off candidate**: it was not built from a fresh scaffold under the
  build protocol (fixed prompt, token budget, logged steering), so its build cost is not
  comparable. It is a reference implementation and a dry run of the contract, suite and harness.
* T9 (≥ 99 % of sessions survive a rolling deploy *without reconnecting*) is **not met**: a draining
  node closes its sessions with `reconnect` (spread over 2 s) and clients resume on the other node
  without loss. Keeping sockets across a process replacement would need socket hand-off between
  processes; not built.
* Worker-kill unit (E5) for this build is the gateway process (one tokio runtime); Docker restarts it.
