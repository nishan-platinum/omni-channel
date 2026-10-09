# Bake-off gateway — wire protocol (as implemented)

The external contract of the ScicomCX gateway bake-off, as this build implements it (ADR-0014).
Everything the conformance suite and the load harness can see is here. JSON, UTF-8, timestamps
RFC 3339 with milliseconds and `Z`, ids ULIDs (26 chars).

## Deployment
`docker compose --profile gateway up -d` → HAProxy `:8088` (round-robin, no stickiness) in front of
`gw1` (`:4001`) and `gw2` (`:4002`); `gateway-db` PostgreSQL; Redis for the event stream.
Token: `GATEWAY_TOKEN` (default `dev-gateway-token-change-me`, development only).

## HTTP (every request: `Authorization: Bearer <token>`; missing/wrong → `401`)

| Method and path | Request | Response |
|---|---|---|
| `POST /ingress/whatsapp` | `{external_id, from, text, sent_at}` | `202 {message_id, conversation_id, seq}`; `409` duplicate `external_id`; `400` invalid |
| `POST /ingress/sip` | `{call_id, from, event: invite\|bye\|dtmf, at, digits?}` | `202 {message_id, conversation_id, seq}`; `409` duplicate `(call_id, event, at)` |
| `GET /conversations/{id}` | — | `200 {id, channel, customer, skill, status: queued\|assigned\|closed, assigned_agent, last_seq, messages[]}`; `404` |
| `GET /conversations/{id}/messages?after={seq}&limit=` | — | `200 {messages[]}` (ordered by `seq`, max 10,000) |
| `GET /presence` | — | `200 {agents[{id, skills, status: available\|unavailable\|offline, connected, open_conversations, idle_since}], queues{skill: depth}}` |
| `POST /config/reload` | empty, or a fixture JSON (extension) | `204`; `400` invalid fixture; `503` fixture source unreachable |
| `GET /healthz` | — | `200` once the node accepts WebSockets; `503` while starting or draining |
| `GET /metrics` | — | Prometheus text: `gateway_sessions_open{role}`, `gateway_messages_total{channel,direction}`, `gateway_queue_depth{skill}`, `gateway_delivery_seconds` histogram, `gateway_duplicates_total` |

Errors: `{"error": "<code>", "message": "..."}`. Every route except `/healthz` and `/metrics`
answers `503` until the node is ready.

## Canonical message
```json
{"message_id": "01M…", "conversation_id": "01M…", "seq": 17, "channel": "whatsapp",
 "direction": "inbound", "actor": {"kind": "customer", "id": "60123"}, "kind": "text",
 "body": {"text": "hi", "sent_at": "…"}, "received_at": "2026-10-08T09:00:00.123Z", "external_id": "wamid.1"}
```
Bodies: `text` → `{text, sent_at?}`; `call_event` → `{call_id, event, at, digits?}`;
`assignment` → `{agent_id, skill}` (actor `system`/`router`); `disposition` → `{code}` (actor agent).

## WebSocket (`/ws/customer`, `/ws/agent`, `?token=<token>`)
Frames are JSON objects with `type`.

| Frame | Direction | Payload |
|---|---|---|
| `hello` | client → server (first frame) | `{role, id, skills[]}` → `welcome{session_id, resume_from{conversation_id: last_seq}, node}` |
| `resume` | client → server (first frame or later) | `{session_id, last_seq_by_conversation{}}` → `welcome`, then every missed message in order |
| `ping` / `pong` | client → server / server → client | `{}`; no `ping` for 60 s → close 4408 |
| `send` | client → server | `{conversation_id, text, client_ref}` → `ack{client_ref, message_id, seq, conversation_id}` |
| `status` | agent → server | `{available}` → `status{available}` |
| `disposition` | agent → server | `{conversation_id, code, client_ref?}` → `ack`; conversation closed |
| `message` | server → client | `{message: <canonical message>}` |
| `presence` | server → agent | `{agent_id, available}` |
| `error` | server → client | `{code, message, ref}` (`ref` = the `client_ref`) |
| `reconnect` | server → client | node draining; followed by close 1012 |

Subscriptions: a customer session gets every message of its open conversations (and of a
conversation it names in `resume`); an agent session gets every message of conversations assigned
to it (the full history at assignment time) and all `presence` frames. Delivery is at-least-once,
in order per conversation; de-duplicate on `message_id`.

Close codes: 4401 bad token (before `welcome`), 4400 bad handshake, 4408 ping timeout,
1013 slow consumer (resume), 1012 node restarting (resume), 1011 internal error.

## Routing
* Conversation skill = `channel_to_skill[channel]` at creation (`default` if unmapped).
* An inbound message on a queued conversation triggers routing: the available agent with the skill
  who has been idle longest gets it (`assignment` message, history delivered to the agent).
* No available agent → the skill's queue; served in arrival order when an agent becomes available.
* Available = connected (live session on a live node) and `status{available: true}`. Disconnect
  sets status false; conversations stay assigned 30 s, then re-route (or queue). `resume` within
  30 s keeps the assignment and restores the status.
* Agents in the fixture use the fixture's skills; unknown agents use the skills from `hello`.

## Fixture
`{"skills": [...], "agents": [{"id": "...", "skills": [...]}], "channel_to_skill": {"whatsapp": "...", "sip": "..."}}`
— `config/gateway-fixture.json` (200 agents `agent-001…agent-200`, skills `chat`/`voice`).
