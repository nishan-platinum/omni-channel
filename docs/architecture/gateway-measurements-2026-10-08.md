# Bake-off gateway — measurements, 2026-10-08

Reference build (ADR-0014) measured with `scripts/gateway_eval.sh` and `gw_load`. Raw run files are in
`eval-results/` (one JSON per run: generator results + image ids); runs from debugging are kept in
`eval-results/superseded/` with the reason below.

**Environment — not the bake-off's fixed environment.** One Windows laptop, WSL2, 20 cores, 7.6 GB
RAM shared by everything: two gateway nodes (Docker, **capped at 4 CPUs each**, memory not capped),
HAProxy, PostgreSQL, Redis, the load generator containers, and the unrelated CRM stack. The database
disk is a WSL virtual disk: `pg_test_fsync` measured **5.4 ms per fdatasync (≈180/s)**, and the
kernel's pressure-stall counters show I/O stalls. The spec's environment (2 × 4 vCPU / 8 GB nodes,
a separate 2 vCPU / 4 GB DB box, separate load boxes) would differ, mostly in the database tail.
Results here are indicative, not official.

## Eliminators (T1–T10) — after the review fixes (runs in `eval-results/v2/`, median of 3)

1× = 50,000 idle sessions per node + 500 msg/s, 200 agents, 2,000 open conversations.
2× = 50,000 idle sessions per node + 1,000 msg/s, 200 agents, 4,000 open conversations.

| ID | Threshold | Result | Verdict | Runs (`v2/`) |
|---|---|---|---|---|
| T1 | ≥ 100,000 idle sessions per node | 100,000 in each of 3 runs (4 CPUs), 0 failed, 0 dropped; pong p99 ≈ 1.15 s / 1.4 ms / 1.3 ms → median 1.4 ms | **met** | `122555`, `130049`, `130434-e1` |
| T2 | ≤ 40 KB per idle session at 100k | 16.4 KB (all 3 runs) | **met** | same |
| T3 | p99 inbound→agent ≤ 250 ms at 1× | p99 12.7 / 11.9 / 12.3 ms → **median 12.3 ms** (p50 3.0 ms); 0 errors, 0 lost | **met** | `122853`, `123201`, `123508-e2` |
| T4 | p99 ≤ 1,000 ms at 2× | p99 20.4 / 15.1 / 14.0 ms → **median 15.1 ms** | **met** | `123815`, `124123`, `124435-e2` |
| T5 | error rate ≤ 0.1 % at 2× | **0 %** (0 of 360,000) | **met** | same |
| T6 | 0 acknowledged lost on worker kill | 0 of 109,999; 100/100 agent sessions back (max 3.2 s) | **met** | `e5` |
| T7 | 0 acknowledged lost on node kill | 0 of 131,991; 0 seq gaps | **met** | `125112-e6` |
| T8 | re-routed within 30 s, 100 % | 100 of 100 within 30 s (median 0.46 s, max 3.5 s); gw2 serving 1.6 s after restart | **met** | `125112-e6` |
| T9 | ≥ 99 % survive rolling deploy without reconnecting | **0 of 20,000 (0 %)**; all resumed, 0 failed | **not met** (ADR-0014) | `125518-e7` |
| T10 | C30–C33 under load | 3/3 runs passed during 1× load (75,000 msgs, 0 lost) | **met** | `125731-t10` |

The first E1 run's pong tail (≈ 1.15 s) did not repeat in the next two runs (1.4 / 1.3 ms); it is
machine noise of the kind described under Environment. Pre-fix runs are in `eval-results/` (top level).

## Review (spec section 8 checklist) — self-review, not independent

| Severity | Finding | Status |
|---|---|---|
| Medium | A failed DB write at agent disconnect left a "connected" row on a live node: the agent stayed available and its conversations never re-routed | **Fixed**: nodes track live agent connections and remove other rows of their own every 10 s (`reconcile_node`); the disconnect write is retried (store test) |
| Medium | An event lost by the stream (Redis outage, failed publish) delayed delivery until the conversation's next message or a client resume | **Fixed**: re-subscribing nodes and failed publishes trigger a `Resync`; sessions re-read subscriptions and catch up only conversations that moved (spread over 3 s). Verified: Redis restarted mid-run under 1× load — 30,000 acknowledged, 0 lost, 0 undelivered |
| Medium | Every customer `hello` cost one DB query: a reconnect storm became a DB burst | **Fixed**: lookups batched (≤ 500 customers per query, 3 ms linger, bounded queue) — `application/loader.rs` (store test) |
| Low | Redis client's receive buffer is unbounded (drained without blocking) | Open |
| Low | Development default token in Compose / scripts (the binary has no default) | Open |
| Low | WebSocket token in the query string (contract) can reach proxy logs if access logging is enabled | Open |

Found on the way (CRM, not the gateway): the M10 hub's `/ready` stayed "bus not OK" after a Redis
restart until the next publish (cached broken connection). Fixed: a failed health ping resets it.

## Scores (reported, not thresholds)

| Measure | Result |
|---|---|
| E3 step load (no idle sessions) | 0.5×: p99 640 ms (first step, cold) · 1×: 57 ms · 2×: 92 ms · 3× (1,500/s): 175 ms; 0 errors, 0 lost, 0 duplicates at every step |
| E9 ingest ceiling (webhooks only) | 4,000/s with 0 errors; at an 8,000/s target the gateway accepted ≈ 4,750/s, still 0 errors (the 0.1 % error point was not reached) |
| Duplicates delivered | 0 in every run (clients de-duplicate on `message_id` anyway) |
| Stored-but-unacknowledged (node died after commit, before the 202) | 5–6 per kill run; the client's retry got `409` — correct, not a loss |
| Startup | 0.17 s process start → ready (`startup_ms` 166); ≈ 2 s container start → `/healthz` 200 |
| Idle node memory | ≈ 8–10 MB RSS (cgroup 12–14 MiB) |
| Image | gateway-only image (`docker/gateway/Dockerfile`): ≈ 113 MB of layers (Debian slim 85 MB, CA certs 10 MB, binary 17.6 MB); Docker reports 143 MB |

## Findings fixed during the runs

1. **Routing stalled the pool under a burst** (first E2: p99 1.8 s). Every ingest of a new
   conversation routed inline and waited on the skill's advisory lock while holding a pooled
   connection; 500 new conversations/s exhausted the 32-connection pool. Fixed: a per-node routing
   worker per skill (`application/router.rs`) — ingest never waits; requests that arrive during a
   drain mark it dirty.
2. **Slow, long drain transactions.** Fixed: chunks of 100 conversations, candidates read once,
   set-based writes, each chunk committed and published before the next.
3. **Pool warm-up.** The pool now opens all connections at startup (no SCRAM handshakes during a burst).
4. **Harness left state behind.** Conversations of a finished run were re-routed into the queue
   30 s later and the next run served that backlog first (arrival order), which inflated latency in
   the debugging runs. Fixed: harness agents close their conversations at the end; the eval script
   refuses to start with non-empty queues (`gateway_eval.sh reset`).
5. **HAProxy flapped during disk stalls.** Health checks now tolerate a 2 s pause (`fall 3`,
   `timeout check 2s`); a killed node still drops out within ~3 s (connection refused).

Superseded runs (`eval-results/superseded/`): `084953/085234/090755-e2` (single runs before the final
median-of-3 series), `093817-e7` (before sessions-never-dropped was counted), `080802-e1` and `081258-e1` (harness counted sessions
at close; no CPU cap), `081606-e2` (before fix 1), `082504/083110/083417/083835-e2` (debugging with a
backlog from earlier runs — finding 4), `091912-e6` (before reconnect timing was recorded).
