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

## Eliminators (T1–T10) — final runs (median of 3 where the spec requires 3 runs)

1× = 50,000 idle sessions per node + 500 msg/s, 200 agents, 2,000 open conversations.
2× = 50,000 idle sessions per node + 1,000 msg/s, 200 agents, 4,000 open conversations.

| ID | Threshold | Result here | Verdict | Runs |
|---|---|---|---|---|
| T1 | ≥ 100,000 idle sessions per node | 100,000 on one node (4 CPUs), 0 failed, 0 dropped; pong p50 0.13 ms, p99 ≈ 15 ms | **met** | `094427-e1` |
| T2 | ≤ 40 KB per idle session at 100k | 16.4 KB (container memory 9.6 → 1,609 MiB, incl. kernel socket memory) | **met** | `094427-e1` |
| T3 | p99 inbound→agent ≤ 250 ms at 1× | p99 12.4 / 101.5 / 13.6 ms → **median 13.6 ms** (p50 3.1 ms); 0 errors, 0 lost | **met** | `112039`, `112347`, `112654-e2` |
| T4 | p99 ≤ 1,000 ms at 2× | p99 14.9 / 15.3 / 16.9 ms → **median 15.3 ms** | **met** | `113001`, `113312`, `113625-e2` |
| T5 | error rate ≤ 0.1 % at 2× | **0 %** in all three runs (0 failed, 0 retried of 120,000 each) | **met** | same |
| T6 | 0 acknowledged lost on worker kill | 0 lost of 109,994 acknowledged | **met** | `093431-e5` |
| T7 | 0 acknowledged lost on node kill | 0 lost of 131,995 acknowledged, 0 seq gaps | **met** | `093020-e6` |
| T8 | sessions re-routed within 30 s of node kill, 100 % | 100 of 100 agent sessions back within 30 s (median 0.44 s, max 3.5 s) | **met** | `093020-e6` |
| T9 | ≥ 99 % of sessions survive a rolling deploy without reconnecting | **0 of 20,000 (0 %)** — all reconnected and resumed, 0 failed | **not met** (drain + resume by design, ADR-0014) | `113936-e7` |
| T10 | C30–C33 pass under load | C30–C33 passed 3 runs in a row while the cluster carried 1× load (75,000 msgs, 0 lost) | **met** | `111629-t10` |

Earlier single runs of T3 (596 ms and 14.4 s p99) were taken while the laptop was busier and before
the HAProxy check change; they are kept in `superseded/` and show how sensitive the tail is to this
machine's disk (see Environment).

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
