# M10 hub gateway — first measurements (2026-10-07)

**Environment:** one laptop, WSL2 VM — 12th Gen Intel Core i7-12700H, 20 logical cores, 7.5 GiB RAM,
Docker 29.8.1. Load generator (`target/release/hub_load`), hub node (release build, `cargo run
--release` equivalent, `ACCESS_LOG=false`, `DB_MAX_CONNECTIONS=20`), PostgreSQL 16 and Redis 7 all on
the **same machine**, so client and server compete for CPU and memory. Treat the numbers as
indicative, not as a formal bake-off result. Code: branch `feature/m10-hub` after commit `63b09ee`
plus the session/routing/caching changes recorded in ADR-0011.

## 1. Idle sessions on one node (`hub_load idle`)

Customer web-chat sockets, each authenticated (`auth` frame → token lookup → history replay) and then
held idle; the server pings every 20 s. Spread over 8 loopback addresses (≈28k ports per address pair).

| Sessions | Connected | Failed | Dropped during hold | Ramp | Server RSS |
|---:|---:|---:|---:|---:|---:|
| 10 000 | 10 000 | 0 | 0 | 4.6 s | 141 MiB (fresh node: 13 MiB) |
| 50 000 | 50 000 | 0 | 0 | 28.4 s | 986 MiB (≈20 KiB/session) |
| **100 000** | **100 000** | **0** | **0** | 78.7 s | 2 325 MiB* |

\* Same process as the 50k run (not restarted), so it includes memory retained from that run.

History of this measurement (what had to change to get here):
* default tungstenite buffers (128 KiB read + 128 KiB write per socket): 10k sessions = 1 357 MiB →
  **4 KiB buffers**: 10k sessions = 141 MiB;
* a 50k connection storm exhausted the 20-connection DB pool (47 631 failures) → **handshake
  admission control** (semaphore; queue ≤ 20 s, then close 1013 + client backoff): 0 failures.

## 2. Delivery latency under load (`hub_load latency`)

500 customers × 20 messages (one every 600 ms each, below the 20 msg / 10 s per-customer limit),
10 agents (skill `latency`, capacity 50). Delivery = customer send → the assigned agent's socket
receives it (one machine clock). Ack = customer send → server ack after the commit.

| Run | Messages | Errors | Delivered | msg/s | ack p50 / p95 / p99 | delivery p50 / p95 / p99 / max |
|---|---:|---:|---:|---:|---|---|
| before routing change | 10 000 | 0 | 10 000 | 693 | 5.9 / 25.4 / 822 ms | 5.6 / 25.2 / 834 / 892 ms |
| after #1 | 10 000 | 0 | 10 000 | 717 | 5.8 / 39.3 / 97.4 ms | 5.5 / 40.6 / 96.9 / 148 ms |
| after #2 | 10 000 | 0 | 10 000 | 715 | 5.0 / 14.8 / 57.1 ms | 4.8 / 14.6 / 57.2 / 127 ms |
| after #3 | 10 000 | 0 | 10 000 | 719 | 5.1 / 13.7 / 112 ms | 4.9 / 13.6 / 116 / 210 ms |

The p99 tail came from the start burst (500 new conversations routed at once, each routing locking
*all* available agents of the skill). Routing now locks only the chosen agent (optimistic re-check).

## 3. Node kill and rolling restart (`scripts/hub_cluster_test.sh`)

Two nodes (`app1`, `app2`) behind nginx (round robin, not sticky), 50 customers + 2 agents chatting
for 60 s. At 15 s `app1` gets SIGKILL; it is started again; at ~35 s `app2` is restarted with SIGTERM
(graceful: `reconnect` + close 1012).

```
RESULT chaos: acked=4561 stored_missing=0 agent_missing=0 customer_reconnects=81 agent_reconnects=4 resent_after_reconnect=81 rate_limited=0
  customer connections per node: {"app1": 66, "app2": 65}
  agent connections per node:    {"app1": 2, "app2": 4}
zero acknowledged messages lost ✔
```

Re-run on the final Docker image (commit `74eab24`, receipts persisted in `hub.sim_callbacks`):

```
RESULT chaos: acked=4567 stored_missing=0 agent_missing=0 customer_reconnects=93 agent_reconnects=5 resent_after_reconnect=93 rate_limited=0
  customer connections per node: {"app1": 76, "app2": 67}
  agent connections per node:    {"app1": 4, "app2": 2}
zero acknowledged messages lost ✔
```

Every acknowledged message was stored (gap-free per conversation) and reached an agent, across a
node crash and a rolling restart, with customers and agents spread over both nodes.

## Not measured / caveats

* 100k sessions **per node** was reached once on a shared laptop; a clean run on dedicated hardware
  (client on another machine) is still to do.
* Worker kill (as opposed to node kill) is not separately scripted: the hub has no separate worker
  process — delivery/routing workers run in every node and are covered by the node kill.
* WhatsApp/SIP are simulators (ADR-0012); provider latency is not part of these numbers.
