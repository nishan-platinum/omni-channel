# Performance Testing Procedure (M01, Rust)

Purpose: produce **repeatable**, comparable numbers for the cross-language evaluation without ever
weakening authorization, RLS or tenant isolation.

## What is measured

| Metric | How |
|---|---|
| Requests/second | `oha` (`requestsPerSec`) for constant-body scenarios |
| p50 / p95 / p99 latency | `oha` latency percentiles; `curl -w %{time_total}` loops for write scenarios |
| Error rate | non-expected status codes / total (expected = 200, or 403 for the isolation scenario) |
| Process memory, CPU | `docker stats --no-stream` (container) or `ps -o rss,pcpu` (local process) |
| DB pool usage | `pg_stat_activity` rows with `application_name = 'omni-m01'` |
| Startup time | `startup_ms` in the `listening` log line (config → migrations → seed → bind) |

## Scenarios (`scripts/benchmark.sh`)

| Scenario | Request | Notes |
|---|---|---|
| `session_validation` | `GET /v1/tenants/{id}` as Tenant Admin | bearer lookup + tenant gate + RLS read; representative authenticated read |
| `tenant_list` | `GET /v1/tenants?limit=25` as Super Admin | keyset pagination, plan join |
| `tenant_fetch_super_admin` | `GET /v1/tenants/{id}` as Super Admin | includes the mandatory `platform.elevated_access` audit insert (M01-F02) |
| `config_read` | `GET /v1/tenants/{id}/config` | plan + config + flags |
| `quota_check` | `POST /v1/tenants/{id}/quota/consume` | row-locked atomic counter (one hot row → contention by design) |
| `isolation_rejection` | Tenant A reads tenant B → 403 | includes the mandatory security-audit insert |
| `login` | `POST /v1/bootstrap/token` (concurrency 8) | Argon2id (19 MiB, t=2), bounded blocking pool |
| `tenant_creation` | `POST /v1/tenants` × LOOP_N, 8 parallel | full provisioning saga incl. isolation smoke test |
| `config_update` | alternating `PATCH …/config` | real writes (+ audit + outbox event) |
| `lifecycle_update` | alternating suspend/reinstate, sequential | state machine + audit + event + session revocation |

## Procedure

1. Use a release build with access logging off and nothing else running on the host:
   ```bash
   cp .env.example .env            # once
   sed -i 's/^ACCESS_LOG=true/ACCESS_LOG=false/' .env
   docker compose up --build -d    # the image is a release build (LTO thin)
   ```
   Local alternative: `docker compose up -d central-db tenant-pg tenant-mysql && ACCESS_LOG=false cargo run --release`.
   (For local runs set `MALLOC_MMAP_THRESHOLD_=131072 MALLOC_ARENA_MAX=4` in the shell — the Docker image does this.)
2. Install the load generator (benchmark only): `cargo install oha`.
3. Warm up once, then run: `DURATION=30s CONCURRENCY=32 ./scripts/benchmark.sh`.
4. Results land in `bench-results/<UTC timestamp>/` (`summary.md`, `resources.md`, raw `oha` JSON, raw loop
   timings). Record CPU model, core count, RAM, Docker version and git revision with each run.
5. Repeat ≥ 3 times; report the median run. Compare implementations only on identical hardware and
   identical `DURATION` / `CONCURRENCY` / `LOOP_N`.

The benchmark creates its own tenants (`bench-*` codes) and raises only the benchmark tenant's
`api_requests_per_minute` and `campaign_sends_per_hour` limits through the normal host quota form, so the
noisy-neighbour guardrail does not cap the run. No authorization, RLS or audit is disabled.

## Reference run (development laptop, WSL2, 20 vCPU, Docker release image, 10 s × 32 concurrency)

Indicative only — run on 2026-10-06 with `ACCESS_LOG=true` (default `.env`), all four containers on one host:

| scenario | rps | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|
| session_validation | 4909 | 6.44 | 7.60 | 8.34 | 0 % |
| tenant_list | 6683 | 4.71 | 5.86 | 6.57 | 0 % |
| tenant_fetch_super_admin | 633 | 49.40 | 58.11 | 65.79 | 0 % |
| config_read | 2747 | 11.52 | 13.40 | 14.87 | 0 % |
| quota_check | 547 | 35.27 | 162.72 | 246.27 | 0 % |
| isolation_rejection | 638 | 48.99 | 57.53 | 61.25 | 0 % |
| login (c=8) | 168 | 46.34 | 57.97 | 64.69 | 0 % |
| tenant_creation (100 × 8 par.) | — | 60.95 | 89.45 | 100.02 | 0 % |
| config_update (100 × 8 par.) | — | 9.46 | 20.37 | 26.01 | 0 % |
| lifecycle_update (100, seq.) | — | 5.39 | 9.23 | 28.88 | 0 % |

Resources after the run: app container **19.7 MiB**, central PostgreSQL 176 MiB, 19 app pool connections,
startup 607 ms from process start to listening on an empty database (all control-plane migrations applied, Super Admin seeded).

## Known performance characteristics (documented, intentional)

* **Audited reads serialise on the audit hash chain.** Super Admin reads of a specific tenant and every
  cross-tenant rejection must write an audit row (M01-F02 steps 4–5). `shared.audit_log` is hash-chained
  (SEC-141) through a trigger holding a transaction advisory lock, so these inserts are serialised
  (~600–700 rps on the reference machine vs ~4,500 rps for unaudited tenant reads). Production options:
  per-tenant chains, periodic Merkle anchoring, or an async audit pipeline into M30 — each needs a
  security decision before changing.
* **Quota consumption on a single counter is row-locked** (`SELECT … FOR UPDATE`) to prevent lost updates;
  high concurrency on one tenant/metric shows queueing in p95/p99. Sharded counters could relax this.
* **Every DB call opens a scoped transaction** (`BEGIN; set_config ×2; query; COMMIT`). This is the price
  of transaction-local tenant context with pooled connections; it is not removed for benchmarks.
* **Argon2id** runs on Tokio's blocking pool behind a semaphore (2–8 permits). Before this, an 8-way
  login burst stalled async workers and glibc kept ~900 MiB of 19 MiB Argon2 buffers in per-thread
  arenas; the container now sets `MALLOC_MMAP_THRESHOLD_=131072` and `MALLOC_ARENA_MAX=4`.
* Logging: `ACCESS_LOG=false` removes one structured log line per request; `LOG_FORMAT=json` in Docker.

## NFR targets for context

NFR-003 single-record API ≤ 500 ms P95 and NFR-001 page ≤ 2 s P95 are met with large margin in the
reference run; capacity targets (NFR-004/005) require production-sized data and are out of scope for M01.
