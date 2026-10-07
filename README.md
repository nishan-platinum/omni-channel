# TM CPaaS Omni Channel CRM — M01 Multi-Tenancy + M10 Omnichannel Hub gateway (Rust)

A runnable Rust implementation of **M01 — Multi-Tenancy & Tenant Management** from the *TM CPaaS Omni
Channel CRM Unified Functional & Design Specification v2.0*, built so its correctness, performance and
resource usage can be compared with implementations in other languages.

* **Scope:** M01 only — all 27 unified requirements (P1 and P2), business rules BR-M01-001…005, field
  dependencies FD-008…011, notifications NT-001…003, the tenant state machine and the M01 `/v1` API.
  Other modules (M02–M40) appear only as ports with clearly-labelled **reference adapters**.
* **Plus the M10 omnichannel hub gateway slice:** simulated WhatsApp and SIP channels → one canonical
  message → durable, ordered store → skill-based routing with queues → live agent and customer
  WebSocket sessions (heartbeat, resume), across two nodes behind a load balancer. **No real WhatsApp
  or telco credentials are needed** — the channels are labelled simulators (ADR-0011, ADR-0012).
* **Stack:** Rust · Axum · Tokio · Askama (server-rendered HTML) · HTMX · SQLx · Serde · Tower ·
  tracing · PostgreSQL (control plane + shared/schema tenant data) · PostgreSQL & MySQL (dedicated tenant
  databases). No PHP/Laravel/Angular/Node.
* **Docs:** [`DESIGN.md`](DESIGN.md) (architecture), [`AGENTS.md`](AGENTS.md) (contributor contract),
  [`CLAUDE.md`](CLAUDE.md), [`docs/requirements/`](docs/requirements/) (requirements + traceability),
  [`docs/decisions/`](docs/decisions/) (ADRs), [`docs/architecture/performance-testing.md`](docs/architecture/performance-testing.md).

## Prerequisites

* Docker with Docker Compose v2 (for the canonical start).
* For local development and tests: Rust stable (1.85+), `curl`, `python3` (smoke/benchmark scripts only).

## Quick start (Docker Compose)

```bash
cp .env.example .env
docker compose up --build
```

Open **http://localhost:3000** and sign in with the development Super Admin from `.env`:

| | |
|---|---|
| Email | `BOOTSTRAP_SUPERADMIN_EMAIL` (default `superadmin@omni.local`) |
| Password | `BOOTSTRAP_SUPERADMIN_PASSWORD` (default `ChangeMe-SuperAdmin-2026!`) |
| Organisation code | leave empty |

The Super Admin is created at first start if it does not exist. Passwords live only in `.env`
(development placeholders — change them for any shared environment); nothing is hard-coded in Rust.

Services: `app` (:3000), `central-db` PostgreSQL 16 (control plane, :55432), `tenant-pg` PostgreSQL 16
(dedicated tenant databases, :55433), `tenant-mysql` MySQL 8.4 (dedicated tenant databases, :53306),
`redis` Redis 7 (hub real-time bus, :56379).
All have health checks and named volumes. The app applies the control-plane migrations itself at startup
(idempotent) using the owner role, then serves with the restricted runtime role `crm_app`
(NOBYPASSRLS, not table owner). `GET /health` (liveness) and `GET /ready` (central DB reachable).

Reset everything: `docker compose down -v`.

## Local development with `cargo run`

```bash
cp .env.example .env                                   # once
docker compose up -d central-db tenant-pg tenant-mysql redis # databases + Redis only
cargo run                                              # http://localhost:3000
```

`.env` already points at the Docker-published ports (`localhost:55432/55433/53306`) and at
`config/tenant-db-targets.local.toml`. For release-mode measurements use `cargo run --release`.

## Try the omnichannel gateway (no credentials needed)

With `HUB_DEMO_SEED=true` (the default in `.env.example`, development only) the app creates tenant
**demo** with simulated channels and three agents. Every password is `HUB_DEMO_PASSWORD`
(default `Demo-Hub-Passw0rd!`); the organisation code is `demo`.

| Who | Email | Skills |
|---|---|---|
| Tenant Admin | `admin@demo.omni.local` | — |
| Agent | `agent.sales@demo.omni.local` | sales |
| Agent | `agent.support@demo.omni.local` | support |
| Agent | `agent.lead@demo.omni.local` | sales, support |

1. **Agent:** in one browser, sign in as `agent.support@…` → you land on the **Agent desktop**. Set your
   status to **Available**.
2. **Customer on WhatsApp (simulated):** in a *private* window, sign in as `admin@demo.omni.local` →
   **Simulator** → *WhatsApp: customer sends a message*. The message appears on the agent desktop within
   a second (routed by skill `support`). Reply from the desktop: the reply goes to the fake WhatsApp
   provider and its status moves **queued → sent → delivered → read**. Put `[fail]` in a reply to see the
   retry ladder.
3. **Phone call (simulated SBC):** Simulator → *Voice*: send `ringing`, `answered`, `ended` with the same
   call id → one ordered voice conversation on the desktop.
4. **Web chat:** Simulator (or *Contact centre*) → **Open customer chat**. Sign in as `agent.sales@…` /
   `agent.lead@…` and go Available; chat live in both directions; the agent sees **read** when the
   customer has seen the reply. Reload either page: history and conversation resume.
5. **Contact centre** (Tenant Admin) shows queues per skill, agents with live status/load, channels and
   recent conversations, and lets you add agents and simulated channels.

Two nodes behind a load balancer: `docker compose --profile cluster up -d` → http://localhost:8080
(nodes `app1` + `app2`, nginx round robin, not sticky). `./scripts/hub_cluster_test.sh` kills a node and
restarts the other while customers and agents chat, then verifies **zero acknowledged messages lost**.

## Walk-through

1. **Create a tenant** — *New tenant*: name, admin email, plan, region (and, for the Regulated plan, a
   dedicated database target). Reference plans: *Standard* → shared PostgreSQL + RLS, *Premium* → schema per
   tenant, *Regulated* → dedicated PostgreSQL **or** MySQL database pinned to the region. Choosing a plan shows
   its entitled features (FD-008); optional templates seed modules and hand packs to downstream modules.
   The tenant is created in **Draft**; the provisioning steps and the isolation smoke test are shown on its
   overview.
2. **Activate it** — tenant overview → *Activate / reinstate*. Only transitions allowed by the state machine
   are offered; destructive ones ask for confirmation and a reason where required.
3. **Get the invitation** — *Dev outbox* (development only, Super Admin only) shows the invitation email with
   the "set your password" link (no email is sent; M25 is not implemented). The link is also valid before
   activation, but login works only once the tenant is active.
4. **Sign in as Tenant Admin** — sign out, open the link, set a password, sign in with the admin email.
   The Tenant Admin sees only `/tenant/…` pages for their own tenant: configuration and feature flags
   (within the plan), quotas and usage, branding (logo, colours, custom domain, sender domain), storage
   status, support-access approvals, sandboxes, baselines, maintenance window, keys, exports, audit history.
   Host pages return 403; other tenants' ids are refused and audited.
5. **Exercise the rest** as Super Admin: suspend/reinstate/grace/terminate/purge, quotas and the reference
   metering feed, break-glass support grants, releases by ring, analytics, entitlement matrix, DR report.

Simulated verification: custom domains and sender domains ending in **`.verified.test`** verify successfully
(`SIMULATED_VERIFIED_DOMAIN_SUFFIXES`); everything else fails verification. Unverified domains are not served
(HTTP 421).

### PostgreSQL / MySQL tenant database demonstration

Create two Regulated tenants, one with target `dedicated-pg-my-central`, one with
`dedicated-mysql-my-central`. Each gets its own database `tn_<id>` on `tenant-pg` / `tenant-mysql` and its own
runtime login (password derived from `TENANT_DB_RUNTIME_SEED`, never stored). The *Storage & isolation* tab
shows the connection profile (no credentials), *Test DB connectivity*, and the isolation smoke-test results —
for MySQL explicitly "no RLS; dedicated database + login boundary". Inspect them directly:

```bash
docker compose exec tenant-pg psql -U tenant_admin -d postgres -c '\l tn_*'
docker compose exec tenant-mysql mysql -uroot -p"$TENANT_DB_MYSQL_ADMIN_PASSWORD" -e "SHOW DATABASES LIKE 'tn\_%'"
```

## API

```bash
TOKEN=$(curl -s -X POST localhost:3000/v1/bootstrap/token -H 'content-type: application/json' \
  -d '{"email":"superadmin@omni.local","password":"ChangeMe-SuperAdmin-2026!"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["access_token"])')
curl -s -X POST localhost:3000/v1/tenants -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"name":"Acme Retail","region":"my-central","plan_id":"01920000-0000-7000-8000-000000000001","primary_admin_email":"admin@acme.example"}'
```

Spec endpoints: `POST /v1/tenants`, `GET /v1/tenants/{id}`, `PATCH /v1/tenants/{id}/status`,
`GET|PATCH /v1/tenants/{id}/config`, `GET /v1/tenants/{id}/quota`, `PATCH /v1/tenants/{id}/branding`.
Extensions (documented in DESIGN.md §11): `GET /v1/tenants`, `POST …/quota/consume`, `GET …/usage/statement`,
`POST …/isolation-check`, `POST /v1/reference/metering/{id}`, `POST /v1/bootstrap/token`.
Reference plan ids: Standard `…0001`, Premium `…0002`, Regulated `…0003` (`01920000-0000-7000-8000-00000000000N`).

## Tests and quality gates

The integration tests need the Docker databases and Redis
(`docker compose up -d central-db tenant-pg tenant-mysql redis`) and `.env` (copied from `.env.example`).

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features            # everything (unit + integration)
cargo test --lib                     # unit tests only (no database needed)
cargo test --test isolation          # tenant-isolation suite — release blocker
cargo test --test postgres --test mysql --test m01
cargo test --test hub                # M10 gateway: WebSockets, routing, simulated channels
./scripts/smoke_test.sh              # end-to-end HTTP smoke test against a running stack (incl. hub)
./scripts/hub_cluster_test.sh        # 2 hub nodes + nginx: node kill + rolling restart, zero acked loss
```

| Suite | Count | Covers |
|---|---|---|
| unit (`src/**`) | 104 | domain rules (M01 + hub routing/statuses), platform primitives, session registry, simulated channels |
| `tests/m01` | 44 | provisioning, lifecycle, config/flags, quotas, branding, support access, P2, UI forms |
| `tests/isolation` | 17 | cross-tenant read/update/delete, id tampering, body/query tenant_id, missing context, suspended/terminated, SA elevation + audit, RLS with the non-bypass role; hub: cross-tenant conversations, RLS on `hub.*`, endpoint-only tenant resolution, role separation, suspension |
| `tests/hub` | 12 | routing/queueing/capacity, receipts, retry ladder, ordering + idempotency, resume, web chat, voice events, webhook auth, presence reaper, user quota |
| `tests/postgres` | 6 | forced RLS, optimistic locking, constraints, append-only audit, schema/dedicated stores |
| `tests/mysql` | 2 | dedicated MySQL provisioning, routing, login boundary, decommission, outage isolation |
| `scripts/smoke_test.sh` | 51 checks | browser forms + CSRF, invitation flow, TA isolation, API, PG/MySQL tenants, hub end to end |
| `scripts/hub_cluster_test.sh` | chaos run | two nodes, SIGKILL + rolling restart, zero acknowledged messages lost |

CI (`.github/workflows/ci.yml`) runs fmt, clippy, unit tests, the isolation suite (separate blocking step),
the database tests on service containers, and the Docker build.

## Benchmark

```bash
cargo install oha                                  # benchmark-only tool
sed -i 's/^ACCESS_LOG=true/ACCESS_LOG=false/' .env  # optional: no per-request log lines
docker compose up --build -d
DURATION=30s CONCURRENCY=32 ./scripts/benchmark.sh
```

Measures rps, p50/p95/p99, error rate, memory/CPU, DB pool usage and startup time for login, session
validation, tenant list/fetch/creation, config read/update, quota check, lifecycle update and isolation
rejection. Results go to `bench-results/<timestamp>/`. See the
[procedure](docs/architecture/performance-testing.md); no security control is disabled for benchmarks.

Gateway (M10 hub) measurements use the bundled driver (needs `HUB_DEMO_SEED=true`):

```bash
cargo build --release --bin hub_load
./target/release/hub_load latency --base http://localhost:3000 --customers 500 --messages 20
./target/release/hub_load idle --base http://localhost:3000 --sessions 100000 --hold 45 \
    --hosts 127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5,127.0.0.6,127.0.0.7,127.0.0.8
./scripts/hub_cluster_test.sh                       # node kill + rolling restart, zero acked loss
```

First results (one laptop, client and server on the same machine): 100 000 idle sessions on one node
with 0 failures; delivery p50 ≈ 5 ms / p99 ≈ 60–115 ms at ≈ 720 msg/s; 4 561 messages through a node
kill and a rolling restart with 0 lost — see `bench-results/20261007T083045Z-hub/summary.md`.

## Repository layout

```
src/platform/           config, db (scoped tenant transactions), errors, events (outbox), audit, middleware, …
src/bootstrap_auth/     minimal login/sessions/invitations/CSRF — replaceable by M02
src/modules/m01_tenancy/{domain,application,infrastructure,web}
src/modules/m10_hub/{domain,application,infrastructure,web}   omnichannel hub gateway slice
src/bin/hub_load.rs     gateway driver: smoke / idle / latency / chaos
src/demo_seed.rs        development-only demo tenant, agents, simulated channels
templates/ static/      Askama templates, CSS, vendored HTMX, hub-agent.js / hub-chat.js
migrations/control      central PostgreSQL;   migrations/tenant/{postgres,mysql}  tenant data plane
config/                 dedicated tenant DB target catalogues (Docker / local)
docker/lb               nginx config for the two-node cluster profile
tests/                  m01, hub, isolation, postgres, mysql (+ shared harness)
scripts/                smoke_test.sh, benchmark.sh, hub_cluster_test.sh
```

## Reference adapters (not production integrations)

| Port | Stands in for | Reference behaviour |
|---|---|---|
| PlanCatalog | M19 | seeded `ref-standard`, `ref-premium`, `ref-regulated` (+ retired `ref-legacy`) |
| IdentityPort / bootstrap auth | M02 | Super Admin + Tenant Admin, Argon2id, sessions, invitations |
| NotificationPort | M25 | stored in `shared.notification_outbox`, viewable at `/dev/outbox` in development |
| DownstreamProvisioningPort | M23/M02/M09/M15/M24/M31 | template packs recorded only |
| DomainVerificationPort / EmailSenderVerificationPort | M06 + DNS/TLS | simulated: `*.verified.test` verifies |
| KeyManagementPort | KMS/HSM, BYOK | local envelope encryption, master key from env |
| ObjectStoragePort | object storage | local filesystem under `DATA_DIR/objects`, tenant-prefixed |
| BackupService | backup/PITR/DR | encrypted per-tenant M01 configuration snapshots + restore |
| ReleaseManagementPort | CI/CD | rollouts scheduled and marked applied; no deployment |
| AnonymisedDataCopyPort | M29/M38 | copies nothing (no M01 business data) |
| Metering endpoint | M21 | `POST /v1/reference/metering/{id}` (Super Admin) + automatic API-call metering |
| Secret resolver | vault | `env:TENANT_DB_*` variables only; HMAC-derived per-tenant DB logins |
| ChannelAdapter `whatsapp` | WhatsApp BSP / Meta Cloud API (M05) | **simulated**: signed Meta-shaped webhooks, fake BSP with delivered/read receipts, `[fail]` triggers retries |
| ChannelAdapter `voice` | TM SBC / voice connector (M03) | **simulated**: signed JSON call-event feed, no SIP/media |

## Known prototype limitations

See DESIGN.md §18: reference adapters above; per-instance rate limiter; audit hash chain serialises audited
writes; Super Admin, Tenant Admin and Agent roles only; tenant data plane holds only the M01 isolation
canary; no real DNS/TLS/email/PDF. Hub: see DESIGN.md §20 and `docs/requirements/m10-hub-traceability.md`
(no contact/case linking, no priority/business-hours/SLA routing, no WhatsApp templates/24 h window, simulated
channels only). The confidential specification PDF is git-ignored and must never be committed.
