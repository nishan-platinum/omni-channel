# ADR-0013 — Real WhatsApp Cloud API adapter + fake-meta server (supersedes ADR-0012 for WhatsApp)

* Status: Accepted · Date: 2026-10-08

## Context
ADR-0012 used an in-process WhatsApp simulator. Two needs made that insufficient: (1) testing with
**real WhatsApp phones** (a Meta app with a test number is available), and (2) **bulk performance
tests**, which Meta does not allow on test numbers (5 recipients, throughput limits, templates for
business-initiated messages) and which an in-process simulator cannot measure honestly (no HTTP, no
webhooks). The person running the system wants to switch between the two by editing `.env` only.

## Decision
* **One WhatsApp adapter**, `channels/whatsapp_cloud.rs`, implements the Cloud API exactly as in
  production: `POST {base}/{version}/{phone_number_id}/messages` with the access token; inbound
  webhooks verified with `X-Hub-Signature-256` (app secret); GET `hub.verify_token` handshake.
  Errors are classified: throttling (429 / 130429 …) → retry in ~1 s without counting an attempt;
  transient (5xx, 131000 …) → retry ladder; everything else (131047 24-hour window, 131026, 131031
  locked account, 190 token) → failed at once. Secrets are redacted from Meta's error texts (Meta echoes
  malformed tokens).
* **`WHATSAPP_PROVIDER`** chooses the other end:
  * `meta` → `https://graph.facebook.com` with `WHATSAPP_ACCESS_TOKEN`, `WHATSAPP_APP_SECRET`,
    `WHATSAPP_PHONE_NUMBER_ID` (connected to tenant `demo` in development);
  * `fake` → the **fake-meta** server (`src/fake_meta`, binary `fake_meta`, Compose service
    `fake-meta`) with its own `FAKE_META_*` credentials, so real secrets never reach it.
* **fake-meta** imitates the Cloud API over real HTTP: send endpoint with latency, per-number
  throughput limit (default 80/s like Meta's base tier), injected errors, the 24-hour window,
  `[fail]` / `[retry]` markers; signed inbound and status webhooks (sent / delivered / read, optional
  duplicates) delivered by a worker pool with retries; app webhook subscription with Meta's GET
  verification handshake; and a bulk generator (`/_fake/load`) with statistics (hub ack latency,
  customer → agent-reply round trip, throttling). It is clearly labelled "not WhatsApp".
* **Automatic Meta setup** (`channels/whatsapp_setup.rs`, provider `meta`): number/token check
  (`GET /{phone_number_id}`), webhook registration (`POST /{app_id}/subscriptions` with the app
  token `{app_id}|{app_secret}`, verify token, `fields=messages`) and WABA subscription
  (`POST /{waba_id}/subscribed_apps`), re-run whenever the public URL changes. The public URL comes
  from `WHATSAPP_PUBLIC_BASE_URL`, `NGROK_DOMAIN`, or the cloudflared quick tunnel's `/quicktunnel`
  metrics endpoint. Status is shown on the Simulator and Contact centre pages. Manual webhook
  settings in Meta remain a fallback.
* **Tunnel in Compose**: `COMPOSE_PROFILES=tunnel` (cloudflared, no account) or `tunnel-ngrok`
  (fixed domain) in `.env`.
* Outbound delivery sends up to 32 jobs in parallel (a lease holds at most one job per
  conversation, so per-conversation order is kept) and drains backlogs without waiting for the tick.
* The in-process simulator and its `hub.sim_callbacks` table are removed (migration 0007). Voice stays
  a simulated SBC feed (ADR-0012).

## Consequences
* Real-phone tests prove the integration; bulk numbers come from fake-meta and measure **our**
  system through the same code path. Neither claims the other's role.
* Not yet exercised against a real Meta account at the time of writing (credentials pending); the
  wire format was checked against a real status webhook received from Meta, and real Graph API
  errors were observed in a dry run with a dummy token.
* Outbound throughput toward real Meta is bounded by the number's messaging tier (80 msg/s base).
