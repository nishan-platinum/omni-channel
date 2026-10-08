# ADR-0012 — Simulated channel adapters (no real WhatsApp / telco credentials)

* Status: Accepted · Date: 2026-10-07 · **WhatsApp part superseded by ADR-0013** (real Cloud API adapter +
  fake-meta server); the simulated SBC voice feed below still applies.

## Context
The gateway must ingest WhatsApp messages (OCC-M05-R001) and SIP call events (OCC-M03-R001/R012) and
send WhatsApp replies with receipts (OCC-M10-R030/R031). No Meta/BSP account, phone number, SBC or SIP
trunk is available, and the system must be fully testable after `docker compose up` without any real
credential or configuration.

## Decision
* Every channel implements the common adapter contract `ChannelAdapter` — connect · ingest · deliver ·
  health · backfill (OCC-M10-R022 / OCC-M26-R029). The hub core never sees a provider format.
* **WhatsApp (SIMULATED):** Meta Cloud API webhook shape (`whatsapp_business_account` → `entry` →
  `changes` → `value` with `metadata.phone_number_id`, `contacts`, `messages`, `statuses`), verified
  with `X-Hub-Signature-256` (HMAC-SHA256 of the raw body) and the `hub.verify_token` handshake. The
  secrets are development values from `.env` (`HUB_SIM_WHATSAPP_APP_SECRET`,
  `HUB_SIM_WHATSAPP_VERIFY_TOKEN`). Outbound goes to an in-process **fake BSP** that returns a
  `wamid.SIM.*` id and then emits signed `delivered` and `read` status webhooks which are processed by
  the same ingest code as real webhooks. A reply containing `[fail]` makes the fake BSP fail, to
  exercise the retry ladder.
* **Voice (SIMULATED):** a signed JSON SBC event feed (`X-Sim-Signature`, `HUB_SIM_SIP_SECRET`) with
  normalised events ringing / answered / held / retrieved / transferred / ended / abandoned. No SIP
  signalling or media. Unknown DIDs are rejected (the SIP 404 equivalent).
* **Web chat** is not simulated: customers use the hub's own WebSocket.
* Simulated endpoints are flagged `simulated = true` and labelled "simulated" in the UI, logs and docs.
  The simulator console (`/hub/simulator`, Tenant Admin, disabled when `APP_ENV=production`) fires
  signed requests through the real webhook path and shows a `hub.sim_provider_log`.

## Consequences
* Replacing a simulator with a real provider means a new `ChannelAdapter` implementation (real
  signature scheme, real `deliver` HTTP call, OAuth/token vault per OCC-M05-R013) and registering it in
  `app.rs`; routing, storage, sessions and UI are unchanged.
* Nothing here is a production integration, and nothing claims to be one.
