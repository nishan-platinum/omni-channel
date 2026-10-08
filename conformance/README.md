# Gateway conformance suite (C01–C51)

Black-box tests for the ScicomCX gateway bake-off contract (docs/architecture/gateway-protocol.md,
ADR-0014). The suite talks to a build only over HTTP and WebSocket — plus the Docker API for the
node-kill tests — so it can run against any candidate that implements the contract.

Written in Python (pytest, httpx, websockets) as the bake-off spec requires. It was written for
this reference build from the spec's test table; the bake-off's own suite (owned by the bake-off
organisers) remains the authority.

```bash
docker compose --profile gateway up -d   # gw1, gw2, HAProxy :8088, gateway-db, redis
./conformance/run.sh                     # whole suite, one line per test ID
RUNS=3 ./conformance/run.sh              # conformance rule: 3 passing runs in a row
FILTER=test_c2 ./conformance/run.sh      # a subset (pytest -k)
NO_FAULTS=1 ./conformance/run.sh         # without node kills (C40–C43, C50)
```

Settings (environment): `GW_URL` (load balancer), `GW_NODE_A` / `GW_NODE_B` (nodes, direct),
`GATEWAY_TOKEN`, `NETWORK` (Docker network the runner joins), `GW_SERVICE_A` / `GW_SERVICE_B`
(compose services the fault tests kill).

| File | Tests |
|---|---|
| `test_c0_ingress.py` | C01–C06 ingress, duplicates (409), SIP call events, auth, contiguous `seq`, fixture skills |
| `test_c1_sessions.py` | C10–C17 welcome, 4401, ping timeout, send/ack, outbound, unassigned refusal, resume, slow reader |
| `test_c2_routing.py` | C20–C28 longest idle, queue depth, availability, arrival order, 30 s re-route, reconnect, disposition, reopen, reload |
| `test_c3_multinode.py` | C30–C33 cross-node delivery, presence, assignment, exactly-once after de-duplication |
| `test_c4_durability.py` | C40–C43 SIGKILL after 202 / ack, kill mid-run, clean restart of both nodes |
| `test_c5_observability.py` | C50–C51 `/healthz` during startup, `/metrics` series |

Routing tests change the platform fixture through `POST /config/reload` with the fixture as the body
(an extension of this build; see ADR-0014). Each test uses its own skill and ids, so tests do not
interfere, but they do replace the fixture: do not run the suite while a load test is running.
