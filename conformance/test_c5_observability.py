"""C50-C51: health and metrics."""

import time

import httpx
import websockets

from gw import AUTH, NODE_A, SERVICE_A, aio, restart_node, ws_url


@aio
async def test_c50_healthz_non_200_until_websockets_accepted_then_200():
    import threading

    observed: list[int] = []
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            try:
                observed.append(httpx.get(NODE_A + "/healthz", headers=AUTH, timeout=0.5).status_code)
            except httpx.HTTPError:
                observed.append(0)
            time.sleep(0.05)

    t = threading.Thread(target=poll, daemon=True)
    t.start()
    restart_node(SERVICE_A)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not (observed and observed[-1] == 200 and any(s != 200 for s in observed)):
        time.sleep(0.05)
    stop.set()
    t.join()
    first_down = next(i for i, s in enumerate(observed) if s != 200)
    up_again = next(i for i in range(first_down, len(observed)) if observed[i] == 200)
    assert all(s != 200 for s in observed[first_down:up_again])
    # 200 means WebSocket sessions are accepted.
    ws = await websockets.connect(ws_url(NODE_A, "customer"), ping_interval=None)
    await ws.send('{"type":"hello","role":"customer","id":"c50"}')
    assert '"welcome"' in await ws.recv()
    await ws.close()


def test_c51_metrics_expose_required_series():
    r = httpx.get(NODE_A + "/metrics", headers=AUTH, timeout=5)
    assert r.status_code == 200
    text = r.text
    for name in ("gateway_sessions_open", "gateway_messages_total", "gateway_queue_depth{", "gateway_delivery_seconds_bucket"):
        assert name in text, f"{name} missing"
    assert "# TYPE gateway_delivery_seconds histogram" in text
