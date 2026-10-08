"""Black-box client helpers for the gateway conformance suite.

Talks to a build only through the external contract (HTTP + WebSocket), plus Docker for the
fault-injection tests (C40-C43, C50). Configuration comes from the environment:

  GW_URL        load balancer           (default http://gw-lb:8088)
  GW_NODE_A     node A, direct          (default http://gw1:4000)
  GW_NODE_B     node B, direct          (default http://gw2:4000)
  GW_TOKEN      shared bearer token     (default dev-gateway-token-change-me)
  GW_SERVICE_A / GW_SERVICE_B   docker compose service names of the nodes (default gw1 / gw2)
  GW_BASE_FIXTURE  optional fixture file whose agents/skills are kept in every fixture a test sets
                   (T10: the load generator's agents stay routable while the tests run)
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import time
import uuid
from datetime import datetime, timezone

import httpx
import websockets

GW_URL = os.environ.get("GW_URL", "http://gw-lb:8088")
NODE_A = os.environ.get("GW_NODE_A", "http://gw1:4000")
NODE_B = os.environ.get("GW_NODE_B", "http://gw2:4000")
TOKEN = os.environ.get("GW_TOKEN", "dev-gateway-token-change-me")
SERVICE_A = os.environ.get("GW_SERVICE_A", "gw1")
SERVICE_B = os.environ.get("GW_SERVICE_B", "gw2")
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def aio(fn):
    """Run an async test body with asyncio (no pytest plugin needed)."""

    @functools.wraps(fn)
    def wrapper(*a, **kw):
        return asyncio.run(fn(*a, **kw))

    return wrapper


def uid(prefix: str = "t") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def http(base: str = GW_URL) -> httpx.Client:
    return httpx.Client(base_url=base, headers=AUTH, timeout=10.0)


def post_whatsapp(c: httpx.Client, frm: str, text: str = "hello", external_id: str | None = None) -> httpx.Response:
    return c.post(
        "/ingress/whatsapp",
        json={"external_id": external_id or uid("wa"), "from": frm, "text": text, "sent_at": now()},
    )


def post_sip(c: httpx.Client, call_id: str, frm: str, event: str, at: str | None = None) -> httpx.Response:
    return c.post("/ingress/sip", json={"call_id": call_id, "from": frm, "event": event, "at": at or now()})


def set_fixture(skills: list[str], agents: dict[str, list[str]], channel_to_skill: dict[str, str], base: str = GW_URL) -> None:
    """Applies a platform fixture (POST /config/reload with the fixture as body)."""
    body = {"skills": skills, "agents": [{"id": a, "skills": s} for a, s in agents.items()], "channel_to_skill": channel_to_skill}
    base_file = os.environ.get("GW_BASE_FIXTURE")
    if base_file:
        with open(base_file) as f:
            keep = json.load(f)
        body["skills"] = sorted(set(body["skills"]) | set(keep.get("skills", [])))
        body["agents"] = keep.get("agents", []) + [a for a in body["agents"] if a["id"] not in {k["id"] for k in keep.get("agents", [])}]
        body["channel_to_skill"] = {**body["channel_to_skill"], **keep.get("channel_to_skill", {})}
    with http(base) as c:
        r = c.post("/config/reload", json=body)
        assert r.status_code == 204, r.text


def conversation(cid: str, base: str = GW_URL) -> dict:
    with http(base) as c:
        r = c.get(f"/conversations/{cid}")
        assert r.status_code == 200, r.text
        return r.json()


def presence(base: str = GW_URL) -> dict:
    with http(base) as c:
        r = c.get("/presence")
        assert r.status_code == 200, r.text
        return r.json()


def wait_until(pred, timeout: float = 5.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = pred()
        if last:
            return last
        time.sleep(interval)
    return last


def ws_url(base: str, role: str, token: str = TOKEN) -> str:
    return base.replace("http://", "ws://").replace("https://", "wss://") + f"/ws/{role}?token={token}"


class Session:
    """One customer or agent WebSocket. A reader task files frames into a list; tests wait on it."""

    def __init__(self, role: str, ident: str, base: str = GW_URL, skills: list[str] | None = None):
        self.role, self.id, self.base, self.skills = role, ident, base, skills or []
        self.frames: list[dict] = []
        self.ws = None
        self.session_id = None
        self.welcome = None
        self._reader = None
        self._paused = asyncio.Event()
        self._paused.set()
        self.closed: tuple[int, str] | None = None

    async def open(self, resume: dict | None = None, session_id: str | None = None, timeout: float = 5.0):
        self.ws = await websockets.connect(ws_url(self.base, self.role), max_size=2**22, ping_interval=None, open_timeout=timeout)
        if resume is not None:
            await self.ws.send(json.dumps({"type": "resume", "session_id": session_id or self.session_id, "last_seq_by_conversation": resume}))
        else:
            await self.ws.send(json.dumps({"type": "hello", "role": self.role, "id": self.id, "skills": self.skills}))
        self._reader = asyncio.create_task(self._read())
        self.welcome = await self.wait(lambda f: f["type"] == "welcome", timeout=timeout)
        self.session_id = self.welcome["session_id"]
        return self

    async def _read(self):
        try:
            async for raw in self.ws:
                await self._paused.wait()
                self.frames.append(json.loads(raw))
        except websockets.ConnectionClosed as e:
            self.closed = (e.rcvd.code if e.rcvd else 1006, e.rcvd.reason if e.rcvd else "")
            return
        self.closed = (self.ws.close_code or 1006, self.ws.close_reason or "")

    def pause(self):
        """Stop reading from the socket (slow consumer)."""
        self._paused.clear()

    def unpause(self):
        self._paused.set()

    async def wait(self, pred, timeout: float = 5.0, start: int = 0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for f in self.frames[start:]:
                if pred(f):
                    return f
            if self.closed and not any(pred(f) for f in self.frames[start:]):
                raise AssertionError(f"{self.role} {self.id} closed {self.closed} while waiting; frames={self.frames[-5:]}")
            await asyncio.sleep(0.01)
        raise AssertionError(f"{self.role} {self.id}: timed out after {timeout}s; last frames={self.frames[-5:]}")

    def messages(self, conversation_id: str | None = None) -> list[dict]:
        out, seen = [], set()
        for f in self.frames:
            if f["type"] == "message":
                m = f["message"]
                if (conversation_id is None or m["conversation_id"] == conversation_id) and m["message_id"] not in seen:
                    seen.add(m["message_id"])
                    out.append(m)
        return out

    async def wait_message(self, pred, timeout: float = 5.0):
        f = await self.wait(lambda f: f["type"] == "message" and pred(f["message"]), timeout=timeout)
        return f["message"]

    async def send(self, frame: dict):
        await self.ws.send(json.dumps(frame))

    async def ping(self):
        await self.send({"type": "ping"})

    async def status(self, available: bool):
        n = len(self.frames)
        await self.send({"type": "status", "available": available})
        await self.wait(lambda f: f["type"] in ("status", "error"), start=n)

    async def say(self, text: str, conversation_id: str | None = None, client_ref: str | None = None, timeout: float = 5.0) -> dict:
        ref = client_ref or uid("ref")
        frame = {"type": "send", "text": text, "client_ref": ref}
        if conversation_id:
            frame["conversation_id"] = conversation_id
        await self.send(frame)
        return await self.wait(lambda f: f["type"] in ("ack", "error") and f.get("client_ref", f.get("ref")) == ref, timeout=timeout)

    async def dispose(self, conversation_id: str, code: str = "resolved") -> dict:
        ref = uid("disp")
        await self.send({"type": "disposition", "conversation_id": conversation_id, "code": code, "client_ref": ref})
        return await self.wait(lambda f: f["type"] in ("ack", "error") and f.get("client_ref", f.get("ref")) == ref)

    def last_seq(self) -> dict:
        out: dict[str, int] = {}
        for m in self.messages():
            out[m["conversation_id"]] = max(out.get(m["conversation_id"], 0), m["seq"])
        return out

    async def close(self):
        if self.ws is not None:
            await self.ws.close()
        if self._reader:
            try:
                await asyncio.wait_for(self._reader, 2)
            except (asyncio.TimeoutError, Exception):
                self._reader.cancel()


async def agent(ident: str, skills: list[str], base: str = GW_URL, available: bool = True) -> Session:
    s = await Session("agent", ident, base, skills).open()
    if available:
        await s.status(True)
    return s


async def customer(ident: str, base: str = GW_URL) -> Session:
    return await Session("customer", ident, base).open()


def assert_contiguous(messages: list[dict]):
    seqs = [m["seq"] for m in messages]
    assert seqs == list(range(1, len(seqs) + 1)), f"seq not contiguous from 1: {seqs}"


# --- fault injection (Docker) -------------------------------------------------------------------


def _container(service: str):
    import docker  # noqa: PLC0415 (only the fault tests need the Docker SDK)

    client = docker.from_env()
    found = client.containers.list(all=True, filters={"label": f"com.docker.compose.service={service}"})
    assert found, f"no container for compose service {service}"
    return found[0]


def kill_node(service: str):
    _container(service).kill(signal="SIGKILL")


def start_node(service: str):
    _container(service).start()


def restart_node(service: str, timeout: int = 10):
    _container(service).restart(timeout=timeout)


def wait_healthy(base: str, timeout: float = 60.0) -> bool:
    def ok():
        try:
            return httpx.get(base + "/healthz", headers=AUTH, timeout=1.0).status_code == 200
        except httpx.HTTPError:
            return False

    return bool(wait_until(ok, timeout=timeout, interval=0.2))
