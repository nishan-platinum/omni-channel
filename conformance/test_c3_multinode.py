"""C30-C33: two nodes, no stickiness. Sessions are pinned to node A or B by connecting directly."""

import asyncio
import time

from gw import NODE_A, NODE_B, agent, aio, customer, http, post_whatsapp, presence, set_fixture, uid


def setup(agents: list[str]) -> str:
    skill = uid("sk")
    set_fixture([skill, "voice"], {a: [skill] for a in agents}, {"whatsapp": skill, "sip": "voice"})
    return skill


@aio
async def test_c30_customer_on_a_agent_on_b_within_1s():
    a_id = uid("agent")
    skill = setup([a_id])
    ag = await agent(a_id, [skill], base=NODE_B)
    cu = await customer(uid("cust"), base=NODE_A)
    first = await cu.say("hello")
    conv_id = first["conversation_id"]
    await ag.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    t0 = time.monotonic()
    ack = await cu.say("are you there?", conversation_id=conv_id)
    await ag.wait_message(lambda m: m["message_id"] == ack["message_id"], timeout=1.0)
    assert time.monotonic() - t0 <= 1.0
    reply = await ag.say("yes", conversation_id=conv_id)
    await cu.wait_message(lambda m: m["message_id"] == reply["message_id"], timeout=1.0)
    await cu.close()
    await ag.close()


@aio
async def test_c31_presence_from_b_visible_on_a_within_1s():
    a_id = uid("agent")
    skill = setup([a_id])
    ag = await agent(a_id, [skill], base=NODE_B, available=False)
    t0 = time.monotonic()
    await ag.send({"type": "status", "available": True})
    seen = None
    while time.monotonic() - t0 < 1.0:
        view = next(a for a in presence(NODE_A)["agents"] if a["id"] == a_id)
        if view["status"] == "available":
            seen = time.monotonic() - t0
            break
        await asyncio.sleep(0.02)
    assert seen is not None and seen <= 1.0
    await ag.close()


@aio
async def test_c32_queued_on_a_assigned_on_b_within_1s():
    a_id = uid("agent")
    skill = setup([a_id])
    with http(NODE_A) as c:
        conv_id = post_whatsapp(c, uid("cust")).json()["conversation_id"]
    ag = await agent(a_id, [skill], base=NODE_B, available=False)
    t0 = time.monotonic()
    await ag.send({"type": "status", "available": True})
    await ag.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment", timeout=1.0)
    assert time.monotonic() - t0 <= 1.0
    await ag.close()


@aio
async def test_c33_100_customers_10_agents_split_exactly_once():
    agent_ids = [uid("agent") for _ in range(10)]
    skill = setup(agent_ids)
    agents = [await agent(a, [skill], base=NODE_A if i % 2 else NODE_B) for i, a in enumerate(agent_ids)]
    customers = [await customer(uid("cust"), base=NODE_A if i % 2 else NODE_B) for i in range(100)]
    acks = await asyncio.gather(*(c.say(f"hello from {c.id}") for c in customers))
    assert all(a["type"] == "ack" for a in acks)
    second = await asyncio.gather(*(c.say("second", conversation_id=a["conversation_id"]) for c, a in zip(customers, acks)))
    expected = {a["message_id"] for a in acks} | {a["message_id"] for a in second}
    deadline = time.monotonic() + 10
    got: dict[str, int] = {}
    while time.monotonic() < deadline:
        got = {}
        for ag in agents:
            for m in ag.messages():
                if m["message_id"] in expected:
                    got[m["message_id"]] = got.get(m["message_id"], 0) + 1
        if len(got) == len(expected):
            break
        await asyncio.sleep(0.1)
    assert set(got) == expected, f"missing {len(expected - set(got))} of {len(expected)}"
    assert all(n == 1 for n in got.values()), "a message reached more than one agent"
    for s in customers + agents:
        await s.close()
