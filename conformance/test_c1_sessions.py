"""C10-C17: WebSocket sessions."""

import asyncio
import time

import websockets

from gw import (
    GW_URL,
    Session,
    agent,
    aio,
    assert_contiguous,
    conversation,
    customer,
    http,
    post_whatsapp,
    set_fixture,
    uid,
    ws_url,
)


def fresh_skill(agents: dict[str, list[str]] | None = None) -> str:
    skill = uid("sk")
    set_fixture([skill, "voice"], agents or {}, {"whatsapp": skill, "sip": "voice"})
    return skill


@aio
async def test_c10_customer_hello_gets_welcome_within_1s():
    t0 = time.monotonic()
    s = await Session("customer", uid("cust")).open(timeout=1.0)
    assert time.monotonic() - t0 < 1.0
    assert s.welcome["session_id"] and "resume_from" in s.welcome
    await s.close()


@aio
async def test_c11_bad_token_closes_4401_before_welcome():
    ws = await websockets.connect(ws_url(GW_URL, "customer", token="wrong"), ping_interval=None)
    frames = []
    try:
        await ws.send('{"type":"hello","role":"customer","id":"x"}')
        async for raw in ws:
            frames.append(raw)
    except websockets.ConnectionClosed:
        pass
    assert ws.close_code == 4401
    assert not any('"welcome"' in f for f in frames)


@aio
async def test_c12_no_ping_for_61s_closes_the_session():
    s = await Session("customer", uid("cust")).open()
    t0 = time.monotonic()
    while s.closed is None and time.monotonic() - t0 < 66:
        await asyncio.sleep(0.25)
    assert s.closed is not None, "session still open after 66 s without ping"
    assert time.monotonic() - t0 >= 59


@aio
async def test_c13_customer_send_is_acked_and_reaches_the_agent():
    a_id = uid("agent")
    skill = fresh_skill({a_id: []})
    set_fixture([skill, "voice"], {a_id: [skill]}, {"whatsapp": skill, "sip": "voice"})
    ag = await agent(a_id, [skill])
    cu = await customer(uid("cust"))
    ack = await cu.say("hello agent")
    assert ack["type"] == "ack" and ack["seq"] >= 1 and ack["message_id"]
    got = await ag.wait_message(lambda m: m["message_id"] == ack["message_id"])
    assert got["direction"] == "inbound"
    await cu.close()
    await ag.close()


@aio
async def test_c14_agent_send_reaches_customer_and_is_outbound():
    a_id = uid("agent")
    skill = uid("sk")
    set_fixture([skill, "voice"], {a_id: [skill]}, {"whatsapp": skill, "sip": "voice"})
    ag = await agent(a_id, [skill])
    cu = await customer(uid("cust"))
    ack = await cu.say("help")
    conv_id = ack["conversation_id"]
    await ag.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    reply = await ag.say("on it", conversation_id=conv_id)
    assert reply["type"] == "ack"
    got = await cu.wait_message(lambda m: m["message_id"] == reply["message_id"])
    assert got["direction"] == "outbound" and got["actor"]["kind"] == "agent"
    stored = [m for m in conversation(conv_id)["messages"] if m["message_id"] == reply["message_id"]]
    assert stored and stored[0]["direction"] == "outbound"
    await cu.close()
    await ag.close()


@aio
async def test_c15_agent_send_on_unassigned_conversation_is_refused():
    a_id, b_id = uid("agent"), uid("agent")
    skill = uid("sk")
    set_fixture([skill, "voice"], {a_id: [skill], b_id: [skill]}, {"whatsapp": skill, "sip": "voice"})
    owner = await agent(a_id, [skill])
    with http() as c:
        r = post_whatsapp(c, uid("cust"), "for a")
    conv_id = r.json()["conversation_id"]
    await owner.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    intruder = await agent(b_id, [skill], available=False)
    before = len(conversation(conv_id)["messages"])
    resp = await intruder.say("not mine", conversation_id=conv_id)
    assert resp["type"] == "error"
    assert len(conversation(conv_id)["messages"]) == before
    await owner.close()
    await intruder.close()


@aio
async def test_c16_resume_replays_everything_after_last_seq_in_order():
    a_id = uid("agent")
    skill = uid("sk")
    set_fixture([skill, "voice"], {a_id: [skill]}, {"whatsapp": skill, "sip": "voice"})
    ag = await agent(a_id, [skill])
    cu = await customer(uid("cust"))
    ack = await cu.say("first")
    conv_id = ack["conversation_id"]
    await ag.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    await cu.wait_message(lambda m: m["kind"] == "assignment")
    last = cu.last_seq()[conv_id]
    session_id = cu.session_id
    await cu.close()
    sent = []
    for i in range(5):
        sent.append((await ag.say(f"while away {i}", conversation_id=conv_id))["message_id"])
    back = Session("customer", cu.id)
    await back.open(resume={conv_id: last}, session_id=session_id)
    for mid in sent:
        await back.wait_message(lambda m, mid=mid: m["message_id"] == mid)
    replayed = back.messages(conv_id)
    assert [m["seq"] for m in replayed] == list(range(last + 1, last + 1 + len(replayed)))
    await back.close()
    await ag.close()


@aio
async def test_c17_slow_agent_gets_all_200_messages_in_order():
    a_id = uid("agent")
    skill = uid("sk")
    set_fixture([skill, "voice"], {a_id: [skill]}, {"whatsapp": skill, "sip": "voice"})
    ag = await agent(a_id, [skill])
    frm = uid("cust")
    with http() as c:
        r = post_whatsapp(c, frm, "m0")
        conv_id = r.json()["conversation_id"]
        await ag.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
        ag.pause()
        for i in range(1, 200):
            assert post_whatsapp(c, frm, f"m{i}").status_code == 202
    await asyncio.sleep(1.0)
    ag.unpause()
    total = conversation(conv_id)["last_seq"]
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and len(ag.messages(conv_id)) < total:
        await asyncio.sleep(0.05)
    msgs = ag.messages(conv_id)
    texts = [m["body"]["text"] for m in msgs if m["kind"] == "text"]
    assert texts == [f"m{i}" for i in range(200)], f"got {len(texts)} texts"
    assert_contiguous(msgs)
    await ag.close()
