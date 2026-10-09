"""C20-C28: routing and presence."""

import asyncio
import time

from gw import Session, agent, aio, conversation, customer, http, post_whatsapp, presence, set_fixture, uid


def setup(agents: list[str]) -> str:
    skill = uid("sk")
    set_fixture([skill, "voice"], {a: [skill] for a in agents}, {"whatsapp": skill, "sip": "voice"})
    return skill


def depth(skill: str) -> int:
    return presence()["queues"].get(skill, 0)


def agent_view(agent_id: str) -> dict:
    return next(a for a in presence()["agents"] if a["id"] == agent_id)


@aio
async def test_c20_longest_idle_agent_gets_the_conversation():
    older, newer = uid("agent"), uid("agent")
    skill = setup([older, newer])
    a = await agent(older, [skill])
    await asyncio.sleep(0.3)
    b = await agent(newer, [skill])
    with http() as c:
        conv_id = post_whatsapp(c, uid("cust")).json()["conversation_id"]
    m = await a.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    assert m["body"]["agent_id"] == older
    assert conversation(conv_id)["assigned_agent"] == older
    assert not b.messages(conv_id)
    await a.close()
    await b.close()


@aio
async def test_c21_no_available_agent_queues_and_shows_depth():
    skill = setup([uid("agent")])
    with http() as c:
        conv_id = post_whatsapp(c, uid("cust")).json()["conversation_id"]
    assert conversation(conv_id)["status"] == "queued"
    assert depth(skill) == 1


@aio
async def test_c22_agent_becoming_available_takes_the_queue_within_1s():
    a_id = uid("agent")
    skill = setup([a_id])
    with http() as c:
        conv_id = post_whatsapp(c, uid("cust")).json()["conversation_id"]
    assert depth(skill) == 1
    a = await agent(a_id, [skill], available=False)
    t0 = time.monotonic()
    await a.send({"type": "status", "available": True})
    await a.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment", timeout=1.0)
    assert time.monotonic() - t0 <= 1.0
    assert depth(skill) == 0
    await a.close()


@aio
async def test_c23_queue_is_served_in_arrival_order():
    a_id = uid("agent")
    skill = setup([a_id])
    convs = []
    with http() as c:
        for i in range(3):
            convs.append(post_whatsapp(c, uid("cust"), f"q{i}").json()["conversation_id"])
            time.sleep(0.02)
    a = await agent(a_id, [skill])
    for cid in convs:
        await a.wait_message(lambda m, cid=cid: m["conversation_id"] == cid and m["kind"] == "assignment")
    order = [m["conversation_id"] for m in a.messages() if m["kind"] == "assignment"]
    assert order == convs
    await a.close()


@aio
async def test_c24_disconnected_agent_conversation_reroutes_after_30s():
    first, second = uid("agent"), uid("agent")
    skill = setup([first, second])
    a = await agent(first, [skill])
    with http() as c:
        conv_id = post_whatsapp(c, uid("cust")).json()["conversation_id"]
    await a.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    b = await agent(second, [skill])
    await a.close()
    t0 = time.monotonic()
    # B receives the conversation's history too, including the first assignment (to A).
    await b.wait_message(
        lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment" and m["body"]["agent_id"] == second, timeout=40
    )
    elapsed = time.monotonic() - t0
    assert 28 <= elapsed <= 32, f"re-routed after {elapsed:.1f}s"
    await b.close()


@aio
async def test_c25_reconnect_within_30s_keeps_assignment_and_replays():
    a_id = uid("agent")
    skill = setup([a_id])
    a = await agent(a_id, [skill])
    frm = uid("cust")
    with http() as c:
        conv_id = post_whatsapp(c, frm, "before").json()["conversation_id"]
        await a.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
        last = a.last_seq()[conv_id]
        sid = a.session_id
        await a.close()
        missed = post_whatsapp(c, frm, "while away").json()["message_id"]
    await asyncio.sleep(5)
    back = await Session("agent", a_id, skills=[skill]).open(resume={conv_id: last}, session_id=sid)
    await back.wait_message(lambda m: m["message_id"] == missed)
    await asyncio.sleep(27)  # past the 30 s mark since the disconnect
    assert conversation(conv_id)["assigned_agent"] == a_id
    assert conversation(conv_id)["status"] == "assigned"
    await back.close()


@aio
async def test_c26_disposition_closes_and_is_the_last_message():
    a_id = uid("agent")
    skill = setup([a_id])
    a = await agent(a_id, [skill])
    with http() as c:
        conv_id = post_whatsapp(c, uid("cust")).json()["conversation_id"]
    await a.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    before = agent_view(a_id)["open_conversations"]
    ack = await a.dispose(conv_id, "resolved")
    assert ack["type"] == "ack"
    conv = conversation(conv_id)
    assert conv["status"] == "closed"
    assert conv["messages"][-1]["kind"] == "disposition" and conv["messages"][-1]["seq"] == conv["last_seq"]
    assert agent_view(a_id)["open_conversations"] == before - 1
    await a.close()


@aio
async def test_c27_message_on_closed_conversation_opens_a_new_one():
    a_id = uid("agent")
    skill = setup([a_id])
    a = await agent(a_id, [skill])
    cu = await customer(uid("cust"))
    first = await cu.say("one")
    conv_id = first["conversation_id"]
    await a.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    await a.dispose(conv_id)
    again = await cu.say("two", conversation_id=conv_id)
    assert again["type"] == "ack" and again["conversation_id"] != conv_id and again["seq"] == 1
    await cu.close()
    await a.close()


@aio
async def test_c28_reload_removing_a_skill_stops_new_work_keeps_existing():
    a_id = uid("agent")
    skill = setup([a_id])
    a = await agent(a_id, [skill])
    with http() as c:
        kept = post_whatsapp(c, uid("cust")).json()["conversation_id"]
        await a.wait_message(lambda m: m["conversation_id"] == kept and m["kind"] == "assignment")
        set_fixture([skill, "voice"], {a_id: []}, {"whatsapp": skill, "sip": "voice"})
        new = post_whatsapp(c, uid("cust")).json()["conversation_id"]
    await asyncio.sleep(1.5)
    assert conversation(new)["status"] == "queued"
    assert conversation(kept)["assigned_agent"] == a_id and conversation(kept)["status"] == "assigned"
    await a.close()
