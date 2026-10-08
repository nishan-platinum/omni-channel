"""C40-C43: durability under node kill and restart (needs the Docker socket)."""

import asyncio
import concurrent.futures as cf
import time

import httpx

from gw import (
    AUTH,
    GW_URL,
    NODE_A,
    NODE_B,
    SERVICE_A,
    SERVICE_B,
    Session,
    agent,
    aio,
    assert_contiguous,
    conversation,
    customer,
    http,
    kill_node,
    post_whatsapp,
    presence,
    restart_node,
    set_fixture,
    start_node,
    uid,
    wait_healthy,
)


def setup(agents: list[str]) -> str:
    skill = uid("sk")
    set_fixture([skill, "voice"], {a: [skill] for a in agents}, {"whatsapp": skill, "sip": "voice"})
    return skill


def test_c40_acked_ingress_survives_sigkill_of_its_node():
    with http(NODE_A) as c:
        r = post_whatsapp(c, uid("cust"), "durable")
    assert r.status_code == 202
    kill_node(SERVICE_A)
    try:
        conv = conversation(r.json()["conversation_id"], base=NODE_B)
        assert any(m["message_id"] == r.json()["message_id"] for m in conv["messages"])
    finally:
        start_node(SERVICE_A)
        assert wait_healthy(NODE_A)


@aio
async def test_c41_acked_agent_send_survives_sigkill_and_reaches_customer():
    a_id = uid("agent")
    skill = setup([a_id])
    ag = await agent(a_id, [skill], base=NODE_A)
    cu = await customer(uid("cust"), base=NODE_B)
    first = await cu.say("hi")
    conv_id = first["conversation_id"]
    await ag.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    ack = await ag.say("answer before crash", conversation_id=conv_id)
    assert ack["type"] == "ack"
    kill_node(SERVICE_A)
    try:
        conv = conversation(conv_id, base=NODE_B)
        assert any(m["message_id"] == ack["message_id"] for m in conv["messages"])
        last = cu.last_seq().get(conv_id, 0)
        sid = cu.session_id
        await cu.close()
        back = await Session("customer", cu.id, base=NODE_B).open(resume={conv_id: min(last, first["seq"])}, session_id=sid)
        await back.wait_message(lambda m: m["message_id"] == ack["message_id"])
        await back.close()
    finally:
        start_node(SERVICE_A)
        assert wait_healthy(NODE_A)
    await ag.close()


def test_c42_sigkill_mid_run_loses_no_acked_message_and_keeps_seq_contiguous():
    customers = [uid("c42") for _ in range(100)]
    acked: dict[str, list[str]] = {}

    def send(i):
        cust = customers[i % 100]
        for _ in range(20):
            try:
                with httpx.Client(base_url=GW_URL, headers=AUTH, timeout=5) as c:
                    r = post_whatsapp(c, cust, f"m{i}", external_id=f"{cust}-{i}")
                if r.status_code == 202:
                    return cust, r.json()["conversation_id"], r.json()["message_id"]
                if r.status_code == 409:  # an earlier attempt was stored; not acked to us
                    return cust, None, None
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
        return cust, None, None

    with cf.ThreadPoolExecutor(32) as pool:
        futures = [pool.submit(send, i) for i in range(1000)]
        time.sleep(0.5)
        kill_node(SERVICE_B)
        results = [f.result() for f in futures]
    start_node(SERVICE_B)
    assert wait_healthy(NODE_B)
    convs: dict[str, str] = {}
    for cust, cid, mid in results:
        if mid:
            acked.setdefault(cid, []).append(mid)
            convs[cust] = cid
    assert sum(len(v) for v in acked.values()) >= 900, "too few acknowledged messages to be meaningful"
    for cid, mids in acked.items():
        conv = conversation(cid)
        stored = {m["message_id"] for m in conv["messages"]}
        assert set(mids) <= stored, f"acknowledged messages missing in {cid}"
        assert_contiguous(conv["messages"])


@aio
async def test_c43_clean_restart_of_both_nodes_keeps_state_and_rebuilds_presence():
    a_id = uid("agent")
    skill = setup([a_id])
    ag = await agent(a_id, [skill])
    with http() as c:
        conv_id = post_whatsapp(c, uid("cust")).json()["conversation_id"]
    await ag.wait_message(lambda m: m["conversation_id"] == conv_id and m["kind"] == "assignment")
    before = conversation(conv_id)
    sid, last = ag.session_id, ag.last_seq()
    restart_node(SERVICE_A)
    assert wait_healthy(NODE_A)
    restart_node(SERVICE_B)
    assert wait_healthy(NODE_B)
    await ag.close()
    back = await Session("agent", a_id, skills=[skill]).open(resume=last, session_id=sid)
    after = conversation(conv_id)
    assert after["messages"] == before["messages"]
    assert after["assigned_agent"] == a_id and after["status"] == "assigned"
    deadline = time.monotonic() + 5
    status = None
    while time.monotonic() < deadline:
        status = next(a for a in presence()["agents"] if a["id"] == a_id)["status"]
        if status == "available":
            break
        await asyncio.sleep(0.1)
    assert status == "available"
    await back.close()
