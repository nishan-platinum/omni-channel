"""C01-C06: ingress and the canonical model."""

import httpx

from gw import AUTH, GW_URL, assert_contiguous, conversation, http, post_sip, post_whatsapp, set_fixture, uid


def test_c01_whatsapp_from_new_customer_opens_conversation():
    with http() as c:
        r = post_whatsapp(c, uid("cust"), "hi there")
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["message_id"]
    conv = conversation(body["conversation_id"])
    assert conv["channel"] == "whatsapp"
    assert len(conv["messages"]) == 1
    m = conv["messages"][0]
    assert (m["seq"], m["message_id"], m["channel"], m["direction"], m["kind"]) == (1, body["message_id"], "whatsapp", "inbound", "text")
    assert m["actor"]["kind"] == "customer" and m["received_at"].endswith("Z") and len(m["message_id"]) == 26


def test_c02_duplicate_external_id_is_409_and_stored_once():
    ext, frm = uid("ext"), uid("cust")
    with http() as c:
        first = post_whatsapp(c, frm, "one", external_id=ext)
        second = post_whatsapp(c, frm, "one", external_id=ext)
    assert first.status_code == 202 and second.status_code == 409
    assert len(conversation(first.json()["conversation_id"])["messages"]) == 1


def test_c03_sip_invite_dtmf_bye_is_one_conversation_in_order():
    call, frm = uid("call"), uid("caller")
    with http() as c:
        ids = [post_sip(c, call, frm, ev) for ev in ("invite", "dtmf", "bye")]
    assert all(r.status_code == 202 for r in ids), [r.text for r in ids]
    convs = {r.json()["conversation_id"] for r in ids}
    assert len(convs) == 1
    conv = conversation(convs.pop())
    assert conv["channel"] == "sip"
    assert [m["kind"] for m in conv["messages"]] == ["call_event"] * 3
    assert [m["body"]["event"] for m in conv["messages"]] == ["invite", "dtmf", "bye"]
    assert_contiguous(conv["messages"])
    with http() as c:
        dup = post_sip(c, call, frm, "bye", at=conv["messages"][2]["body"]["at"])
    assert dup.status_code == 409


def test_c04_missing_or_wrong_token_is_401_and_nothing_stored():
    ext, frm = uid("ext"), uid("cust")
    payload = {"external_id": ext, "from": frm, "text": "x", "sent_at": "2026-10-05T09:00:00.000Z"}
    assert httpx.post(GW_URL + "/ingress/whatsapp", json=payload, timeout=5).status_code == 401
    assert httpx.post(GW_URL + "/ingress/whatsapp", json=payload, headers={"Authorization": "Bearer wrong"}, timeout=5).status_code == 401
    # Nothing was stored: the same external_id is still new.
    r = httpx.post(GW_URL + "/ingress/whatsapp", json=payload, headers=AUTH, timeout=5)
    assert r.status_code == 202 and r.json()["seq"] == 1


def test_c05_interleaved_customers_get_contiguous_sequences():
    customers = [uid("c05") for _ in range(100)]
    convs = {}
    with httpx.Client(base_url=GW_URL, headers=AUTH, timeout=10, limits=httpx.Limits(max_connections=50)) as c:
        import concurrent.futures as cf

        def send(i):
            r = post_whatsapp(c, customers[i % 100], f"m{i}")
            assert r.status_code == 202, r.text
            return customers[i % 100], r.json()["conversation_id"]

        with cf.ThreadPoolExecutor(32) as pool:
            for cust, cid in pool.map(send, range(1000)):
                convs.setdefault(cust, set()).add(cid)
    assert all(len(v) == 1 for v in convs.values())
    for cids in convs.values():
        conv = conversation(next(iter(cids)))
        assert len(conv["messages"]) == 10
        assert_contiguous(conv["messages"])
        assert len({m["message_id"] for m in conv["messages"]}) == 10


def test_c06_channel_to_skill_from_fixture():
    skill = uid("chat")
    set_fixture([skill, "voice"], {}, {"whatsapp": skill, "sip": "voice"})
    with http() as c:
        r = post_whatsapp(c, uid("cust"))
    conv = conversation(r.json()["conversation_id"])
    assert conv["skill"] == skill and conv["status"] in ("queued", "assigned")
