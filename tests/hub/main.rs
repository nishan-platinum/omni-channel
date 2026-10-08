//! M10 hub (gateway slice) integration tests: simulated channels → durable ordered store →
//! skill routing → agent/customer WebSockets. Needs the Docker databases and Redis.
#[path = "../common/mod.rs"]
mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use common::hub::*;

fn conv_id(v: &Value) -> String {
    v["conversation"]["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn whatsapp_message_reaches_available_agent_and_duplicates_are_ignored() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, tok) = h.agent(&t, &["support"], 3).await;
    let (mut ws, welcome) = h.agent_online(&tok, true).await;
    assert_eq!(welcome["conversations"], json!([]));

    let id = wamid();
    let (st, body) = h.whatsapp_inbound(&t.whatsapp, "60111222333", "My internet is down", &id).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["messages"], 1);
    let a = recv_type(&mut ws, "conversation.assigned").await;
    assert_eq!(a["conversation"]["channel"], "whatsapp");
    assert_eq!(a["conversation"]["required_skill"], "support");
    assert_eq!(a["messages"][0]["seq"], 1);
    assert_eq!(a["messages"][0]["body"], "My internet is down");

    // Same provider message id again → acknowledged, not stored twice.
    let (st, body) = h.whatsapp_inbound(&t.whatsapp, "60111222333", "My internet is down", &id).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["data"]["duplicates"], 1);
    let (_, _) = h.whatsapp_inbound(&t.whatsapp, "60111222333", "Still down", &wamid()).await;
    let m = recv_type(&mut ws, "message.new").await;
    assert_eq!(m["message"]["seq"], 2);
    let r = h.app.get(&format!("/v1/hub/conversations/{}/messages", conv_id(&a)), &tok).await;
    assert_eq!(r.data()["messages"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn conversation_waits_in_skill_queue_until_an_agent_becomes_available() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, tok) = h.agent(&t, &["support"], 3).await;
    h.whatsapp_inbound(&t.whatsapp, "60100000001", "Hello?", &wamid()).await;
    let q = h.app.get("/v1/hub/queues", &t.ta).await;
    assert_eq!(q.data(), &json!([{ "skill": "support", "queued": 1, "oldest_wait_secs": q.data()[0]["oldest_wait_secs"] }]));

    let (mut ws, _) = h.agent_online(&tok, true).await;
    let a = recv_type(&mut ws, "conversation.assigned").await;
    assert_eq!(a["messages"][0]["body"], "Hello?");
    assert_eq!(h.app.get("/v1/hub/queues", &t.ta).await.data(), &json!([]));
}

#[tokio::test]
async fn routing_respects_skills_and_capacity_and_close_frees_capacity() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, sales) = h.agent(&t, &["sales"], 3).await;
    let (_, support) = h.agent(&t, &["support"], 1).await;
    let (mut ws_sales, _) = h.agent_online(&sales, true).await;
    let (mut ws_sup, _) = h.agent_online(&support, true).await;

    h.whatsapp_inbound(&t.whatsapp, "60100000011", "first", &wamid()).await;
    let first = recv_type(&mut ws_sup, "conversation.assigned").await;
    h.whatsapp_inbound(&t.whatsapp, "60100000012", "second", &wamid()).await;
    // Support agent is at capacity (1); the sales agent has no 'support' skill → queued.
    assert_no_frame(&mut ws_sup, "conversation.assigned", 400).await;
    assert_no_frame(&mut ws_sales, "conversation.assigned", 100).await;

    send(&mut ws_sup, json!({ "type": "conversation.close", "conversation_id": conv_id(&first) })).await;
    let upd = recv_type(&mut ws_sup, "conversation.updated").await;
    assert_eq!(upd["conversation"]["status"], "closed");
    let second = recv_type(&mut ws_sup, "conversation.assigned").await;
    assert_eq!(second["messages"][0]["body"], "second");
}

#[tokio::test]
async fn agent_reply_goes_through_the_cloud_api_and_receipts_reach_read() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, tok) = h.agent(&t, &["support"], 3).await;
    let (mut ws, _) = h.agent_online(&tok, true).await;
    // Customer writes via fake-meta → signed webhook over HTTP → hub.
    h.customer_writes(&t.whatsapp, "60100000021", "Can you help?").await;
    let a = recv_type(&mut ws, "conversation.assigned").await;
    assert_eq!(a["messages"][0]["body"], "Can you help?");
    let conv = conv_id(&a);

    send(&mut ws, json!({ "type": "message.send", "conversation_id": conv, "client_msg_id": "r1", "body": "Yes, checking now" })).await;
    let ack = recv_type(&mut ws, "ack").await;
    assert_eq!(ack["client_msg_id"], "r1");
    assert_eq!(ack["message"]["seq"], 2);
    assert_eq!(ack["message"]["delivery_status"], "queued");

    // Real adapter: POST /{v}/{phone_number_id}/messages to fake-meta, which answers with
    // sent → delivered → read status webhooks.
    let data = h
        .pump_until(t.tenant_id, &tok, &conv, |d: &Value| {
            d["messages"].as_array().is_some_and(|m| m.iter().any(|x| x["seq"] == 2 && x["delivery_status"] == "read"))
        })
        .await;
    assert_eq!(data["messages"][1]["direction"], "outbound");
    let st = recv_type(&mut ws, "message.status").await;
    assert_eq!(st["seq"], 2);
    let phone = h.fake_outbox(&t.whatsapp).await;
    assert!(
        phone.iter().any(|m| m["to"] == "60100000021" && m["text"] == "Yes, checking now"),
        "the customer's phone got the reply: {phone:?}"
    );
}

async fn queued_reply(h: &HubApp, t: &HubTenant, customer: &str, via_fake_meta: bool, body: &str) -> (String, String, Uuid) {
    let (_, tok) = h.agent(t, &["support"], 3).await;
    let (mut ws, _) = h.agent_online(&tok, true).await;
    if via_fake_meta {
        h.customer_writes(&t.whatsapp, customer, "hi").await;
    } else {
        h.whatsapp_inbound(&t.whatsapp, customer, "hi", &wamid()).await;
    }
    let conv = conv_id(&recv_type(&mut ws, "conversation.assigned").await);
    send(&mut ws, json!({ "type": "message.send", "conversation_id": conv, "client_msg_id": "f1", "body": body })).await;
    let ack = recv_type(&mut ws, "ack").await;
    (tok, conv, Uuid::parse_str(ack["message"]["id"].as_str().unwrap()).unwrap())
}

#[tokio::test]
async fn transient_cloud_api_error_enters_retry_ladder() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (tok, conv, mid) = queued_reply(&h, &t, "60100000031", true, "this will [retry]").await;
    // fake-meta answers 500 / 131000 → attempts 1, next attempt ≈ +60 s, still queued.
    let mut attempts = 0i32;
    for _ in 0..50 {
        h.app.state.hub.delivery_tick_for_tenant(t.tenant_id).await.unwrap();
        attempts = sqlx::query_scalar("SELECT attempts FROM hub.outbound_queue WHERE message_id = $1")
            .bind(mid)
            .fetch_optional(&h.app.state.db.owner)
            .await
            .unwrap()
            .unwrap_or(0);
        if attempts > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(attempts, 1);
    let wait: f64 =
        sqlx::query_scalar("SELECT extract(epoch FROM next_attempt_at - now())::float8 FROM hub.outbound_queue WHERE message_id = $1")
            .bind(mid)
            .fetch_one(&h.app.state.db.owner)
            .await
            .unwrap();
    assert!(wait > 45.0 && wait <= 61.0, "retry scheduled in {wait}s");
    let r = h.app.get(&format!("/v1/hub/conversations/{conv}/messages"), &tok).await;
    assert_eq!(r.data()["messages"][1]["delivery_status"], "queued");
}

async fn wait_failed(h: &HubApp, t: &HubTenant, tok: &str, conv: &str, mid: Uuid) -> String {
    h.pump_until(t.tenant_id, tok, conv, |d: &Value| d["messages"][1]["delivery_status"] == "failed").await;
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM hub.outbound_queue WHERE message_id = $1")
        .bind(mid)
        .fetch_one(&h.app.state.db.owner)
        .await
        .unwrap();
    assert_eq!(queued, 0, "permanent errors are not retried");
    sqlx::query_scalar("SELECT detail FROM hub.message_status_events WHERE message_id = $1 AND status = 'failed'")
        .bind(mid)
        .fetch_one(&h.app.state.db.owner)
        .await
        .unwrap()
}

#[tokio::test]
async fn permanent_cloud_api_error_fails_at_once() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (tok, conv, mid) = queued_reply(&h, &t, "60100000032", true, "this will [fail]").await;
    let detail = wait_failed(&h, &t, &tok, &conv, mid).await;
    assert!(detail.contains("131026"), "{detail}");
}

#[tokio::test]
async fn reply_outside_the_24_hour_window_is_refused_like_whatsapp() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    // The customer's message reached the hub but fake-meta never saw it → no open window.
    let (tok, conv, mid) = queued_reply(&h, &t, "60100000033", false, "hello again").await;
    let detail = wait_failed(&h, &t, &tok, &conv, mid).await;
    assert!(detail.contains("131047"), "{detail}");
}

#[tokio::test]
async fn per_conversation_order_is_strict_and_client_retries_are_idempotent() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, tok) = h.agent(&t, &["support"], 3).await;
    let (mut ws, _) = h.agent_online(&tok, true).await;
    h.whatsapp_inbound(&t.whatsapp, "60100000041", "start", &wamid()).await;
    let conv = conv_id(&recv_type(&mut ws, "conversation.assigned").await);

    // Inbound messages arriving concurrently get distinct, gap-free sequence numbers.
    let mut tasks = Vec::new();
    for i in 0..15 {
        let app = h.app.router.clone();
        let (wa, sec) = (t.whatsapp.clone(), FAKE_SECRET.to_string());
        tasks.push(tokio::spawn(async move {
            use tower::ServiceExt;
            let body = omni_m01::modules::m10_hub::infrastructure::channels::whatsapp::inbound_payload(
                &wa,
                "60100000041",
                "C",
                &format!("m{i}"),
                &wamid(),
            )
            .to_string();
            let sig = omni_m01::modules::m10_hub::infrastructure::channels::sign(sec.as_bytes(), body.as_bytes());
            let req = axum::http::Request::post("/v1/hub/channels/whatsapp/webhook")
                .header("x-hub-signature-256", sig)
                .body(axum::body::Body::from(body))
                .unwrap();
            app.oneshot(req).await.unwrap().status()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), StatusCode::OK);
    }
    // Agent replies, one of them retried with the same client_msg_id.
    for i in 0..5 {
        send(
            &mut ws,
            json!({ "type": "message.send", "conversation_id": conv, "client_msg_id": format!("o{i}"), "body": format!("reply {i}") }),
        )
        .await;
    }
    send(&mut ws, json!({ "type": "message.send", "conversation_id": conv, "client_msg_id": "o2", "body": "reply 2" })).await;
    let mut acks = Vec::new();
    while acks.len() < 6 {
        let a = recv_type(&mut ws, "ack").await;
        acks.push((a["client_msg_id"].as_str().unwrap().to_string(), a["message"]["seq"].as_i64().unwrap()));
    }
    let o2: Vec<i64> = acks.iter().filter(|(c, _)| c == "o2").map(|(_, s)| *s).collect();
    assert_eq!(o2.len(), 2);
    assert_eq!(o2[0], o2[1], "retry must return the stored message");

    let r = h.app.get(&format!("/v1/hub/conversations/{conv}/messages"), &tok).await;
    let seqs: Vec<i64> = r.data()["messages"].as_array().unwrap().iter().map(|m| m["seq"].as_i64().unwrap()).collect();
    assert_eq!(seqs, (1..=21).collect::<Vec<_>>(), "1 + 15 inbound + 5 replies, gap-free");
}

#[tokio::test]
async fn reconnecting_agent_resumes_from_last_sequence() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, tok) = h.agent(&t, &["support"], 3).await;
    let (mut ws, _) = h.agent_online(&tok, true).await;
    h.whatsapp_inbound(&t.whatsapp, "60100000051", "one", &wamid()).await;
    let conv = conv_id(&recv_type(&mut ws, "conversation.assigned").await);
    ws.close(None).await.unwrap();

    for text in ["two", "three", "four"] {
        h.whatsapp_inbound(&t.whatsapp, "60100000051", text, &wamid()).await;
    }
    let mut ws = h.agent_ws(&tok).await;
    send(&mut ws, json!({ "type": "hello", "resume": { conv.clone(): 1 } })).await;
    let w = recv_type(&mut ws, "welcome").await;
    let msgs = w["conversations"][0]["messages"].as_array().unwrap();
    let bodies: Vec<&str> = msgs.iter().map(|m| m["body"].as_str().unwrap()).collect();
    assert_eq!(bodies, vec!["two", "three", "four"]);
    assert_eq!(msgs[0]["seq"], 2);
}

#[tokio::test]
async fn web_chat_customer_and_agent_talk_live_with_read_receipts() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, tok) = h.agent(&t, &["sales"], 3).await;
    let (mut agent, _) = h.agent_online(&tok, true).await;

    let (st, s) = h.raw_post("/v1/hub/customer/sessions", &json!({ "widget_key": t.widget, "name": "Mei" }).to_string(), &[]).await;
    assert_eq!(st, StatusCode::CREATED, "{s}");
    let ctoken = s["data"]["token"].as_str().unwrap().to_string();
    let (mut cust, w) = h.customer_ws(&ctoken, 0).await;
    assert_eq!(w["messages"], json!([]));

    send(&mut cust, json!({ "type": "message.send", "client_msg_id": "c1", "body": "Do you have fibre in Penang?" })).await;
    let ack = recv_type(&mut cust, "ack").await;
    assert_eq!(ack["message"]["seq"], 1);
    let a = recv_type(&mut agent, "conversation.assigned").await;
    assert_eq!(a["conversation"]["channel"], "webchat");
    assert_eq!(a["conversation"]["customer_name"], "Mei");
    let conv = conv_id(&a);

    send(&mut agent, json!({ "type": "message.send", "conversation_id": conv, "client_msg_id": "a1", "body": "Yes we do!" })).await;
    // The customer's own message is echoed too (other open tabs); skip it.
    let got = loop {
        let m = recv_type(&mut cust, "message.new").await;
        if m["message"]["from"] == "agent" {
            break m;
        }
        assert_eq!(m["message"]["body"], "Do you have fibre in Penang?");
    };
    assert_eq!(got["message"]["body"], "Yes we do!");
    assert_eq!(got["message"]["from"], "agent");
    assert!(got["message"].get("sender_id").is_none(), "agent ids are not exposed to customers");

    send(&mut cust, json!({ "type": "seen", "seq": 2 })).await;
    loop {
        let st = recv_type(&mut agent, "message.status").await;
        if st["status"] == "read" {
            assert_eq!(st["seq"], 2);
            break;
        }
    }
    // A reconnecting customer resumes from its last sequence number.
    cust.close(None).await.unwrap();
    let (_, w) = h.customer_ws(&ctoken, 1).await;
    assert_eq!(w["messages"].as_array().unwrap().len(), 1);
    assert_eq!(w["messages"][0]["body"], "Yes we do!");
}

#[tokio::test]
async fn sip_call_events_become_an_ordered_voice_conversation() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, tok) = h.agent(&t, &["support"], 3).await;
    let (mut ws, _) = h.agent_online(&tok, true).await;
    for ev in ["ringing", "answered", "ended"] {
        let (st, b) = h.sip_event(&t.did, "call-77", ev, "+60123334444").await;
        assert_eq!(st, StatusCode::OK, "{b}");
    }
    let a = recv_type(&mut ws, "conversation.assigned").await;
    assert_eq!(a["conversation"]["channel"], "voice");
    let conv = conv_id(&a);
    let r = h.app.get(&format!("/v1/hub/conversations/{conv}/messages"), &tok).await;
    let bodies: Vec<String> = r.data()["messages"].as_array().unwrap().iter().map(|m| m["body"].as_str().unwrap().to_string()).collect();
    assert!(bodies[0].starts_with("Incoming call from +60123334444"));
    assert!(bodies[1].starts_with("Call answered"));
    assert!(bodies[2].starts_with("Call ended"));
    assert!(r.data()["messages"].as_array().unwrap().iter().all(|m| m["kind"] == "call_event"));
    // Re-delivered event is ignored; voice has no text reply.
    let (_, b) = h.sip_event(&t.did, "call-77", "ended", "+60123334444").await;
    assert_eq!(b["data"]["duplicates"], 1);
    send(&mut ws, json!({ "type": "message.send", "conversation_id": conv, "client_msg_id": "v1", "body": "hello" })).await;
    let e = recv_type(&mut ws, "error").await;
    assert_eq!(e["code"], "VALIDATION_FAILED");
}

#[tokio::test]
async fn webhooks_reject_bad_signatures_and_unknown_numbers() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let body = omni_m01::modules::m10_hub::infrastructure::channels::whatsapp::inbound_payload(&t.whatsapp, "6011", "x", "hi", &wamid())
        .to_string();
    let (st, _) = h.raw_post("/v1/hub/channels/whatsapp/webhook", &body, &[("x-hub-signature-256", "sha256=deadbeef")]).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    let (st, _) = h.raw_post("/v1/hub/channels/whatsapp/webhook", &body, &[]).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    let (st, _) = h.whatsapp_inbound("000000000000000", "6011", "hi", &wamid()).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "unknown phone_number_id belongs to no tenant");
    let (st, _) = h.sip_event("+60999999999", "c1", "ringing", "+6011").await;
    assert_eq!(st, StatusCode::NOT_FOUND, "unknown DID = SIP 404");

    let ok = h
        .app
        .send(
            axum::http::Method::GET,
            "/v1/hub/channels/whatsapp/webhook?hub.mode=subscribe&hub.verify_token=test-verify-token&hub.challenge=12345",
            None,
            None,
            &[],
        )
        .await;
    assert_eq!(ok.status, StatusCode::OK);
    assert_eq!(ok.text, "12345");
    let bad = h
        .app
        .send(
            axum::http::Method::GET,
            "/v1/hub/channels/whatsapp/webhook?hub.mode=subscribe&hub.verify_token=nope&hub.challenge=1",
            None,
            None,
            &[],
        )
        .await;
    assert_eq!(bad.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn stale_agent_is_reaped_and_work_is_rerouted() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (gone, tok1) = h.agent(&t, &["support"], 3).await;
    let (_, tok2) = h.agent(&t, &["support"], 3).await;
    let (mut ws1, _) = h.agent_online(&tok1, true).await;
    h.whatsapp_inbound(&t.whatsapp, "60100000061", "hello", &wamid()).await;
    let a = recv_type(&mut ws1, "conversation.assigned").await;
    let first_agent = a["conversation"]["assigned_agent"].as_str().unwrap().to_string();
    assert_eq!(first_agent, gone.to_string());
    drop(ws1); // the node "crashes": no more heartbeats

    let (mut ws2, _) = h.agent_online(&tok2, true).await;
    sqlx::query("UPDATE hub.agent_presence SET heartbeat_at = now() - interval '5 minutes' WHERE user_id = $1")
        .bind(gone)
        .execute(&h.app.state.db.owner)
        .await
        .unwrap();
    h.app.state.hub.reaper_tick().await.unwrap();
    let re = recv_type(&mut ws2, "conversation.assigned").await;
    assert_eq!(conv_id(&re), conv_id(&a));
    let agents = h.app.get("/v1/hub/agents", &t.ta).await;
    let me = agents.data().as_array().unwrap().iter().find(|x| x["user_id"] == json!(gone)).unwrap().clone();
    assert_eq!(me["presence"], "offline");
}

#[tokio::test]
async fn agent_creation_counts_against_the_m01_user_quota() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let r = h
        .app
        .post("/v1/hub/agents", &t.ta, json!({ "email": "x@y.example", "display_name": "X", "password": "short", "skills": ["support"] }))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = h
        .app
        .post(
            "/v1/hub/agents",
            &t.ta,
            json!({ "email": "x@y.example", "display_name": "X", "password": AGENT_PASSWORD, "skills": ["bad skill"] }),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    h.agent(&t, &["support"], 3).await;
    let q = h.app.get(&format!("/v1/tenants/{}/quota", t.tenant_id), &t.ta).await;
    assert_eq!(q.status, StatusCode::OK, "{}", q.text);
    assert_eq!(q.data()["usage"]["users"], 2, "Tenant Admin + 1 agent are counted: {}", q.text);
}

#[tokio::test]
async fn going_offline_hands_open_conversations_to_another_agent() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let (_, tok1) = h.agent(&t, &["support"], 3).await;
    let (_, tok2) = h.agent(&t, &["support"], 3).await;
    let (mut ws1, _) = h.agent_online(&tok1, true).await;
    h.whatsapp_inbound(&t.whatsapp, "60100000071", "first", &wamid()).await;
    let a = recv_type(&mut ws1, "conversation.assigned").await;
    let (mut ws2, _) = h.agent_online(&tok2, true).await;
    // Agent 1 signs off: the open conversation moves to agent 2, and so do new messages.
    send(&mut ws1, json!({ "type": "presence.set", "status": "offline" })).await;
    let re = recv_type(&mut ws2, "conversation.assigned").await;
    assert_eq!(conv_id(&re), conv_id(&a));
    h.whatsapp_inbound(&t.whatsapp, "60100000071", "second", &wamid()).await;
    let m = recv_type(&mut ws2, "message.new").await;
    assert_eq!(m["message"]["body"], "second");
}

#[tokio::test]
async fn bulk_whatsapp_traffic_from_fake_meta_is_all_stored_in_order() {
    let h = HubApp::new().await;
    let t = h.tenant().await;
    let sim = h.app.state.hub.simulator.clone().unwrap();
    // 20 customers × 5 messages at 200/s, all through signed HTTP webhooks.
    sim.start_load(&t.whatsapp, 20, 5, 200).await.unwrap();
    let mut stored = 0i64;
    for _ in 0..100 {
        stored = sqlx::query_scalar(
            "SELECT count(*) FROM hub.messages m JOIN hub.conversations c ON c.id = m.conversation_id
              JOIN hub.channel_endpoints e ON e.id = c.endpoint_id WHERE e.address = $1",
        )
        .bind(&t.whatsapp)
        .fetch_one(&h.app.state.db.owner)
        .await
        .unwrap();
        if stored == 100 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(stored, 100, "every bulk message stored exactly once");
    let gaps: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM hub.conversations c JOIN hub.channel_endpoints e ON e.id = c.endpoint_id
          WHERE e.address = $1 AND c.last_seq <> (SELECT count(*) FROM hub.messages m WHERE m.conversation_id = c.id)",
    )
    .bind(&t.whatsapp)
    .fetch_one(&h.app.state.db.owner)
    .await
    .unwrap();
    assert_eq!(gaps, 0, "per-conversation sequences are gap-free");
    let stats = sim.stats().await.unwrap();
    assert_eq!(stats["webhooks_given_up"], 0);
    assert!(stats["inbound_ack"]["count"].as_u64().unwrap() >= 100);
}

fn meta_settings(h: &HubApp, token: &str, app_secret: &str) -> omni_m01::platform::config::WhatsAppSettings {
    let mut w = h.app.state.config.whatsapp.clone();
    w.provider = "meta".into();
    w.base_url = format!("http://{}", h.fake_meta);
    w.access_token = token.into();
    w.app_secret = app_secret.into();
    w.phone_number_id = Some("1279886618550689".into());
    w.app_id = Some("1101641942562622".into());
    w.business_account_id = Some("933506166148717".into());
    w.public_base_url = Some(format!("http://{}", h.addr));
    w
}

#[tokio::test]
async fn meta_auto_setup_checks_token_and_registers_the_webhook() {
    use omni_m01::modules::m10_hub::infrastructure::channels::whatsapp_setup::MetaLink;
    let h = HubApp::new().await;
    // Against fake-meta, which behaves like Meta: number lookup with the token, subscription with
    // the app token {app_id}|{app_secret} and a GET verification handshake to our callback.
    let link = MetaLink::new(meta_settings(&h, FAKE_TOKEN, FAKE_SECRET));
    link.sync(true).await;
    let st = link.status();
    assert!(st.number.as_ref().unwrap().ok, "{st:?}");
    assert!(st.webhook.as_ref().unwrap().ok, "{st:?}");
    assert!(st.waba.as_ref().unwrap().ok, "{st:?}");
    assert!(st.all_ok());
    assert_eq!(st.callback_url.as_deref(), Some(format!("http://{}/v1/hub/channels/whatsapp/webhook", h.addr).as_str()));

    // Wrong token → number check shows Meta's error 190; wrong app secret → registration fails.
    let bad = MetaLink::new(meta_settings(&h, "expired-token", "wrong-app-secret"));
    bad.sync(true).await;
    let st = bad.status();
    assert!(!st.number.as_ref().unwrap().ok && st.number.as_ref().unwrap().detail.contains("190"), "{st:?}");
    assert!(
        !format!("{st:?}").contains("expired-token") && !format!("{st:?}").contains("wrong-app-secret"),
        "secrets must be redacted: {st:?}"
    );
    assert!(!st.webhook.as_ref().unwrap().ok, "{st:?}");
    assert!(!st.all_ok());

    // Missing IDs → manual setup hint, no registration attempt.
    let mut w = meta_settings(&h, FAKE_TOKEN, FAKE_SECRET);
    w.app_id = None;
    let manual = MetaLink::new(w);
    manual.sync(true).await;
    let st = manual.status();
    assert!(st.manual_reason.is_some() && st.webhook.is_none(), "{st:?}");
}

#[tokio::test]
async fn webhook_registration_fails_when_the_verify_token_does_not_match() {
    use omni_m01::modules::m10_hub::infrastructure::channels::whatsapp_setup::MetaLink;
    let h = HubApp::new().await;
    let mut w = meta_settings(&h, FAKE_TOKEN, FAKE_SECRET);
    w.verify_token = "not-the-hub-verify-token".into();
    let link = MetaLink::new(w);
    link.sync(true).await;
    let st = link.status();
    let hook = st.webhook.unwrap();
    assert!(!hook.ok && hook.detail.contains("2200"), "callback verification must fail: {}", hook.detail);
}
