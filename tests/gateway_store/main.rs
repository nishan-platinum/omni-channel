//! Bake-off gateway store invariants (ADR-0014) against PostgreSQL: gap-free `seq` under
//! concurrency, duplicate refusal without burning a `seq`, queue order, longest-idle routing,
//! failure detection. Needs the gateway database (`docker compose --profile gateway up -d gateway-db`);
//! uses its own database `gateway_test` (created when missing).
//! Behaviour over the wire is covered by the black-box suite in `conformance/`.

use omni_m01::modules::gateway::application::GwError;
use omni_m01::modules::gateway::domain::Channel;
use omni_m01::modules::gateway::domain::{ulid, Actor, ActorKind, Direction, Fixture, FixtureAgent, MessageKind, NewMessage};
use omni_m01::modules::gateway::infrastructure::store::{CustomerTarget, Store};
use serde_json::json;
use sqlx::{Connection, PgConnection};
use tokio::sync::OnceCell;

/// URL of the test database, created once per test binary. Each test opens its own pool: a pool
/// is bound to the runtime of the test that created it.
static TEST_DB: OnceCell<String> = OnceCell::const_new();

/// Tests that route take turns: the reaper's safety pass assigns any routable queue in the database.
static ROUTING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn admin_url() -> String {
    std::env::var("GATEWAY_TEST_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://gateway:dev-gateway-db-password-change-me@localhost:55434/gateway".into())
}

async fn store() -> Store {
    let url = TEST_DB
        .get_or_init(|| async {
            let admin = admin_url();
            let mut c =
                PgConnection::connect(&admin).await.expect("gateway-db reachable (docker compose --profile gateway up -d gateway-db)");
            let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'gateway_test')")
                .fetch_one(&mut c)
                .await
                .unwrap();
            if !exists {
                sqlx::query("CREATE DATABASE gateway_test").execute(&mut c).await.unwrap();
            }
            admin.rsplit_once('/').map(|(b, _)| format!("{b}/gateway_test")).unwrap()
        })
        .await;
    Store::connect(url, 8).await.expect("migrate gateway_test")
}

fn uid(p: &str) -> String {
    format!("{p}-{}", &ulid()[14..])
}

fn text(customer: &str, ext: Option<&str>) -> NewMessage {
    NewMessage {
        direction: Direction::Inbound,
        actor: Actor { kind: ActorKind::Customer, id: customer.into() },
        kind: MessageKind::Text,
        body: json!({"text": "hi"}),
        external_id: ext.map(str::to_string),
        dedup_key: ext.map(|e| format!("wa:{e}")),
        client_ref: None,
    }
}

fn fixture(skill: &str, agents: &[&str]) -> Fixture {
    Fixture {
        skills: vec![skill.to_string()],
        agents: agents.iter().map(|a| FixtureAgent { id: a.to_string(), skills: vec![skill.to_string()] }).collect(),
        channel_to_skill: [("whatsapp".to_string(), skill.to_string())].into(),
    }
}

/// Registers agents (without replacing other tests' fixture agents) and connects them on a live node.
async fn online(s: &Store, node: &str, skill: &str, agents: &[&str]) {
    s.heartbeat(node).await.unwrap();
    for a in agents {
        sqlx::query(
            "INSERT INTO gw.agents (id, skills, in_fixture) VALUES ($1, $2, true) ON CONFLICT (id) DO UPDATE SET skills = EXCLUDED.skills",
        )
        .bind(*a)
        .bind(vec![skill.to_string()])
        .execute(s.pool())
        .await
        .unwrap();
        s.agent_connected(a, &[], &ulid(), &ulid(), node, false).await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_appends_keep_seq_gap_free() {
    let s = store().await;
    let customer = uid("cust");
    let f = fixture(&uid("sk"), &[]);
    let mut set = tokio::task::JoinSet::new();
    for i in 0..40 {
        let (s, customer, f) = (s.clone(), customer.clone(), f.clone());
        set.spawn(async move {
            let target = CustomerTarget { channel: Channel::Whatsapp, customer: &customer, conversation_id: None, fixture: &f };
            s.append_customer(target, &text(&customer, Some(&format!("{customer}-{i}")))).await.unwrap()
        });
    }
    let mut conv = None;
    while let Some(r) = set.join_next().await {
        let a = r.unwrap();
        conv.get_or_insert(a.message.conversation_id.clone());
        assert_eq!(conv.as_deref(), Some(a.message.conversation_id.as_str()), "one open conversation per customer");
    }
    let msgs = s.messages_after(conv.as_deref().unwrap(), 0, 1000).await.unwrap();
    assert_eq!(msgs.iter().map(|m| m.seq).collect::<Vec<_>>(), (1..=40).collect::<Vec<i64>>());
}

#[tokio::test]
async fn duplicate_is_refused_and_does_not_burn_a_seq() {
    let s = store().await;
    let customer = uid("cust");
    let f = fixture(&uid("sk"), &[]);
    let ext = uid("ext");
    let t = || CustomerTarget { channel: Channel::Whatsapp, customer: &customer, conversation_id: None, fixture: &f };
    let first = s.append_customer(t(), &text(&customer, Some(&ext))).await.unwrap();
    assert!(matches!(s.append_customer(t(), &text(&customer, Some(&ext))).await, Err(GwError::Duplicate)));
    let next = s.append_customer(t(), &text(&customer, Some(&uid("ext")))).await.unwrap();
    assert_eq!((first.message.seq, next.message.seq), (1, 2));
}

#[tokio::test]
async fn queue_is_served_in_arrival_order_to_the_longest_idle_agent() {
    let _turn = ROUTING.lock().await;
    let s = store().await;
    let skill = uid("sk");
    let (older, newer) = (uid("agent"), uid("agent"));
    let f = fixture(&skill, &[]);
    let mut queued = Vec::new();
    for _ in 0..3 {
        let c = uid("cust");
        let a = s
            .append_customer(
                CustomerTarget { channel: Channel::Whatsapp, customer: &c, conversation_id: None, fixture: &f },
                &text(&c, None),
            )
            .await
            .unwrap();
        assert!(a.routing_skill.is_some());
        assert!(s.route(&a.message.conversation_id).await.unwrap().is_empty(), "no agent yet");
        queued.push(a.message.conversation_id);
    }
    let node = uid("node");
    online(&s, &node, &skill, &[&older, &newer]).await;
    s.set_status(&older, true).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    s.set_status(&newer, true).await.unwrap();
    let assigned = s.drain_queues(std::slice::from_ref(&skill)).await.unwrap();
    assert_eq!(assigned.iter().map(|a| a.message.conversation_id.clone()).collect::<Vec<_>>(), queued);
    // Longest idle first, and each assignment makes that agent the most recently busy.
    let who: Vec<String> = assigned.iter().map(|a| a.agent.clone().unwrap()).collect();
    assert_eq!(who, vec![older.clone(), newer.clone(), older.clone()]);
    assert!(assigned.iter().all(|a| a.message.kind == MessageKind::Assignment && a.message.seq == 2));
}

#[tokio::test]
async fn dead_node_agents_lose_their_conversations_after_the_grace_period() {
    let _turn = ROUTING.lock().await;
    let s = store().await;
    let skill = uid("sk");
    let (gone, spare) = (uid("agent"), uid("agent"));
    let (dead_node, live_node) = (uid("node"), uid("node"));
    online(&s, &dead_node, &skill, &[&gone]).await;
    s.set_status(&gone, true).await.unwrap();
    let f = fixture(&skill, &[]);
    let c = uid("cust");
    let a = s
        .append_customer(CustomerTarget { channel: Channel::Whatsapp, customer: &c, conversation_id: None, fixture: &f }, &text(&c, None))
        .await
        .unwrap();
    let conv = a.message.conversation_id.clone();
    assert_eq!(s.route(&conv).await.unwrap()[0].agent.as_deref(), Some(gone.as_str()));

    // The node died 10 s ago: the agent goes unavailable, but keeps the conversation (grace 30 s).
    sqlx::query("UPDATE gw.nodes SET heartbeat_at = now() - interval '10 seconds' WHERE node_id = $1")
        .bind(&dead_node)
        .execute(s.pool())
        .await
        .unwrap();
    online(&s, &live_node, &skill, &[&spare]).await;
    s.set_status(&spare, true).await.unwrap();
    s.reap().await.unwrap();
    let (c1, _) = s.conversation(&conv).await.unwrap().unwrap();
    assert_eq!(c1.assigned_agent.as_deref(), Some(gone.as_str()));

    // 31 s after the node's last heartbeat the conversation re-routes to the available agent.
    sqlx::query("UPDATE gw.agents SET disconnected_at = now() - interval '31 seconds' WHERE id = $1")
        .bind(&gone)
        .execute(s.pool())
        .await
        .unwrap();
    let out = s.reap().await.unwrap();
    assert!(out.requeued >= 1);
    let (c2, msgs) = s.conversation(&conv).await.unwrap().unwrap();
    assert_eq!(c2.assigned_agent.as_deref(), Some(spare.as_str()));
    assert_eq!(msgs.iter().map(|m| m.seq).collect::<Vec<_>>(), (1..=msgs.len() as i64).collect::<Vec<_>>());
}
