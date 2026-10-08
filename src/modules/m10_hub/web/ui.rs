//! Server-rendered hub screens:
//! * `/agent` — agent desktop (presence, assigned conversations, live thread, reply, close)
//! * `/chat/{widget_key}` — public customer chat page (web-chat channel)
//! * `/hub/admin` — Tenant Admin: agents, channels, queue depth, recent conversations
//! * `/hub/simulator` — Tenant Admin: fire SIMULATED WhatsApp messages and SIP call events
//!
//! The live parts use a small vanilla script over the WebSocket protocol (no build pipeline).

use askama::Template;
use axum::extract::{Path, State};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::app::AppState;
use crate::bootstrap_auth::extract::{CsrfForm, TenantAdmin, WebUser};
use crate::platform::errors::AppError;
use crate::platform::security::random_token;
use crate::web_support::{redirect_with, render, IncomingFlash, PageCtx, PageError, PageResult};

use super::super::application::ports::{AgentView, ChannelHealth, ConversationView, Endpoint, QueueDepth, SimLogEntry};
use super::super::domain::{CallEvent, Channel};
use super::super::infrastructure::channels::sign;
use super::super::infrastructure::channels::whatsapp_setup::MetaStatus;
use super::{agent_ctx, tenant_admin_tenant};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/agent", get(agent_desktop))
        .route("/chat/{key}", get(chat_page))
        .route("/hub/admin", get(admin_page))
        .route("/hub/admin/agents", post(admin_create_agent))
        .route("/hub/admin/channels", post(admin_provision))
        .route("/hub/simulator", get(simulator_page))
        .route("/hub/simulator/whatsapp", post(sim_whatsapp))
        .route("/hub/simulator/whatsapp/load", post(sim_whatsapp_load))
        .route("/hub/simulator/whatsapp/live", get(sim_whatsapp_live))
        .route("/hub/admin/whatsapp", post(admin_connect_whatsapp))
        .route("/hub/whatsapp/check", post(meta_check))
        .route("/hub/simulator/sip", post(sim_sip))
}

pub fn badge(status: &str) -> &'static str {
    match status {
        "available" | "assigned" | "read" | "delivered" => "badge-ok",
        "queued" | "sent" | "busy" | "wrap_up" => "badge-info",
        "away" => "badge-warn",
        "failed" => "badge-bad",
        _ => "badge-muted",
    }
}

// ------------------------------------------------------------------------------------------------
// Agent desktop
// ------------------------------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "hub/agent.html")]
struct AgentPage {
    page: PageCtx,
    agent: AgentView,
    node: String,
    /// fake-meta (true) or real Meta (false): the `[fail]` test marker only works on fake-meta.
    fake_whatsapp: bool,
}

async fn agent_desktop(State(state): State<AppState>, WebUser(p, _): WebUser, IncomingFlash(flash): IncomingFlash) -> PageResult {
    let ctx = agent_ctx(&p)?;
    let agent = state.hub.agent_profile(ctx).await?;
    let page = PageCtx::for_user("Agent desktop", &p, "agent", flash.clone(), false);
    let fake_whatsapp = state.config.whatsapp.is_fake();
    Ok(render(&AgentPage { page, agent, node: state.hub.node_id.clone(), fake_whatsapp }, flash.is_some()))
}

// ------------------------------------------------------------------------------------------------
// Customer chat (public)
// ------------------------------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "hub/chat.html")]
struct ChatPage {
    page: PageCtx,
    widget_key: String,
    label: String,
}

async fn chat_page(State(state): State<AppState>, Path(key): Path<String>) -> PageResult {
    let ep = state
        .hub
        .repo
        .resolve_endpoint(Channel::WebChat, &key)
        .await?
        .ok_or_else(|| PageError(AppError::not_found("This chat link is not valid")))?;
    Ok(render(&ChatPage { page: PageCtx::anonymous("Chat with us"), widget_key: ep.address, label: ep.label }, false))
}

// ------------------------------------------------------------------------------------------------
// Hub admin (Tenant Admin)
// ------------------------------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "hub/admin.html")]
struct AdminPage {
    page: PageCtx,
    meta: Option<MetaStatus>,
    agents: Vec<AgentView>,
    endpoints: Vec<Endpoint>,
    queues: Vec<QueueDepth>,
    conversations: Vec<ConversationView>,
    health: Vec<ChannelHealth>,
    node: String,
    bus: &'static str,
    agent_sockets: u64,
    customer_sockets: u64,
}

impl AdminPage {
    fn badge(&self, s: &str) -> &'static str {
        badge(s)
    }

    fn agent_name(&self, id: &Option<Uuid>) -> String {
        match id {
            Some(id) => self.agents.iter().find(|a| a.user_id == *id).map(|a| a.display_name.clone()).unwrap_or_else(|| "—".into()),
            None => "—".into(),
        }
    }
}

async fn admin_page(State(state): State<AppState>, TenantAdmin(p, _): TenantAdmin, IncomingFlash(flash): IncomingFlash) -> PageResult {
    let t = tenant_admin_tenant(&p)?;
    let (agent_sockets, customer_sockets) = state.sessions.counts();
    let page = PageCtx::for_user("Contact centre hub", &p, "hub", flash.clone(), false);
    let v = AdminPage {
        page,
        meta: state.meta_link.as_ref().map(|l| l.status()),
        agents: state.hub.repo.agents(t).await?,
        endpoints: state.hub.repo.endpoints(t).await?,
        queues: state.hub.repo.queue_depths(t).await?,
        conversations: state.hub.repo.recent_conversations(t, 25).await?,
        health: state.hub.channel_health().await,
        node: state.hub.node_id.clone(),
        bus: state.hub.bus.name(),
        agent_sockets,
        customer_sockets,
    };
    Ok(render(&v, flash.is_some()))
}

#[derive(Deserialize)]
struct AgentForm {
    email: String,
    display_name: String,
    password: String,
    skills: String,
    max_concurrent: String,
}

async fn admin_create_agent(State(state): State<AppState>, f: CsrfForm<AgentForm>) -> Response {
    let cap = f.form.max_concurrent.trim().parse::<i64>().unwrap_or(0);
    let new = super::api::NewAgent {
        email: &f.form.email,
        display_name: &f.form.display_name,
        password: &f.form.password,
        skills: &f.form.skills,
        max_concurrent: cap,
    };
    match super::api::create_agent_for(&state, &f.user, &f.ctx, new).await {
        Ok(_) => redirect_with(
            "/hub/admin",
            "success",
            &format!("Agent {} created. They can sign in at /login with the organisation code.", f.form.email.trim()),
        ),
        Err(e) => redirect_with("/hub/admin", "error", &e.message),
    }
}

#[derive(Deserialize)]
struct ProvisionForm {
    whatsapp_skill: String,
    voice_skill: String,
    webchat_skill: String,
}

async fn admin_provision(State(state): State<AppState>, f: CsrfForm<ProvisionForm>) -> Response {
    let r = async {
        let t = tenant_admin_tenant(&f.user)?;
        state
            .hub
            .provision_simulated_channels(
                t,
                &super::api::skill_or(Some(&f.form.whatsapp_skill), "support")?,
                &super::api::skill_or(Some(&f.form.voice_skill), "support")?,
                &super::api::skill_or(Some(&f.form.webchat_skill), "sales")?,
            )
            .await
    }
    .await;
    match r {
        Ok(_) => redirect_with("/hub/admin", "success", "Simulated WhatsApp number, voice DID and web-chat widget created."),
        Err(e) => redirect_with("/hub/admin", "error", &e.message),
    }
}

// ------------------------------------------------------------------------------------------------
// Simulator console (Tenant Admin) — SIMULATED providers only
// ------------------------------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "hub/simulator.html")]
struct SimulatorPage {
    page: PageCtx,
    meta: Option<MetaStatus>,
    /// fake-meta (true) or real Meta (false).
    fake_whatsapp: bool,
    whatsapp_label: String,
    webhook_url: String,
    display_number: String,
    whatsapp: Vec<Endpoint>,
    voice: Vec<Endpoint>,
    webchat: Vec<Endpoint>,
    log: Vec<SimLogEntry>,
    call_id: String,
    events: Vec<&'static str>,
    base_url: String,
}

async fn simulator_page(State(state): State<AppState>, TenantAdmin(p, _): TenantAdmin, IncomingFlash(flash): IncomingFlash) -> PageResult {
    simulator_allowed(&state)?;
    let t = tenant_admin_tenant(&p)?;
    let eps = state.hub.repo.endpoints(t).await?;
    let by = |c: Channel| eps.iter().filter(|e| e.channel == c).cloned().collect::<Vec<_>>();
    let page = PageCtx::for_user("Channel simulator", &p, "simulator", flash.clone(), false);
    let wa = &state.config.whatsapp;
    let v = SimulatorPage {
        page,
        meta: state.meta_link.as_ref().map(|l| l.status()),
        fake_whatsapp: state.hub.simulator.is_some(),
        whatsapp_label: match &state.hub.simulator {
            Some(sim) => sim.label(),
            None => format!("Meta WhatsApp Cloud API ({})", wa.base_url),
        },
        webhook_url: format!("{}/v1/hub/channels/whatsapp/webhook", state.config.public_base_url.trim_end_matches('/')),
        display_number: wa.display_number.clone().unwrap_or_else(|| "your WhatsApp test number".into()),
        whatsapp: by(Channel::WhatsApp),
        voice: by(Channel::Voice),
        webchat: by(Channel::WebChat),
        log: state.hub.repo.sim_logs(t, 30).await?,
        call_id: format!("call-{}", &Uuid::now_v7().simple().to_string()[20..]),
        events: vec!["ringing", "answered", "held", "retrieved", "transferred", "ended", "abandoned"],
        base_url: state.config.public_base_url.clone(),
    };
    Ok(render(&v, flash.is_some()))
}

/// The simulator console exists for development and demos; production never exposes it.
fn simulator_allowed(state: &AppState) -> Result<(), AppError> {
    if state.config.app_env == crate::platform::config::AppEnv::Production {
        return Err(AppError::not_found("The channel simulator is not available in production"));
    }
    Ok(())
}

/// The endpoint must belong to the admin's own tenant (RLS-scoped list), never any id from input.
async fn own_endpoint(state: &AppState, tenant: Uuid, id: &str, channel: Channel) -> Result<Endpoint, AppError> {
    let id = Uuid::parse_str(id.trim()).map_err(|_| AppError::validation("endpoint", "Choose a channel endpoint"))?;
    state
        .hub
        .repo
        .endpoints(tenant)
        .await?
        .into_iter()
        .find(|e| e.id == id && e.channel == channel)
        .ok_or_else(|| AppError::not_found("Endpoint not found"))
}

#[derive(Deserialize)]
struct WhatsAppForm {
    endpoint: String,
    from: String,
    name: String,
    text: String,
}

/// The fake-meta control client; absent when the hub talks to real Meta.
fn fake_meta(state: &AppState) -> Result<std::sync::Arc<dyn super::super::application::ports::ProviderSimulator>, AppError> {
    state.hub.simulator.clone().ok_or_else(|| {
        AppError::validation("whatsapp", "Real WhatsApp mode (WHATSAPP_PROVIDER=meta): send the message from a verified phone instead")
    })
}

/// A fake customer writes to our WhatsApp number: fake-meta sends Meta's signed webhook to the
/// hub, exactly like Meta would.
async fn sim_whatsapp(State(state): State<AppState>, f: CsrfForm<WhatsAppForm>) -> Response {
    let r = async {
        simulator_allowed(&state)?;
        let t = tenant_admin_tenant(&f.user)?;
        let ep = own_endpoint(&state, t, &f.form.endpoint, Channel::WhatsApp).await?;
        let from: String = f.form.from.chars().filter(|c| c.is_ascii_digit()).take(15).collect();
        if from.len() < 6 {
            return Err(AppError::validation("from", "Enter the customer's WhatsApp number (digits)"));
        }
        let text = f.form.text.trim();
        if text.is_empty() {
            return Err(AppError::validation("text", "Enter a message"));
        }
        let wamid = fake_meta(&state)?.customer_message(&ep.address, &from, f.form.name.trim(), text).await?;
        let payload = json!({ "phone_number_id": ep.address, "from": from, "text": text, "wamid": wamid });
        state
            .hub
            .repo
            .sim_log(t, Channel::WhatsApp, "inbound", &format!("Fake customer +{from} sent a WhatsApp message via fake-meta"), &payload)
            .await?;
        Ok::<(), AppError>(())
    }
    .await;
    match r {
        Ok(()) => {
            redirect_with("/hub/simulator", "success", "fake-meta is delivering the WhatsApp webhook to the hub (watch the agent desktop).")
        }
        Err(e) => redirect_with("/hub/simulator", "error", &e.message),
    }
}

#[derive(Deserialize)]
struct LoadForm {
    endpoint: String,
    customers: String,
    messages: String,
    rate: String,
}

/// Bulk WhatsApp traffic through fake-meta (customers × messages at `rate` per second).
async fn sim_whatsapp_load(State(state): State<AppState>, f: CsrfForm<LoadForm>) -> Response {
    let r = async {
        simulator_allowed(&state)?;
        let t = tenant_admin_tenant(&f.user)?;
        let ep = own_endpoint(&state, t, &f.form.endpoint, Channel::WhatsApp).await?;
        let n = |v: &str, field: &str, max: u64| -> Result<u64, AppError> {
            v.trim()
                .parse::<u64>()
                .ok()
                .filter(|x| (1..=max).contains(x))
                .ok_or_else(|| AppError::validation(field, format!("{field} must be 1..{max}")))
        };
        let (customers, messages, rate) =
            (n(&f.form.customers, "customers", 100_000)?, n(&f.form.messages, "messages", 1_000)?, n(&f.form.rate, "rate", 20_000)?);
        let sim = fake_meta(&state)?;
        sim.reset().await?;
        let run = sim.start_load(&ep.address, customers, messages, rate).await?;
        state
            .hub
            .repo
            .sim_log(t, Channel::WhatsApp, "inbound", &format!("Load run: {customers} customers × {messages} messages at {rate}/s"), &run)
            .await?;
        Ok::<(), AppError>(())
    }
    .await;
    match r {
        Ok(()) => redirect_with("/hub/simulator#load", "success", "Load run started — statistics below refresh every 2 seconds."),
        Err(e) => redirect_with("/hub/simulator#load", "error", &e.message),
    }
}

#[derive(Template)]
#[template(path = "hub/_whatsapp_live.html")]
struct WhatsAppLive {
    stats: Option<serde_json::Value>,
    outbox: Vec<serde_json::Value>,
    error: Option<String>,
}

/// HTML fragment (HTMX, every 2 s): fake-meta statistics + what the fake phones received for
/// THIS tenant's numbers only.
async fn sim_whatsapp_live(State(state): State<AppState>, TenantAdmin(p, _): TenantAdmin) -> PageResult {
    simulator_allowed(&state)?;
    let t = tenant_admin_tenant(&p)?;
    let Some(sim) = state.hub.simulator.clone() else {
        return Ok(render(
            &WhatsAppLive { stats: None, outbox: Vec::new(), error: Some("Real WhatsApp mode: no fake-meta statistics.".into()) },
            false,
        ));
    };
    let eps: Vec<Endpoint> = state.hub.repo.endpoints(t).await?.into_iter().filter(|e| e.channel == Channel::WhatsApp).collect();
    let mut outbox = Vec::new();
    for ep in &eps {
        if let Ok(v) = sim.outbox(Some(&ep.address), 15).await {
            outbox.extend(v.as_array().cloned().unwrap_or_default());
        }
    }
    outbox.sort_by(|a, b| b["at"].as_str().cmp(&a["at"].as_str()));
    outbox.truncate(15);
    let (stats, error) = match sim.stats().await {
        Ok(v) => (Some(v), None),
        Err(e) => (None, Some(e.message)),
    };
    Ok(render(&WhatsAppLive { stats, outbox, error }, false))
}

#[derive(Deserialize)]
struct ConnectForm {
    phone_number_id: String,
    label: String,
    skill: String,
}

/// Connects a WhatsApp Cloud API number (phone number id from Meta's API Setup page).
async fn admin_connect_whatsapp(State(state): State<AppState>, f: CsrfForm<ConnectForm>) -> Response {
    let r = async {
        let t = tenant_admin_tenant(&f.user)?;
        let label =
            if f.form.label.trim().is_empty() { "WhatsApp number".to_string() } else { f.form.label.trim().chars().take(80).collect() };
        state.hub.connect_whatsapp_number(t, &f.form.phone_number_id, &label, &super::api::skill_or(Some(&f.form.skill), "support")?).await
    }
    .await;
    match r {
        Ok(_) => {
            redirect_with("/hub/admin", "success", "WhatsApp number connected. Incoming messages to it are now routed to this tenant.")
        }
        Err(e) => redirect_with("/hub/admin", "error", &e.message),
    }
}

#[derive(Deserialize)]
struct SipForm {
    endpoint: String,
    call_id: String,
    from: String,
    event: String,
}

async fn sim_sip(State(state): State<AppState>, f: CsrfForm<SipForm>) -> Response {
    let r = async {
        simulator_allowed(&state)?;
        let t = tenant_admin_tenant(&f.user)?;
        let ep = own_endpoint(&state, t, &f.form.endpoint, Channel::Voice).await?;
        let event: CallEvent = f.form.event.parse().map_err(|_| AppError::validation("event", "Unknown call event"))?;
        let payload = json!({
            "event_id": format!("evt-{}", random_token(9)),
            "call_id": f.form.call_id.trim(),
            "event": event.as_str(),
            "from": f.form.from.trim(),
            "to": ep.address,
        });
        let body = payload.to_string().into_bytes();
        let signature = sign(state.config.hub_sim_sip_secret.as_bytes(), &body);
        state.hub.ingest_raw(Channel::Voice, Some(&signature), &body).await?;
        state
            .hub
            .repo
            .sim_log(t, Channel::Voice, "inbound", &format!("SBC event {} for call {}", event.as_str(), f.form.call_id.trim()), &payload)
            .await?;
        Ok::<(), AppError>(())
    }
    .await;
    match r {
        Ok(()) => redirect_with("/hub/simulator", "success", "Simulated call event delivered to the hub."),
        Err(e) => redirect_with("/hub/simulator", "error", &e.message),
    }
}

#[derive(Deserialize)]
struct CheckForm {}

/// "Check again now": re-run the Meta number check and webhook registration immediately.
async fn meta_check(State(state): State<AppState>, f: CsrfForm<CheckForm>) -> Response {
    let r = async {
        tenant_admin_tenant(&f.user)?;
        let link = state
            .meta_link
            .clone()
            .ok_or_else(|| AppError::validation("whatsapp", "Not in real WhatsApp mode (WHATSAPP_PROVIDER=fake)"))?;
        link.sync(true).await;
        Ok::<_, AppError>(link.status().all_ok())
    }
    .await;
    match r {
        Ok(true) => redirect_with("/hub/simulator#meta-status", "success", "Meta connection is ready."),
        Ok(false) => redirect_with("/hub/simulator#meta-status", "error", "Meta connection still needs attention — see the status box."),
        Err(e) => redirect_with("/hub/simulator", "error", &e.message),
    }
}
