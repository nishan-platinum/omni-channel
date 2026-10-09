//! Development-only demo data for the M10 hub (`HUB_DEMO_SEED=true`, `APP_ENV=development`), so
//! the gateway can be tried right after `docker compose up` with no manual setup and no real
//! credentials:
//!
//! * tenant `demo` (Standard plan, active) with Tenant Admin `admin@demo.omni.local`
//! * agents `agent.sales@demo.omni.local` (sales), `agent.support@demo.omni.local` (support),
//!   `agent.lead@demo.omni.local` (sales + support)
//! * WhatsApp number on the fake-meta server (→ support; with WHATSAPP_PROVIDER=meta the configured
//!   Meta phone number id is connected instead), simulated voice DID (→ support), web-chat widget (→ sales)
//!
//! All demo users share `HUB_DEMO_PASSWORD` (default documented in `.env.example`). Idempotent:
//! nothing is created twice.

use uuid::Uuid;

use crate::app::AppState;
use crate::modules::m01_tenancy::application::provisioning::CreateTenantCommand;
use crate::modules::m01_tenancy::application::Actor;

pub const DEMO_CODE: &str = "demo";
pub const DEMO_ADMIN: &str = "admin@demo.omni.local";
pub const DEMO_PASSWORD_DEFAULT: &str = "Demo-Hub-Passw0rd!";
const STANDARD_PLAN: &str = "01920000-0000-7000-8000-000000000001";
pub const DEMO_AGENTS: [(&str, &str, &str, i64); 3] = [
    ("agent.sales@demo.omni.local", "Sara (Sales)", "sales", 3),
    ("agent.support@demo.omni.local", "Sam (Support)", "support", 3),
    ("agent.lead@demo.omni.local", "Lee (Team lead)", "sales,support", 5),
];

fn err(e: crate::platform::errors::AppError) -> anyhow::Error {
    anyhow::anyhow!("demo seed: {e}")
}

pub async fn seed(state: &AppState) -> anyhow::Result<()> {
    let password = state.config.hub_demo_password.clone().unwrap_or_else(|| DEMO_PASSWORD_DEFAULT.to_string());
    let system = Actor::system("hub-demo-seed");
    let tenant: Uuid = match state.hub.gate.tenant_by_code(DEMO_CODE).await.map_err(err)? {
        Some(t) => t,
        None => {
            let cmd = CreateTenantCommand {
                name: "Demo Contact Centre".into(),
                region: Some("my-central".into()),
                plan_id: Some(STANDARD_PLAN.into()),
                primary_admin_email: DEMO_ADMIN.into(),
                tenant_code: Some(DEMO_CODE.into()),
                ..Default::default()
            };
            let out = state.m01.provisioning.create(&system, cmd).await.map_err(err)?;
            state.m01.lifecycle.change_status(&system, out.tenant.id, "active", Some("hub demo seed")).await.map_err(err)?;
            out.tenant.id.0
        }
    };
    // Real WhatsApp (WHATSAPP_PROVIDER=meta): route the configured Meta number to the demo tenant.
    if !state.config.whatsapp.is_fake() {
        if let Some(pnid) = &state.config.whatsapp.phone_number_id {
            let label = state.config.whatsapp.display_number.clone().unwrap_or_else(|| "Meta WhatsApp number".into());
            match state.hub.connect_whatsapp_number(tenant, pnid, &format!("{label} (real WhatsApp)"), "support").await {
                Ok(_) => tracing::info!(tenant_code = DEMO_CODE, "Meta WhatsApp number connected to the demo tenant"),
                Err(e) => tracing::warn!(error = %e, "could not connect the Meta WhatsApp number to the demo tenant"),
            }
        }
    }
    // Already seeded once the simulated voice / web-chat channels exist.
    if state.hub.repo.endpoints(tenant).await.map_err(err)?.iter().any(|e| e.channel != crate::modules::m10_hub::domain::Channel::WhatsApp)
    {
        return Ok(());
    }
    // Tenant Admin: accept the (re-issued) invitation with the demo password.
    let (_, token) = state.auth.invite_tenant_admin(tenant, DEMO_ADMIN, "Demo Tenant Admin").await.map_err(err)?;
    if let Some(t) = token {
        state.auth.accept_invitation(&t, &password, &password).await.map_err(err)?;
    }
    for (email, name, skills, cap) in DEMO_AGENTS {
        let user = match state.auth.tenant_user_id(tenant, email).await.map_err(err)? {
            Some(u) => u,
            None => state.auth.create_agent_user(tenant, email, name, &password).await.map_err(err)?,
        };
        let skills: Vec<String> = skills.split(',').map(str::to_string).collect();
        state.hub.repo.upsert_agent(tenant, user, &skills, cap).await.map_err(err)?;
    }
    state.hub.provision_simulated_channels(tenant, "support", "support", "sales").await.map_err(err)?;
    tracing::info!(
        tenant_code = DEMO_CODE,
        admin = DEMO_ADMIN,
        agents = "agent.sales@ / agent.support@ / agent.lead@demo.omni.local",
        "hub demo seeded (password: HUB_DEMO_PASSWORD, see .env.example)"
    );
    Ok(())
}
