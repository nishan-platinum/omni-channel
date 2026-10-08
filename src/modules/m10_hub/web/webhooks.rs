//! Provider-facing endpoints: the WhatsApp Cloud API webhook (real Meta or fake-meta, ADR-0013)
//! and the SIMULATED SBC event feed (ADR-0012). They authenticate with HMAC signatures over the
//! raw body; the tenant is resolved from the endpoint registry inside the hub service — never
//! from the payload. Meta's webhook dashboard needs the GET verification handshake.

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::app::AppState;
use crate::platform::errors::AppError;
use crate::platform::security::constant_time_eq;

use super::super::domain::Channel;
use super::envelope;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/hub/channels/whatsapp/webhook", get(whatsapp_verify).post(whatsapp_webhook))
        .route("/v1/hub/channels/sip/events", post(sip_events))
}

#[derive(Deserialize)]
struct VerifyQuery {
    #[serde(rename = "hub.mode")]
    mode: Option<String>,
    #[serde(rename = "hub.verify_token")]
    token: Option<String>,
    #[serde(rename = "hub.challenge")]
    challenge: Option<String>,
}

/// Meta-style subscription handshake: echo `hub.challenge` when the verify token matches.
async fn whatsapp_verify(State(state): State<AppState>, Query(q): Query<VerifyQuery>) -> Response {
    let ok = q.mode.as_deref() == Some("subscribe")
        && q.token.as_deref().is_some_and(|t| constant_time_eq(t.as_bytes(), state.config.whatsapp.verify_token.as_bytes()));
    match (ok, q.challenge) {
        (true, Some(c)) => c.chars().take(200).collect::<String>().into_response(),
        _ => AppError::forbidden("Verification failed").into_response(),
    }
}

fn header<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

async fn ingest(state: &AppState, channel: Channel, signature: Option<&str>, body: &[u8]) -> Response {
    match state.hub.ingest_raw(channel, signature, body).await {
        Ok(r) => (StatusCode::OK, Json(envelope(serde_json::to_value(r).unwrap_or_default()))).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn whatsapp_webhook(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    ingest(&state, Channel::WhatsApp, header(&headers, "x-hub-signature-256"), &body).await
}

async fn sip_events(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    ingest(&state, Channel::Voice, header(&headers, "x-sim-signature"), &body).await
}
