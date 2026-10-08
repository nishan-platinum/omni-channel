//! Web-chat channel (minimal M08 slice). Customers talk over the hub's own WebSocket, so there
//! is no provider webhook to ingest and no provider to deliver to: agent replies are pushed to the
//! customer's socket and receipts come from the customer's read markers (OCC-M10-R031).

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::platform::errors::{AppError, AppResult};

use super::super::super::application::ports::{ChannelAdapter, ChannelHealth, DeliveryError, Inbound, OutboundJob};
use super::super::super::domain::Channel;

pub struct WebChatAdapter;

#[async_trait]
impl ChannelAdapter for WebChatAdapter {
    fn channel(&self) -> Channel {
        Channel::WebChat
    }

    fn simulated(&self) -> bool {
        false
    }

    async fn connect(&self) -> AppResult<()> {
        Ok(())
    }

    fn ingest(&self, _signature: Option<&str>, _body: &[u8]) -> AppResult<Vec<Inbound>> {
        Err(AppError::not_found("Web chat has no provider webhook; customers use the WebSocket"))
    }

    async fn deliver(&self, _job: &OutboundJob) -> Result<String, DeliveryError> {
        Err(DeliveryError::permanent("web chat replies are pushed over the customer WebSocket, not queued".into()))
    }

    async fn health(&self) -> ChannelHealth {
        ChannelHealth { channel: Channel::WebChat, simulated: false, healthy: true, detail: "Hub WebSocket".into() }
    }

    async fn backfill(&self, _since: DateTime<Utc>) -> AppResult<Vec<Inbound>> {
        Ok(Vec::new())
    }
}
