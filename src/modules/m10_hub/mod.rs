//! M10 Omnichannel Conversation Hub — the gateway slice (spec Chapter 41, OCC-M10-R014…R034):
//! channel adapters → canonical messages → durable ordered store → skill routing → agent and
//! customer WebSocket sessions, across several nodes. WhatsApp and SIP are SIMULATED (ADR-0012).
pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod web;
