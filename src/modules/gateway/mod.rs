//! ScicomCX gateway bake-off slice (ADR-0014): the external contract from the bake-off spec —
//! simulated WhatsApp / SIP ingress → one canonical message → durable ordered store → event stream
//! → skill routing (longest idle) → customer and agent WebSocket sessions, on N nodes behind a
//! load balancer. Single tenant, one shared bearer token, configuration from a platform fixture.
//! Runs as its own binary (`gateway`) with its own database; independent of the M01/M10 CRM.
pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod web;
