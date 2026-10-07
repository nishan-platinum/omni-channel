//! Deliberately limited bootstrap authentication (ADR-0007). Replaceable by M02 later:
//! M01 talks to it only through `IdentityPort` and `TenantGate`.

pub mod extract;
pub mod identity_adapter;
pub mod service;
pub mod web;
