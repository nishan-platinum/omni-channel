//! M01 domain: pure business concepts and invariants. No Axum, Askama, SQLx or HTTP here.

pub mod analytics;
pub mod baseline;
pub mod branding;
pub mod config;
pub mod errors;
pub mod events;
pub mod features;
pub mod hierarchy;
pub mod ids;
pub mod keys;
pub mod plan;
pub mod quota;
pub mod release;
pub mod sandbox;
pub mod storage;
pub mod support;
pub mod tenant;

pub use errors::DomainError;
pub use ids::{TenantCode, TenantId};
pub use tenant::{Tenant, TenantStatus};
