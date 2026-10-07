//! Cross-cutting platform concerns shared by every module. No module business rules live here.

pub mod audit;
pub mod config;
pub mod db;
pub mod errors;
pub mod events;
pub mod idempotency;
pub mod middleware;
pub mod observability;
pub mod ratelimit;
pub mod security;
pub mod time;
