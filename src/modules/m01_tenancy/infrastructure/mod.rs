//! M01 infrastructure: SQLx repositories, tenant data-plane routing (PostgreSQL/MySQL) and
//! reference adapters for external ports.

pub mod adapters;
pub mod gate;
pub mod persistence;
pub mod tenant_data;
