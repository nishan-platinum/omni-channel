//! Sandbox tenants (OCC-M01-R017, P2): linked to one production tenant, config copied, never real
//! production data (anonymised subset via a port; M01 owns no business rows).

use super::errors::DomainError;
use super::ids::TenantCode;
use super::tenant::{Tenant, TenantStatus};

pub const MAX_SANDBOXES: usize = 3;

pub fn sandbox_code(production: &TenantCode, ordinal: usize) -> TenantCode {
    production.with_suffix(&format!("sbx{ordinal}"))
}

/// A sandbox can be created for an active, non-sandbox tenant with fewer than MAX_SANDBOXES.
pub fn ensure_can_create(production: &Tenant, existing: usize) -> Result<(), DomainError> {
    if production.is_sandbox {
        return Err(DomainError::conflict("A sandbox cannot have its own sandbox"));
    }
    if production.status != TenantStatus::Active {
        return Err(DomainError::conflict("Sandboxes can only be created for active production tenants"));
    }
    if existing >= MAX_SANDBOXES {
        return Err(DomainError::conflict(format!("At most {MAX_SANDBOXES} sandboxes per production tenant")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::m01_tenancy::domain::tenant::tests::sample_tenant;

    #[test]
    fn rules() {
        let prod = sample_tenant(TenantStatus::Active);
        assert!(ensure_can_create(&prod, 0).is_ok());
        assert!(ensure_can_create(&prod, MAX_SANDBOXES).is_err());
        let mut sbx = sample_tenant(TenantStatus::Active);
        sbx.is_sandbox = true;
        assert!(ensure_can_create(&sbx, 0).is_err());
        assert!(ensure_can_create(&sample_tenant(TenantStatus::Draft), 0).is_err());
    }

    #[test]
    fn codes() {
        let c = TenantCode::parse("acme-retail").unwrap();
        assert_eq!(sandbox_code(&c, 1).as_str(), "acme-retail-sbx1");
    }
}
