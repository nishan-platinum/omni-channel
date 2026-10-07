//! Reseller hierarchy rule for `parent_tenant_id`: max depth 3, no cycles (spec §10.3.1).

use super::errors::DomainError;
use super::ids::TenantId;

pub const MAX_DEPTH: usize = 3;
pub const HIERARCHY_ERROR: &str = "Circular or too-deep hierarchy";

/// * `tenant` — the tenant being (re)parented.
/// * `parent_ancestry` — the proposed parent followed by its ancestors up to the root.
/// * `subtree_height` — levels in the tenant's own subtree including itself (1 for a leaf/new tenant).
pub fn validate_parent(tenant: TenantId, parent_ancestry: &[TenantId], subtree_height: usize) -> Result<(), DomainError> {
    if parent_ancestry.contains(&tenant) {
        return Err(DomainError::conflict(HIERARCHY_ERROR));
    }
    let depth = parent_ancestry.len() + subtree_height.max(1);
    if depth > MAX_DEPTH {
        return Err(DomainError::conflict(HIERARCHY_ERROR));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_three_allowed_four_rejected() {
        let (a, b, c, d) = (TenantId::new(), TenantId::new(), TenantId::new(), TenantId::new());
        assert!(validate_parent(b, &[a], 1).is_ok()); // depth 2
        assert!(validate_parent(c, &[b, a], 1).is_ok()); // depth 3
        assert!(validate_parent(d, &[c, b, a], 1).is_err()); // depth 4
    }

    #[test]
    fn cycles_rejected() {
        let (a, b) = (TenantId::new(), TenantId::new());
        // Making a a child of b while b's ancestry contains a.
        assert_eq!(validate_parent(a, &[b, a], 1), Err(DomainError::Conflict(HIERARCHY_ERROR.into())));
        assert!(validate_parent(a, &[a], 1).is_err(), "self-parent");
    }

    #[test]
    fn subtree_height_counts() {
        let (a, b) = (TenantId::new(), TenantId::new());
        // b has children and grandchildren (height 3); placing under a gives depth 4.
        assert!(validate_parent(b, &[a], 3).is_err());
        assert!(validate_parent(b, &[a], 2).is_ok());
    }
}
