//! Tiers, storage strategies, isolation modes, regions and database engines (OCC-M01-R008, ADR-0004).

use serde::{Deserialize, Serialize};

use super::errors::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Standard,
    Premium,
    Regulated,
}

impl Tier {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "standard" => Ok(Self::Standard),
            "premium" => Ok(Self::Premium),
            "regulated" => Ok(Self::Regulated),
            _ => Err(DomainError::field("tier", "Unknown tier")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Premium => "premium",
            Self::Regulated => "regulated",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::Standard => "Standard",
            Self::Premium => "Premium",
            Self::Regulated => "Regulated",
        }
    }
    /// Storage strategy mandated by R008 for the tier.
    pub fn storage_strategy(self) -> StorageStrategy {
        match self {
            Self::Standard => StorageStrategy::SharedRowLevel,
            Self::Premium => StorageStrategy::SchemaPerTenant,
            Self::Regulated => StorageStrategy::DedicatedDatabase,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageStrategy {
    SharedRowLevel,
    SchemaPerTenant,
    DedicatedDatabase,
}

impl StorageStrategy {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "shared_row_level" => Ok(Self::SharedRowLevel),
            "schema_per_tenant" => Ok(Self::SchemaPerTenant),
            "dedicated_database" => Ok(Self::DedicatedDatabase),
            _ => Err(DomainError::field("storage_strategy", "Unknown storage strategy")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SharedRowLevel => "shared_row_level",
            Self::SchemaPerTenant => "schema_per_tenant",
            Self::DedicatedDatabase => "dedicated_database",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::SharedRowLevel => "Shared PostgreSQL (tenant_id + RLS)",
            Self::SchemaPerTenant => "PostgreSQL schema per tenant",
            Self::DedicatedDatabase => "Dedicated database per tenant",
        }
    }
    pub fn isolation_mode(self) -> IsolationMode {
        match self {
            Self::SharedRowLevel => IsolationMode::RowLevel,
            Self::SchemaPerTenant => IsolationMode::SchemaPerTenant,
            Self::DedicatedDatabase => IsolationMode::DatabasePerTenant,
        }
    }
}

/// Spec field `isolation_mode` (row_level | schema_per_tenant), extended with
/// `database_per_tenant` for the Regulated tier (ADR-0004).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationMode {
    RowLevel,
    SchemaPerTenant,
    DatabasePerTenant,
}

impl IsolationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RowLevel => "row_level",
            Self::SchemaPerTenant => "schema_per_tenant",
            Self::DatabasePerTenant => "database_per_tenant",
        }
    }
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "row_level" => Ok(Self::RowLevel),
            "schema_per_tenant" => Ok(Self::SchemaPerTenant),
            "database_per_tenant" => Ok(Self::DatabasePerTenant),
            _ => Err(DomainError::field("isolation_mode", "Unknown isolation mode")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseEngine {
    Postgres,
    Mysql,
}

impl DatabaseEngine {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s {
            "postgres" | "postgresql" => Ok(Self::Postgres),
            "mysql" => Ok(Self::Mysql),
            _ => Err(DomainError::field("engine", "Unsupported database engine")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::Postgres => "PostgreSQL",
            Self::Mysql => "MySQL",
        }
    }
    /// Whether the engine offers database-enforced row-level security. MySQL does not.
    pub fn supports_rls(self) -> bool {
        matches!(self, Self::Postgres)
    }
}

/// Data region (field `region`): pins residency (M29). Default `my-central`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Region {
    #[default]
    #[serde(rename = "my-central")]
    MyCentral,
    #[serde(rename = "sg")]
    Sg,
    #[serde(rename = "apac")]
    Apac,
}

pub const ALL_REGIONS: [Region; 3] = [Region::MyCentral, Region::Sg, Region::Apac];

impl Region {
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        match s.trim() {
            "my-central" => Ok(Self::MyCentral),
            "sg" => Ok(Self::Sg),
            "apac" => Ok(Self::Apac),
            _ => Err(DomainError::field("region", "Unsupported region")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MyCentral => "my-central",
            Self::Sg => "sg",
            Self::Apac => "apac",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::MyCentral => "Malaysia (central)",
            Self::Sg => "Singapore",
            Self::Apac => "Asia-Pacific",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiers_map_to_r008_strategies() {
        assert_eq!(Tier::Standard.storage_strategy(), StorageStrategy::SharedRowLevel);
        assert_eq!(Tier::Premium.storage_strategy(), StorageStrategy::SchemaPerTenant);
        assert_eq!(Tier::Regulated.storage_strategy(), StorageStrategy::DedicatedDatabase);
        assert_eq!(StorageStrategy::SharedRowLevel.isolation_mode(), IsolationMode::RowLevel);
    }

    #[test]
    fn regions_follow_field_rule() {
        assert_eq!(Region::parse("my-central").unwrap(), Region::MyCentral);
        assert_eq!(Region::default(), Region::MyCentral);
        assert!(Region::parse("us-east").is_err());
    }

    #[test]
    fn mysql_has_no_rls() {
        assert!(!DatabaseEngine::Mysql.supports_rls());
        assert!(DatabaseEngine::Postgres.supports_rls());
    }
}
