//! Configuration for Cassandra connectivity.

use crate::util::{
    from_duration_env_with_fallback, from_env_with_fallback, from_option_env, from_vec_env,
};
use derive_builder::Builder;
use educe::Educe;
use humantime::format_duration;
use std::time::Duration;
use validator::Validate;

/// Configuration for Cassandra connectivity across all Prosody components.
#[derive(Builder, Clone, Educe, Validate)]
#[educe(Debug)]
pub struct CassandraConfiguration {
    /// List of Cassandra contact nodes (hostnames or IPs).
    ///
    /// Environment variable: `PROSODY_CASSANDRA_NODES`
    /// Default: None (must be specified)
    ///
    /// At least one Cassandra node must be provided to establish an initial
    /// connection with the cluster. Multiple nodes improve failover behavior.
    #[builder(default = "from_vec_env(\"PROSODY_CASSANDRA_NODES\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub nodes: Vec<String>,

    /// Keyspace to use for storing all Prosody data.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_KEYSPACE`
    /// Default: `"prosody"`
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_CASSANDRA_KEYSPACE\", \"prosody\".to_owned())?",
        setter(into)
    )]
    #[validate(length(min = 1_u64))]
    pub keyspace: String,

    /// Preferred datacenter for query routing.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_DATACENTER`
    /// Default: None
    #[builder(
        default = "from_option_env(\"PROSODY_CASSANDRA_DATACENTER\")?",
        setter(into)
    )]
    pub datacenter: Option<String>,

    /// Preferred rack identifier for topology-aware routing.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_RACK`
    /// Default: None
    #[builder(default = "from_option_env(\"PROSODY_CASSANDRA_RACK\")?", setter(into))]
    pub rack: Option<String>,

    /// Username for authenticating with Cassandra.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_USER`
    /// Default: None
    #[builder(default = "from_option_env(\"PROSODY_CASSANDRA_USER\")?", setter(into))]
    pub user: Option<String>,

    /// Password for authenticating with Cassandra.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_PASSWORD`
    /// Default: None
    #[builder(
        default = "from_option_env(\"PROSODY_CASSANDRA_PASSWORD\")?",
        setter(into)
    )]
    #[educe(Debug(ignore))]
    pub password: Option<String>,

    /// Retention period for data in Cassandra.
    ///
    /// This defines how long data remains available after its intended
    /// processing time. The retention period is added to target times
    /// to calculate Cassandra TTLs.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_RETENTION`
    /// Default: 1 year
    /// Maximum: ~20 years (Cassandra's TTL limit of 630,720,000 seconds)
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_CASSANDRA_RETENTION\", \
                   Duration::from_secs(365 * 24 * 60 * 60))?",
        setter(into)
    )]
    #[validate(custom(function = "validate_retention_ttl"))]
    pub retention: Duration,
}

fn validate_retention_ttl(retention: &Duration) -> Result<(), validator::ValidationError> {
    if retention.as_secs() > super::MAX_CASSANDRA_TTL_SECS as u64 {
        let max_ttl = Duration::from_secs(super::MAX_CASSANDRA_TTL_SECS as u64);
        let mut err = validator::ValidationError::new("retention_exceeds_max_ttl");
        err.message = Some(
            format!(
                "retention {} exceeds Cassandra's maximum TTL of {}",
                format_duration(*retention),
                format_duration(max_ttl)
            )
            .into(),
        );
        return Err(err);
    }
    Ok(())
}

impl CassandraConfiguration {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> CassandraConfigurationBuilder {
        CassandraConfigurationBuilder::default()
    }
}
