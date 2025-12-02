//! Configuration for Cassandra connectivity.

use crate::util::{
    from_duration_env_with_fallback, from_env_with_fallback, from_option_env, from_vec_env,
};
use derive_builder::Builder;
use educe::Educe;
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
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_CASSANDRA_RETENTION\", \
                   Duration::from_secs(365 * 24 * 60 * 60))?",
        setter(into)
    )]
    pub retention: Duration,
}

impl CassandraConfiguration {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> CassandraConfigurationBuilder {
        CassandraConfigurationBuilder::default()
    }
}
