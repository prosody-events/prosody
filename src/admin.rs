//! Provides functionality for administrative operations on Kafka topics.

use std::time::Duration;

use derive_builder::Builder;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaError;
use thiserror::Error;
use validator::{Validate, ValidationErrors};

use crate::util::{from_option_duration_env, from_option_env, from_vec_env};

/// Configuration for the Kafka admin client.
///
/// This struct holds all the necessary configuration options for creating a
/// Kafka admin client. It uses the Builder pattern for flexible initialization
/// and supports loading values from environment variables.
#[derive(Builder, Clone, Debug, Validate)]
pub struct AdminConfiguration {
    /// List of Kafka bootstrap servers.
    ///
    /// Environment variable: `PROSODY_BOOTSTRAP_SERVERS`
    /// Default: None (must be specified)
    ///
    /// At least one server must be specified.
    #[builder(default = "from_vec_env(\"PROSODY_BOOTSTRAP_SERVERS\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,
}

impl AdminConfiguration {
    /// Creates a new `AdminConfigurationBuilder`.
    #[must_use]
    pub fn builder() -> AdminConfigurationBuilder {
        AdminConfigurationBuilder::default()
    }

    /// Creates a basic admin configuration with bootstrap servers.
    ///
    /// # Arguments
    ///
    /// * `bootstrap_servers` - A list of Kafka bootstrap servers.
    ///
    /// # Errors
    ///
    /// Returns a `ValidationErrors` if the bootstrap servers list is invalid.
    pub fn new(bootstrap_servers: Vec<String>) -> Result<Self, ValidationErrors> {
        let config = Self { bootstrap_servers };
        config.validate()?;
        Ok(config)
    }
}

/// Configuration for creating Kafka topics.
///
/// This struct holds all the necessary configuration options for creating a
/// Kafka topic. It uses the Builder pattern for flexible initialization and
/// supports loading values from environment variables.
#[derive(Builder, Clone, Debug, Default, Validate)]
pub struct TopicConfiguration {
    /// The name of the topic to create.
    ///
    /// This field is required and has no environment variable fallback.
    #[builder(setter(into))]
    #[validate(length(min = 1_u64))]
    pub name: String,

    /// Number of partitions for the topic.
    ///
    /// Environment variable: `PROSODY_TOPIC_PARTITIONS`
    /// Default: None (uses broker default)
    ///
    /// If None, librdkafka will use -1 to signal broker default.
    #[builder(
        default = "from_option_env(\"PROSODY_TOPIC_PARTITIONS\")?",
        setter(into, strip_option)
    )]
    #[validate(range(min = 1_u16))]
    pub partition_count: Option<u16>,

    /// Replication factor for the topic.
    ///
    /// Environment variable: `PROSODY_TOPIC_REPLICATION_FACTOR`
    /// Default: None (uses broker default)
    ///
    /// If None, librdkafka will use -1 to signal broker default.
    #[builder(
        default = "from_option_env(\"PROSODY_TOPIC_REPLICATION_FACTOR\")?",
        setter(into, strip_option)
    )]
    #[validate(range(min = 1_u16))]
    pub replication_factor: Option<u16>,

    /// Cleanup policy for the topic.
    ///
    /// Environment variable: `PROSODY_TOPIC_CLEANUP_POLICY`
    /// Default: None (uses cluster default)
    ///
    /// Valid values: "delete", "compact", "delete,compact", etc.
    #[builder(
        default = "from_option_env(\"PROSODY_TOPIC_CLEANUP_POLICY\")?",
        setter(into, strip_option)
    )]
    pub cleanup_policy: Option<String>,

    /// Retention time for messages.
    ///
    /// Environment variable: `PROSODY_TOPIC_RETENTION`
    /// Default: None (uses cluster default)
    ///
    /// How long messages are retained before being eligible for deletion.
    /// Can be specified as humantime strings like "1d", "2h 30m", "7days".
    #[builder(
        default = "from_option_duration_env(\"PROSODY_TOPIC_RETENTION\")?",
        setter(into, strip_option)
    )]
    pub retention: Option<Duration>,
}

impl TopicConfiguration {
    /// Creates a new `TopicConfigurationBuilder`.
    #[must_use]
    pub fn builder() -> TopicConfigurationBuilder {
        TopicConfigurationBuilder::default()
    }

    /// Creates a basic topic configuration with just a name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic to create.
    ///
    /// # Errors
    ///
    /// Returns a `ValidationErrors` if the topic name is invalid.
    pub fn new<N: Into<String>>(name: N) -> Result<Self, ValidationErrors> {
        let config = Self {
            name: name.into(),
            ..Default::default()
        };
        config.validate()?;
        Ok(config)
    }
}

/// A client for performing administrative operations on Kafka topics.
pub struct ProsodyAdminClient {
    client: AdminClient<DefaultClientContext>,
    options: AdminOptions,
}

impl ProsodyAdminClient {
    /// Creates a new `ProsodyAdminClient` with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The admin configuration containing bootstrap servers.
    ///
    /// # Errors
    ///
    /// Returns a `ProsodyAdminClientError` if the client creation fails.
    pub fn new(config: &AdminConfiguration) -> Result<ProsodyAdminClient, ProsodyAdminClientError> {
        // Validate the configuration
        config.validate()?;

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", config.bootstrap_servers.join(","));

        Ok(Self {
            client: client_config.create()?,
            options: AdminOptions::default(),
        })
    }

    /// Creates a new Kafka topic with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The topic configuration containing name, partitions,
    ///   replication, etc.
    ///
    /// # Errors
    ///
    /// Returns a `ProsodyAdminClientError` if the topic creation fails.
    pub async fn create_topic(
        &self,
        config: &TopicConfiguration,
    ) -> Result<(), ProsodyAdminClientError> {
        // Validate the configuration
        config.validate()?;

        let replication = match config.replication_factor {
            Some(factor) => TopicReplication::Fixed(i32::from(factor)),
            None => TopicReplication::Fixed(-1), // Use broker default per librdkafka docs
        };

        let partition_count = match config.partition_count {
            Some(count) => i32::from(count),
            None => -1_i32, // Use broker default per librdkafka docs
        };

        let mut new_topic = NewTopic::new(&config.name, partition_count, replication);

        // Set cleanup.policy if specified
        if let Some(ref policy) = config.cleanup_policy {
            new_topic = new_topic.set("cleanup.policy", policy);
        }

        // Set retention.ms if specified - need to handle the string lifetime issue
        let retention_str;
        if let Some(retention) = config.retention {
            retention_str = retention.as_millis().to_string();
            new_topic = new_topic.set("retention.ms", &retention_str);
        }

        self.client
            .create_topics([&new_topic], &self.options)
            .await?;

        Ok(())
    }

    /// Deletes a Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic to delete.
    ///
    /// # Errors
    ///
    /// Returns a `ProsodyAdminClientError` if the topic deletion fails.
    pub async fn delete_topic(&self, name: &str) -> Result<(), ProsodyAdminClientError> {
        self.client.delete_topics(&[name], &self.options).await?;
        Ok(())
    }
}

/// Errors that can occur during Prosody admin client operations.
#[derive(Debug, Error)]
pub enum ProsodyAdminClientError {
    /// Indicates a Kafka operation failure.
    #[error("Kafka error: {0:#}")]
    Kafka(#[from] KafkaError),

    /// Configuration validation error.
    #[error("Configuration validation error: {0:#}")]
    Validation(#[from] ValidationErrors),
}
