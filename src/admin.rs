//! This module provides functionality for administrative operations on Kafka
//! topics.

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaError;
use rdkafka::ClientConfig;
use thiserror::Error;

/// A client for performing administrative operations on Kafka topics.
pub struct ProsodyAdminClient {
    client: AdminClient<DefaultClientContext>,
}

impl ProsodyAdminClient {
    /// Creates a new `ProsodyAdminClient`.
    ///
    /// # Arguments
    ///
    /// * `bootstrap_servers` - A list of Kafka bootstrap servers.
    ///
    /// # Errors
    ///
    /// Returns a `ProsodyAdminClientError` if the client creation fails.
    pub fn new(bootstrap_servers: &[&str]) -> Result<ProsodyAdminClient, ProsodyAdminClientError> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", bootstrap_servers.join(","));

        Ok(Self {
            client: client_config.create()?,
        })
    }

    /// Creates a new Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic to create.
    /// * `partition_count` - The number of partitions for the new topic.
    /// * `replication_factor` - The replication factor for the new topic.
    ///
    /// # Errors
    ///
    /// Returns a `ProsodyAdminClientError` if the topic creation fails.
    pub async fn create_topic(
        &self,
        name: &str,
        partition_count: u16,
        replication_factor: u16,
    ) -> Result<(), ProsodyAdminClientError> {
        let replication = TopicReplication::Fixed(i32::from(replication_factor));
        let new_topic = NewTopic::new(name, i32::from(partition_count), replication);

        self.client
            .create_topics([&new_topic], &AdminOptions::default())
            .await?;

        Ok(())
    }
}

/// Errors that can occur during Prosody admin client operations.
#[derive(Debug, Error)]
pub enum ProsodyAdminClientError {
    /// Indicates a Kafka operation failure.
    #[error("Kafka error: {0:#}")]
    Kafka(#[from] KafkaError),
}
