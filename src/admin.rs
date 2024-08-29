//! Provides functionality for administrative operations on Kafka topics.

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaError;
use rdkafka::ClientConfig;
use thiserror::Error;

/// A client for performing administrative operations on Kafka topics.
pub struct ProsodyAdminClient {
    client: AdminClient<DefaultClientContext>,
    options: AdminOptions,
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
    pub fn new<T>(bootstrap_servers: &[T]) -> Result<ProsodyAdminClient, ProsodyAdminClientError>
    where
        T: AsRef<str>,
    {
        let mut client_config = ClientConfig::new();
        client_config.set(
            "bootstrap.servers",
            bootstrap_servers
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>()
                .join(","),
        );

        Ok(Self {
            client: client_config.create()?,
            options: AdminOptions::default(),
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
}
