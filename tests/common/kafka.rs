//! Kafka fixtures: creating a per-test topic, waiting for a consumer's
//! partition assignment, and the live consumer/producer pair the direct
//! `ProsodyConsumer` harnesses build on.

use super::create_cassandra_trigger_store_config;
use color_eyre::eyre::{Result, eyre};
use prosody::JsonCodec;
use prosody::Topic;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::consumer::{
    ConsumerConfiguration, EventHandler, HandlerProvider, KeyedStateConfiguration, ProsodyConsumer,
};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::telemetry::Telemetry;
use serde_json::Value;
use std::time::Duration as StdDuration;
use tokio::time::timeout;
use tracing::{error, info};
use uuid::Uuid;

/// The broker the fixtures in this module connect to.
const BOOTSTRAP: &str = "localhost:9094";

/// Creates a uuid-named test topic with the given number of partitions.
///
/// Returns the created topic and an admin client for cleanup tasks.
///
/// # Errors
///
/// Returns an error if the topic creation fails.
pub(crate) async fn create_topic_with_partitions(
    partition_count: u16,
) -> Result<(Topic, &'static ProsodyAdminClient)> {
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let admin_client =
        ProsodyAdminClient::cached(&AdminConfiguration::new(vec![BOOTSTRAP.to_owned()])?)?;

    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(partition_count)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    info!("created topic: {topic}");
    Ok((topic, admin_client))
}

/// Awaits the consumer's partition assignment under a generous hang-guard.
/// Fails with a clear error instead of hanging if the rebalance stalls or
/// only partially assigns.
///
/// # Errors
///
/// Returns an error if the consumer does not receive `count` partition
/// assignments within the hang-guard.
pub(crate) async fn wait_for_assignment(
    consumer: &ProsodyConsumer<JsonCodec>,
    count: u32,
) -> Result<()> {
    timeout(
        StdDuration::from_secs(30),
        consumer.wait_for_assigned_partitions(count),
    )
    .await
    .map_err(|_| eyre!("consumer did not receive a partition assignment in time"))?;
    Ok(())
}

/// A live single-partition consumer + producer pair with a dedicated topic.
///
/// Owns the per-test infrastructure every direct-`ProsodyConsumer` harness
/// would otherwise duplicate: topic lifecycle (created in [`ConsumerEnv::new`],
/// deleted in [`ConsumerEnv::shutdown`]), a uniquely named consumer group, the
/// producer, and the wait for the consumer's partition assignment. Per-file
/// harnesses wrap this and add only their handler, event channels, and
/// assertions.
pub(crate) struct ConsumerEnv {
    topic: Topic,
    admin: &'static ProsodyAdminClient,
    consumer: ProsodyConsumer<JsonCodec>,
    producer: ProsodyProducer<JsonCodec>,
}

impl ConsumerEnv {
    /// Creates a fresh topic and consumer group named after `test_name`,
    /// builds the handler provider from the consumer configuration via
    /// `build_provider`, starts the consumer and producer, and waits for the
    /// partition assignment before returning — so a message sent immediately
    /// after `new` can never race the subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to start or the consumer does
    /// not receive its partition assignment within the hang-guard.
    pub(crate) async fn new<P, F>(test_name: &str, build_provider: F) -> Result<Self>
    where
        P: HandlerProvider,
        P::Handler: EventHandler<Payload = Value>,
        F: AsyncFnOnce(&ConsumerConfiguration) -> Result<P>,
    {
        let (topic, admin) = create_topic_with_partitions(1).await?;

        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(vec![BOOTSTRAP.to_owned()])
            .group_id(format!("{test_name}-consumer-{}", Uuid::new_v4()))
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None) // Disable probe server to allow parallel test execution
            .build()?;

        let handler_provider = build_provider(&consumer_config).await?;

        let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
            &consumer_config,
            &create_cassandra_trigger_store_config(),
            KeyedStateConfiguration::builder().build()?,
            handler_provider,
            Telemetry::new(),
        )
        .await?;

        let producer = ProsodyProducer::<JsonCodec>::new(
            &ProducerConfiguration::builder()
                .bootstrap_servers(vec![BOOTSTRAP.to_owned()])
                .source_system(test_name.to_owned())
                .build()?,
            Telemetry::new().sender(),
        )?;

        // Wait until Kafka has assigned the consumer its partition before
        // producing — records sent before the subscription is live can be
        // missed. Awaits the assignment signal the rebalance callback
        // publishes (not a poll) under a generous hang-guard.
        wait_for_assignment(&consumer, 1).await?;

        Ok(Self {
            topic,
            admin,
            consumer,
            producer,
        })
    }

    /// The topic this environment produces to and consumes from.
    #[must_use]
    pub(crate) fn topic(&self) -> Topic {
        self.topic
    }

    /// Sends a message with the given key and payload to the test topic.
    ///
    /// # Errors
    ///
    /// Returns an error if the send fails.
    pub(crate) async fn send_message(&self, key: &str, payload: Value) -> Result<()> {
        self.producer.send([], self.topic, key, payload).await?;
        Ok(())
    }

    /// Shuts down the consumer, then deletes the test topic.
    ///
    /// Callers must invoke this before propagating a test failure — dropping
    /// a live consumer leaves rdkafka threads hanging.
    pub(crate) async fn shutdown(self) {
        if let Err(error) = self.consumer.shutdown().await {
            error!(%error, "Failed to shut down consumer");
        }
        if let Err(e) = self.admin.delete_topic(&self.topic).await {
            error!("Failed to clean up topic {}: {e}", self.topic);
        }
    }
}
