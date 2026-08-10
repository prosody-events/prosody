//! Integration test for the README quickstart example.
//!
//! The handler and client setup below mirror the README's High-Level Client
//! Example exactly. The only differences are a channel added to `MyHandler`
//! for observability, a unique topic and group ID for test isolation, a
//! disabled probe server to avoid port conflicts with parallel tests, and
//! the ephemeral topic this test creates and deletes around the example
//! (the README assumes the topic already exists).

#![recursion_limit = "256"]

use color_eyre::eyre::{Result, eyre};
use prosody::Topic;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::codec::JsonCodec;
use prosody::high_level::CassandraHighLevelClient;
use prosody::prelude::*;
use prosody::tracing::init_test_logging;
use serde_json::json;
use std::convert::Infallible;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[derive(Clone)]
struct MyHandler {
    sender: Sender<String>,
}

impl FallibleHandler for MyHandler {
    type Error = Infallible;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: ConsumerMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let _ = self.sender.send(message.key().to_string()).await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn shutdown(self) {}
}

impl ClientHandler for MyHandler {
    type Codecs = Codecs<JsonCodec, UnitCodec>;
}

#[tokio::test]
async fn quickstart() -> Result<()> {
    init_test_logging();

    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap_servers = vec!["localhost:9094".to_owned()];

    let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers.clone())?)?;
    admin
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    let mut consumer_config = ConsumerConfiguration::builder();
    consumer_config
        .bootstrap_servers(bootstrap_servers.clone())
        .group_id(Uuid::new_v4().to_string())
        .probe_port(None)
        .subscribed_topics([topic.to_string()]);

    let mut producer_config = ProducerConfiguration::builder();
    producer_config
        .bootstrap_servers(bootstrap_servers)
        .source_system("my-source");

    let mut cassandra_config = CassandraConfigurationBuilder::default();
    cassandra_config.nodes(vec!["localhost:9042".to_owned()]);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_config,
        ..ConsumerBuilders::new()?
    };

    let (sender, mut receiver) = channel(1);

    let client = CassandraHighLevelClient::<MyHandler>::new(
        cassandra_config.build()?,
        Mode::Pipeline,
        &mut producer_config,
        &consumer_builders,
    )
    .await?;

    client.subscribe(MyHandler { sender }).await?;

    client
        .send(topic, "message-key", json!({"value": "Hello, Kafka!"}))
        .await?;

    // Hang-guard for the round-trip; sized generously so cluster slowness never
    // trips it. Correctness is the `assert_eq!` on the key below, not the wait.
    let message_key = timeout(Duration::from_mins(1), receiver.recv())
        .await
        .map_err(|_| eyre!("timed out waiting for message"))?
        .ok_or_else(|| eyre!("channel closed before message arrived"))?;

    assert_eq!(message_key, "message-key");

    client.unsubscribe().await?;
    admin.delete_topic(&topic).await?;

    Ok(())
}
