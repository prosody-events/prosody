use color_eyre::eyre;
use serde_json::json;

use prosody::consumer::{ConsumerConfiguration, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::Topic;

#[tokio::test]
async fn round_trip() -> eyre::Result<()> {
    tracing_subscriber::fmt().init();

    let topic: Topic = "test".into();
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(["localhost:9092".to_owned()])
        .mock(true)
        .build()?;

    let producer = ProsodyProducer::new(&producer_config)?;
    producer
        .send(topic, "test-key", json!({ "hello": "world"}))
        .await?;

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(["localhost:9092".to_owned()])
        .group_id("test-consumer")
        .subscribed_topics(&[topic.to_string()])
        .mock(true)
        .build()?;

    // let consumer = ProsodyConsumer::new(consumer_config)

    Ok(())
}
