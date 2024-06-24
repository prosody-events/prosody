use std::convert::Infallible;
use std::mem::take;
use std::sync::Arc;

use color_eyre::eyre;
use parking_lot::Mutex;
use rdkafka::mocking::MockCluster;
use serde_json::{json, Value};
use tokio::sync::Notify;

use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::consumer::{ConsumerConfiguration, MessageHandler, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::Topic;

#[tokio::test]
async fn round_trip() -> eyre::Result<()> {
    tracing_subscriber::fmt().init();

    let mock = MockCluster::new(3)?;
    let bootstrap: Vec<String> = mock
        .bootstrap_servers()
        .split(',')
        .map(str::to_owned)
        .collect();

    let topic: Topic = "test".into();
    let key = "test-key";
    let payload = json!({ "hello": "world"});
    let handler = TestHandler::default();

    mock.create_topic(&topic, 16, 3)?;

    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap)
        .group_id("test-consumer")
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    let producer = ProsodyProducer::new(&producer_config)?;
    let consumer = ProsodyConsumer::new(consumer_config, handler.clone())?;

    producer.send(topic, key, payload.clone()).await?;
    handler.notify.notified().await;

    let result = handler.take();
    assert_eq!(result, vec![payload]);
    consumer.shutdown().await;

    Ok(())
}

#[derive(Clone, Debug, Default)]
struct TestHandler {
    received: Arc<Mutex<Vec<Value>>>,
    notify: Arc<Notify>,
}

impl TestHandler {
    fn take(self) -> Vec<Value> {
        take(&mut self.received.lock())
    }
}

impl MessageHandler for TestHandler {
    type Error = Infallible;

    async fn handle(
        &self,
        _context: &mut MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let (_, payload, uncommitted) = message.into_inner();
        self.received.lock().push(payload);
        self.notify.notify_one();

        uncommitted.commit();
        Ok(())
    }
}
