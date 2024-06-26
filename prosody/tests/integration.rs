use std::cmp::max;
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};

use ahash::{HashMap, HashSet};
use color_eyre::eyre::{eyre, OptionExt};
use color_eyre::{eyre, Report};
use itertools::Itertools;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use rdkafka::mocking::MockCluster;
use serde_json::{json, Value};
use tokio::runtime::Builder;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::watch;
use tokio::task::JoinSet;

use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::consumer::{ConsumerConfiguration, MessageHandler, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::Topic;

#[test]
fn receives_all_in_key_order() {
    const TEST_COUNT: u64 = 3;

    fn prop(
        messages: HashMap<u64, BTreeSet<u64>>,
        partition_count: SmallCount,
        producer_count: SmallCount,
        consumer_count: SmallCount,
        max_enqueued_per_key: SmallCount,
    ) -> TestResult {
        let Ok(runtime) = Builder::new_multi_thread().enable_time().build() else {
            return TestResult::error("failed to initialize runtime");
        };

        let Err(error) = runtime.block_on(receives_all_in_key_order_impl(
            messages,
            partition_count,
            producer_count,
            consumer_count,
            max_enqueued_per_key,
        )) else {
            return TestResult::passed();
        };

        TestResult::error(error.to_string())
    }

    QuickCheck::new().tests(TEST_COUNT).quickcheck(
        prop as fn(
            HashMap<u64, BTreeSet<u64>>,
            SmallCount,
            SmallCount,
            SmallCount,
            SmallCount,
        ) -> TestResult,
    );
}

async fn receives_all_in_key_order_impl(
    messages: HashMap<u64, BTreeSet<u64>>,
    partition_count: SmallCount,
    producer_count: SmallCount,
    consumer_count: SmallCount,
    max_enqueued_per_key: SmallCount,
) -> eyre::Result<()> {
    let mock = MockCluster::new(3)?;
    let bootstrap: Vec<String> = mock
        .bootstrap_servers()
        .split(',')
        .map(str::to_owned)
        .collect();

    let topic: Topic = "test".into();
    mock.create_topic(&topic, partition_count.value() as i32, 3)?;

    let message_count = messages.len();
    let producer_message_count = max(message_count / producer_count.value(), 1);

    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap)
        .group_id("test-consumer")
        .subscribed_topics(&[topic.to_string()])
        .max_enqueued_per_key(max_enqueued_per_key.value())
        .build()?;

    let (messages_tx, mut messages_rx) = unbounded_channel();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handler = TestHandler::new(messages_tx);

    let mut tasks: JoinSet<eyre::Result<()>> = JoinSet::new();

    for producer_messages in messages
        .clone()
        .into_iter()
        .chunks(producer_message_count)
        .into_iter()
        .map(Iterator::collect::<Vec<_>>)
    {
        let producer_config = producer_config.clone();
        tasks.spawn(async move {
            let producer = ProsodyProducer::new(&producer_config)?;
            for (key, messages) in producer_messages {
                let key = key.to_string();
                for message in messages {
                    producer.send(topic, &key, json!(message)).await?;
                }

                producer.send(topic, &key, Value::Null).await?;
            }

            Ok(())
        });
    }

    for _ in 0..consumer_count.value() {
        let consumer_config = consumer_config.clone();
        let handler = handler.clone();
        let mut shutdown_rx = shutdown_rx.clone();

        tasks.spawn(async move {
            let consumer = ProsodyConsumer::new(consumer_config, handler)?;
            shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await?;
            consumer.shutdown().await;
            Ok(())
        });
    }

    tasks.spawn(async move {
        let mut keys: HashSet<String> = messages.keys().map(ToString::to_string).collect();
        let mut received: HashMap<String, Vec<u64>> = HashMap::default();

        while let Some((key, payload)) = messages_rx.recv().await {
            let Value::Number(number) = payload else {
                keys.remove(&key);
                if keys.is_empty() {
                    break;
                };
                continue;
            };
            let number = number.as_u64().ok_or_eyre("invalid number")?;
            received.entry(key).or_default().push(number);
        }

        shutdown_tx.send(true)?;

        for (_, actual) in received {
            let mut expected = actual.clone();
            expected.sort_unstable();

            if actual != expected {
                return Err(eyre!("{:?} != {:?}", actual, expected));
            }
        }

        Ok(())
    });

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct TestHandler {
    messages_tx: UnboundedSender<(String, Value)>,
}

impl TestHandler {
    fn new(messages_tx: UnboundedSender<(String, Value)>) -> Self {
        Self { messages_tx }
    }
}

impl MessageHandler for TestHandler {
    type Error = Report;

    async fn handle(
        &self,
        _context: &mut MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let (key, payload, uncommitted) = message.into_inner();
        self.messages_tx.send((key.to_string(), payload))?;
        uncommitted.commit();
        Ok(())
    }
}

#[derive(Copy, Clone)]
struct SmallCount(u8);

impl SmallCount {
    fn value(self) -> usize {
        self.0 as usize
    }
}

impl Arbitrary for SmallCount {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(max(1, u8::arbitrary(g)))
    }
}

impl Debug for SmallCount {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(&format!("{}", self.0))
    }
}
