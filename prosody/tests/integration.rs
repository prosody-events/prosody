use std::cmp::max;
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use ahash::{HashMap, HashMapExt, HashSet};
use color_eyre::eyre::{eyre, OptionExt};
use color_eyre::{eyre, Report};
use itertools::Itertools;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::FromClientConfig;
use rdkafka::ClientConfig;
use serde_json::{json, Value};
use tokio::runtime::Builder;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{error, info};
use tracing_subscriber::fmt;
use uuid::Uuid;

use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::consumer::{ConsumerConfiguration, MessageHandler, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::Topic;

#[test]
fn receives_all_in_key_order() {
    const TEST_COUNT: u64 = 100;

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

        if messages.is_empty() {
            return TestResult::discard();
        }

        for key_messages in messages.values() {
            if key_messages.is_empty() {
                return TestResult::discard();
            }
        }

        let Err(error) = runtime.block_on(receives_all_in_key_order_impl(
            messages,
            partition_count,
            producer_count,
            consumer_count,
            max_enqueued_per_key,
        )) else {
            return TestResult::passed();
        };

        error!("test failed with error: {error:#}");
        TestResult::error(error.to_string())
    }

    fmt().compact().init();
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

#[allow(clippy::too_many_lines)] // todo: refactor and remove
async fn receives_all_in_key_order_impl(
    messages: HashMap<u64, BTreeSet<u64>>,
    partition_count: SmallCount,
    producer_count: SmallCount,
    consumer_count: SmallCount,
    max_enqueued_per_key: SmallCount,
) -> eyre::Result<()> {
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", "localhost:9094");
    let admin_options = AdminOptions::default().operation_timeout(Some(Duration::from_secs(5)));
    let admin_client = AdminClient::from_config(&config)?;
    admin_client
        .create_topics(
            &[NewTopic::new(
                &topic,
                partition_count.value() as i32,
                TopicReplication::Fixed(1),
            )],
            &admin_options,
        )
        .await?;

    info!("topic: {topic}");
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
        .commit_interval(Duration::from_secs(1))
        .partition_shutdown_timeout(Some(Duration::from_secs(60)))
        .build()?;

    let (messages_tx, mut messages_rx) = channel(partition_count.value());
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
        let mut received: HashMap<String, Vec<u64>> = HashMap::with_capacity(messages.len());

        info!("receiving messages");
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

        info!("verifying results");
        let mut result = Ok(());
        let mut actual: HashMap<u64, BTreeSet<u64>> = HashMap::with_capacity(messages.len());
        for (key, received_messages) in received {
            actual.insert(key.parse()?, received_messages.iter().copied().collect());

            let mut sorted = received_messages.clone();
            sorted.sort_unstable();

            if received_messages != sorted {
                result = Err(eyre!(
                    "invalid order for key {}; expected: {:?} != actual: {:?}",
                    &key,
                    sorted,
                    received_messages,
                ));
            }
        }

        if messages != actual {
            result = Err(eyre!(
                "all messages were not received; expected: {:?} != actual: {:?}",
                messages,
                actual
            ));
        }

        info!("sleeping long enough to commit");
        sleep(Duration::from_secs(6)).await;

        info!("sending shutdown signal");
        shutdown_tx.send(true)?;

        result
    });

    info!("waiting for tasks to complete");
    while let Some(result) = tasks.join_next().await {
        result??;
    }

    info!("test passed");
    // admin_client
    //     .delete_topics(&[topic.as_ref()], &admin_options)
    //     .await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct TestHandler {
    messages_tx: Sender<(String, Value)>,
}

impl TestHandler {
    fn new(messages_tx: Sender<(String, Value)>) -> Self {
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
        self.messages_tx.send((key.to_string(), payload)).await?;
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
        const VALUES: [u8; 12] = const_array();
        Self(*max(&1, g.choose(&VALUES).unwrap_or(&1)))
    }
}

impl Debug for SmallCount {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(&format!("{}", self.0))
    }
}

const fn const_array<const N: usize>() -> [u8; N] {
    let mut arr = [0; N];
    let mut i = 0;
    while i < N {
        arr[i] = i as u8 + 1;
        i += 1;
    }
    arr
}
