//! End-to-end integration tests for the keyed-state layer.
//!
//! Both tests drive [`CartHandler`] through a real pipeline consumer (Kafka at
//! `localhost:9094`, Cassandra at `localhost:9042`): a value cell accumulates
//! across messages, a Kafka-message cell records the last message seen, and an
//! `Application` timer reads both back — the value cell from durable state, the
//! Kafka-message cell by re-fetching the original body from Kafka through the
//! consumer's loader.
//!
//! The two tests differ only in registration. One configures no subsystem, so
//! every collection is private and the publication machinery is off entirely.
//! The other publishes under a subsystem and additionally checks what that made
//! discoverable; see [`publication`].

#![recursion_limit = "256"]

use crate::cart::{CartHandler, Observation, RECEIPT, cart, last_seen, verify_observations};
use crate::publication::{assert_routing_row, read_cart_via_standalone_reader};
use color_eyre::eyre::{Result, eyre};
use prosody::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::{
    CommonConfiguration, ConsumerConfiguration, ConsumerSetup, KeyedStateConfiguration,
    PipelineMiddlewareConfiguration, ProsodyConsumer, message_state,
};
use prosody::loader::KafkaLoader;
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::state::descriptor::StateDescriptor;
use prosody::subsystem::SubsystemName;
use prosody::telemetry::Telemetry;
use prosody::tracing::init_test_logging;
use prosody::{JsonCodec, Topic, admin::ProsodyAdminClient};
use serde_json::json;
use std::future::Future;
use tokio::sync::mpsc::{Receiver, channel};
use uuid::Uuid;

mod cart;
#[path = "../common/mod.rs"]
mod common;
mod publication;

/// The single message key both tests produce under, so every event serializes
/// per-key and the timer fires only after both messages settle.
const KEY: &str = "cart-key";

/// Whether the environment's collections are discoverable outside the group.
enum Registration {
    /// No subsystem configured; every collection stays private.
    Private,

    /// `cart` and `receipt` are published under this subsystem.
    Published { subsystem: SubsystemName },
}

/// A live pipeline consumer driving [`CartHandler`] over a freshly created
/// topic.
struct CartEnv {
    topic: Topic,
    admin: &'static ProsodyAdminClient,
    consumer: ProsodyConsumer<JsonCodec>,
    producer: ProsodyProducer<JsonCodec>,
    observations: Receiver<Observation>,
    consumer_config: ConsumerConfiguration,
    group_id: String,
}

/// What the environment hands a scenario once the messages have settled.
struct CartFacts {
    group_id: String,
    consumer_config: ConsumerConfiguration,
    topic: Topic,
}

impl CartEnv {
    /// An environment whose collections are all private.
    async fn private() -> Result<Self> {
        Self::start(Registration::Private).await
    }

    /// An environment publishing `cart` and `receipt` under `subsystem`.
    async fn published(subsystem: SubsystemName) -> Result<Self> {
        Self::start(Registration::Published { subsystem }).await
    }

    async fn start(registration: Registration) -> Result<Self> {
        init_test_logging();

        let (topic, admin) = common::kafka::create_topic_with_partitions(1).await?;
        let bootstrap = vec!["localhost:9094".to_owned()];
        let group_id = Uuid::new_v4().to_string();
        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .group_id(group_id.clone())
            .probe_port(None)
            .subscribed_topics(&[topic.to_string()])
            .build()?;

        let (observations_tx, observations) = channel(10);

        let mut keyed_state = KeyedStateConfiguration::builder().build()?;
        let published = match registration {
            Registration::Private => false,
            Registration::Published { subsystem } => {
                keyed_state.subsystem = Some(subsystem);
                true
            }
        };
        // `cart`'s token is the only way the handler can bind it. `last_seen` and
        // `receipt` are reached through the erased seam by name, so their tokens
        // exist only to register the collections' identities.
        let cart = keyed_state.register(cart().published(published));
        let _last_seen = keyed_state.register(last_seen());
        let _receipt = keyed_state
            .register(message_state::<KafkaLoader<JsonCodec>>(RECEIPT).published(published));

        let common_config = CommonConfiguration {
            scheduler: SchedulerConfigurationBuilder::default().build()?,
            timeout: TimeoutConfigurationBuilder::default().build()?,
            dedup: DeduplicationConfigurationBuilder::default().build()?,
            keyed_state,
        };
        let trigger_store = common::create_cassandra_trigger_store_config();

        let telemetry = Telemetry::new();
        let producer = ProsodyProducer::<JsonCodec>::new(
            &ProducerConfiguration::builder()
                .bootstrap_servers(bootstrap)
                .source_system("test-producer")
                .build()?,
            telemetry.sender(),
        )?;

        let consumer = ProsodyConsumer::<JsonCodec>::pipeline_consumer(
            ConsumerSetup::<JsonCodec> {
                consumer: &consumer_config,
                trigger_store: &trigger_store,
                common: &common_config,
                deps: None,
            },
            PipelineMiddlewareConfiguration {
                retry: RetryConfigurationBuilder::default().build()?,
                monopolization: MonopolizationConfigurationBuilder::default().build()?,
                defer: DeferConfigurationBuilder::default().build()?,
            },
            telemetry,
            CartHandler {
                observations_tx,
                cart,
            },
        )
        .await?;

        Ok(Self {
            topic,
            admin,
            consumer,
            producer,
            observations,
            consumer_config,
            group_id,
        })
    }

    /// Sends the two messages and verifies the three observations.
    async fn run(self) -> Result<()> {
        self.run_then(|_| async { Ok(()) }).await
    }

    /// [`Self::run`], then `scenario` against the settled environment.
    ///
    /// Always shuts the consumer down and deletes the topic before returning,
    /// even when the scenario fails: an early return would leave the consumer's
    /// client threads alive and hang the test binary far past every hang-guard.
    async fn run_then<F, Fut>(mut self, scenario: F) -> Result<()>
    where
        F: FnOnce(CartFacts) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let second = json!({ "id": "evt-2", "item": "banana" });
        let outcome = async {
            let first = json!({ "id": "evt-1", "item": "apple" });
            self.producer.send([], self.topic, KEY, first).await?;
            self.producer
                .send([], self.topic, KEY, second.clone())
                .await?;
            verify_observations(&mut self.observations, &second).await?;
            scenario(CartFacts {
                group_id: self.group_id.clone(),
                consumer_config: self.consumer_config.clone(),
                topic: self.topic,
            })
            .await
        }
        .await;

        self.consumer.shutdown().await;
        let cleanup: Result<()> = self
            .admin
            .delete_topic(&self.topic)
            .await
            .map_err(Into::into);
        outcome.and(cleanup)
    }
}

/// Cart accumulation across messages, with Kafka-message read-back on a timer.
///
/// Two messages with the same key flow through the full pipeline stack with no
/// subsystem configured. The handler's value cell must show `["apple"]` then
/// `["apple", "banana"]` — read-your-committed-writes across events — and the
/// timer the second message scheduled must observe the accumulated cart plus
/// that message re-fetched from Kafka by offset.
#[tokio::test]
async fn test_keyed_state_round_trip_through_pipeline() -> Result<()> {
    CartEnv::private().await?.run().await
}

/// A published collection's first durable write publishes a routing row into
/// `keyed_state_publication` carrying the topic's live Kafka partition count.
///
/// Same scenario as the round trip, but `cart` and `receipt` are registered
/// published under a configured subsystem. The timer read-back fires only after
/// both message events settle, so by the time the assertions run the writes and
/// the routing row are durable.
#[tokio::test]
async fn test_published_collection_writes_routing_row() -> Result<()> {
    let subsystem = fresh_subsystem()?;
    CartEnv::published(subsystem.clone())
        .await?
        .run_then(|facts| async move {
            assert_routing_row(&subsystem, &facts.group_id, facts.topic).await?;
            read_cart_via_standalone_reader(&subsystem, &facts.consumer_config, KEY).await
        })
        .await
}

/// A fresh subsystem name per run. The publication table is keyed by
/// `(subsystem, name)` and the reader discovers every group that published
/// under it, so a fixed name would accumulate one source row per run against
/// the shared keyspace and eventually breach `MAX_PUBLICATION_SOURCES`.
fn fresh_subsystem() -> Result<SubsystemName> {
    SubsystemName::try_new(format!("orders-{}", Uuid::new_v4()))
        .map_err(|error| eyre!("subsystem: {error}"))
}
