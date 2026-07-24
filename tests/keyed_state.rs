//! End-to-end integration test for the keyed-state layer.
//!
//! Drives a generic [`FallibleHandler`] through a real pipeline consumer
//! (Kafka at `localhost:9094`, Cassandra at `localhost:9042`): a value
//! cell accumulates across messages, a Kafka-message cell records the
//! last message seen, and an `Application` timer reads both back — the
//! value cell from durable state, the Kafka-message cell by re-fetching
//! the original message body from Kafka through the consumer's loader.
//!
//! The handler stays generic over `C: EventContext` (no concrete context type
//! named anywhere), which the typed `MessageDescriptor<L>` handle cannot do —
//! its resolver names a concrete loader `L`, so it needs `C::State` pinned to
//! that exact `L`, unknowable from `C: EventContext` alone. The erased vend
//! method `message_value_state(name)` returns a handle whose `.set()`/`.get()`
//! resolve through whatever loader `C::State` carries, bounded only by the
//! `MessageLoader` trait, so this test exercises that loader-agnostic path for
//! the Kafka-message cell instead.

#![recursion_limit = "256"]

use color_eyre::eyre::{Result, ensure, eyre};
use prosody::cassandra::CassandraStore;
use prosody::codec::JsonCodecError;
use prosody::consumer::event_context::{ErasedStateError, EventContext, StateAccessError};
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::{
    CommonConfiguration, ConsumerConfiguration, DemandType, KeyedStateConfiguration,
    MessageDescriptor, PipelineMiddlewareConfiguration, ProsodyConsumer, message_state,
};
use prosody::error::{ClassifyError, ErrorCategory};
use prosody::loader::KafkaLoader;
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::state::cassandra::{CassandraPublicationStore, PublicationQueries};
use prosody::state::descriptor::{
    CellStateError, Registered, StateDescriptor, ValueDescriptor, value_state,
};
use prosody::state::publication::PublicationStore;
use prosody::state::{StateName, StateType};
use prosody::state_reader::{ReaderLoader, SharedDeps, StateReader};
use prosody::subsystem::SubsystemName;
use prosody::telemetry::Telemetry;
use prosody::timers::datetime::{CompactDateTime, CompactDateTimeError};
use prosody::timers::duration::CompactDuration;
use prosody::timers::{TimerType, Trigger};
use prosody::tracing::init_test_logging;
use prosody::{
    JsonCodec, Offset, Topic,
    admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration},
};
use serde_json::{Value, json};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::timeout;
use tracing::error;
use uuid::Uuid;

mod common;

/// Number of messages the test produces; the handler schedules the
/// read-back timer once the cart holds this many items.
const MESSAGE_COUNT: usize = 2;

fn cart() -> ValueDescriptor {
    value_state("cart")
}

fn last_seen() -> MessageDescriptor<KafkaLoader<JsonCodec>> {
    message_state("last_seen")
}

/// The `last_seen` collection's registered name — the handler reaches it
/// through the erased, loader-agnostic `message_value_state(name)` vend method
/// (whose handle's `.set()`/`.get()` it calls) rather than a typed
/// [`Registered`] token (see the module doc).
const LAST_SEEN: &str = "last_seen";

/// A second Kafka-message collection the handler records the current message
/// into, registered `.published(true)` in the publication test so a standalone
/// [`StateReader`] can read it back — exercising the reader's Kafka loader arm
/// (`ReaderLoader::Kafka`), which resolves a message-ref cell by re-fetching
/// the body from Kafka. `last_seen` cannot serve this because it stays private
/// (its absent routing row is asserted end-to-end).
const RECEIPT: &str = "receipt";

/// What the handler saw, streamed to the test for content assertions.
#[derive(Debug)]
enum Observation {
    /// `on_message`: the cart value after this message's read-modify-write.
    Message { cart: Value },

    /// `on_timer`: the accumulated cart plus the re-fetched last-seen
    /// message (offset and payload), both read through descriptors.
    Timer {
        cart: Option<Value>,
        last_seen: Option<(Offset, Value)>,
    },
}

/// A generic handler — no concrete context type named anywhere — that
/// accumulates message `"item"` fields into the `cart` cell, records the
/// message in `last_seen`, and schedules an `Application` timer once the
/// cart is full. The timer reads both cells back.
#[derive(Clone)]
struct CartHandler {
    observations_tx: Sender<Observation>,
    /// The registration handle for the `cart` value collection — the handler
    /// can bind only collections it was handed a token for.
    cart: Registered<ValueDescriptor>,
}

impl CartHandler {
    async fn handle_message<C>(
        &self,
        ctx: C,
        message: ConsumerMessage<Value>,
    ) -> Result<(), CartHandlerError>
    where
        C: EventContext<Payload = Value>,
    {
        // Read-modify-write on the value cell: each message appends its
        // item to the array committed by the previous event.
        let cart = ctx.state(self.cart)?;
        let mut items = match cart.get().await? {
            Some(Value::Array(items)) => items,
            Some(other) => return Err(CartHandlerError::UnexpectedCell(other)),
            None => Vec::new(),
        };
        items.push(
            message
                .payload()
                .get("item")
                .cloned()
                .unwrap_or(Value::Null),
        );
        let full = items.len() == MESSAGE_COUNT;
        let updated = Value::Array(items);
        cart.set(updated.clone()).await?;

        ctx.clone()
            .boxed()
            .message_value_state(LAST_SEEN)?
            .set(message.clone())
            .await?;

        // Record the same message into the published receipt collection so a
        // cross-group reader can resolve it back through the Kafka loader arm.
        ctx.clone()
            .boxed()
            .message_value_state(RECEIPT)?
            .set(message)
            .await?;

        // The final message completes the cart; schedule the timer that
        // reads the accumulated state back. Per-key serialization
        // guarantees the fire dispatches only after this event commits.
        if full {
            let fire =
                CompactDateTime::now().and_then(|now| now.add_duration(CompactDuration::new(2)))?;
            ctx.schedule(fire, TimerType::Application)
                .await
                .map_err(|e| CartHandlerError::Schedule(e.to_string()))?;
        }

        self.observations_tx
            .send(Observation::Message { cart: updated })
            .await
            .map_err(|_| CartHandlerError::ChannelClosed)?;
        Ok(())
    }

    async fn handle_timer<C>(&self, ctx: C) -> Result<(), CartHandlerError>
    where
        C: EventContext<Payload = Value>,
    {
        let cart = ctx.state(self.cart)?.get().await?;
        // Re-fetches the original message body from Kafka through the
        // consumer's loader, decoded by the consumer's own codec.
        let last_seen = ctx
            .clone()
            .boxed()
            .message_value_state(LAST_SEEN)?
            .get()
            .await?
            .map(|message| (message.offset(), message.payload().clone()));

        self.observations_tx
            .send(Observation::Timer { cart, last_seen })
            .await
            .map_err(|_| CartHandlerError::ChannelClosed)?;
        Ok(())
    }
}

impl FallibleHandler for CartHandler {
    type Error = CartHandlerError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        ctx: C,
        message: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let result = self.handle_message(ctx, message).await;
        if let Err(error) = &result {
            // Surface the full error chain in the test log; the pipeline's
            // own logging shows only the outer middleware display.
            error!(?error, "cart handler failed on message");
        }
        result
    }

    async fn on_timer<C>(
        &self,
        ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let result = self.handle_timer(ctx).await;
        if let Err(error) = &result {
            error!(?error, "cart handler failed on timer");
        }
        result
    }

    async fn shutdown(self) {}
}

/// Errors the handler can surface; everything classifies Permanent so a
/// failure fails the test fast instead of retrying into a timeout.
#[derive(Debug, Error)]
enum CartHandlerError {
    /// Binding a descriptor failed.
    #[error(transparent)]
    Access(#[from] StateAccessError),

    /// A value-cell access or codec failure.
    #[error(transparent)]
    Value(#[from] CellStateError<JsonCodecError>),

    /// A Kafka-message-cell access failure through the erased seam.
    #[error(transparent)]
    Kafka(#[from] ErasedStateError),

    /// The cart cell held something other than an array.
    #[error("unexpected cart cell: {0}")]
    UnexpectedCell(Value),

    /// Computing the read-back timer's fire time failed.
    #[error(transparent)]
    FireTime(#[from] CompactDateTimeError),

    /// Scheduling the read-back timer failed.
    #[error("failed to schedule the read-back timer: {0}")]
    Schedule(String),

    /// The test dropped the observation receiver.
    #[error("observation channel closed")]
    ChannelClosed,
}

impl ClassifyError for CartHandlerError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

async fn next_observation(rx: &mut Receiver<Observation>, secs: u64) -> Result<Observation> {
    timeout(Duration::from_secs(secs), rx.recv())
        .await
        .map_err(|_| eyre!("timed out waiting for an observation"))?
        .ok_or_else(|| eyre!("observation channel closed"))
}

/// Cart accumulation across messages, Kafka-message read-back on a timer.
///
/// Two messages with the same key flow through the full pipeline stack.
/// The handler's value cell must show `["apple"]` then
/// `["apple", "banana"]` (read-your-committed-writes across events), and
/// the timer fired by the second message must observe the accumulated
/// cart plus the second message re-fetched from Kafka by offset.
///
/// # Errors
///
/// Returns an error if topic setup, producer/consumer initialization, or
/// observation verification fails.
#[tokio::test]
async fn test_keyed_state_round_trip_through_pipeline() -> Result<()> {
    init_test_logging();

    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap.clone())?)?;

    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap)
        .group_id(Uuid::new_v4().to_string())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    let (observations_tx, mut observations_rx) = channel(10);

    // Register the collections. `cart`'s token is the only way the handler
    // can bind it; `last_seen` is reached through the erased seam by name
    // (see the module doc), so its token is only needed to register the
    // collection's identity, not held by the handler.
    let mut keyed_state = KeyedStateConfiguration::default();
    let cart = keyed_state.register(cart());
    let _last_seen = keyed_state.register(last_seen());
    // Registered so the handler's erased write resolves; stays private here (no
    // subsystem is configured in this test).
    let _receipt = keyed_state.register(message_state::<KafkaLoader<JsonCodec>>(RECEIPT));
    let handler = CartHandler {
        observations_tx,
        cart,
    };

    let telemetry = Telemetry::new();
    let producer = ProsodyProducer::<JsonCodec>::new(&producer_config, telemetry.sender())?;

    let pipeline_config = PipelineMiddlewareConfiguration {
        retry: RetryConfigurationBuilder::default().build()?,
        monopolization: MonopolizationConfigurationBuilder::default().build()?,
        defer: DeferConfigurationBuilder::default().build()?,
    };

    let common_config = CommonConfiguration {
        scheduler: SchedulerConfigurationBuilder::default().build()?,
        timeout: TimeoutConfigurationBuilder::default().build()?,
        dedup: DeduplicationConfigurationBuilder::default().build()?,
        keyed_state,
    };

    let consumer = ProsodyConsumer::<JsonCodec>::pipeline_consumer(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        pipeline_config,
        &common_config,
        telemetry,
        handler,
        None,
    )
    .await?;

    let key = "cart-key";
    let first = json!({ "id": "evt-1", "item": "apple" });
    let second = json!({ "id": "evt-2", "item": "banana" });

    // Always shut the consumer down before propagating a failure — an
    // early return would leave the consumer's client threads alive and
    // hang the test binary far past the hang-guard timeouts.
    let outcome = async {
        producer.send([], topic, key, first).await?;
        producer.send([], topic, key, second.clone()).await?;
        verify_observations(&mut observations_rx, &second).await
    }
    .await;
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    outcome
}

/// The content assertions, in the deterministic per-key order: message 1,
/// message 2, then the timer the second message scheduled. Timeouts are
/// hang-guards only; content assertions decide.
async fn verify_observations(rx: &mut Receiver<Observation>, second: &Value) -> Result<()> {
    let obs = next_observation(rx, 60).await?;
    match obs {
        Observation::Message { cart } => ensure!(
            cart == json!(["apple"]),
            "first message must start the cart, got {cart}"
        ),
        other @ Observation::Timer { .. } => {
            return Err(eyre!("expected first message observation, got {other:?}"));
        }
    }

    let obs = next_observation(rx, 60).await?;
    match obs {
        Observation::Message { cart } => ensure!(
            cart == json!(["apple", "banana"]),
            "second message must read the first's committed cart, got {cart}"
        ),
        other @ Observation::Timer { .. } => {
            return Err(eyre!("expected second message observation, got {other:?}"));
        }
    }

    let obs = next_observation(rx, 60).await?;
    match obs {
        Observation::Timer { cart, last_seen } => {
            ensure!(
                cart == Some(json!(["apple", "banana"])),
                "timer must observe the accumulated cart, got {cart:?}"
            );
            let (offset, payload) =
                last_seen.ok_or_else(|| eyre!("timer observed no last-seen message"))?;
            ensure!(
                offset == 1,
                "last-seen must reference the second message's offset, got {offset}"
            );
            ensure!(
                payload == *second,
                "last-seen must re-fetch the second message's payload, got {payload}"
            );
        }
        other @ Observation::Message { .. } => {
            return Err(eyre!("expected timer observation, got {other:?}"));
        }
    }
    Ok(())
}

/// A `Published` collection's first durable write publishes a routing row into
/// `keyed_state_publication` carrying the topic's live Kafka partition count.
/// Mirrors the round-trip test, but `cart` is `.published(true)` under a
/// configured subsystem; after the writes are observably durable (the timer
/// read-back fires only after both message events settle), the row is present
/// with the correct group, topic, and partition count.
#[tokio::test]
async fn test_published_collection_writes_routing_row() -> Result<()> {
    init_test_logging();

    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap = vec!["localhost:9094".to_owned()];
    let admin_client = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap.clone())?)?;

    admin_client
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-producer")
        .build()?;

    let group_id = Uuid::new_v4().to_string();
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap)
        .group_id(group_id.clone())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    let (observations_tx, mut observations_rx) = channel(10);

    // `cart` is published under a subsystem; `last_seen` stays private. The
    // subsystem is minted fresh per run: the publication table is keyed by
    // `(subsystem, name)` and the reader discovers every group that published
    // under it, so a fixed name would accumulate one source row per run against
    // the shared keyspace and eventually breach `MAX_PUBLICATION_SOURCES`.
    let subsystem = SubsystemName::try_new(format!("orders-{}", Uuid::new_v4()))
        .map_err(|e| eyre!("subsystem: {e}"))?;
    let mut keyed_state = KeyedStateConfiguration::default();
    keyed_state.subsystem = Some(subsystem.clone());
    let cart = keyed_state.register(cart().published(true));
    let _last_seen = keyed_state.register(last_seen());
    // A published Kafka-message collection: its routing row lets a standalone
    // reader discover the source and resolve the cell through the Kafka loader.
    let _receipt =
        keyed_state.register(message_state::<KafkaLoader<JsonCodec>>(RECEIPT).published(true));
    let handler = CartHandler {
        observations_tx,
        cart,
    };

    let telemetry = Telemetry::new();
    let producer = ProsodyProducer::<JsonCodec>::new(&producer_config, telemetry.sender())?;

    let pipeline_config = PipelineMiddlewareConfiguration {
        retry: RetryConfigurationBuilder::default().build()?,
        monopolization: MonopolizationConfigurationBuilder::default().build()?,
        defer: DeferConfigurationBuilder::default().build()?,
    };

    let common_config = CommonConfiguration {
        scheduler: SchedulerConfigurationBuilder::default().build()?,
        timeout: TimeoutConfigurationBuilder::default().build()?,
        dedup: DeduplicationConfigurationBuilder::default().build()?,
        keyed_state,
    };

    let consumer = ProsodyConsumer::<JsonCodec>::pipeline_consumer(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        pipeline_config,
        &common_config,
        telemetry,
        handler,
        None,
    )
    .await?;

    let key = "cart-key";
    let first = json!({ "id": "evt-1", "item": "apple" });
    let second = json!({ "id": "evt-2", "item": "banana" });

    let outcome = async {
        producer.send([], topic, key, first).await?;
        producer.send([], topic, key, second.clone()).await?;
        // The timer read-back fires only after both message events fully settle
        // (per-key serialization), so by the time it is observed the cart's
        // publication row is durable.
        verify_observations(&mut observations_rx, &second).await?;
        assert_routing_row(&subsystem, &group_id, topic).await?;
        // The published cart is now durable and advertised: a standalone
        // Cassandra-backed reader discovers the source, validates identity, and
        // reads the committed value over the production read path.
        read_cart_via_standalone_reader(&subsystem, &consumer_config, key).await
    }
    .await;
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    outcome
}

/// Reads the published `cart` value back through a standalone Cassandra-backed
/// [`StateReader`] — exercising the full production read wiring end-to-end:
/// `SharedDeps::connect`, publication-source discovery, frozen-identity
/// validation against the reader's descriptor, probe-and-pin, and the committed
/// projection. The read is expected to observe exactly the value the pipeline
/// consumer committed.
async fn read_cart_via_standalone_reader(
    subsystem: &SubsystemName,
    consumer_config: &ConsumerConfiguration,
    key: &str,
) -> Result<()> {
    // One `connect` opens the session, prepares the reader's queries, and
    // builds the Kafka loader (required by the Cassandra bundle but never
    // consulted for a plain Value; a Kafka-ref collection would exercise it).
    let budget = NonZeroU64::new(1_048_576_u64).ok_or_else(|| eyre!("nonzero budget"))?;
    let deps =
        SharedDeps::<JsonCodec>::connect(consumer_config, &common::test_cassandra_config(), budget)
            .await?;
    let reader = StateReader::new(&deps, subsystem.clone(), cart())?;

    let value = reader.get(key).await?;
    ensure!(
        value == Some(json!(["apple", "banana"])),
        "standalone reader must observe the committed cart, got {value:?}"
    );

    // The published receipt is a Kafka-message cell: reading it through the
    // reader exercises `ReaderLoader::Kafka`, resolving the committed message
    // ref by re-fetching the body from Kafka over the production loader. The
    // reader binds the same message identity under its own `ReaderLoader` (the
    // resolver id is loader-independent), so the source discovered above serves
    // the second message the consumer recorded (offset 1, body `banana`).
    let receipt_reader = StateReader::new(
        &deps,
        subsystem.clone(),
        message_state::<ReaderLoader<JsonCodec>>(RECEIPT),
    )?;
    let receipt = receipt_reader
        .get(key)
        .await?
        .ok_or_else(|| eyre!("standalone reader observed no published receipt"))?;
    ensure!(
        receipt.offset() == 1,
        "receipt must reference the second message's offset, got {}",
        receipt.offset()
    );
    ensure!(
        receipt.payload() == &json!({ "id": "evt-2", "item": "banana" }),
        "receipt must re-fetch the second message's body, got {}",
        receipt.payload()
    );
    Ok(())
}

/// Reads the `keyed_state_publication` table directly and asserts exactly one
/// routing row for `group_id` under `(subsystem, cart)`, carrying `topic` and
/// the topic's live partition count (1, since the test topic has one
/// partition).
async fn assert_routing_row(subsystem: &SubsystemName, group_id: &str, topic: Topic) -> Result<()> {
    let store = CassandraStore::new(&common::test_cassandra_config()).await?;
    let queries = Arc::new(PublicationQueries::new(store.session(), common::TEST_KEYSPACE).await?);
    let publication_store = CassandraPublicationStore::new(store, queries);
    let name = StateName::try_new("cart").map_err(|e| eyre!("name: {e}"))?;
    let rows = publication_store
        .read_publications(subsystem, StateType::Application, &name)
        .await?;
    let own: Vec<_> = rows
        .into_iter()
        .filter(|r| r.group_id.as_ref() == group_id)
        .collect();
    ensure!(
        own.len() == 1,
        "exactly one routing row for this group, got {}",
        own.len()
    );
    ensure!(
        own[0].topic == topic,
        "row must carry the writing topic, got {:?}",
        own[0].topic
    );
    ensure!(
        i32::from(own[0].partition_count) == 1_i32,
        "row must carry the topic's live partition count (1), got {}",
        i32::from(own[0].partition_count)
    );

    // The private `last_seen` collection was durably written alongside `cart`
    // but never publishes: its routing row must be absent end-to-end (the
    // visibility gate against the real table, not just the mock stores).
    let private = StateName::try_new(LAST_SEEN).map_err(|e| eyre!("name: {e}"))?;
    ensure!(
        publication_store
            .read_publications(subsystem, StateType::Application, &private)
            .await?
            .is_empty(),
        "a private collection must never write a routing row"
    );
    Ok(())
}
