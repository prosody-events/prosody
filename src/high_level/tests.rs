use super::*;
use crate::EventIdentity;
use crate::JsonCodec;
use crate::Key;
use crate::PeerConfiguration;
use crate::cassandra::config::CassandraConfigurationBuilder;
use crate::codec::UnitCodec;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::{ConsumerConfiguration, DemandType, KeyedStateConfiguration};
use crate::high_level::erased::{
    ErasedConsumerState, ErasedReadCache, ErasedReaderBuildError, new_erased,
};
use crate::high_level::mode::Mode;
use crate::producer::ProducerConfiguration;
use crate::state::descriptor::value_state;
use crate::state::registry::{CollectionDef, RegisterStateError};
use crate::state_reader::ReaderBackend;
use crate::state_reader::tests::support::{
    mock_count, owner_commit, publish_source, registry_of, source_state_key, state_name, topic,
};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::timers::Trigger;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use serde_json::{Value, json};
use std::convert::Infallible;
use std::time::Duration;
use tokio::time::timeout;

/// Builds a mock-mode pipeline `HighLevelClient<T>`, ready to `register`
/// (Configured) and `subscribe`/`unsubscribe`. Optionally overrides the
/// producer's source system (default: derived from `group_id`).
fn create_test_client<T>(
    group_id: &str,
    source_system: Option<&str>,
) -> Result<MemoryHighLevelClient<T>>
where
    T: ClientHandler,
    T::Payload: EventIdentity + Clone,
{
    let mut producer_builder = ProducerConfiguration::builder();
    producer_builder
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .mock(false);
    if let Some(source) = source_system {
        producer_builder.source_system(source);
    }

    let mut consumer_builder = ConsumerConfiguration::builder();
    consumer_builder
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .group_id(group_id)
        .subscribed_topics(&["test-topic".to_owned()])
        .mock(false);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        ..ConsumerBuilders::new()?
    };
    Ok(TEST_RUNTIME.block_on(MemoryHighLevelClient::<T>::new(
        Mode::Pipeline,
        &mut producer_builder,
        &consumer_builders,
    ))?)
}

fn create_peer_test_client<T>(group_id: &str) -> Result<MemoryHighLevelClient<T>>
where
    T: ClientHandler,
    T::Payload: EventIdentity + Clone,
{
    let mut producer = ProducerConfiguration::builder();
    producer
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .source_system("peer-requester");
    let mut consumer = ConsumerConfiguration::builder();
    consumer
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .group_id(group_id)
        .subscribed_topics(&["test-topic".to_owned()])
        .poll_interval(Duration::from_millis(1));
    let builders = ConsumerBuilders {
        consumer,
        keyed_state: KeyedStateConfiguration::builder()
            .subsystem(Some(subsystem("echo")?))
            .build()?,
        ..ConsumerBuilders::new()?
    };
    Ok(TEST_RUNTIME.block_on(MemoryHighLevelClient::<T>::new(
        Mode::Pipeline,
        &mut producer,
        &builders,
    ))?)
}

#[derive(Clone)]
struct EchoHandler;

impl FallibleHandler for EchoHandler {
    type Error = Infallible;
    type Output = Value;
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _ctx: C,
        message: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> Result<Value, Infallible>
    where
        C: EventContext<Payload = Value>,
    {
        Ok(message.payload().clone())
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<Value, Infallible>
    where
        C: EventContext<Payload = Value>,
    {
        Ok(Value::Null)
    }

    async fn shutdown(self) {}
}

impl ClientHandler for EchoHandler {
    type Codecs = JsonCodecs;
}

/// A mock client answers through its explicit response subsystem.
#[test]
fn a_mock_client_round_trips_one_peer_request() -> Result<()> {
    init_test_logging();
    let client = create_peer_test_client::<EchoHandler>("peer-round-trip")?;
    TEST_RUNTIME.block_on(async {
        client.subscribe(EchoHandler).await?;
        let outcome: Result<()> = async {
            let state = client.consumer_state().await;
            let ConsumerState::Running { consumer, .. } = &*state else {
                return Err(eyre!("the subscribed client is not running"));
            };
            let assigned = timeout(
                Duration::from_secs(10),
                consumer.wait_for_assigned_partitions(3),
            )
            .await
            .map_err(|_| eyre!("the mock consumer did not receive its partition"))?;
            ensure!(
                assigned == 3,
                "the mock consumer does not own all partitions"
            );
            drop(state);
            let payload = json!({"answer": 42_i32});
            let subsystem = SubsystemName::try_new("echo")?;
            let outcomes = client
                .request(
                    [],
                    Topic::from("test-topic"),
                    "key",
                    payload.clone(),
                    &[subsystem],
                    Duration::from_secs(1),
                )
                .await?;
            assert_eq!(outcomes, vec![Ok(payload)]);
            Ok(())
        }
        .await;
        client.unsubscribe().await?;
        outcome
    })
}

/// Request validation uses the router before the consumer starts.
#[test]
fn a_request_does_not_require_subscription() -> Result<()> {
    let client = create_peer_test_client::<EchoHandler>("peer-before-subscribe")?;
    let subsystems = [];
    let error = TEST_RUNTIME
        .block_on(client.request(
            [],
            Topic::from("test-topic"),
            "key",
            Value::Null,
            &subsystems,
            Duration::from_secs(1),
        ))
        .err()
        .ok_or_else(|| eyre!("the request accepted an empty subsystem list"))?;
    assert!(matches!(error, RequestError::NoSubsystems));
    Ok(())
}

#[test]
fn source_system_uses_the_explicit_value_or_consumer_group() -> Result<()> {
    let group_id = "my-test-group";
    let explicit_source = "my-explicit-source";
    for (configured, expected) in [(None, group_id), (Some(explicit_source), explicit_source)] {
        let client = create_test_client::<NoOpHandler>(group_id, configured)?;
        assert_eq!(client.source_system(), expected);
        assert!(client.producer_config().mock);
    }
    Ok(())
}

/// Minimal no-op handler so the lifecycle tests can drive `subscribe` /
/// `unsubscribe` in mock mode. It never errors, so `Infallible` is its error.
#[derive(Clone)]
struct NoOpHandler;

impl FallibleHandler for NoOpHandler {
    type Error = Infallible;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _ctx: C,
        _message: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn shutdown(self) {}
}

impl ClientHandler for NoOpHandler {
    type Codecs = Codecs<JsonCodec, UnitCodec>;
}

/// Erased backend selection reads only mock mode. Invalid consumer-only fields
/// remain deferred, so the producer half constructs and subscription reports
/// the retained configuration error.
#[test]
fn erased_client_retains_consumer_failure_until_subscribe() -> Result<()> {
    let mut producer = ProducerConfiguration::builder();
    producer
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .source_system("producer-only");
    let mut consumer = ConsumerConfiguration::builder();
    consumer.mock(true);
    let consumers = ConsumerBuilders {
        consumer,
        peer: PeerConfiguration::builder().build()?,
        ..ConsumerBuilders::new()?
    };

    let client = TEST_RUNTIME.block_on(new_erased::<NoOpHandler>(
        Mode::Pipeline,
        &mut producer,
        &consumers,
        &CassandraConfigurationBuilder::default(),
    ))?;
    assert_eq!(client.source_system(), "producer-only");
    let state = TEST_RUNTIME.block_on(client.consumer_state());
    assert!(matches!(state, ErasedConsumerState::ConfigurationFailed(_)));
    let subscribed = TEST_RUNTIME.block_on(client.subscribe(NoOpHandler));
    assert!(matches!(
        subscribed,
        Err(HighLevelClientError::ConsumerConfiguration(_))
    ));
    let requested = TEST_RUNTIME.block_on(client.request(
        Vec::new(),
        Topic::from("test-topic"),
        "key".to_owned(),
        Value::Null,
        Vec::new(),
        Duration::from_secs(1),
    ));
    assert!(matches!(requested, Err(RequestError::NoSubsystems)));
    Ok(())
}

/// Every erased reader constructor validates its subsystem before touching
/// storage. This keeps the four foreign-language APIs aligned at the shared
/// boundary instead of relying on wrapper-specific validation.
#[test]
fn erased_reader_kinds_share_subsystem_validation() -> Result<()> {
    let mut producer = ProducerConfiguration::builder();
    producer.bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()]);
    let mut consumer = ConsumerConfiguration::builder();
    consumer
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .group_id("erased-readers")
        .subscribed_topics(&["test-topic".to_owned()])
        .mock(true);
    let consumers = ConsumerBuilders {
        consumer,
        peer: PeerConfiguration::builder().build()?,
        ..ConsumerBuilders::new()?
    };
    let client = TEST_RUNTIME.block_on(new_erased::<NoOpHandler>(
        Mode::Pipeline,
        &mut producer,
        &consumers,
        &CassandraConfigurationBuilder::default(),
    ))?;

    TEST_RUNTIME.block_on(async {
        let value = client
            .value_state(
                " ".to_owned(),
                "value".to_owned(),
                ErasedReadCache::default(),
            )
            .await;
        let map = client
            .map_state(String::new(), "map".to_owned(), ErasedReadCache::default())
            .await;
        let deque = client
            .deque_state(
                "\t".to_owned(),
                "deque".to_owned(),
                ErasedReadCache::default(),
            )
            .await;

        assert!(matches!(
            value,
            Err(ErasedReaderBuildError::InvalidSubsystem(_))
        ));
        assert!(matches!(
            map,
            Err(ErasedReaderBuildError::InvalidSubsystem(_))
        ));
        assert!(matches!(
            deque,
            Err(ErasedReaderBuildError::InvalidSubsystem(_))
        ));
    });
    Ok(())
}

/// In the `Configured` state, `register` mints a capability handle.
#[test]
fn register_in_configured_state_binds() -> Result<()> {
    let client = create_test_client::<NoOpHandler>("register-configured", None)?;
    let registered = TEST_RUNTIME.block_on(client.register(value_state::<JsonCodec>("cart")));
    assert!(registered.is_ok(), "private registration must succeed");

    let published = TEST_RUNTIME
        .block_on(client.register(value_state::<JsonCodec>("published").published(true)));
    assert!(matches!(
        published,
        Err(HighLevelClientError::StateRegistration(
            RegisterStateError::PublishedWithoutSubsystem { .. }
        ))
    ));
    Ok(())
}

/// After `subscribe`, the registry is frozen: `register` returns
/// `AlreadySubscribed` rather than mutating a running consumer's collections.
#[test]
fn register_after_subscribe_is_rejected() -> Result<()> {
    let client = create_test_client::<NoOpHandler>("register-after-subscribe", None)?;
    TEST_RUNTIME.block_on(async {
        client.subscribe(NoOpHandler).await?;
        let late = client.register(value_state::<JsonCodec>("late")).await;
        // Shut the consumer down *before* asserting: a failed assertion must
        // not leave rdkafka client threads alive and hang the test binary.
        client.unsubscribe().await?;
        assert!(
            matches!(late, Err(HighLevelClientError::AlreadySubscribed)),
            "register after subscribe must be AlreadySubscribed, got {late:?}"
        );
        Result::<()>::Ok(())
    })
}

/// Registrations survive the re-subscribe cycle: `register → subscribe →
/// unsubscribe → subscribe` works without re-registering (the config is
/// cloned, never drained), and after `unsubscribe` more collections can be
/// registered before re-subscribing.
#[test]
fn registrations_survive_resubscribe_cycle() -> Result<()> {
    let client = create_test_client::<NoOpHandler>("resubscribe-cycle", None)?;
    TEST_RUNTIME.block_on(async {
        // Register, then run the full bidirectional cycle without
        // re-registering — a drained config would rebuild an empty registry,
        // but the intact config is moved back to `Configured` on unsubscribe.
        let _cart = client.register(value_state::<JsonCodec>("cart")).await?;
        client.subscribe(NoOpHandler).await?;
        client.unsubscribe().await?;
        client.subscribe(NoOpHandler).await?;
        client.unsubscribe().await?;

        // After unsubscribe the client is `Configured` again: a fresh
        // registration is accepted and a further subscribe succeeds.
        let _wishlist = client
            .register(value_state::<JsonCodec>("wishlist"))
            .await?;
        client.subscribe(NoOpHandler).await?;
        client.unsubscribe().await?;
        Result::<()>::Ok(())
    })
}

/// Builds a `SubsystemName`, converting its error into `eyre`.
fn subsystem(name: &str) -> Result<SubsystemName> {
    SubsystemName::try_new(name).map_err(|error| eyre!("subsystem name: {error}"))
}

/// Readers and subscriptions share one bundle across the client lifecycle.
#[test]
fn state_and_subscriptions_share_one_bundle() -> Result<()> {
    let client = create_test_client::<NoOpHandler>("share-one-bundle", None)?;
    TEST_RUNTIME.block_on(async {
        let before = client
            .state(subsystem("carts")?, value_state::<JsonCodec>("cart"))
            .await?;
        let expected = before.deps_instance_id();

        client.subscribe(NoOpHandler).await?;
        let running = client
            .state(subsystem("carts")?, value_state::<JsonCodec>("cart"))
            .await?;
        client.unsubscribe().await?;

        client.subscribe(NoOpHandler).await?;
        client.unsubscribe().await?;
        let after = client
            .state(subsystem("carts")?, value_state::<JsonCodec>("cart"))
            .await?;

        assert_eq!(running.deps_instance_id(), expected);
        assert_eq!(after.deps_instance_id(), expected);
        Result::<()>::Ok(())
    })
}

/// The consumer group under which the committed `cart` value is published.
const GROUP: &str = "group-aaa";

/// A reader built from the client observes committed state in the client's
/// single retained `StateReaderDependencies` bundle. That bundle holds the same
/// in-memory cell store the running consumer holds, so this proves
/// `client.state()` composes readers over the consumer's shared stores instead
/// of a separate set.
///
/// A faithful end-to-end version of this test would drive the write through a
/// produced Kafka record. The in-process mock cluster cannot do that: it
/// delivers no records and does not serve admin topic creation. Instead this
/// test seeds the committed write directly into the retained bundle's shared
/// stores, using the same owner `KeyedStateSession` path (finalize then
/// promote) as the reader-suite seeding helpers. It then reads the value back
/// through a reader the client composes.
///
/// Falsify by breaking the bundle memoization so `state()` builds a fresh
/// bundle every call: the reader would then read empty stores and the
/// assertion would fail.
#[test]
fn reader_sees_write_through_client_shared_bundle() -> Result<()> {
    let client = create_test_client::<NoOpHandler>("shared-bundle-ryow", None)?;
    TEST_RUNTIME.block_on(async {
        // Start the consumer so the retained bundle is the one it holds.
        client.subscribe(NoOpHandler).await?;

        // Run the scenario in a block that returns Result, so shutdown below
        // always runs, even if the scenario fails.
        let outcome: Result<()> = async {
            // The bundle the running consumer and the client's readers share.
            let Some(deps) = client.retained_deps() else {
                return Err(color_eyre::eyre::eyre!("reader dependencies are absent"));
            };
            let backend = deps.backend();
            let cells = backend.cells();
            let publications = backend.publications();
            let identities = backend.identities();

            // Seed a committed `cart` value plus its published routing row and
            // frozen identity into the shared stores — as the owning consumer
            // would on a committed write.
            let descriptor = value_state::<JsonCodec>("cart");
            let sub = subsystem("carts")?;
            let name = state_name("cart")?;
            let orders = topic("orders");
            let count = mock_count();
            let key = Key::from("user-1");
            let registry = registry_of(&descriptor, CollectionDef::new(None))?;
            let state_key = source_state_key(orders, GROUP, &key, count)?;
            publish_source(
                (publications, identities),
                &sub,
                &name,
                GROUP,
                orders,
                count,
                &descriptor,
            )
            .await;
            owner_commit(
                cells,
                &registry,
                &state_key,
                descriptor,
                1,
                |handle| async move {
                    handle
                        .set(json!(["apple"]))
                        .await
                        .map_err(|e| eyre!("set: {e}"))?;
                    Ok(())
                },
            )
            .await?;

            // A reader composed from the client reads the committed value from
            // the shared cells.
            let reader = client
                .state(sub.clone(), value_state::<JsonCodec>("cart"))
                .await?;
            let observed = reader.get("user-1").await?;
            ensure!(
                observed == Some(json!(["apple"])),
                "client reader must observe the committed write in the shared bundle, got \
                 {observed:?}"
            );
            Ok(())
        }
        .await;

        client.unsubscribe().await?;
        outcome
    })
}
