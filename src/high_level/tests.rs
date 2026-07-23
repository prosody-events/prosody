use super::*;
use crate::Key;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::storage::SharedStorage;
use crate::consumer::{ConsumerConfiguration, DemandType};
use crate::high_level::CassandraConfigurationBuilder;
use crate::high_level::mode::Mode;
use crate::producer::ProducerConfiguration;
use crate::state::descriptor::value_state;
use crate::state::registry::CollectionDef;
use crate::state_reader::tests::support::{
    mock_count, owner_commit, publish_source, registry_of, source_state_key, state_name, topic,
};
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::test_util::TEST_RUNTIME;
use crate::timers::Trigger;
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure, eyre};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use serde_json::{Value, json};
use std::convert::Infallible;
/// Owns the helper-produced mock cluster alongside the producer so the
/// cluster's Drop runs when the test ends (no `mem::forget` leaks).
struct ProducerFixture {
    producer: ProsodyProducer,
    _cluster: MockCluster<'static, DefaultProducerContext>,
}

/// Creates a `ProsodyProducer` connected to a mock cluster with specified
/// topics.
fn create_producer_with_topics(topics: &[&str]) -> Result<ProducerFixture> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    let bootstrap = cluster.bootstrap_servers();

    for topic in topics {
        cluster.create_topic(topic, 1, 1)?;
    }

    let config = ProducerConfiguration::builder()
        .bootstrap_servers(vec![bootstrap])
        .source_system("test")
        .build()?;

    let producer = ProsodyProducer::pipeline_producer(config, Telemetry::new().sender())?;
    Ok(ProducerFixture {
        producer,
        _cluster: cluster,
    })
}

#[test]
fn test_missing_topics_finds_missing() -> Result<()> {
    let fixture = create_producer_with_topics(&["existing-topic-1", "existing-topic-2"])?;

    let topics = vec![
        "existing-topic-1".into(),
        "missing-topic".into(),
        "existing-topic-2".into(),
        "another-missing".into(),
    ];

    let result = missing_topics(&fixture.producer, topics)?;

    assert_eq!(result.len(), 2);
    assert!(result.contains(&Topic::from("missing-topic")));
    assert!(result.contains(&Topic::from("another-missing")));
    Ok(())
}

#[test]
fn test_missing_topics_ignores_pattern_subscriptions() -> Result<()> {
    let fixture = create_producer_with_topics(&["real-topic"])?;

    let topics = vec![
        "real-topic".into(),
        "^pattern-topic.*".into(),
        "missing-topic".into(),
        "^another-pattern".into(),
    ];

    let result = missing_topics(&fixture.producer, topics)?;

    // Pattern topics (starting with ^) should be filtered out
    assert_eq!(result.len(), 1);
    assert!(result.contains(&Topic::from("missing-topic")));
    assert!(!result.contains(&Topic::from("^pattern-topic.*")));
    assert!(!result.contains(&Topic::from("^another-pattern")));
    Ok(())
}

#[test]
fn test_missing_topics_all_exist() -> Result<()> {
    let fixture = create_producer_with_topics(&["topic-1", "topic-2", "topic-3"])?;

    let topics = vec!["topic-1".into(), "topic-2".into(), "topic-3".into()];

    let result = missing_topics(&fixture.producer, topics)?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_handles_duplicates() -> Result<()> {
    let fixture = create_producer_with_topics(&["existing"])?;

    let topics = vec![
        "existing".into(),
        "missing".into(),
        "missing".into(),  // Duplicate
        "existing".into(), // Duplicate
    ];

    let result = missing_topics(&fixture.producer, topics)?;

    // Should deduplicate and return only unique missing topics
    assert_eq!(result.len(), 1);
    assert!(result.contains(&Topic::from("missing")));
    Ok(())
}

#[test]
fn test_missing_topics_empty_list() -> Result<()> {
    let fixture = create_producer_with_topics(&["some-topic"])?;

    let topics = vec![];

    let result = missing_topics(&fixture.producer, topics)?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_only_patterns() -> Result<()> {
    let fixture = create_producer_with_topics(&["real-topic"])?;

    let topics = vec!["^pattern1.*".into(), "^pattern2.*".into()];

    let result = missing_topics(&fixture.producer, topics)?;

    // All pattern topics should be filtered out
    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_edge_cases() -> Result<()> {
    let fixture = create_producer_with_topics(&["normal-topic"])?;

    let topics = vec![
        "normal-topic".into(),
        "^".into(),                   // Edge case: just ^
        "^a".into(),                  // Minimal pattern
        "missing^not-pattern".into(), // ^ not at start
        "".into(),                    // Empty string
    ];

    let result = missing_topics(&fixture.producer, topics)?;

    // Should filter out ^ and ^a (start with ^), but not missing^not-pattern
    // Empty string should be processed normally
    assert_eq!(result.len(), 2); // "missing^not-pattern" and ""
    assert!(result.contains(&Topic::from("missing^not-pattern")));
    assert!(result.contains(&Topic::from("")));
    assert!(!result.contains(&Topic::from("^")));
    assert!(!result.contains(&Topic::from("^a")));
    Ok(())
}

/// Owns the helper-produced mock cluster alongside the `HighLevelClient` so
/// the cluster's Drop runs at end of test (no `mem::forget` leaks).
struct ClientFixture<T> {
    client: HighLevelClient<T>,
    _cluster: MockCluster<'static, DefaultProducerContext>,
}

/// Builds a mock-mode pipeline `HighLevelClient<T>`, ready to `register`
/// (Configured) and `subscribe`/`unsubscribe`. Optionally overrides the
/// producer's source system (default: derived from `group_id`).
fn create_test_client<T>(group_id: &str, source_system: Option<&str>) -> Result<ClientFixture<T>> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    let bootstrap = cluster.bootstrap_servers();
    cluster.create_topic("test-topic", 1, 1)?;

    let mut producer_builder = ProducerConfiguration::builder();
    producer_builder
        .bootstrap_servers(vec![bootstrap.clone()])
        .mock(true);
    if let Some(source) = source_system {
        producer_builder.source_system(source);
    }

    let mut consumer_builder = ConsumerConfiguration::builder();
    consumer_builder
        .bootstrap_servers(vec![bootstrap])
        .group_id(group_id)
        .subscribed_topics(&["test-topic".to_owned()])
        .mock(true);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        ..Default::default()
    };
    let cassandra_builder = CassandraConfigurationBuilder::default();

    let client = HighLevelClient::new(
        Mode::Pipeline,
        &mut producer_builder,
        &consumer_builders,
        &cassandra_builder,
    )?;
    Ok(ClientFixture {
        client,
        _cluster: cluster,
    })
}

#[test]
fn test_source_system_defaults_to_consumer_group() -> Result<()> {
    let group_id = "my-test-group";

    // Create client WITHOUT specifying source_system
    let fixture = create_test_client::<()>(group_id, None)?;

    // Verify that source_system() returns the consumer group_id
    assert_eq!(fixture.client.source_system(), group_id);
    Ok(())
}

#[test]
fn test_source_system_explicit_value_preserved() -> Result<()> {
    let explicit_source = "my-explicit-source";
    let group_id = "my-test-group";

    // Create client WITH explicit source_system
    let fixture = create_test_client::<()>(group_id, Some(explicit_source))?;

    // Verify that source_system() returns the explicit value, NOT group_id
    assert_eq!(fixture.client.source_system(), explicit_source);
    assert_ne!(fixture.client.source_system(), group_id);
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

/// In the `Configured` state, `register` mints a capability handle.
#[test]
fn register_in_configured_state_binds() -> Result<()> {
    let fixture = create_test_client::<()>("register-configured", None)?;
    let registered =
        TEST_RUNTIME.block_on(fixture.client.register(value_state::<JsonCodec>("cart")));
    assert!(registered.is_ok(), "register must succeed while Configured");
    Ok(())
}

/// After `subscribe`, the registry is frozen: `register` returns
/// `AlreadySubscribed` rather than mutating a running consumer's collections.
#[test]
fn register_after_subscribe_is_rejected() -> Result<()> {
    let fixture = create_test_client::<NoOpHandler>("register-after-subscribe", None)?;
    TEST_RUNTIME.block_on(async {
        fixture.client.subscribe(NoOpHandler).await?;
        let late = fixture
            .client
            .register(value_state::<JsonCodec>("late"))
            .await;
        // Shut the consumer down *before* asserting: a failed assertion must
        // not leave rdkafka client threads alive and hang the test binary.
        fixture.client.unsubscribe().await?;
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
    let fixture = create_test_client::<NoOpHandler>("resubscribe-cycle", None)?;
    TEST_RUNTIME.block_on(async {
        // Register, then run the full bidirectional cycle without
        // re-registering — a drained config would rebuild an empty registry,
        // but the intact config is moved back to `Configured` on unsubscribe.
        let _cart = fixture
            .client
            .register(value_state::<JsonCodec>("cart"))
            .await?;
        fixture.client.subscribe(NoOpHandler).await?;
        fixture.client.unsubscribe().await?;
        fixture.client.subscribe(NoOpHandler).await?;
        fixture.client.unsubscribe().await?;

        // After unsubscribe the client is `Configured` again: a fresh
        // registration is accepted and a further subscribe succeeds.
        let _wishlist = fixture
            .client
            .register(value_state::<JsonCodec>("wishlist"))
            .await?;
        fixture.client.subscribe(NoOpHandler).await?;
        fixture.client.unsubscribe().await?;
        Result::<()>::Ok(())
    })
}

/// Builds a `SubsystemName`, surfacing the (non-`eyre`) name error.
fn subsystem(name: &str) -> Result<SubsystemName> {
    SubsystemName::try_new(name).map_err(|error| eyre!("subsystem name: {error}"))
}

/// `state()` before and after `subscribe` draw from ONE retained bundle: the
/// reader minted while `Configured` and the reader minted while `Running` carry
/// the same `SharedDeps` construction id, proving the client builds the bundle
/// once and reuses it across the state transition (and hands that same bundle
/// to the consumer it starts).
#[test]
fn state_before_and_after_subscribe_share_one_bundle() -> Result<()> {
    let fixture = create_test_client::<NoOpHandler>("share-one-bundle", None)?;
    TEST_RUNTIME.block_on(async {
        // Configured: builds and retains the bundle.
        let before = fixture
            .client
            .state(subsystem("carts")?, value_state::<JsonCodec>("cart"))
            .await?;
        let id_before = before.deps_instance_id();

        fixture.client.subscribe(NoOpHandler).await?;

        // Running: must reuse the retained bundle, not build a second one.
        let after = fixture
            .client
            .state(subsystem("carts")?, value_state::<JsonCodec>("cart"))
            .await;

        // Shut the consumer down before asserting: a failed assertion must not
        // leave rdkafka client threads alive and hang the test binary.
        fixture.client.unsubscribe().await?;
        let id_after = after?.deps_instance_id();
        assert_eq!(
            id_before, id_after,
            "state() before and after subscribe must share one bundle"
        );
        Result::<()>::Ok(())
    })
}

/// The owning group whose committed `cart` the reader admits.
const GROUP: &str = "group-aaa";

/// A reader minted from the client observes committed state present in the
/// client's ONE retained `SharedDeps` bundle — the same in-memory cell store
/// the running consumer holds — proving `client.state()` composes readers over
/// the consumer's shared stores rather than a second set.
///
/// A faithful end-to-end variant would drive the write through a produced Kafka
/// record; the in-process mock cluster delivers no records (nor serves admin
/// topic creation), so the committed write is seeded directly into the retained
/// bundle's shared stores through the real owner `KeyedStateSession`
/// (finalize then promote) — the reader suite's seeding — and read back through
/// a reader the client composes. Falsify by breaking the bundle memoization (a
/// fresh bundle per `state()`): the reader then reads empty stores and the
/// assert goes red.
#[test]
fn reader_sees_write_through_client_shared_bundle() -> Result<()> {
    let fixture = create_test_client::<NoOpHandler>("shared-bundle-ryow", None)?;
    TEST_RUNTIME.block_on(async {
        // Start the consumer so the retained bundle is the one it holds.
        fixture.client.subscribe(NoOpHandler).await?;

        // Collect the scenario into a Result so shutdown runs on every path.
        let outcome: Result<()> = async {
            // The bundle the running consumer and the client's readers share.
            let deps = fixture
                .client
                .retained_deps()
                .await
                .ok_or_else(|| eyre!("client retained no shared bundle after subscribe"))?;
            let SharedStorage::Memory {
                cells,
                publications,
                identities,
            } = deps.shared_storage()
            else {
                bail!("mock client must build a memory bundle");
            };

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
                (&publications, &identities),
                &sub,
                &name,
                GROUP,
                orders,
                count,
                &descriptor,
            )
            .await;
            owner_commit(
                &cells,
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
            let reader = fixture
                .client
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

        fixture.client.unsubscribe().await?;
        outcome
    })
}
