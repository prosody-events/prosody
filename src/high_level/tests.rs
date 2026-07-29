use super::*;
use crate::JsonCodec;
use crate::Key;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::{ConsumerConfiguration, DemandType};
use crate::high_level::mode::Mode;
use crate::producer::ProducerConfiguration;
use crate::state::descriptor::value_state;
use crate::state::registry::CollectionDef;
use crate::state_reader::ReaderBackend;
use crate::state_reader::tests::support::{
    mock_count, owner_commit, publish_source, registry_of, source_state_key, state_name, topic,
};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::timers::Trigger;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use serde_json::{Value, json};
use std::convert::Infallible;

struct ClientFixture<T> {
    client: HighLevelClient<T, JsonCodec, MemoryClientBackend<JsonCodec>>,
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
        ..ConsumerBuilders::new()?
    };
    let client = HighLevelClient::new(
        MemoryClientBackend::new(),
        Mode::Pipeline,
        &mut producer_builder,
        &consumer_builders,
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

/// `unsubscribe` drops the retained bundle so the next `subscribe` builds a
/// fresh one. The bundle's heartbeat registry tracks the running consumer's
/// poll-loop heartbeat, which stops beating at shutdown. Reusing the bundle
/// across a resubscribe would leave that dead heartbeat in `is_stalled` and
/// grow the registry with no way to remove it.
///
/// This test checks two things: the bundle is cleared on `unsubscribe`, and
/// the `SharedDeps` construction id changes across the resubscribe cycle. If
/// `unsubscribe` retained `self.deps` instead, the bundle would survive and
/// fail the first assertion. The construction id would then repeat and fail
/// the second.
#[test]
fn unsubscribe_rebuilds_bundle_on_resubscribe() -> Result<()> {
    let fixture = create_test_client::<NoOpHandler>("resubscribe-fresh-bundle", None)?;
    TEST_RUNTIME.block_on(async {
        // Configured: build and retain the first bundle.
        let first = fixture
            .client
            .state(subsystem("carts")?, value_state::<JsonCodec>("cart"))
            .await?;
        let id_first = first.deps_instance_id();

        fixture.client.subscribe(NoOpHandler).await?;
        fixture.client.unsubscribe().await?;

        // The retained bundle (holding the now-dead poll-loop heartbeat) is
        // gone, so its stale registry cannot be reused.
        assert!(
            fixture.client.retained_deps().await.is_none(),
            "unsubscribe must drop the retained bundle"
        );

        // A reader built after the cycle draws from the freshly built bundle.
        let second = fixture
            .client
            .state(subsystem("carts")?, value_state::<JsonCodec>("cart"))
            .await?;
        assert_ne!(
            id_first,
            second.deps_instance_id(),
            "resubscribe must rebuild the bundle, not reuse the stale one"
        );
        Result::<()>::Ok(())
    })
}

/// Builds a `SubsystemName`, converting its error into `eyre`.
fn subsystem(name: &str) -> Result<SubsystemName> {
    SubsystemName::try_new(name).map_err(|error| eyre!("subsystem name: {error}"))
}

/// `state()` builds one bundle and reuses it across `subscribe`. The reader
/// built while `Configured` and the reader built while `Running` carry the
/// same `SharedDeps` construction id, proving the client reuses the bundle
/// instead of building a second one. The consumer started by `subscribe`
/// receives that same bundle.
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

/// The consumer group under which the committed `cart` value is published.
const GROUP: &str = "group-aaa";

/// A reader built from the client observes committed state in the client's
/// single retained `SharedDeps` bundle. That bundle holds the same in-memory
/// cell store the running consumer holds, so this proves `client.state()`
/// composes readers over the consumer's shared stores instead of a separate
/// set.
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
    let fixture = create_test_client::<NoOpHandler>("shared-bundle-ryow", None)?;
    TEST_RUNTIME.block_on(async {
        // Start the consumer so the retained bundle is the one it holds.
        fixture.client.subscribe(NoOpHandler).await?;

        // Run the scenario in a block that returns Result, so shutdown below
        // always runs, even if the scenario fails.
        let outcome: Result<()> = async {
            // The bundle the running consumer and the client's readers share.
            let deps = fixture
                .client
                .retained_deps()
                .await
                .ok_or_else(|| eyre!("client retained no shared bundle after subscribe"))?;
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
