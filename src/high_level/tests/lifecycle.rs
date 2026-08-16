use super::*;

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
    let retained = client.clone();
    TEST_RUNTIME.block_on(client.shutdown())?;
    let after_shutdown = TEST_RUNTIME.block_on(retained.subscribe(NoOpHandler));
    assert!(matches!(after_shutdown, Err(HighLevelClientError::Closed)));
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
    TEST_RUNTIME.block_on(client.shutdown())?;
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
pub(super) fn subsystem(name: &str) -> Result<SubsystemName> {
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

/// An erased reader uses the payload's state codec and the client's retained
/// reader dependencies. The binary state codec preserves bytes without adding
/// message metadata.
///
/// A faithful end-to-end version of this test would drive the write through a
/// produced Kafka record. The in-process mock cluster cannot do that: it
/// delivers no records and does not serve admin topic creation. Instead this
/// test seeds the committed write directly into the retained bundle's shared
/// stores, using the same owner `KeyedStateSession` path (finalize then
/// promote) as the reader-suite seeding helpers. It then reads the value back
/// through a reader the client composes.
///
/// Falsify by selecting the message codec in `erased::value`: the observed
/// payload gets an event ID and the assertion fails.
#[test]
fn erased_reader_uses_payload_state_codec() -> Result<()> {
    let client = create_test_client::<BinaryHandler>("binary-state-reader", None)?;
    TEST_RUNTIME.block_on(async {
        // Start the consumer so the retained bundle is the one it holds.
        client.subscribe(BinaryHandler).await?;

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
            let descriptor = value_state::<JsonBinaryCodec>("cart");
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
                        .set(BinaryPayload::new(
                            br#"{"id":"state-cell","items":["apple"]}"#.to_vec(),
                            None::<String>,
                            None::<String>,
                        ))
                        .await
                        .map_err(|e| eyre!("set: {e}"))?;
                    Ok(())
                },
            )
            .await?;

            // A reader composed from the client reads the committed value from
            // the shared cells.
            let reader =
                erased::value(&client, sub.to_string(), "cart", ErasedReadCache::Disabled).await?;
            let observed = reader.get("user-1".to_owned()).await?;
            let Some(observed) = observed else {
                return Err(eyre!("erased reader observed no committed value"));
            };
            ensure!(
                observed.event_id().is_none(),
                "state decode added message metadata"
            );
            ensure!(
                observed.bytes == br#"{"id":"state-cell","items":["apple"]}"#,
                "state decode changed the stored bytes"
            );
            Ok(())
        }
        .await;

        client.shutdown().await?;
        outcome
    })
}
