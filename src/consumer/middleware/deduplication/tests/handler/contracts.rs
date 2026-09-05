use super::*;
use std::future::ready;

#[tokio::test]
async fn dedup_skip_records_no_second_marker() -> color_eyre::Result<()> {
    let msg = create_test_message("key1", Some("evt1"))?;
    let id = dedup_uuid_for_message(test_identity(), &msg);

    let state_key = StateKey::new(Uuid::from_u128(0xDD), Arc::from("key1"));
    let (session, _cell_store, _dirty, recorded) = recording_session(
        CollectionDefRegistry::default(),
        state_key,
        EventRef::Message { dedup_id: id },
    );
    let context = MockEventContext::new().with_session(session);

    let inner = MockHandler::success();
    let handler = create_handler(inner.clone());
    handler.store.insert(id).await?;

    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let tracker = OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_secs(5), version);
    let uncommitted = tracker.take(0).await?;
    let message = msg.into_uncommitted(uncommitted);

    EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

    assert_eq!(inner.call_count(), 0, "the skip short-circuits the inner");
    assert!(
        recorded.lock().is_empty(),
        "a dedup skip must not record a second marker",
    );
    assert_eq!(
        tracker.shutdown().await,
        Some(0),
        "the skipped dispatch commits the offset",
    );
    Ok(())
}

#[test]
fn dedup_uuid_is_deterministic() -> color_eyre::Result<()> {
    let msg1 = create_test_message("key1", Some("evt1"))?;
    let msg2 = create_test_message("key1", Some("evt1"))?;
    assert_eq!(
        dedup_uuid_for_message(test_identity(), &msg1),
        dedup_uuid_for_message(test_identity(), &msg2),
    );
    Ok(())
}

#[test]
fn dedup_uuid_differs_by_dimension() -> color_eyre::Result<()> {
    let base_msg = create_test_message("key1", Some("evt1"))?;
    let base = dedup_uuid_for_message(test_identity(), &base_msg);

    let variants = [
        DedupIdentity {
            version: "2",
            ..test_identity()
        },
        DedupIdentity {
            group_id: "other-group",
            ..test_identity()
        },
        DedupIdentity {
            topic: "other-topic",
            ..test_identity()
        },
        DedupIdentity {
            partition: 1,
            ..test_identity()
        },
    ];
    for identity in variants {
        assert_ne!(base, dedup_uuid_for_message(identity, &base_msg));
    }

    // Different key
    let diff_key_msg = create_test_message("key2", Some("evt1"))?;
    assert_ne!(base, dedup_uuid_for_message(test_identity(), &diff_key_msg));

    // Different event_id
    let diff_evt_msg = create_test_message("key1", Some("evt2"))?;
    assert_ne!(base, dedup_uuid_for_message(test_identity(), &diff_evt_msg));

    // Offset fallback (no event_id) differs from event_id path
    let offset_msg = create_test_message("key1", None)?;
    assert_ne!(base, dedup_uuid_for_message(test_identity(), &offset_msg));

    Ok(())
}

#[test]
fn ttl_exceeding_max_rejected() {
    let config = DeduplicationConfiguration {
        version: "1".to_owned(),
        cache_capacity: NonZeroUsize::MIN,
        ttl: Duration::from_secs(700_000_000),
    };
    let result = DeduplicationMiddleware::<_, serde_json::Value>::new(
        &config,
        "group",
        MemoryDeduplicationStoreProvider::new(),
    );
    assert!(result.is_err());
}

#[test]
fn ttl_below_minimum_rejected() {
    let config = DeduplicationConfiguration {
        version: "1".to_owned(),
        cache_capacity: NonZeroUsize::MIN,
        ttl: Duration::from_secs(30),
    };
    let result = DeduplicationMiddleware::<_, serde_json::Value>::new(
        &config,
        "group",
        MemoryDeduplicationStoreProvider::new(),
    );
    assert!(result.is_err());
}

/// Record each handler call and apply hook.
#[derive(Clone, Default)]
struct ApplyProbe {
    log: Arc<parking_lot::Mutex<Vec<ApplyEvent>>>,
    error: Option<TestError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ApplyEvent {
    Handler,
    InnerAfterCommit,
    InnerAfterAbort,
}

impl FallibleHandler for ApplyProbe {
    type Error = TestError;
    type Output = ();
    type Payload = serde_json::Value;

    fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(ApplyEvent::Handler);
        ready(match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        })
    }

    fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(ApplyEvent::Handler);
        ready(match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        })
    }

    fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(ApplyEvent::Handler);
        ready(match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        })
    }

    async fn after_commit<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(ApplyEvent::InnerAfterCommit);
    }

    async fn after_abort<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(ApplyEvent::InnerAfterAbort);
    }

    async fn shutdown(self) {}
}

/// Both hooks forward results only when the inner handler ran.
#[tokio::test]
async fn apply_hooks_follow_inner_calls() -> color_eyre::Result<()> {
    for presence in [
        Ok(Presence::Absent),
        Ok(Presence::Settled),
        Ok(Presence::Inherited),
        Err(TestError::Transient),
    ] {
        for error in [None, Some(TestError::Permanent)] {
            for commit in [false, true] {
                for timer in [false, true] {
                    let inner = ApplyProbe {
                        error: error.clone(),
                        ..ApplyProbe::default()
                    };
                    let log = inner.log.clone();
                    let handler = DeduplicationHandler {
                        inner,
                        store: LookupStore(presence.clone()),
                    };
                    let message = create_test_message("key1", Some("evt1"))?;
                    let context =
                        session_context(dedup_uuid_for_message(test_identity(), &message));
                    let result = if timer {
                        let trigger = Trigger::for_testing(
                            "key1".into(),
                            CompactDateTime::from(1000_u32),
                            TimerType::default(),
                        );
                        FallibleHandler::on_timer(
                            &handler,
                            context.clone(),
                            trigger,
                            DemandType::Normal,
                        )
                        .await
                    } else {
                        FallibleHandler::on_message(
                            &handler,
                            context.clone(),
                            message,
                            DemandType::Normal,
                        )
                        .await
                    };
                    if commit {
                        FallibleHandler::after_commit(&handler, context, result).await;
                    } else {
                        FallibleHandler::after_abort(&handler, context, result).await;
                    }
                    let ran = timer || matches!(presence, Ok(Presence::Absent));
                    let hook = if commit {
                        ApplyEvent::InnerAfterCommit
                    } else {
                        ApplyEvent::InnerAfterAbort
                    };
                    let expected = [ApplyEvent::Handler, hook];
                    assert_eq!(log.lock().as_slice(), if ran { &expected[..] } else { &[] });
                }
            }
        }
    }
    Ok(())
}

/// Marker writers and readers must derive the same ID.
/// Both event IDs and offset fallbacks must agree.
#[test]
fn dedup_id_writer_matches_canonical_reader_derivation() -> color_eyre::Result<()> {
    const VERSION: &str = "1";
    const GROUP: &str = "test-group";
    const TOPIC: &str = "test-topic";
    const PARTITION: i32 = 3;

    let identity = DedupIdentity {
        version: VERSION,
        group_id: GROUP,
        topic: TOPIC,
        partition: PARTITION,
    };

    for event_id in [Some("evt-1"), None] {
        let msg = create_test_message("key-a", event_id)?;

        let writer_id = dedup_uuid_for_message(identity, &msg);
        let reader_id = dedup_uuid(
            VERSION,
            GROUP,
            TOPIC,
            PARTITION,
            msg.key().as_bytes(),
            msg.payload()
                .get("id")
                .and_then(|v| v.as_str())
                .map(str::as_bytes),
            msg.offset(),
        );
        assert_eq!(
            writer_id, reader_id,
            "writer and canonical reader derivations must agree (event_id = {event_id:?})"
        );

        // Regression guard: the original buggy reader hardcoded an empty
        // version and a `None` event_id. For a message carrying an event_id
        // that takes the wrong hash branch *and* the wrong version, so the
        // ids must differ — proving the test would have failed on the bug.
        let buggy_id = dedup_uuid(
            "",
            GROUP,
            TOPIC,
            PARTITION,
            msg.key().as_bytes(),
            None,
            msg.offset(),
        );
        if event_id.is_some() {
            assert_ne!(
                writer_id, buggy_id,
                "the buggy reader derivation must not collide with the writer id"
            );
        }
    }
    Ok(())
}
