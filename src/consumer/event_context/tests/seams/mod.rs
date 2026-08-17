use super::*;

// --- Kafka message seam -----------------------------------------------------

/// The erased Kafka-message value ops mirror the typed `MessageDescriptor`
/// path: `message_value_state(..).set(message)` records the message in hand and
/// `.get()` resolves it back to the full [`ConsumerMessage`] through the
/// loader.
#[tokio::test]
async fn erased_kafka_record_then_get_matches_typed() -> Result<()> {
    let topic = Topic::from("orders.v1");
    let (partition, offset) = (3_i32, 42_i64);
    let key: Key = Arc::from("user-1");
    let payload = json!({ "order": 7_i32 });

    let loader = MemoryLoader::<Value>::new();
    loader.store_message(topic, partition, offset, key.clone(), payload.clone());

    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &message_state::<MemoryLoader<Value>>("last_seen"),
        CollectionDef::new(None),
    )?;
    let session = test_session(loader, registry);
    let ctx = MockEventContext::<Value>::new().with_session(session);

    let message = ConsumerMessage::for_testing(topic, partition, offset, key, payload.clone())?;

    ctx.message_value_state("last_seen")
        .map_err(|e| eyre!("vend message value: {e}"))?
        .set(message)
        .await
        .map_err(|e| eyre!("erased record: {e}"))?;
    let erased = ctx
        .message_value_state("last_seen")
        .map_err(|e| eyre!("vend message value: {e}"))?
        .get()
        .await
        .map_err(|e| eyre!("erased get: {e}"))?
        .ok_or_else(|| eyre!("erased get resolved nothing"))?;
    assert_eq!(erased.topic(), topic);
    assert_eq!(erased.partition(), partition);
    assert_eq!(erased.offset(), offset);
    assert_eq!(erased.record().message(), Some(&payload));

    let typed = ctx
        .state(Registered::new(message_state("last_seen")))
        .map_err(|e| eyre!("typed kafka bind: {e}"))?
        .get()
        .await
        .map_err(|e| eyre!("typed kafka get: {e}"))?
        .ok_or_else(|| eyre!("typed get resolved nothing"))?;
    assert_eq!(typed.offset(), erased.offset());
    assert_eq!(typed.record().message(), erased.record().message());
    Ok(())
}

/// The erased Kafka-message *map* seam drives a distinct impl from the value
/// seam — the borrowed-write `ErasedWrite for MessageCell` lowering a
/// `handle.set(key, &message)` — so it is pinned separately. `set(key,
/// message)` records the message by key; `.get(key)` resolves it back through
/// the loader, matching the typed `message_map_state` path.
#[tokio::test]
async fn erased_kafka_map_set_then_get_matches_typed() -> Result<()> {
    let topic = Topic::from("orders.v1");
    let (partition, offset) = (3_i32, 42_i64);
    let key: Key = Arc::from("user-1");
    let payload = json!({ "order": 7_i32 });

    let loader = MemoryLoader::<Value>::new();
    loader.store_message(topic, partition, offset, key.clone(), payload.clone());

    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &message_map_state::<Utf8KeyCodec, MemoryLoader<Value>>("seen_by_key"),
        CollectionDef::new(None),
    )?;
    let session = test_session(loader, registry);
    let ctx = MockEventContext::<Value>::new().with_session(session);

    let message = ConsumerMessage::for_testing(topic, partition, offset, key, payload.clone())?;

    ctx.message_map_state("seen_by_key")
        .map_err(|e| eyre!("vend message map: {e}"))?
        .set("k".to_owned(), message)
        .await
        .map_err(|e| eyre!("erased map set: {e}"))?;
    let erased = ctx
        .message_map_state("seen_by_key")
        .map_err(|e| eyre!("vend message map: {e}"))?
        .get("k".to_owned())
        .await
        .map_err(|e| eyre!("erased map get: {e}"))?
        .ok_or_else(|| eyre!("erased map get resolved nothing"))?;
    assert_eq!(erased.offset(), offset);
    assert_eq!(erased.record().message(), Some(&payload));

    let typed = ctx
        .state(Registered::new(message_map_state::<
            Utf8KeyCodec,
            MemoryLoader<Value>,
        >("seen_by_key")))
        .map_err(|e| eyre!("typed map bind: {e}"))?
        .get(&"k".to_owned())
        .await
        .map_err(|e| eyre!("typed map get: {e}"))?
        .ok_or_else(|| eyre!("typed map get resolved nothing"))?;
    assert_eq!(typed.offset(), erased.offset());
    assert_eq!(typed.record().message(), erased.record().message());
    Ok(())
}

/// The erased Kafka-message *deque* seam drives the same borrowed-write impl
/// via `handle.push_back(&message)`, distinct from the value seam. `push_back`
/// appends the message; `.get(0)` resolves it back through the loader, matching
/// the typed `message_deque_state` path.
#[tokio::test]
async fn erased_kafka_deque_push_then_get_matches_typed() -> Result<()> {
    let topic = Topic::from("orders.v1");
    let (partition, offset) = (3_i32, 42_i64);
    let key: Key = Arc::from("user-1");
    let payload = json!({ "order": 7_i32 });

    let loader = MemoryLoader::<Value>::new();
    loader.store_message(topic, partition, offset, key.clone(), payload.clone());

    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &message_deque_state::<MemoryLoader<Value>>("seen_log"),
        CollectionDef::new(None),
    )?;
    let session = test_session(loader, registry);
    let ctx = MockEventContext::<Value>::new().with_session(session);

    let message = ConsumerMessage::for_testing(topic, partition, offset, key, payload.clone())?;

    ctx.message_deque_state("seen_log")
        .map_err(|e| eyre!("vend message deque: {e}"))?
        .push_back(message)
        .await
        .map_err(|e| eyre!("erased push_back: {e}"))?;
    let erased = ctx
        .message_deque_state("seen_log")
        .map_err(|e| eyre!("vend message deque: {e}"))?
        .get(0)
        .await
        .map_err(|e| eyre!("erased deque get: {e}"))?
        .ok_or_else(|| eyre!("erased deque get resolved nothing"))?;
    assert_eq!(erased.offset(), offset);
    assert_eq!(erased.record().message(), Some(&payload));

    let typed = ctx
        .state(Registered::new(message_deque_state::<MemoryLoader<Value>>(
            "seen_log",
        )))
        .map_err(|e| eyre!("typed deque bind: {e}"))?
        .get(0)
        .await
        .map_err(|e| eyre!("typed deque get: {e}"))?
        .ok_or_else(|| eyre!("typed deque get resolved nothing"))?;
    assert_eq!(typed.offset(), erased.offset());
    assert_eq!(typed.record().message(), erased.record().message());
    Ok(())
}

// --- Object safety / cloneability -------------------------------------------

/// The erased seam is the FFI deliverable, so `Box<dyn DynEventContext<Payload
/// = P>>` must construct (object safety), be callable, and clone into an alias
/// that shares the same per-event session.
#[tokio::test]
async fn dyn_event_context_state_is_object_safe_and_cloneable() -> Result<()> {
    let erased: Box<dyn DynEventContext<Payload = Value>> = Box::new(parity_context::<Value>()?);
    let alias = erased.clone();
    erased
        .value_state(VALUE_NAME)
        .map_err(|e| eyre!("vend through trait object: {e}"))?
        .set(json!({ "x": 1_i32 }))
        .await
        .map_err(|e| eyre!("set through trait object: {e}"))?;
    let observed = alias
        .value_state(VALUE_NAME)
        .map_err(|e| eyre!("vend through cloned trait object: {e}"))?
        .get()
        .await
        .map_err(|e| eyre!("get through cloned trait object: {e}"))?;
    assert_eq!(observed, Some(json!({ "x": 1_i32 })));
    Ok(())
}

// --- Registration classification --------------------------------------------

/// An unregistered name fails vending with a Permanent classification — a wrong
/// collection name is business logic, never retried. The compile-time
/// capability handle cannot express this (the erased seam mints its own token
/// by name), so the access-time check is the backstop.
#[tokio::test]
async fn erased_unregistered_name_is_permanent() -> Result<()> {
    let ctx = parity_context::<Value>()?;
    let Err(error) = ctx.value_state("never-registered") else {
        return Err(eyre!("an unregistered name must fail vending"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

// --- Null-write rejection ---------------------------------------------------

/// The JSON-null absent sentinel is rejected on every value-family `set`/`push`
/// with a Permanent error, and the store is left untouched — for both payload
/// erasures. `clear`/`remove` express deletion instead.
async fn assert_null_rejected<P>() -> Result<()>
where
    P: ParityPayload + Send + Sync + 'static,
{
    let ctx = parity_context::<P>()?;

    // Seed a prior value so "store untouched" is observable as the survivor.
    let seed = {
        let mut g = Gen::new(8);
        P::arbitrary_value(&mut g)
    };

    let value = ctx
        .value_state(VALUE_NAME)
        .map_err(|e| eyre!("vend value: {e}"))?;
    value
        .set(seed.clone())
        .await
        .map_err(|e| eyre!("seed set: {e}"))?;
    let Err(error) = value.set(P::null_value()).await else {
        return Err(eyre!("null value set must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    let after = value.get().await.map_err(|e| eyre!("value get: {e}"))?;
    if !opt_same::<P>(after.as_ref(), Some(&seed)) {
        return Err(eyre!("a rejected null set must leave the cell untouched"));
    }

    let map = ctx
        .map_state(MAP_NAME)
        .map_err(|e| eyre!("vend map: {e}"))?;
    let Err(error) = map.set("k".to_owned(), P::null_value()).await else {
        return Err(eyre!("null map set must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    assert!(
        map.get("k".to_owned())
            .await
            .map_err(|e| eyre!("map get: {e}"))?
            .is_none(),
        "a rejected null map set must not insert the key"
    );

    let deque = ctx
        .deque_state(DEQUE_NAME)
        .map_err(|e| eyre!("vend deque: {e}"))?;
    let Err(error) = deque.push_back(P::null_value()).await else {
        return Err(eyre!("null push_back must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    let Err(error) = deque.push_front(P::null_value()).await else {
        return Err(eyre!("null push_front must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    assert_eq!(
        deque.len().await.map_err(|e| eyre!("deque len: {e}"))?,
        0,
        "a rejected null push must not extend the deque"
    );
    Ok(())
}

/// Null-write rejection for `serde_json::Value` (`Value::Null`).
#[tokio::test]
async fn erased_null_write_rejected_json() -> Result<()> {
    assert_null_rejected::<Value>().await
}

/// Null-write rejection for `BinaryPayload` (the literal `null` document).
#[tokio::test]
async fn erased_null_write_rejected_binary() -> Result<()> {
    assert_null_rejected::<BinaryPayload>().await
}

/// The C# byte path also rejects a whitespace-padded `null` document.
#[tokio::test]
async fn erased_null_write_rejected_binary_padded() -> Result<()> {
    let ctx = parity_context::<BinaryPayload>()?;
    let padded = BinaryPayload::new(b"  null\n".to_vec(), None::<String>, None::<String>);
    let Err(error) = ctx
        .value_state(VALUE_NAME)
        .map_err(|e| eyre!("vend value: {e}"))?
        .set(padded)
        .await
    else {
        return Err(eyre!("a padded null document must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    Ok(())
}

// --- Never-Terminal fold ----------------------------------------------------

mod state_contracts;
