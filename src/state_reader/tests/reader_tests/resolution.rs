//! Message reference resolution through the reader loader.

use super::*;

/// The owner writes a `MessageRef`, the Kafka coordinates of the message it
/// has in hand. The standalone reader reads the committed ref and resolves
/// it to the full message body through `MemoryLoader`. This is the same read
/// path as a plain Value; only the loader operation differs.
///
/// The owner and reader share the same loader and collection identity (codec
/// `message-ref`, value kind, unit key).
///
/// Falsify: never seed the loader body. The resolve then returns a loader
/// error instead of the message, and the assertion goes red.
#[tokio::test]
async fn kafka_ref_reader_resolves_through_loader() -> Result<()> {
    use crate::consumer::message::ConsumerMessage;
    use crate::consumer::message_state;
    use crate::loader::MemoryLoader;
    use crate::state_reader::deps::StateReaderDependencies;
    use std::sync::Arc;

    let harness = MemoryHarness::new();
    let owner_descriptor = message_state::<MemoryLoader<Value>>("mref");
    let reader_descriptor = message_state::<MemoryLoader<Value>>("mref");
    let name = state_name("mref")?;
    let sub = subsystem()?;
    let tp = topic("orders");
    let count = mock_count();
    let key = Key::from("user-42");
    let state_key = source_state_key(tp, GROUP_A, &key, count)?;
    let registry = registry_of(&owner_descriptor, CollectionDef::new(None))?;

    // The Kafka coordinates the ref points at, independent of the state segment.
    let msg_topic = topic("orders.v1");
    let msg_partition = 3_i32;
    let msg_offset = 42_i64;
    let payload = Value::from(7i64);
    let message = ConsumerMessage::for_testing(
        msg_topic,
        msg_partition,
        msg_offset,
        Arc::from("user-42"),
        payload.clone(),
    )?;

    let to_write = message.clone();
    owner_commit(
        &harness.cells,
        &registry,
        &state_key,
        owner_descriptor,
        1,
        |handle| async move { handle.set(&to_write).await.map_err(|e| eyre!("set: {e}")) },
    )
    .await?;
    publish_source(
        (&harness.publications, &harness.identities),
        &sub,
        &name,
        GROUP_A,
        tp,
        count,
        &reader_descriptor,
    )
    .await;

    // Seed the reader's loader with the body at those coordinates.
    let loader = MemoryLoader::<Value>::new();
    loader.store_message(
        msg_topic,
        msg_partition,
        msg_offset,
        Arc::from("user-42"),
        payload.clone(),
    );
    let deps = StateReaderDependencies::<JsonCodec>::memory(
        "reader-test".to_owned(),
        Duration::from_secs(30),
        harness.cells.clone(),
        harness.publications.clone(),
        harness.identities.clone(),
        loader,
        NonZeroU64::MAX,
    );
    let reader = StateReader::new(&deps, sub, reader_descriptor)?;

    let resolved = reader
        .get(key)
        .await?
        .ok_or_else(|| eyre!("expected a resolved message"))?;
    assert_eq!(resolved.topic(), msg_topic);
    assert_eq!(resolved.partition(), msg_partition);
    assert_eq!(resolved.offset(), msg_offset);
    assert_eq!(resolved.payload(), &payload);
    Ok(())
}
