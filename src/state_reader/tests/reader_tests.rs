//! End-to-end reads through the public [`StateReader`].
//!
//! The committed round-trip for Value / Map / Deque is the memory instantiation
//! of the backend-generic [`reader_suite`](super::reader_suite) runner
//! (`prop_reader_*_committed`); the focused examples below pin invariants too
//! narrow to generalize: the commit→promote window (unprovable post-promotion),
//! Kafka-ref resolution through the loader, identity validation, and the
//! boundary errors.
//!
//! Committed state is seeded through the real owner session; the reader reads
//! it back over the oracle-free carriers under the segment the owner wrote.

use super::reader_suite::{
    ReaderCase, ValueOp, run_reader_deque_trace, run_reader_map_trace, run_reader_value_trace,
};
use super::support::{
    GROUP_A, MemoryHarness, MemoryReaderBackend, mock_count, owner_commit, owner_stage,
    publish_source, registry_of, source_state_key, state_name, subsystem, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::state::descriptor::{DescriptorIdentity, deque_state, map_state, value_state};
use crate::state::descriptor_identity::DurableDescriptorIdentity;
use crate::state::order_codec::I64KeyCodec;
use crate::state::publication::PublicationStore;
use crate::state::registry::CollectionDef;
use crate::state::tests::collection_suite::{DequeOp, MapOp, Trace};
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::eyre::{Result, bail, eyre};
use futures::executor::block_on;
use serde_json::Value;
use std::time::Duration;

/// The reader observes exactly the committed Value the owner wrote, over an
/// arbitrary overwrite trace — the memory instantiation of
/// [`run_reader_value_trace`]. `QUICKCHECK_TESTS` sets the iteration count.
#[test]
fn prop_reader_value_committed() {
    fn property(trace: Trace<ValueOp>) -> Result<bool> {
        let descriptor = value_state::<JsonCodec>("reader-value");
        let backend = MemoryReaderBackend::new(&descriptor, CollectionDef::new(None))?;
        let sub = subsystem()?;
        let key = Key::from("user-1");
        let case = ReaderCase {
            sub: &sub,
            group: GROUP_A,
            topic: topic("orders"),
            key: &key,
            count: mock_count(),
        };
        block_on(run_reader_value_trace(&backend, descriptor, &case, trace))
    }
    quickcheck::QuickCheck::new().quickcheck(property as fn(Trace<ValueOp>) -> Result<bool>);
}

/// The reader's point `get`, `get_many`, and ordered `stream` equal a
/// `BTreeMap` model of the committed live entries after every event, over an
/// arbitrary set/remove/clear trace — the memory instantiation of
/// [`run_reader_map_trace`].
#[test]
fn prop_reader_map_committed() {
    fn property(trace: Trace<MapOp>) -> Result<bool> {
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("reader-map");
        let backend = MemoryReaderBackend::new(&descriptor, CollectionDef::new(None))?;
        let sub = subsystem()?;
        let key = Key::from("user-1");
        let case = ReaderCase {
            sub: &sub,
            group: GROUP_A,
            topic: topic("orders"),
            key: &key,
            count: mock_count(),
        };
        block_on(run_reader_map_trace(&backend, descriptor, &case, trace))
    }
    quickcheck::QuickCheck::new().quickcheck(property as fn(Trace<MapOp>) -> Result<bool>);
}

/// The reader's `len`, front-relative `get`, and ordered `stream` equal a
/// `VecDeque` model of the committed elements after every event, over an
/// arbitrary push/pop/clear trace — the memory instantiation of
/// [`run_reader_deque_trace`].
#[test]
fn prop_reader_deque_committed() {
    fn property(trace: Trace<DequeOp>) -> Result<bool> {
        let descriptor = deque_state::<JsonCodec>("reader-deque");
        let backend = MemoryReaderBackend::new(&descriptor, CollectionDef::new(None))?;
        let sub = subsystem()?;
        let key = Key::from("user-1");
        let case = ReaderCase {
            sub: &sub,
            group: GROUP_A,
            topic: topic("orders"),
            key: &key,
            count: mock_count(),
        };
        block_on(run_reader_deque_trace(&backend, descriptor, &case, trace))
    }
    quickcheck::QuickCheck::new().quickcheck(property as fn(Trace<DequeOp>) -> Result<bool>);
}

/// In the commit→promote window (owner staged a provisional cell but has not
/// promoted), the reader observes the committed `prev`, never the in-flight
/// provisional value.
///
/// Falsify: point the reader read at a resolving store `get` — it returns the
/// in-flight `2` and the assert goes red.
#[tokio::test]
async fn reader_reads_prev_in_commit_window() -> Result<()> {
    let harness = MemoryHarness::new();
    let descriptor = value_state::<JsonCodec>("v-window");
    let name = state_name("v-window")?;
    let sub = subsystem()?;
    let tp = topic("orders");
    let count = mock_count();
    let key = Key::from("user-7");
    let state_key = source_state_key(tp, GROUP_A, &key, count)?;
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    // Event 1: commit prev = 1.
    owner_commit(
        &harness.cells,
        &registry,
        &state_key,
        descriptor,
        1,
        |handle| async move {
            handle
                .set(Value::from(1i64))
                .await
                .map_err(|e| eyre!("set: {e}"))?;
            Ok(())
        },
    )
    .await?;
    // Event 2: stage data = 2 but do NOT promote — the window.
    owner_stage(
        &harness.cells,
        &registry,
        &state_key,
        descriptor,
        2,
        |handle| async move {
            handle
                .set(Value::from(2i64))
                .await
                .map_err(|e| eyre!("set: {e}"))?;
            Ok(())
        },
    )
    .await?;

    publish_source(
        (&harness.publications, &harness.identities),
        &sub,
        &name,
        GROUP_A,
        tp,
        count,
        &descriptor,
    )
    .await;

    let deps = harness.deps(1 << 20);
    let reader = StateReader::new(&deps, sub, descriptor)?;
    assert_eq!(
        reader.get(key).await?,
        Some(Value::from(1i64)),
        "reader sees committed prev, not the in-flight provisional"
    );
    Ok(())
}

/// A Kafka-message-ref Value: the owner writes a `MessageRef` (the Kafka
/// coordinates of the message in hand), and the standalone reader reads the
/// committed ref and resolves it to the full message body through
/// `ReaderLoader::Memory` — the same read path as a plain Value, only the
/// loader arm differs. Owner and reader carry the same collection under
/// different loader types (`MemoryLoader` vs `ReaderLoader`), which share one
/// structural identity (codec `message-ref`, value kind, unit key).
///
/// Falsify: never seed the loader body — the resolve returns a loader error
/// instead of the message and the assert reds.
#[tokio::test]
async fn kafka_ref_reader_resolves_through_loader() -> Result<()> {
    use crate::consumer::message::ConsumerMessage;
    use crate::consumer::message_state;
    use crate::loader::MemoryLoader;
    use crate::state_reader::deps::SharedDeps;
    use crate::state_reader::loader::ReaderLoader;
    use std::sync::Arc;

    let harness = MemoryHarness::new();
    let owner_descriptor = message_state::<MemoryLoader<Value>>("mref");
    let reader_descriptor = message_state::<ReaderLoader<JsonCodec>>("mref");
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
    let deps = SharedDeps::<JsonCodec>::memory(
        "reader-test".to_owned(),
        Duration::from_secs(30),
        harness.cells.clone(),
        harness.publications.clone(),
        harness.identities.clone(),
        loader,
        1 << 20,
    );
    let reader = StateReader::new(&deps, sub, reader_descriptor)?;

    let resolved = reader
        .get(key)
        .await?
        .ok_or_else(|| eyre!("expected a resolved message"))?;
    assert_eq!(resolved.topic(), msg_topic);
    assert_eq!(resolved.partition(), msg_partition);
    assert_eq!(resolved.offset(), msg_offset);
    assert_eq!(*resolved.payload(), payload);
    Ok(())
}

/// A source whose frozen identity differs from the reader's descriptor in any
/// one field fails `IdentityMismatch` (Permanent); an unperturbed identity
/// acquires; a raw unknown `kind` discriminant compares unequal (never a decode
/// failure).
///
/// Falsify: widen `descriptor_identity::validate` to compare only `format_id`
/// — the kind-perturbed arm then acquires instead of failing.
#[tokio::test]
async fn identity_mismatch_is_permanent() -> Result<()> {
    use crate::state::descriptor_identity::DescriptorIdentityStore;
    use crate::state::publication::StatePublication;
    use std::sync::Arc;

    let descriptor = value_state::<JsonCodec>("v-ident");
    let name = state_name("v-ident")?;
    let sub = subsystem()?;
    let tp = topic("orders");
    let count = mock_count();

    let base = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        name.as_str(),
        &descriptor.structural_identity(),
    );

    let mut kind_perturbed = base.clone();
    kind_perturbed.kind = base.kind.wrapping_add(1);
    let mut fmt_perturbed = base.clone();
    fmt_perturbed.format_id = format!("{}-x", base.format_id);
    let mut key_fmt_perturbed = base.clone();
    key_fmt_perturbed.key_format_id = format!("{}-x", base.key_format_id);
    // A raw discriminant no CollectionKindId uses — compares unequal, never a
    // decode failure.
    let mut unknown_kind = base.clone();
    unknown_kind.kind = 127;

    for perturbed in [
        kind_perturbed,
        fmt_perturbed,
        key_fmt_perturbed,
        unknown_kind,
    ] {
        let harness = MemoryHarness::new();
        harness
            .publications
            .upsert(
                &sub,
                &name,
                &StatePublication {
                    group_id: Arc::from(GROUP_A),
                    topic: tp,
                    partition_count: count,
                },
            )
            .await
            .unwrap_or_else(|e| match e {});
        harness
            .identities
            .register_identity(GROUP_A, &perturbed)
            .await
            .unwrap_or_else(|e| match e {});

        let deps = harness.deps(1 << 20);
        let reader = StateReader::new(&deps, sub.clone(), descriptor)?;
        match reader.get(Key::from("user-1")).await {
            Err(StateReaderError::IdentityMismatch { .. }) => {}
            other => bail!("expected IdentityMismatch, got {other:?}"),
        }
    }

    // The unperturbed identity acquires (no rows written, so the read returns
    // None, not an identity error).
    let harness = MemoryHarness::new();
    publish_source(
        (&harness.publications, &harness.identities),
        &sub,
        &name,
        GROUP_A,
        tp,
        count,
        &descriptor,
    )
    .await;
    let deps = harness.deps(1 << 20);
    let reader = StateReader::new(&deps, sub, descriptor)?;
    assert_eq!(reader.get(Key::from("user-1")).await?, None);
    Ok(())
}

/// An empty key is rejected `EmptyKey` before any acquisition; a collection
/// with no publication rows fails `UnknownPublication` (Transient).
///
/// Falsify: drop the `key.is_empty()` guard in `StateReader::session` — the
/// empty-key read no longer short-circuits and the `EmptyKey` arm is unreached.
#[tokio::test]
async fn boundary_errors() -> Result<()> {
    let harness = MemoryHarness::new();
    let descriptor = value_state::<JsonCodec>("v-empty");
    let sub = subsystem()?;
    let deps = harness.deps(1 << 20);
    let reader = StateReader::new(&deps, sub, descriptor)?;

    match reader.get(Key::from("")).await {
        Err(StateReaderError::EmptyKey) => {}
        other => bail!("expected EmptyKey, got {other:?}"),
    }
    match reader.get(Key::from("user-1")).await {
        Err(StateReaderError::UnknownPublication { .. }) => {}
        other => bail!("expected UnknownPublication, got {other:?}"),
    }
    Ok(())
}
