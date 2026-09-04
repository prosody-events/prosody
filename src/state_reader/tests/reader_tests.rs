//! End-to-end reads through the public [`StateReader`].
//!
//! The committed round-trip for Value / Map / Deque is the memory
//! instantiation of the backend-generic [`reader_suite`](super::reader_suite)
//! runner (`prop_reader_*_committed`). The focused tests below pin invariants
//! that runner cannot reach: the commit-to-promote window, since the runner
//! always promotes before reading; Kafka-ref resolution through the loader;
//! identity validation; and the boundary errors.
//!
//! Committed state is seeded through the real owner session. The reader
//! reads it back through the stores that bypass the commit oracle, under the
//! segment the owner wrote.

use super::reader_suite::{
    ReaderCase, ValueOp, run_reader_deque_trace, run_reader_map_trace, run_reader_set_trace,
    run_reader_value_trace,
};
use super::support::{
    GROUP_A, MemoryHarness, MemoryReaderBackend, mock_count, owner_commit, owner_stage,
    publish_source, registry_of, source_state_key, state_name, subsystem, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::state::cell_key::{Coordinate, Direction, Scan, Section};
use crate::state::descriptor::{
    DescriptorIdentity, StateDescriptor, deque_state, map_state, set_state, value_state,
};
use crate::state::descriptor_identity::DurableDescriptorIdentity;
use crate::state::identity::CollectionId;
use crate::state::order_codec::I64KeyCodec;
use crate::state::publication::PublicationStore;
use crate::state::registry::CollectionDef;
use crate::state::store::CoordinateBatch;
use crate::state::tests::collection_suite::{DequeOp, MapOp, SetOp, Trace};
use crate::state::{ReadCachePolicy, StateType};
use crate::state_reader::CommittedCellSource;
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::eyre::{Result, bail, eyre};
use futures::{TryStreamExt, executor::block_on};
use serde_json::Value;
use std::num::NonZeroU64;
use std::time::Duration;

/// Instantiates a memory `prop_reader_<kind>_committed` test.
///
/// Builds a fresh [`MemoryReaderBackend`] for `$descriptor_ctor($name)`,
/// drives an arbitrary `Trace<$op>` through `$runner`, and asserts the
/// committed value always matches the oracle model. `QUICKCHECK_TESTS` sets
/// the iteration count. The three instantiations below differ only in the
/// descriptor constructor, collection name, trace op, and runner.
macro_rules! reader_prop {
    ($test_name:ident, $op:ty, $descriptor_ctor:expr, $name:expr, $runner:ident) => {
        #[test]
        fn $test_name() {
            fn property(trace: Trace<$op>) -> Result<bool> {
                let descriptor = $descriptor_ctor($name);
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
                block_on($runner(&backend, descriptor, &case, trace))
            }
            quickcheck::QuickCheck::new().quickcheck(property as fn(Trace<$op>) -> Result<bool>);
        }
    };
}

// The reader observes exactly the committed Value the owner wrote, over an
// arbitrary overwrite trace — the memory instantiation of
// `run_reader_value_trace`.
reader_prop!(
    prop_reader_value_committed,
    ValueOp,
    value_state::<JsonCodec>,
    "reader-value",
    run_reader_value_trace
);

// The reader's point `get`, `get_many`, and ordered `stream` equal a
// `BTreeMap` model of the committed live entries after every event, over an
// arbitrary set/remove/clear trace — the memory instantiation of
// `run_reader_map_trace`.
reader_prop!(
    prop_reader_map_committed,
    MapOp,
    map_state::<I64KeyCodec, JsonCodec>,
    "reader-map",
    run_reader_map_trace
);

// The reader set surface equals a `BTreeSet` model after each event.
reader_prop!(
    prop_reader_set_committed,
    SetOp,
    set_state::<I64KeyCodec>,
    "reader-set",
    run_reader_set_trace
);

// The reader's `len`, front-relative `get`, and ordered `stream` equal a
// `VecDeque` model of the committed elements after every event, over an
// arbitrary push/pop/clear trace — the memory instantiation of
// `run_reader_deque_trace`.
reader_prop!(
    prop_reader_deque_committed,
    DequeOp,
    deque_state::<JsonCodec>,
    "reader-deque",
    run_reader_deque_trace
);

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

    let id = CollectionId::new(state_key, StateType::Application, name.clone());
    let section = Section::new(0);
    let batch = CoordinateBatch::chunks([Coordinate::empty(), Coordinate::from_bytes("missing")])
        .next()
        .ok_or_else(|| eyre!("the test batch is empty"))?;
    let values = harness.cells.load_many(&id, section, &batch).await?;
    let presence = harness
        .cells
        .load_presence_many(&id, section, &batch)
        .await?;
    let expected: Vec<bool> = values.into_iter().map(|value| value.is_some()).collect();
    assert_eq!(presence.as_slice(), expected);
    let scan = Scan::over(section, Direction::Forward);
    let values = harness.cells.scan(&id, scan).map_ok(|(cell, _)| cell);
    assert_eq!(
        harness
            .cells
            .scan_presence(&id, scan)
            .try_collect::<Vec<_>>()
            .await?,
        values.try_collect::<Vec<_>>().await?
    );

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

    let deps = harness.deps();
    let reader = StateReader::new(&deps, sub, descriptor)?;
    assert_eq!(
        reader.get(key).await?,
        Some(Value::from(1i64)),
        "reader sees committed prev, not the in-flight provisional"
    );
    Ok(())
}

/// An inherited policy uses the bundle-wide default TTL. A disabled policy
/// bypasses that default. Within an effective TTL the reader keeps serving the
/// cached committed value after the owner commits a newer one.
///
/// Falsify: resolve [`ReadCachePolicy::Disabled`] to the bundle default. The
/// disabled case then returns the cached `1` instead of the new commit.
#[tokio::test]
async fn read_cache_policy_resolves_against_the_bundle_default() -> Result<()> {
    for (policy, default_ttl, expected_second_read) in [
        (
            ReadCachePolicy::Inherit,
            Some(Duration::from_mins(5)),
            Value::from(1i64),
        ),
        (
            ReadCachePolicy::Disabled,
            Some(Duration::from_mins(5)),
            Value::from(2i64),
        ),
        (ReadCachePolicy::Inherit, None, Value::from(2i64)),
    ] {
        let harness = MemoryHarness::new();
        let descriptor = value_state::<JsonCodec>("v-bundle-ttl").read_cache(policy);
        let name = state_name("v-bundle-ttl")?;
        let sub = subsystem()?;
        let tp = topic("orders");
        let count = mock_count();
        let key = Key::from("user-9");
        let state_key = source_state_key(tp, GROUP_A, &key, count)?;
        let registry = registry_of(&descriptor, CollectionDef::new(None))?;

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

        let deps = harness.deps().with_default_read_cache_ttl(default_ttl);
        let reader = StateReader::new(&deps, sub, descriptor)?;
        assert_eq!(reader.get(key.clone()).await?, Some(Value::from(1i64)));

        // A newer committed value distinguishes the two cases. A cached read
        // still returns 1 inside the TTL; an uncached read returns 2.
        owner_commit(
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
        assert_eq!(
            reader.get(key).await?,
            Some(expected_second_read),
            "second read under {policy:?} with bundle default {default_ttl:?}"
        );
    }
    Ok(())
}

/// A zero bundle default fails reader construction exactly like an explicit
/// collection TTL would. The reader validates the resolved policy, not just the
/// collection's own setting, so a degenerate default read from configuration
/// cannot slip through.
///
/// Falsify: move the TTL validation before the bundle-default resolution.
/// The collection inherits, so it validates `None` and the reader
/// constructs instead of rejecting.
#[tokio::test]
async fn zero_bundle_default_is_rejected_at_reader_construction() -> Result<()> {
    let harness = MemoryHarness::new();
    let descriptor = value_state::<JsonCodec>("v-zero-default");
    let deps = harness
        .deps()
        .with_default_read_cache_ttl(Some(Duration::ZERO));
    match StateReader::new(&deps, subsystem()?, descriptor) {
        Err(StateReaderError::InvalidReadCache { .. }) => Ok(()),
        Err(other) => bail!("expected InvalidReadCache, got {other:?}"),
        Ok(_) => bail!("expected InvalidReadCache, got a reader"),
    }
}

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

/// A source whose frozen identity differs from the reader's descriptor in
/// any one field fails with `IdentityMismatch` (Permanent). An unperturbed
/// identity acquires normally. A raw, unknown `kind` discriminant compares
/// unequal instead of failing to decode.
///
/// Falsify: widen `descriptor_identity::validate` to compare only
/// `format_id`. Then the kind-perturbed case acquires instead of failing.
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
    // A raw discriminant that no CollectionKindId uses. It compares unequal
    // instead of failing to decode.
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
                StateType::Application,
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

        let deps = harness.deps();
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
    let deps = harness.deps();
    let reader = StateReader::new(&deps, sub, descriptor)?;
    assert_eq!(reader.get(Key::from("user-1")).await?, None);
    Ok(())
}

/// An empty key is rejected with `EmptyKey` before any acquisition. A
/// collection with no publication rows fails with `UnknownPublication`
/// (Transient).
///
/// Falsify: drop the `key.is_empty()` guard in `StateReader::session`. The
/// empty-key read no longer short-circuits, and the `EmptyKey` arm is never
/// reached.
#[tokio::test]
async fn boundary_errors() -> Result<()> {
    let harness = MemoryHarness::new();
    let descriptor = value_state::<JsonCodec>("v-empty");
    let sub = subsystem()?;
    let deps = harness.deps();
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
