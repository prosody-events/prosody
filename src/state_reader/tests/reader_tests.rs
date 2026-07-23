//! End-to-end reads through the public [`StateReader`]: committed round-trips
//! for Value / Map / Deque, the commit→promote window, identity validation,
//! and the boundary errors.
//!
//! Committed state is seeded through the real owner session; the reader reads
//! it back over the oracle-free carriers under the segment the owner wrote.

use super::support::{
    GROUP_A, MemoryHarness, mock_count, owner_commit, owner_stage, publish_source, registry_of,
    source_state_key, state_name, subsystem, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{DescriptorIdentity, deque_state, map_state, value_state};
use crate::state::descriptor_identity::DurableDescriptorIdentity;
use crate::state::order_codec::I64KeyCodec;
use crate::state::publication::PublicationStore;
use crate::state::registry::CollectionDef;
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use serde_json::Value;
use std::collections::BTreeMap;

/// A committed Value written by the owner is read back by a standalone reader
/// over the same in-memory carriers.
///
/// Falsify: read a different section/name in `ReadSession::get` — the reader
/// diverges from the written value.
#[tokio::test]
async fn value_reader_reads_committed() -> Result<()> {
    let harness = MemoryHarness::new();
    let descriptor = value_state::<JsonCodec>("v-cart");
    let name = state_name("v-cart")?;
    let sub = subsystem()?;
    let tp = topic("orders");
    let count = mock_count();
    let key = Key::from("user-42");
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
                .set(Value::from(7i64))
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
    assert_eq!(reader.get(key).await?, Some(Value::from(7i64)));
    Ok(())
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

/// A Map's committed live entries read back in key order equal the owner's
/// model (a `BTreeMap`), via both `get` and `iter`.
///
/// Falsify: drop the `project_committed` present filter in `scan_committed` — a
/// removed entry reappears and the ordered comparison fails.
#[tokio::test]
async fn map_reader_iter_equals_model() -> Result<()> {
    let harness = MemoryHarness::new();
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("m-cart");
    let name = state_name("m-cart")?;
    let sub = subsystem()?;
    let tp = topic("orders");
    let count = mock_count();
    let key = Key::from("user-map");
    let state_key = source_state_key(tp, GROUP_A, &key, count)?;
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let model: BTreeMap<i64, Value> = [
        (-2, Value::from(10i64)),
        (0, Value::from(20i64)),
        (3, Value::from(30i64)),
    ]
    .into_iter()
    .collect();
    let entries = model.clone();
    owner_commit(
        &harness.cells,
        &registry,
        &state_key,
        descriptor,
        1,
        |handle| async move {
            for (k, v) in entries {
                handle.set(k, v).await.map_err(|e| eyre!("set: {e}"))?;
            }
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

    // Point read.
    assert_eq!(reader.get(key.clone(), &0).await?, Some(Value::from(20i64)));
    assert_eq!(reader.get(key.clone(), &1).await?, None);

    // Ordered scan equals the model.
    let scanned: Vec<(i64, Value)> = reader
        .stream(key, Direction::Forward)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    let expected: Vec<(i64, Value)> = model.into_iter().collect();
    assert_eq!(scanned, expected);
    Ok(())
}

/// A Deque's committed elements read back front-to-back equal the owner's
/// model, via `get(index)`, `len`, and `iter`.
#[tokio::test]
async fn deque_reader_equals_model() -> Result<()> {
    let harness = MemoryHarness::new();
    let descriptor = deque_state::<JsonCodec>("d-log");
    let name = state_name("d-log")?;
    let sub = subsystem()?;
    let tp = topic("orders");
    let count = mock_count();
    let key = Key::from("user-dq");
    let state_key = source_state_key(tp, GROUP_A, &key, count)?;
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let model: Vec<Value> = (0i64..5).map(Value::from).collect();
    let pushes = model.clone();
    owner_commit(
        &harness.cells,
        &registry,
        &state_key,
        descriptor,
        1,
        |handle| async move {
            for v in pushes {
                handle.push_back(v).await.map_err(|e| eyre!("push: {e}"))?;
            }
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

    assert_eq!(reader.len(key.clone()).await?, 5);
    assert_eq!(reader.get(key.clone(), 0).await?, Some(Value::from(0i64)));
    assert_eq!(reader.get(key.clone(), 4).await?, Some(Value::from(4i64)));
    assert_eq!(reader.get(key.clone(), 5).await?, None);

    let scanned: Vec<Value> = reader
        .stream(key, Direction::Forward)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    assert_eq!(scanned, model);
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
