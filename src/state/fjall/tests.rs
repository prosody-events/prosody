//! Boundary tests for the fjall Value cache decode path.
//!
//! The flagship is the **read-path uniqueness invariant**: a present cell read
//! back from the fjall decode path is uniquely owned
//! (`try_into_mut().is_ok()`). This pins the production fast path
//! `StateHandle::get` relies on — the fjall cache decode mints a fresh `Bytes`,
//! so the read parses in place with zero copy — and guards against a future
//! layer re-introducing a shared clone that would silently demote the read to
//! the copying fallback.

use super::codec::cell_key;
use super::{AssignmentEpoch, FjallCellCache, FjallClient, FjallConfiguration};
use crate::state::cell::Committed;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::{CollectionId, StateKey, StateName, StateType};
use crate::test_util::TEST_RUNTIME;
use crate::{Key, Topic};
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use fjall::{CompressionType, Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use quickcheck::{QuickCheck, TestResult};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

/// The single Value cell (`ValueNs::Entries`, empty coordinate).
fn value_cell() -> CellKey {
    CellKey {
        section: Section::new(0),
        coordinate: Coordinate::empty(),
    }
}

/// LZ4 block compression, matching the production workspace's
/// `partition_options`.
fn partition_options() -> PartitionCreateOptions {
    PartitionCreateOptions::default().compression(CompressionType::Lz4)
}

/// Opens a fresh tempdir-backed keyspace and one named partition under it. The
/// returned `TempDir` keeps the backing directory alive; the `PartitionHandle`
/// (and any handle cloned from it) is what operates the store, so the
/// `Keyspace` itself need not outlive the handles in-process.
fn open(name: &str) -> Result<(TempDir, Keyspace, PartitionHandle)> {
    let dir = tempfile::tempdir()?;
    let keyspace = Config::new(dir.path()).open()?;
    let partition = keyspace.open_partition(name, partition_options())?;
    Ok((dir, keyspace, partition))
}

/// Opens a fresh tempdir-backed cache partition and wraps it in a store. The
/// returned `TempDir` keeps the backing directory alive; the store operates
/// through the `PartitionHandle`, so the intermediate `Keyspace` is dropped.
fn setup() -> Result<(TempDir, FjallCellCache)> {
    let (dir, _keyspace, partition) = open("value_cache")?;
    Ok((dir, FjallCellCache::new(partition)))
}

fn collection(name: &str) -> Result<CollectionId> {
    let key: Key = Arc::from("k");
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), key),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// Read-path uniqueness invariant over the fjall cache: a present cell read
/// back from the decode path is uniquely owned, across random non-empty
/// payloads.
#[test]
fn prop_fjall_present_cell_is_uniquely_owned() {
    async fn check(payload: Vec<u8>) -> Result<bool> {
        let (_dir, store) = setup()?;
        let c = collection("uniq")?;
        let cell = value_cell();
        store
            .put(&c, &cell, &Committed::new(Some(Bytes::from(payload))))
            .await?;
        let Some(committed) = store.get(&c, &cell).await? else {
            return Err(eyre!("expected a cache hit"));
        };
        let Some(bytes) = committed.into_inner() else {
            return Err(eyre!("expected a present cell"));
        };
        Ok(bytes.try_into_mut().is_ok())
    }

    fn prop(payload: Vec<u8>) -> TestResult {
        if payload.is_empty() {
            return TestResult::discard();
        }
        match TEST_RUNTIME.block_on(check(payload)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("present cell was a shared clone, not uniquely owned"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

/// Change 1, end-to-end through the cache store: a present cell written via the
/// committed cache is stored `[0x01] ++ raw payload` byte-for-byte.
/// `partition.get` returns the logical value (fjall decompresses any on-disk
/// LZ4 transparently), so an equal-to-raw result proves the app layer dropped
/// its zstd frame — a zstd frame would differ from the raw tail for any
/// payload.
#[test]
fn stored_cells_are_raw_tagged_payload() -> Result<()> {
    let payload = b"a raw, uncompressed keyed-state payload".as_slice();
    let mut expected = vec![0x01_u8];
    expected.extend_from_slice(payload);

    let (_dir, _keyspace, cache_partition) = open("value_cache")?;
    let c = collection("raw")?;
    let cell = value_cell();

    let cache = FjallCellCache::new(cache_partition.clone());
    TEST_RUNTIME.block_on(cache.put(
        &c,
        &cell,
        &Committed::new(Some(Bytes::copy_from_slice(payload))),
    ))?;
    let cache_raw = cache_partition
        .get(cell_key(&c, &cell))?
        .ok_or_else(|| eyre!("cache cell missing"))?;
    assert_eq!(
        cache_raw.as_ref(),
        expected.as_slice(),
        "cache cell not raw"
    );

    Ok(())
}

/// `for_workspace` must *retain* the workspace it is handed, not extract the
/// cache handle and drop the workspace.
///
/// This is the one ownership decision the type system does not enforce: both
/// `new` (bare handle, no workspace) and `for_workspace` return `Self`, so a
/// `for_workspace` rewritten to `Self::new(ws.cache_handle().clone())` compiles
/// — and silently deletes the cache partition the moment the dropped
/// workspace's `Drop` runs. The cache is a read-through optimization, so that
/// degrades every op to a backing read with no other test failing. We move the
/// workspace in with no other binding to it and confirm — through the keyspace,
/// the only channel a `Drop` side-effect is observable on — that the partition
/// is still live after construction. A discarding `for_workspace` would show
/// zero.
#[test]
fn for_workspace_retains_the_workspace() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let client = FjallClient::open(&FjallConfiguration {
        cache_dir: dir.path().to_path_buf(),
    })?;
    let keyspace = client.keyspace().clone();
    let live_cache_partitions = || {
        keyspace
            .list_partitions()
            .iter()
            .filter(|name| name.starts_with("value_cache_"))
            .count()
    };

    let workspace = client.workspace(Topic::from("orders.v1"), 0, AssignmentEpoch::mint())?;
    let _cache = FjallCellCache::for_workspace(workspace);
    assert_eq!(
        live_cache_partitions(),
        1,
        "for_workspace must keep the workspace alive, not drop it on return"
    );
    Ok(())
}
