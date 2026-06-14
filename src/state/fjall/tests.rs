//! Boundary tests for the fjall Value cache decode path.
//!
//! The flagship is the **read-path uniqueness invariant**: a present cell read
//! back from the fjall decode path is uniquely owned
//! (`try_into_mut().is_ok()`). This pins the production fast path
//! `StateHandle::get` relies on — the fjall cache decode mints a fresh `Bytes`,
//! so the read parses in place with zero copy — and guards against a future
//! layer re-introducing a shared clone that would silently demote the read to
//! the copying fallback.

use super::FjallValueStore;
use super::codec::collection_prefix;
use crate::Key;
use crate::state::value::ValueStore;
use crate::state::{CollectionId, Read, StateKey, StateName, StateType, ValueKind};
use crate::test_util::TEST_RUNTIME;
use color_eyre::eyre::{Result, eyre};
use fjall::{CompressionType, Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use quickcheck::{QuickCheck, TestResult};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

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
fn setup() -> Result<(TempDir, FjallValueStore)> {
    let (dir, _keyspace, partition) = open("value_cache")?;
    Ok((dir, FjallValueStore::new(partition)))
}

fn collection(name: &str) -> Result<CollectionId<ValueKind>> {
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
        store.set(&c, &payload).await?;
        let Read::Present(bytes) = store.get(&c).await? else {
            return Err(eyre!("expected present cell"));
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

    let cache = FjallValueStore::new(cache_partition.clone());
    TEST_RUNTIME.block_on(cache.set(&c, payload))?;
    let cache_raw = cache_partition
        .get(collection_prefix(&c))?
        .ok_or_else(|| eyre!("cache cell missing"))?;
    assert_eq!(
        cache_raw.as_ref(),
        expected.as_slice(),
        "cache cell not raw"
    );

    Ok(())
}
