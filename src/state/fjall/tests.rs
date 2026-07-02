//! Boundary tests for the fjall cell cache decode path.
//!
//! The flagship is the **read-path uniqueness invariant**: a present cell read
//! back from the fjall decode path is uniquely owned
//! (`try_into_mut().is_ok()`). This pins the production fast path
//! `StateHandle::get` relies on — the fjall cache decode mints a fresh `Bytes`,
//! so the read parses in place with zero copy — and guards against a future
//! layer re-introducing a shared clone that would silently demote the read to
//! the copying fallback.

use super::codec::cell_key;
use super::{AssignmentEpoch, CacheRead, FjallCellCache, FjallClient, FjallConfiguration};
use crate::state::cell::Committed;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::{CollectionId, StateKey, StateName, StateType};
use crate::test_util::TEST_RUNTIME;
use crate::{Key, Topic};
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use fjall::config::CompressionPolicy;
use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions};
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
/// `keyspace_options`.
fn keyspace_options() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
}

/// Opens a fresh tempdir-backed database and a named cache keyspace plus its
/// sibling warm-index keyspace under it. The returned `TempDir` keeps the
/// backing directory alive; the [`Database`] owns batch writes, so the caller
/// keeps it alongside the [`Keyspace`] handles.
fn open(name: &str) -> Result<(TempDir, Database, Keyspace, Keyspace)> {
    let dir = tempfile::tempdir()?;
    let database = Database::builder(dir.path()).open()?;
    let cache = database.keyspace(name, keyspace_options)?;
    let index = database.keyspace(&format!("{name}_index"), keyspace_options)?;
    Ok((dir, database, cache, index))
}

/// Opens a fresh tempdir-backed cache + index keyspace and wraps it in a store.
/// The returned `TempDir` keeps the backing directory alive.
fn setup() -> Result<(TempDir, FjallCellCache)> {
    let (dir, database, cache, index) = open("value_cache")?;
    Ok((dir, FjallCellCache::new(database, cache, index)))
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
            .put(&c, &cell, &Committed::new(Some(Bytes::from(payload))), 0)
            .await?;
        let CacheRead::Hit(committed) = store.get(&c, &cell).await? else {
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

/// End-to-end through the cache store: a present cell written via the committed
/// cache is stored `[0x01][expiry: u64 BE][raw payload]` byte-for-byte.
/// `partition.get` returns the logical value (fjall decompresses any on-disk
/// LZ4 transparently), so an equal-to-raw result proves the app layer dropped
/// its zstd frame — a zstd frame would differ from the raw tail for any
/// payload — and pins the expiry header position.
#[test]
fn stored_cells_are_raw_tagged_payload_with_expiry() -> Result<()> {
    const EXPIRY: u64 = 1_700_000_000_000;
    let payload = b"a raw, uncompressed keyed-state payload".as_slice();
    let mut expected = vec![0x01_u8];
    expected.extend_from_slice(&EXPIRY.to_be_bytes());
    expected.extend_from_slice(payload);

    let (_dir, database, cache_partition, index_partition) = open("value_cache")?;
    let c = collection("raw")?;
    let cell = value_cell();

    let cache = FjallCellCache::new(database, cache_partition.clone(), index_partition);
    TEST_RUNTIME.block_on(cache.put(
        &c,
        &cell,
        &Committed::new(Some(Bytes::copy_from_slice(payload))),
        EXPIRY,
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

/// An expired present entry reads back as a miss (`None`) under a clock
/// advanced past its stamped expiry; the same entry with a `0`-never expiry, or
/// read at a time before expiry, stays a hit. Drives the read-side TTL check
/// with a deterministic [`Clock::Fixed`], no sleep.
#[test]
fn expired_entry_reads_as_miss() -> Result<()> {
    use super::Clock;
    use color_eyre::eyre::Report;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let now = Arc::new(AtomicU64::new(1_000));
    let (_dir, database, partition, index) = open("ttl_value")?;
    let cache = FjallCellCache::with_clock(database, partition, index, Clock::Fixed(now.clone()));
    let c = collection("ttl")?;
    let cell = value_cell();
    let payload = Committed::new(Some(Bytes::from_static(b"v")));

    TEST_RUNTIME.block_on(async {
        // Stamp an entry that expires at 2_000ms.
        cache.put(&c, &cell, &payload, 2_000).await?;
        // Before expiry: a hit.
        assert!(
            matches!(cache.get(&c, &cell).await?, CacheRead::Hit(_)),
            "live entry must hit"
        );
        // At/after expiry: reported Expired (an entry exists, floor-expired).
        now.store(2_000, Ordering::Relaxed);
        assert!(
            matches!(cache.get(&c, &cell).await?, CacheRead::Expired),
            "expired entry must read as Expired"
        );
        // A `never` (0) expiry never expires, even far in the future.
        cache.put(&c, &cell, &payload, 0).await?;
        now.store(u64::MAX, Ordering::Relaxed);
        assert!(
            matches!(cache.get(&c, &cell).await?, CacheRead::Hit(_)),
            "a never-expiry entry must always hit"
        );
        Ok::<_, Report>(())
    })?;
    Ok(())
}

/// `for_workspace` must *retain* the workspace it is handed, not extract the
/// cache handle and drop the workspace.
///
/// This is the one ownership decision the type system does not enforce: both
/// `new` (bare handle, no workspace) and `for_workspace` return `Self`, so a
/// `for_workspace` rewritten to `Self::new(ws.cache_handle().clone())` compiles
/// — and silently deletes the cache partition the moment the dropped
/// workspace's `Drop` runs. The cache is a hint over the durable lower store,
/// so that degrades every op to a backing read with no other test failing. We
/// move the
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
    let database = client.database().clone();
    let live_cache_partitions = || {
        database
            .list_keyspace_names()
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
