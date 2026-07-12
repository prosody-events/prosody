//! Boundary tests for the fjall cell cache decode path.
//!
//! The flagship is the **read-path uniqueness invariant**: a present cell read
//! back from the fjall decode path is uniquely owned
//! (`try_into_mut().is_ok()`). This pins the production fast path
//! `CellView::get` relies on — the fjall cache decode mints a fresh `Bytes`,
//! so the read parses in place with zero copy — and guards against a future
//! layer re-introducing a shared clone that would silently demote the read to
//! the copying fallback.

use super::codec::cell_key;
use super::test_db;
use super::{CacheRead, FjallCellCache, FjallClient, FjallClientError};
use crate::Topic;
use crate::state::cell::Committed;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::tests::cell_suite::value_cell;
use crate::state::tests::support::fresh_collection;
use crate::test_util::TEST_RUNTIME;
use bytes::Bytes;
use color_eyre::eyre::{Report, Result, eyre};
use fjall::{Database, KeyspaceCreateOptions};
use quickcheck::{QuickCheck, TestResult};
use std::collections::BTreeSet;

/// Read-path uniqueness invariant over the fjall cache: a present cell read
/// back from the decode path is uniquely owned, across random non-empty
/// payloads.
#[test]
fn prop_fjall_present_cell_is_uniquely_owned() {
    async fn check(payload: Vec<u8>) -> Result<bool> {
        let store = test_db::cache("value_cache")?;
        let c = fresh_collection("uniq")?;
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

    let (database, cache_partition, index_partition) = test_db::keyspace_pair("value_cache")?;
    let c = fresh_collection("raw")?;
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
    let cache = test_db::cache_with_clock("ttl_value", Clock::Fixed(now.clone()))?;
    let c = fresh_collection("ttl")?;
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

/// The `delete_section` hop walk deletes exactly the non-excluded keys of one
/// section: seeded past two hop budgets so the walk re-seeks repeatedly, with
/// a sibling section and a second collection sharing the keyspace, a delete of
/// section 0 with a non-empty exclusion set removes every non-excluded
/// section-0 entry and leaves the excluded keys, the sibling section, and the
/// other collection untouched.
#[test]
fn delete_section_hops_delete_exactly_the_section() -> Result<()> {
    // > 2 hops of rows so the re-seek arithmetic is exercised.
    let total = super::SCAN_HOP_ROWS * 2 + 50;
    let cache = test_db::cache("delete_section")?;
    let c = fresh_collection("hop-del")?;
    let other = fresh_collection("hop-del-other")?;
    let cell_in = |section: i8, i: usize| CellKey {
        section: Section::new(section),
        coordinate: Coordinate::from_bytes(
            u32::try_from(i).unwrap_or(u32::MAX).to_be_bytes().to_vec(),
        ),
    };
    let payload = Committed::new(Some(Bytes::from_static(b"v")));

    TEST_RUNTIME.block_on(async {
        for i in 0..total {
            cache.put(&c, &cell_in(0, i), &payload, 0).await?;
        }
        cache.put(&c, &cell_in(1, 7), &payload, 0).await?;
        cache.put(&other, &cell_in(0, 7), &payload, 0).await?;

        // Exclude two survivors, one in each hop region.
        let excluded = [cell_in(0, 3), cell_in(0, super::SCAN_HOP_ROWS + 9)];
        cache.delete_section(&c, Section::new(0), &excluded).await?;

        for i in 0..total {
            let hit = matches!(cache.get(&c, &cell_in(0, i)).await?, CacheRead::Hit(_));
            let survives = excluded.iter().any(|cell| *cell == cell_in(0, i));
            assert_eq!(
                hit, survives,
                "section-0 entry {i}: excluded keys survive, all others are deleted"
            );
        }
        assert!(
            matches!(cache.get(&c, &cell_in(1, 7)).await?, CacheRead::Hit(_)),
            "the sibling section survives"
        );
        assert!(
            matches!(cache.get(&other, &cell_in(0, 7)).await?, CacheRead::Hit(_)),
            "the sibling collection survives"
        );
        Ok::<_, Report>(())
    })?;
    Ok(())
}

/// Warm-index batch round-trip: `index_record_batch` of arbitrary (duplicate-
/// prone) coordinates followed by `index_clear_batch` of an arbitrary subset
/// must leave `index_snapshot` holding exactly the recorded-minus-cleared set —
/// the batch ops must agree with the model a sequence of single-key
/// `index_record`s would produce (one atomic hop instead of N).
#[test]
fn prop_index_batches_round_trip_the_snapshot() {
    fn cells_of(coords: &[u8]) -> Vec<CellKey> {
        coords
            .iter()
            .map(|&b| CellKey {
                section: Section::new(0),
                coordinate: Coordinate::from_bytes(vec![b]),
            })
            .collect()
    }

    async fn check(record: Vec<u8>, clear: Vec<u8>) -> Result<bool> {
        let cache = test_db::cache("index_batch")?;
        let c = fresh_collection("batch")?;
        let recorded = cells_of(&record);
        let cleared = cells_of(&clear);
        cache.index_record_batch(&c, recorded.iter()).await?;
        cache.index_clear_batch(&c, cleared.iter()).await?;

        let want: BTreeSet<u8> = record
            .iter()
            .filter(|b| !clear.contains(b))
            .copied()
            .collect();
        let mut got: Vec<u8> = cache
            .index_snapshot(&c)
            .await?
            .into_iter()
            .map(|cell| cell.coordinate.as_bytes()[0])
            .collect();
        got.sort_unstable();
        got.dedup();
        Ok(got.into_iter().eq(want))
    }

    fn prop(record: Vec<u8>, clear: Vec<u8>) -> TestResult {
        match TEST_RUNTIME.block_on(check(record, clear)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u8>, Vec<u8>) -> TestResult);
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
    let client = FjallClient::open(dir.path())?;
    let database = client.database().clone();
    let live_cache_partitions = || {
        database
            .list_keyspace_names()
            .iter()
            .filter(|name| name.starts_with("value_cache_"))
            .count()
    };

    let workspace = client.workspace(Topic::from("orders.v1"), 0)?;
    let _cache = FjallCellCache::for_workspace(workspace);
    assert_eq!(
        live_cache_partitions(),
        1,
        "for_workspace must keep the workspace alive, not drop it on return"
    );
    Ok(())
}

/// The startup sweep reaps every stale `value_*` keyspace — and only those.
/// Stale keyspaces are seeded through a raw [`Database`] (bypassing
/// [`FjallClient`], whose workspaces would delete them on drop), modeling a
/// crashed prior process.
#[test]
fn open_sweeps_stale_value_keyspaces() -> Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let database = Database::builder(dir.path()).open()?;
        for name in ["value_cache_deadbeef", "value_index_deadbeef", "unrelated"] {
            database
                .keyspace(name, KeyspaceCreateOptions::default)?
                .insert(b"stale", b"row")?;
        }
    }

    let client = FjallClient::open(dir.path())?;
    let names = client.database().list_keyspace_names();
    assert!(
        !names.iter().any(|name| name.starts_with("value_")),
        "open must sweep every stale value_* keyspace, found {names:?}"
    );
    assert!(
        names.iter().any(|name| &**name == "unrelated"),
        "the sweep must reap only value_* keyspaces, found {names:?}"
    );
    Ok(())
}

/// Born-cold invariant of [`FjallClient::workspace`]: re-assigning the same
/// `(topic, partition)` mints fresh keyspace names — a name is never
/// re-derived, so a new workspace can never open a prior assignment's data.
#[test]
fn workspace_names_are_never_reused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let client = FjallClient::open(dir.path())?;
    let database = client.database().clone();
    let value_names = || -> BTreeSet<String> {
        database
            .list_keyspace_names()
            .iter()
            .filter(|name| name.starts_with("value_"))
            .map(|name| (**name).to_owned())
            .collect()
    };

    let first = client.workspace(Topic::from("orders.v1"), 0)?;
    let first_names = value_names();
    assert_eq!(
        first_names.len(),
        2,
        "a workspace owns a cache + index pair"
    );
    drop(first);

    let _second = client.workspace(Topic::from("orders.v1"), 0)?;
    let second_names = value_names();
    assert_eq!(
        second_names.len(),
        2,
        "the re-assigned workspace owns a fresh cache + index pair — without this the disjoint \
         check below passes vacuously if the new keyspaces never appear"
    );
    assert!(
        first_names.is_disjoint(&second_names),
        "re-assigning the same (topic, partition) must mint fresh names, got {first_names:?} then \
         {second_names:?}"
    );
    Ok(())
}

/// Two clients on one `cache_dir` fail fast with [`CacheDirInUse`]: fjall's
/// exclusive directory lock is what makes the startup sweep safe, so
/// contention must surface as a clear, permanent configuration error.
///
/// [`CacheDirInUse`]: FjallClientError::CacheDirInUse
#[test]
fn open_fails_clearly_when_cache_dir_is_in_use() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let _first = FjallClient::open(dir.path())?;
    let second = FjallClient::open(dir.path());
    assert!(
        matches!(second, Err(FjallClientError::CacheDirInUse { .. })),
        "a second client on a live cache_dir must fail with CacheDirInUse, got {second:?}"
    );
    Ok(())
}
