//! Boundary tests for the fjall cell cache decode path.
//!
//! The flagship is the **read-path uniqueness invariant**: a present cell read
//! back from the fjall decode path is uniquely owned
//! (`try_into_mut().is_ok()`). This pins the production fast path that a
//! collection's typed read relies on. The fjall cache decode mints a fresh
//! `Bytes`, so the read parses in place with zero copy. It also guards against
//! a future layer that re-introduces a shared clone, which would silently
//! demote the read to the copying fallback.

use super::codec::cell_key;
use super::test_db;
use super::{CacheRead, Clock, FjallCellCache, FjallClient, FjallClientError};
use crate::Topic;
use crate::state::CollectionId;
use crate::state::cached::Cached;
use crate::state::cell::{Committed, Presence, Values};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::store::{CELL_BATCH, CellRead};
use crate::state::tests::cell_suite::{bytes, value_cell};
use crate::state::tests::support::{batch_of, fresh_collection};
use crate::test_util::TEST_RUNTIME;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Report, Result, eyre};
use fjall::{Database, KeyspaceCreateOptions};
use quickcheck::{QuickCheck, TestResult};
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

mod reads;
mod workspace;

/// The section-0 cell at coordinate byte `b`.
fn batch_cell(b: u8) -> CellKey {
    CellKey {
        section: Section::new(0),
        coordinate: Coordinate::from_bytes(vec![b]),
    }
}

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
            .put::<Values>(
                &c,
                cell.as_ref(),
                Committed::new(Some(Bytes::from(payload))),
                0,
            )
            .await?;
        let CacheRead::Hit((committed, _)) = store.get::<Values>(&c, cell.as_ref()).await? else {
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
    TEST_RUNTIME.block_on(cache.put::<Values>(
        &c,
        cell.as_ref(),
        Committed::new(Some(Bytes::copy_from_slice(payload))),
        EXPIRY,
    ))?;
    let cache_raw = cache_partition
        .get(cell_key(&c, cell.as_ref()))?
        .ok_or_else(|| eyre!("cache cell missing"))?;
    assert_eq!(
        cache_raw.as_ref(),
        expected.as_slice(),
        "cache cell not raw"
    );

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
    let total = super::io::SCAN_HOP_ROWS * 2 + 50;
    let cache = test_db::cache("delete_section")?;
    let c = fresh_collection("hop-del")?;
    let other = fresh_collection("hop-del-other")?;
    let cell_in = |section: i8, i: usize| CellKey {
        section: Section::new(section),
        coordinate: Coordinate::from_bytes(
            u32::try_from(i).unwrap_or(u32::MAX).to_be_bytes().to_vec(),
        ),
    };
    let payload = Committed::<Values>::new(Some(Bytes::from_static(b"v")));

    TEST_RUNTIME.block_on(async {
        for i in 0..total {
            cache
                .put::<Values>(&c, cell_in(0, i).as_ref(), payload.clone(), 0)
                .await?;
        }
        cache
            .put::<Values>(&c, cell_in(1, 7).as_ref(), payload.clone(), 0)
            .await?;
        cache
            .put::<Values>(&other, cell_in(0, 7).as_ref(), payload.clone(), 0)
            .await?;

        // Exclude two survivors, one in each hop region.
        let excluded = [cell_in(0, 3), cell_in(0, super::io::SCAN_HOP_ROWS + 9)];
        cache
            .delete_section(&c, Section::new(0), excluded.iter().map(CellKey::as_ref))
            .await?;

        for i in 0..total {
            let hit = matches!(
                cache.get::<Values>(&c, cell_in(0, i).as_ref()).await?,
                CacheRead::Hit(_)
            );
            let survives = excluded.iter().any(|cell| *cell == cell_in(0, i));
            assert_eq!(
                hit, survives,
                "section-0 entry {i}: excluded keys survive, all others are deleted"
            );
        }
        assert!(
            matches!(
                cache.get::<Values>(&c, cell_in(1, 7).as_ref()).await?,
                CacheRead::Hit(_)
            ),
            "the sibling section survives"
        );
        assert!(
            matches!(
                cache.get::<Values>(&other, cell_in(0, 7).as_ref()).await?,
                CacheRead::Hit(_)
            ),
            "the sibling collection survives"
        );
        Ok::<_, Report>(())
    })?;
    Ok(())
}
