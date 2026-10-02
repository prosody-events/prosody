//! Publish failures and the must-succeed delete retries they trigger.

use super::*;

/// `Cached::provisional_many` delegates the raw read to the lower store and
/// publishes NOTHING into the committed-value cache: it delegates exactly once
/// (one lower batch read), and a subsequent point `get` of a read coordinate
/// still incurs a lower read (a fjall miss) — proving no committed projection
/// was warmed by the raw verb.
#[test]
fn cached_provisional_many_does_not_publish() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let id = collection("cached-no-publish")?;
        let cref = CollectionRef::new(id.clone(), None);
        // Stage a provisional cell in the LOWER store directly, so fjall stays
        // untouched (a cached stage would publish the cell's `prev`).
        let event = probe(0x5EED);
        let prev = CellRead::<Values>::read(&counting, &id, cell_at(2).as_ref())
            .await?
            .0;
        let writes = [(
            cell_at(2),
            ProvisionalWrite::new(Some(bytes(20)), prev, event),
        )];
        let marker = EventMarker::frozen(event, &writes, Vec::new(), &evidence([].into(), None));
        counting
            .write_provisional(&cref, listed(&marker, &writes)?)
            .await?;

        let cached = Cached::new(test_db::cache("cached-no-publish")?, counting.clone());
        counting.reset();

        let batch = batch_of([2])?;
        let out = cached.provisional_many(&id, SECTION, &batch).await?;
        assert_eq!(out.len(), 1, "the staged provisional cell survives");
        assert_eq!(
            counting.raw_batch_reads(),
            1,
            "delegated exactly once to the lower batch"
        );

        // Nothing was published, so a point get of the read coordinate still
        // falls through to the lower store (a fjall miss).
        counting.reset();
        CellRead::<Values>::read(&cached, &id, cell_at(2).as_ref()).await?;
        assert!(
            counting.lower_reads() >= 1,
            "provisional_many must not warm the committed-value cache"
        );
        Ok(())
    })
}

/// Proves that a failed cache update removes the old entry.
///
/// The next read must load and cache the durable value.
#[test]
fn failed_publish_deletes_the_stale_entry() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let fjall = test_db::cache("fault")?;
        let fail = fjall.faults().fail_puts();
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(fjall, counting.clone());
        let id = collection("fault")?;
        let cref = CollectionRef::new(id.clone(), None);

        // First write publishes cleanly and warms `1`.
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(1)))], &[])
            .await?;
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(1))
        );
        assert_eq!(counting.lower_reads(), 0, "the seeded entry is warm");

        // Now force every publish to fail. The lower write still succeeds
        // (durable truth is `2`), but the fjall publish fails → the stale
        // entry is deleted (failed-publish cache guard).
        fail.store(true, Ordering::Relaxed);
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(2)))], &[])
            .await?;

        // Heal the cache fault; the next get is a miss, so it falls through to
        // the durable `2` (exactly one lower read) and re-publishes — never
        // serving the stale fjall `1`.
        fail.store(false, Ordering::Relaxed);
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(2)),
            "a failed publish deletes the entry, so the next read self-heals"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the healed get falls through once"
        );
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(2)),
            "the fall-through re-warmed the cell"
        );
        assert_eq!(counting.lower_reads(), 0, "the re-warmed get reads nothing");
        Ok(())
    })
}

/// Proves that a failed batch update removes every old entry.
#[test]
fn failed_batch_publish_deletes_every_batch_cell() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let cells = MemoryCells::new();
        let fjall = test_db::cache("batch-fault")?;
        let fail = fjall.faults().fail_puts();
        let lower = MemoryCellStore::new(cells);
        let cached = Cached::new(fjall, lower);
        let id = collection("batch-fault")?;
        let cref = CollectionRef::new(id.clone(), None);

        let seed = [(cell_at(1), Some(bytes(1))), (cell_at(2), Some(bytes(2)))];
        let update = [(cell_at(1), Some(bytes(11))), (cell_at(2), Some(bytes(22)))];

        // One multi-cell write-through publishes cleanly and warms both cells.
        cached.write_resolved(&cref, &seed, &[]).await?;
        for (c, v) in [(1u8, 1u8), (2, 2)] {
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, cell_at(c).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(v))
            );
        }

        // Force the batch commit to fail. The lower write still succeeds
        // (durable truth is `11`/`22`), but the atomic batch lands nothing →
        // every coordinate in the batch is deleted (failed-publish cache guard).
        fail.store(true, Ordering::Relaxed);
        cached.write_resolved(&cref, &update, &[]).await?;

        // Heal the fault; both gets are misses, so each falls through to its
        // fresh durable value — never serving the stale batch.
        fail.store(false, Ordering::Relaxed);
        for (c, v) in [(1u8, 11u8), (2, 22)] {
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, cell_at(c).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(v)),
                "a failed batch deletes every coordinate, so each read self-heals"
            );
        }
        Ok(())
    })
}

/// Proves that cache entry removal retries before it disables the cache.
///
/// The promoted value must replace the old cached value.
#[test]
fn promote_delete_retries_before_cache_disablement() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let fjall = test_db::cache("promote-delete")?;
        let fail_deletes = fjall.faults().fail_deletes();
        let lower = MemoryCellStore::new(MemoryCells::new());
        let cached = Cached::new(fjall.clone(), lower);
        let id = collection("promote-delete")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        // Committed base `1`, warm by write-through; stage `5` over it (the
        // stage publishes `prev` = 1, so the entry stays warm with 1).
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        let prev = CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
            .await?
            .0;
        let writes = [(
            cell_at(0),
            ProvisionalWrite::new(Some(bytes(5)), prev, event),
        )];
        let marker = EventMarker::frozen(event, &writes, Vec::new(), &evidence([].into(), None));
        cached
            .write_provisional(&cref, listed(&marker, &writes)?)
            .await?;

        // Make cache removal fail within the retry limit.
        fail_deletes.store(u64::try_from(DELETE_RETRY_BUDGET - 1)?, Ordering::Relaxed);
        cached.mark_resolved(&cref, &[cell_at(0)]).await?;
        assert_eq!(
            fail_deletes.load(Ordering::Relaxed),
            0,
            "the injected delete failures must have fired"
        );
        assert!(
            !fjall.is_disabled(),
            "an in-budget removal does not disable the cache"
        );
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(5)),
            "a failed delete must not leave the stale pre-promote value warm"
        );
        Ok(())
    })
}

/// Proves that cache removal recovers within its retry limit.
#[test]
fn write_path_delete_recovers_within_budget() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let fjall = test_db::cache("write-delete")?;
        let fail_puts = fjall.faults().fail_puts();
        let fail_deletes = fjall.faults().fail_deletes();
        let lower = MemoryCellStore::new(MemoryCells::new());
        let cached = Cached::new(fjall.clone(), lower);
        let id = collection("write-delete")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Warm `1` cleanly, then write `2` with the publish AND the first
        // delete attempts failing (within the budget).
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(1)))], &[])
            .await?;
        fail_puts.store(true, Ordering::Relaxed);
        fail_deletes.store(u64::try_from(DELETE_RETRY_BUDGET - 1)?, Ordering::Relaxed);
        cached
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(2)))], &[])
            .await?;
        fail_puts.store(false, Ordering::Relaxed);

        assert_eq!(
            fail_deletes.load(Ordering::Relaxed),
            0,
            "the injected delete failures must have fired"
        );
        assert!(
            !fjall.is_disabled(),
            "a within-budget removal does not disable the cache"
        );
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(2)),
            "a doubly-failed publish+delete must still evict, so the read self-heals"
        );
        Ok(())
    })
}
