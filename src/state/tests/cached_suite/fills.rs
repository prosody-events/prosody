//! Cache fills over foreign provisional cells, fill failures, and cache
//! disablement.

use super::*;

/// Negative caching (KV2), committed arm: an absent-base cell staged and
/// committed by a prior-event event beneath the cache resolves PRESENT on the
/// fall-through read, which publishes the present value — the second get is
/// warm.
#[test]
fn absent_fill_over_committed_foreign_provisional_publishes_present() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(test_db::cache("neg_committed")?, counting.clone());
        let id = collection("neg-committed")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Stage below the cache (the crash/prior event shape): the cache never
        // saw the stage, so the cell is a genuine miss.
        let a = probe(1);
        let writes = [(
            cell_at(4),
            ProvisionalWrite::new(Some(bytes(44)), Committed::new(None), a),
        )];
        let marker = EventMarker::frozen(a, &writes, Vec::new(), &evidence([].into(), None));
        counting
            .write_provisional(&cref, listed(&marker, &writes)?)
            .await?;
        seed_commit_evidence(&counting, &cref).await?;

        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(4).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(44)),
            "the fill resolves the committed prior event provisional to present"
        );
        assert!(counting.lower_reads() >= 1, "the first get falls through");
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(4).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(44)),
            "the resolved value was published"
        );
        assert_eq!(counting.lower_reads(), 0, "the second get is warm");
        Ok(())
    })
}

/// Negative caching (KV2), aborted arm: an absent-base cell staged by a
/// prior-event event that never committed resolves ABSENT on the fall-through
/// read, which publishes the Absent tag — the second get answers `None` with
/// zero lower reads.
#[test]
fn absent_fill_over_aborted_foreign_provisional_publishes_absent() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(test_db::cache("neg_aborted")?, counting.clone());
        let id = collection("neg-aborted")?;
        let cref = CollectionRef::new(id.clone(), None);

        let a = probe(1);
        let writes = [(
            cell_at(4),
            ProvisionalWrite::new(Some(bytes(44)), Committed::new(None), a),
        )];
        let marker = EventMarker::frozen(a, &writes, Vec::new(), &evidence([].into(), None));
        counting
            .write_provisional(&cref, listed(&marker, &writes)?)
            .await?;
        // No certificate exists, so the cell reads its committed base.

        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(4).as_ref())
                .await?
                .0
                .get(),
            None,
            "the fill resolves the aborted prior event provisional to its absent prev"
        );
        assert!(counting.lower_reads() >= 1, "the first get falls through");
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(4).as_ref())
                .await?
                .0
                .get(),
            None,
            "absence stays correct"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "the Absent tag was published — repeated absent reads are free"
        );
        Ok(())
    })
}

/// Admission reads marker slices and listed coordinates without a cell scan.
/// The counting store checks the exact operation set.
#[test]
fn admission_issues_no_scans() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(test_db::cache("admission_budget")?, counting.clone());
        let id = collection("admission-budget")?;
        let cref = CollectionRef::new(id.clone(), None);
        let (writes, _marker) = stage_committed_marker(&cached, &cref, 6).await?;
        drop(writes);

        counting.reset();
        let resolved = admit_collection(&cached, &dedup, &cref)
            .await
            .map_err(|e| eyre!("admission failed: {e:?}"))?;
        assert!(resolved, "admission resolved the staged marker");
        assert_eq!(
            counting.lower_scans(),
            0,
            "admission uses marker and batch reads without a cell scan"
        );
        assert!(
            counting.marker_reads() >= 1,
            "admission rode the unsettled-marker leg"
        );
        Ok(())
    })
}

/// The KV5 fault clause's fail-N budget: a fill whose publish fails N times
/// then succeeds issues exactly N+1 durable reads, then zero.
#[test]
fn fill_publish_failure_costs_one_read_each() -> Result<()> {
    const N: usize = 3;

    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let fjall = test_db::cache("fill_budget")?;
        let fail_puts = fjall.faults().fail_puts();
        let cached = Cached::new(fjall, counting.clone());
        let id = collection("fill-budget")?;
        let cref = CollectionRef::new(id.clone(), None);
        counting
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(7)))], &[])
            .await?;

        counting.reset();
        fail_puts.store(true, Ordering::Relaxed);
        for _ in 0..N {
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(7)),
                "a degraded fill still answers correctly"
            );
        }
        fail_puts.store(false, Ordering::Relaxed);
        // The (N+1)th read heals: one more durable read, then zero.
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(7))
        );
        assert_eq!(
            counting.lower_reads(),
            N + 1,
            "N failed publishes cost exactly N+1 durable reads"
        );
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(7))
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "the first successful publish restores permanence (KV5)"
        );
        Ok(())
    })
}

/// The read-degrade companion: a corrupt fjall frame at a cell degrades that
/// one get to a durable read (warn-skip, never a failed get), and the fill's
/// publish overwrites the corrupt frame so the next get is warm again.
#[test]
fn fjall_read_failure_degrades_that_get() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let fjall = test_db::cache("read_degrade")?;
        let cached = Cached::new(fjall.clone(), counting.clone());
        let id = collection("read-degrade")?;
        let cref = CollectionRef::new(id.clone(), None);

        counting
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(7)))], &[])
            .await?;
        // Seed a corrupt frame (unknown tag byte) at the cell's fjall key.
        fjall
            .seed_raw_cell(&id, &cell_at(1), Bytes::from_static(&[0xFE, 0, 0]))
            .await?;

        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(7)),
            "a fjall read failure degrades the get to a durable read, never fails it"
        );
        assert_eq!(counting.lower_reads(), 1, "exactly one degraded read");
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(7)),
            "the fill overwrote the corrupt frame"
        );
        assert_eq!(counting.lower_reads(), 0, "the repaired entry is warm");
        Ok(())
    })
}

/// Proves that cache disablement applies to all workspace clones.
///
/// A removal failure disables clone A.
/// Clone B must then use durable storage.
/// Admission must use durable marker state.
#[test]
fn cache_disablement_applies_to_all_workspace_clones() -> Result<()> {
    let metrics = GlobalMetrics::install_global();
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let fjall = test_db::cache("disabled")?;
        let fail_puts = fjall.faults().fail_puts();
        let fail_deletes = fjall.faults().fail_deletes();
        let cached_a = Cached::new(fjall.clone(), counting.clone());
        let cached_b = cached_a.clone();
        let id = collection("disabled")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(1);

        // Add a cached value before the failed stage publication.
        cached_a
            .write_resolved(&cref, &[(cell_at(1), Some(bytes(1)))], &[])
            .await?;
        fail_deletes.store(u64::try_from(DELETE_RETRY_BUDGET + 2)?, Ordering::Relaxed);
        fail_puts.store(true, Ordering::Relaxed);
        let prev1 = CellRead::<Values>::read(&counting, &id, cell_at(1).as_ref())
            .await?
            .0;
        let stage = [(
            cell_at(1),
            ProvisionalWrite::new(Some(bytes(2)), prev1, event),
        )];
        let marker2 = EventMarker::frozen(event, &stage, Vec::new(), &evidence([].into(), None));
        cached_a
            .write_provisional(&cref, listed(&marker2, &stage)?)
            .await?;
        fail_puts.store(false, Ordering::Relaxed);
        fail_deletes.store(0, Ordering::Relaxed);

        assert!(
            fjall.is_disabled(),
            "the cleanup failure disabled the cache"
        );

        assert_eq!(
            metrics.points("prosody.state.cell.cache.disabled_assignments")?,
            vec![(labels([]), 1)]
        );
        fjall.disable();
        assert_eq!(
            metrics.points("prosody.state.cell.cache.disabled_assignments")?,
            vec![(labels([]), 1)],
            "repeated disable must not count the assignment twice"
        );

        // Clone B must not return the old cached value.
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached_b, &id, cell_at(1).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(1)),
            "clone B reads the committed value from durable storage"
        );
        assert!(counting.lower_reads() >= 1, "B's get is a durable read");
        // A repeated read must also use durable storage.
        counting.reset();
        let _ = CellRead::<Values>::read(&cached_b, &id, cell_at(1).as_ref())
            .await?
            .0;
        assert!(
            counting.lower_reads() >= 1,
            "clone B does not update a disabled cache"
        );
        assert!(
            fjall.is_disabled(),
            "the cache remains disabled for the assignment"
        );

        // Add a provisional cell after cache disablement.
        // Admission must resolve its durable marker.
        let prev3 = CellRead::<Values>::read(&counting, &id, cell_at(3).as_ref())
            .await?
            .0;
        let post = [(
            cell_at(3),
            ProvisionalWrite::new(Some(bytes(5)), prev3, event),
        )];
        let marker3 = EventMarker::frozen(event, &post, Vec::new(), &evidence([].into(), None));
        cached_b
            .write_provisional(&cref, listed(&marker3, &post)?)
            .await?;
        assert!(admit_collection(&cached_b, &dedup, &cref).await?);
        assert!(counting.marker_state(&id).await?.staged.is_none());
        Ok(())
    })
}
