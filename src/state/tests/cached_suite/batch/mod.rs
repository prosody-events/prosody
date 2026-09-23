//! Cached batch reads: scan parity, hit and miss routing, and disablement.

use super::*;

mod expiry;

/// A memory lower store wrapped in the batch tests' read counter.
type CountingLower = CountingCellStore<MemoryCellStore>;

/// Builds a `Cached` over a [`CountingLower`] on the shared fjall database,
/// returning the cache handle, the counting handle, and the collection — the
/// batch tests' shared arrange.
pub(super) fn counting_cached(
    name: &str,
) -> Result<(Cached<CountingLower>, CountingLower, CollectionId)> {
    let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
    let cached = Cached::new(test_db::cache(name)?, counting.clone());
    let id = collection(name)?;
    Ok((cached, counting, id))
}

/// Presence scans through the cache match value scan keys.
#[test]
fn prop_cached_projection_scan_parity() {
    fn property(trace: ScanTrace) -> Result<bool> {
        let cells = MemoryCells::new();
        let name = uuid::Uuid::new_v4().to_string();
        let cached = cached_over(&cells, &name)?;
        let probe = MemoryShapeProbe(cells);
        TEST_RUNTIME.block_on(run_bottom_scan_trace(cached, trace, &probe))
    }
    QuickCheck::new().quickcheck(property as fn(ScanTrace) -> Result<bool>);
}

/// A misaligned lower batch is never cached. The caller rejects the answer,
/// so a retry must read the lower store again.
#[test]
fn misaligned_lower_batch_is_never_cached() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let (cached, counting, id) = counting_cached("batch-misaligned")?;
        let cref = CollectionRef::new(id.clone(), None);
        let cold: Vec<(CellKey, Option<Bytes>)> =
            (0u8..4).map(|c| (cell_at(c), Some(bytes(c)))).collect();
        counting.write_resolved(&cref, &cold, &[]).await?;

        counting.short_batches();
        let answers =
            CellRead::<Values>::read_many(&cached, &id, SECTION, &batch_of(0u8..4)?.as_ref())
                .await?;
        assert_eq!(answers.len(), 3, "the lower store dropped one answer");
        for c in 0u8..4 {
            assert_eq!(
                cached.stored_expiry(&id, &cell_at(c)).await?,
                None,
                "coordinate {c} was cached from a misaligned batch"
            );
        }
        Ok(())
    })
}

/// A warm batch returns every value without a lower read.
#[test]
fn batch_get_all_hits_reads_nothing() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let (cached, counting, id) = counting_cached("batch-all-hits")?;
        let cref = CollectionRef::new(id.clone(), None);
        // Warm 16 coordinates through the write-through path.
        let warm: Vec<(CellKey, Option<Bytes>)> =
            (0u8..16).map(|c| (cell_at(c), Some(bytes(c)))).collect();
        cached.write_resolved(&cref, &warm, &[]).await?;

        counting.reset();
        let out =
            CellRead::<Values>::read_many(&cached, &id, SECTION, &batch_of(0u8..16)?.as_ref())
                .await
                .map(|cells| {
                    cells
                        .into_iter()
                        .map(|(committed, _)| committed)
                        .collect::<CommittedBatch>()
                })?;
        assert_eq!(out.len(), 16, "every position answered");
        for c in 0u8..16 {
            assert_eq!(
                out[c as usize],
                Committed::new(Some(bytes(c))),
                "warm coordinate {c} serves its value from fjall"
            );
        }
        assert_eq!(counting.lower_reads(), 0, "all-hits reads no point durable");
        assert_eq!(
            counting.batch_cache_reads(),
            0,
            "all-hits issues no lower cache-fill batch"
        );
        Ok(())
    })
}

/// One cold position causes one lower batch read with durable TTLs.
#[test]
fn batch_get_any_miss_is_one_lower_batch_read() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let (cached, counting, id) = counting_cached("batch-any-miss")?;
        let cref = CollectionRef::new(id.clone(), None);
        // Warm coordinates 0..15; leave 15 cold (and durably absent).
        let warm: Vec<(CellKey, Option<Bytes>)> =
            (0u8..15).map(|c| (cell_at(c), Some(bytes(c)))).collect();
        cached.write_resolved(&cref, &warm, &[]).await?;

        counting.reset();
        let out =
            CellRead::<Values>::read_many(&cached, &id, SECTION, &batch_of(0u8..16)?.as_ref())
                .await
                .map(|cells| {
                    cells
                        .into_iter()
                        .map(|(committed, _)| committed)
                        .collect::<CommittedBatch>()
                })?;
        assert_eq!(out.len(), 16, "every position answered");
        for c in 0u8..15 {
            assert_eq!(
                out[c as usize],
                Committed::new(Some(bytes(c))),
                "coordinate {c} serves durable truth"
            );
        }
        assert_eq!(
            out[15],
            Committed::new(None),
            "the cold coordinate is absent"
        );
        assert_eq!(
            counting.batch_cache_reads(),
            1,
            "any-miss refetches the whole batch via one cache-fill read"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "the fill loops the inner store, not this get"
        );
        Ok(())
    })
}

/// Proves that an accepted cache operation completes after cache disablement.
///
/// One disabled-state check controls the complete operation.
#[test]
fn batch_get_completes_after_cache_disablement() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let lower = HoldingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let holds = lower.holds();
        let fjall = test_db::cache("batch-admitted")?;
        let cached = Cached::new(fjall.clone(), lower);
        let id = collection("batch-admitted")?;
        let task = {
            let cached = cached.clone();
            let id = id.clone();
            holds.read().arm(1);
            tokio::spawn(async move {
                CellRead::<Values>::read_many(&cached, &id, SECTION, &batch_of([0])?.as_ref())
                    .await
                    .map(|cells| {
                        cells
                            .into_iter()
                            .map(|(committed, _)| committed)
                            .collect::<CommittedBatch>()
                    })
                    .map_err(color_eyre::Report::from)
            })
        };
        holds.read().entered().await;
        fjall.disable();
        holds.read().release();
        assert_eq!(task.await??.len(), 1);
        assert!(
            fjall.stored_expiry(&id, &cell_at(0)).await?.is_some(),
            "an accepted batch completes its publish after disablement"
        );
        Ok(())
    })
}

/// Proves that one batch miss reloads all values from durable storage.
///
/// A sampled cache hit can be stale after a prior section clear.
#[test]
fn batch_get_discards_sampled_hits_on_any_miss() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(test_db::cache("batch-discard")?, counting.clone());
        let id = collection("batch-discard")?;
        let cref = CollectionRef::new(id.clone(), None);

        // (1) A = V, warm + durable.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(42)))], &[])
            .await?;
        counting
            .write_resolved(&cref, &[(cell_at(0), None)], &[])
            .await?;

        // Batch [A (Hit), B (Miss)]: the miss forces a refetch that discards the
        // sampled A=Some(42) and re-reads post-clear truth — A is absent.
        let out = CellRead::<Values>::read_many(&cached, &id, SECTION, &batch_of([0, 1])?.as_ref())
            .await
            .map(|cells| {
                cells
                    .into_iter()
                    .map(|(committed, _)| committed)
                    .collect::<CommittedBatch>()
            })?;
        assert_eq!(out.len(), 2, "every position answered");
        assert_eq!(
            out[0].get(),
            None,
            "the sampled hit is discarded; A serves post-clear absence, never the stale Some(V)"
        );
        assert_eq!(out[1].get(), None, "B is absent");
        Ok(())
    })
}

/// Proves that a failed cache read does not remove an equal live entry.
#[test]
fn batch_get_failed_publish_keeps_hidden_live_entry_warm() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let fjall = test_db::cache("batch-hidden")?;
        let cached = Cached::new(fjall.clone(), counting.clone());
        let id = collection("batch-hidden")?;
        let cref = CollectionRef::new(id.clone(), None);

        // A = V, warm + durable.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(9)))], &[])
            .await?;
        counting.reset();
        // Probe errors (over the live A entry) AND the publish fails.
        fjall.faults().fail_reads().store(true, Ordering::Relaxed);
        fjall.faults().fail_puts().store(true, Ordering::Relaxed);
        let out = CellRead::<Values>::read_many(&cached, &id, SECTION, &batch_of([0])?.as_ref())
            .await
            .map(|cells| {
                cells
                    .into_iter()
                    .map(|(committed, _)| committed)
                    .collect::<CommittedBatch>()
            })?;
        assert_eq!(out.len(), 1, "the single position answered");
        assert_eq!(
            out[0].get(),
            Some(&bytes(9)),
            "the refetch resolved the durable value"
        );
        assert_eq!(
            counting.batch_cache_reads(),
            1,
            "the probe error fired: a refetch happened (an unfired fault would be an all-hit)"
        );

        // Heal both faults. The hidden live entry (V) survived untouched, so the
        // next point get is a warm hit with zero lower reads.
        fjall.faults().fail_reads().store(false, Ordering::Relaxed);
        fjall.faults().fail_puts().store(false, Ordering::Relaxed);
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(9)),
            "the failed publish deleted nothing: A stayed warm"
        );
        assert_eq!(counting.lower_reads(), 0, "A was warm — no fall-through");
        Ok(())
    })
}
