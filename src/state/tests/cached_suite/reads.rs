//! Cached point reads: expiry refills, cached absence and presence, and load
//! metrics.

use super::*;

/// TTL co-expiry over the real `Cached` with a verified [`Clock`]: a value
/// written through a short-TTL collection is served from fjall while live, then
/// — after the clock advances **past its floor expiry to a sub-second instant**
/// — the expired entry reads as a miss, the get **falls through** to the lower
/// store, yields its current answer, and **re-stamps the fjall entry to
/// `floor(now) + remaining`** (the co-expiry invariant). The lower store
/// reports a live `TTL(data)`-style remaining ([`TtlAwareCellStore`]), so the
/// floored re-stamp is exercised and asserted `≤` the row's death. No sleep;
/// the clock is advanced directly.
///
/// Example test by necessity: the hit-vs-fall-through decision turns on a
/// sub-second clock crossing whose counter grain sits below the model's
/// abstraction, so the fall-through test cannot be generalized into the
/// generator.
#[test]
fn expired_entry_reads_as_miss_and_refills() -> Result<()> {
    // A sub-second instant past the floor expiry (6_000), so the re-stamp's floor
    // sheds the 500 ms remainder; the lower row dies at 30_000 (`TTL(data)`).
    const NOW_EXPIRED: u64 = 6_500;
    const ROW_DEATH: u64 = 30_000;

    TEST_RUNTIME.block_on(async {
        let now = Arc::new(AtomicU64::new(1_000));
        let cells = MemoryCells::new();
        let lower = TtlAwareCellStore::new(
            CountingCellStore::new(MemoryCellStore::new(cells)),
            Clock::Fixed(now.clone()),
            ROW_DEATH,
        );
        let cached = Cached::new(
            test_db::cache_with_clock("ttl", Clock::Fixed(now.clone()))?,
            lower.clone(),
        );
        let id = collection("ttl")?;
        // A 5-second TTL: write-through stamps `floor(now) + 5s`.
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(5)));

        // Write through coordinate 7; it is now warm and served from fjall.
        cached
            .write_resolved(&cref, &[(cell_at(7), Some(bytes(7)))], &[])
            .await?;
        lower.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(7).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(7)),
            "a live warm value serves from fjall"
        );
        assert_eq!(lower.lower_reads(), 0, "a live warm get reads nothing");

        // Advance the clock past the floor expiry (1_000 + 5_000 = 6_000ms).
        now.store(NOW_EXPIRED, Ordering::Relaxed);
        // Rewrite the durable value WITHOUT going through the cache, so fjall
        // still holds the stale (now-expired) `7` while the lower store holds
        // `70`. The expired get must fall through and yield `70`.
        lower
            .write_resolved(&cref, &[(cell_at(7), Some(bytes(70)))], &[])
            .await?;
        lower.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(7).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(70)),
            "an expired get falls through to the fresh durable value"
        );
        assert!(
            lower.lower_reads() > 0,
            "the expired get must read the lower store"
        );

        // The fall-through re-stamped the fjall entry to `floor(now) + remaining`,
        // flooring the sub-second remainder so it never overhangs the row death.
        let remaining_ms = ((ROW_DEATH - NOW_EXPIRED) / 1_000) * 1_000;
        let want_expiry = NOW_EXPIRED - NOW_EXPIRED % 1_000 + remaining_ms;
        let stamped = cached.stored_expiry(&id, &cell_at(7)).await?;
        assert_eq!(
            stamped,
            Some(want_expiry),
            "the re-stamped fjall expiry must be floor(now) + remaining"
        );
        assert!(
            stamped.is_some_and(|e| e <= ROW_DEATH),
            "the re-stamped expiry must not overhang the durable row death"
        );

        lower
            .write_resolved(&cref, &[(cell_at(8), Some(bytes(8)))], &[])
            .await?;
        lower.reset();
        CellRead::<Presence>::read_many(&cached, &id, SECTION, &batch_of([8, 9])?.as_ref()).await?;
        assert_eq!(
            lower.inner.presence_reads(),
            1,
            "the expiry comes from a presence read"
        );
        assert_eq!(
            lower.inner.batch_cache_reads(),
            0,
            "the fill does not fetch values"
        );
        for key in [8, 9] {
            let stamped = cached.stored_expiry(&id, &cell_at(key)).await?;
            assert_eq!(
                stamped,
                Some(want_expiry),
                "presence uses the remaining TTL"
            );
            assert!(
                stamped.is_some_and(|expiry| expiry <= ROW_DEATH),
                "presence expires before the durable row"
            );
        }

        // The fall-through re-published a fresh entry; a get now serves `70`
        // from fjall again (KV5 restored).
        lower.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(7).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(70)),
            "the re-published value serves from fjall"
        );
        assert_eq!(lower.lower_reads(), 0, "the re-published get reads nothing");
        Ok(())
    })
}

/// KV2 negative caching: two gets of a never-written cell issue exactly one
/// lower read — the first falls through and publishes the Absent tag, the
/// second is a warm absent hit.
#[test]
fn absent_get_is_cached() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(test_db::cache("absent")?, counting.clone());
        let id = collection("absent")?;

        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(9).as_ref())
                .await?
                .0
                .get(),
            None
        );
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(9).as_ref())
                .await?
                .0
                .get(),
            None
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "two gets of an absent cell pay exactly one durable read"
        );
        Ok(())
    })
}

/// Two presence reads share one fill for present and absent cells.
#[test]
fn presence_is_cached() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let (cached, counting, id) = counting_cached("presence")?;
        counting
            .write_resolved(
                &CollectionRef::new(id.clone(), None),
                &[(cell_at(1), Some(bytes(1)))],
                &[],
            )
            .await?;
        let batch = batch_of([1, 2])?;
        counting.reset();
        for _ in 0_u8..2 {
            assert_eq!(
                CellRead::<Presence>::read_many(&cached, &id, SECTION, &batch.as_ref())
                    .await
                    .map(|cells| cells
                        .into_iter()
                        .map(|(committed, _)| committed.get().is_some())
                        .collect::<CellBuffer<bool>>())?,
                CellBuffer::from_iter([true, false])
            );
            assert_eq!(
                counting.presence_reads(),
                1,
                "presence reads share one presence fill"
            );
            assert_eq!(counting.lower_reads(), 0, "presence does not read values");
            assert_eq!(
                counting.batch_cache_reads(),
                0,
                "presence does not fetch a value batch"
            );
        }
        Ok(())
    })
}

/// A blown fuse bypasses a stale cache entry for a presence batch.
#[test]
fn blown_fuse_presence_reads_durable_truth() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let fjall = test_db::cache("presence-fuse")?;
        let cached = Cached::new(fjall.clone(), counting.clone());
        let id = collection("presence-fuse")?;
        let collection = CollectionRef::new(id.clone(), None);
        cached
            .write_resolved(&collection, &[(cell_at(1), Some(bytes(1)))], &[])
            .await?;
        counting
            .write_resolved(&collection, &[(cell_at(1), None)], &[])
            .await?;
        fjall.disable();
        counting.reset();

        assert_eq!(
            CellRead::<Presence>::read_many(&cached, &id, SECTION, &batch_of([1])?.as_ref())
                .await
                .map(|cells| cells
                    .into_iter()
                    .map(|(committed, _)| committed.get().is_some())
                    .collect::<CellBuffer<bool>>())?,
            CellBuffer::from_iter([false]),
        );
        assert_eq!(counting.presence_reads(), 1, "the fused read delegates");
        Ok(())
    })
}

/// Cell-load metrics count logical cells. They distinguish cache answers from
/// durable fallbacks without labels that contain user identities.
#[test]
fn cell_load_metrics_report_source_and_cache_result() -> Result<()> {
    let metrics = GlobalMetrics::install();
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let fjall = test_db::cache("cell-load-metrics")?;
        let fail_puts = fjall.faults().fail_puts();
        let cached = Cached::new(fjall, counting).with_metrics(metrics.cell_metrics());
        let id = collection("cell-load-metrics")?;

        CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref()).await?;
        CellRead::<Values>::read(&cached, &id, cell_at(1).as_ref()).await?;
        let batch = batch_of(2u8..5)?;
        CellRead::<Values>::read_many(&cached, &id, SECTION, &batch.as_ref()).await?;
        CellRead::<Values>::read_many(&cached, &id, SECTION, &batch.as_ref()).await?;

        let presence_batch = batch_of(5u8..8)?;
        CellRead::<Presence>::read_many(&cached, &id, SECTION, &presence_batch.as_ref()).await?;
        CellRead::<Presence>::read_many(&cached, &id, SECTION, &presence_batch.as_ref()).await?;

        let points = metrics.points("prosody.state.cell.loads")?;
        assert_eq!(
            points,
            [
                (("get", "values", "cache", "hit"), 1),
                (("get_many", "presence", "cache", "hit"), 3),
                (("get_many", "values", "cache", "hit"), 3),
                (("get", "values", "store", "miss"), 1),
                (("get_many", "presence", "store", "not_all_hit"), 3),
                (("get_many", "values", "store", "not_all_hit"), 3),
            ]
            .into_iter()
            .map(|((operation, projection, source, result), value)| {
                (
                    labels([
                        ("prosody.state.cell.cache.result", result),
                        ("prosody.state.cell.operation.name", operation),
                        ("prosody.state.cell.projection", projection),
                        ("prosody.state.cell.load.source", source),
                    ]),
                    value,
                )
            })
            .collect::<Vec<_>>()
        );
        assert_eq!(
            metrics.points("prosody.state.cell.load.duration")?,
            points
                .into_iter()
                .map(|(attributes, _)| (attributes, 1))
                .collect::<Vec<_>>()
        );
        assert!(metrics.is_exponential_histogram("prosody.state.cell.load.duration")?);
        metrics.metrics().request_latency.record(0.01_f64, &[]);
        assert!(metrics.is_exponential_histogram("prosody.request.duration")?);

        fail_puts.store(true, Ordering::Relaxed);
        CellRead::<Values>::read(&cached, &id, cell_at(9).as_ref()).await?;
        assert_eq!(
            metrics.points("prosody.state.cell.cache.errors")?,
            vec![(
                labels([
                    ("prosody.state.cell.cache.phase", "fill"),
                    ("prosody.state.cell.operation.name", "get"),
                ]),
                1,
            )]
        );

        let failed_metrics = GlobalMetrics::install();
        let failed_lower = FailingCellStore::failing_read(
            MemoryCellStore::new(MemoryCells::new()),
            BTreeMap::from([(8, ErrorCategory::Transient)]),
        );
        let failed = Cached::new(test_db::cache("cell-load-error-metrics")?, failed_lower)
            .with_metrics(failed_metrics.cell_metrics());
        let failed_id = collection("cell-load-error-metrics")?;
        assert!(
            CellRead::<Values>::read(&failed, &failed_id, cell_at(8).as_ref())
                .await
                .is_err()
        );
        assert_eq!(
            failed_metrics.points("prosody.state.cell.load.duration")?,
            vec![(
                labels([
                    ("prosody.error.category", "transient"),
                    ("prosody.state.cell.projection", "values"),
                    ("prosody.state.cell.cache.result", "miss"),
                    ("prosody.state.cell.load.source", "store"),
                    ("prosody.state.cell.operation.name", "get"),
                ]),
                1,
            )]
        );
        Ok(())
    })
}
