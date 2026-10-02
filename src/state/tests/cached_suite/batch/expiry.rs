//! Cached batch reads at expiry boundaries.

use super::*;

/// T-g expired-probe refetch: `get_batch` must classify a floor-expired entry
/// as not-a-hit so the batch refetches durable truth, never serving the stale
/// value. Pins `get_batch`'s own classification (distinct from
/// `expired_entry_reads_as_miss`, which tests the point `FjallCellCache::get`).
///
/// Classifying Expired as a hit makes this test fail: the batch would
/// serve the stale V1 instead of the fresh durable V2.
#[test]
fn batch_get_treats_expired_probe_as_refetch() -> Result<()> {
    const T0: u64 = 1_000;
    const NOW_EXPIRED: u64 = 7_000;

    TEST_RUNTIME.block_on(async {
        let now = Arc::new(AtomicU64::new(T0));
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(
            test_db::cache_with_clock("batch-expired", Clock::Fixed(now.clone()))?,
            counting.clone(),
        );
        let id = collection("batch-expired")?;
        // A 5-second TTL: the fjall entry is stamped floor(T0)+5s = 6_000.
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(5)));

        // Warm A = V1 through the cache; it now holds a stamped fjall entry.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        // Advance past the floor expiry and rewrite durable A = V2 WITHOUT the
        // cache, so fjall holds the stale (expired) V1 while durable holds V2.
        now.store(NOW_EXPIRED, Ordering::Relaxed);
        counting
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(2)))], &[])
            .await?;

        let out = CellRead::<Values>::read_many(&cached, &id, SECTION, &batch_of([0])?.as_ref())
            .await
            .map(|cells| {
                cells
                    .into_iter()
                    .map(|(committed, _)| committed)
                    .collect::<CellBuffer<Committed>>()
            })?;
        assert_eq!(
            out[0].get(),
            Some(&bytes(2)),
            "the expired probe refetches durable truth (V2), never the stale V1"
        );
        Ok(())
    })
}

/// Expiry-boundary degrade test: a warm entry that expires WHILE a degraded
/// batch's delayed lower read is in flight, whose fill publish then FAILS
/// (no-delete degrade), must never be served on a later read — the surviving
/// stale entry re-classifies as expired and refetches durable truth again.
///
/// Parameterized over both refetch triggers the plan names:
/// * a sampled Hit discarded because a second COLD position misses the probe;
/// * an error-probe over the live entry (an injected fjall read fault).
///
/// The failed publish leaves the (now-expired) warm V1 on disk. Classifying
/// Expired as a hit, or a degrade path that re-stamps the surviving entry with
/// a fresh live expiry, would serve the stale V1 on the healed point read →
/// red. (Distinct from `batch_get_treats_expired_probe_as_refetch`, which
/// advances the clock before the call with a succeeding publish; here the entry
/// crosses its floor expiry DURING a parked read and the publish fails.)
#[test]
fn batch_get_expiry_boundary_degrade_never_serves_stale() -> Result<()> {
    /// One case: `coords` is the batch (target is coord 0); `error_probe` arms
    /// a fjall read fault so the whole probe errors instead of a cold miss.
    async fn degrade_case(name: &str, coords: &[u8], error_probe: bool) -> Result<()> {
        const T0: u64 = 1_000;
        const AFTER_EXPIRY: u64 = 7_000; // past floor(T0)+5s = 6_000
        const TTL_SECS: u32 = 5;

        let now = Arc::new(AtomicU64::new(T0));
        let clock = Clock::Fixed(now.clone());
        let lower = HoldingCellStore::new(CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
        )));
        let holds = lower.holds();
        let fjall = test_db::cache_with_clock(name, clock)?;
        let cached = Cached::new(fjall.clone(), lower.clone());
        let id = collection(name)?;
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(TTL_SECS)));

        // Warm the target (coord 0) = V1 through the cache: fjall now holds a
        // live entry stamped floor(T0)+5s = 6_000; durable is also V1.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        // Rewrite durable truth to V2 through the lower store ONLY, so a refetch
        // resolves V2 while fjall still holds the stale warm V1.
        lower
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(2)))], &[])
            .await?;

        // The fill's publish fails (no-delete degrade); optionally the probe
        // errors instead of taking a cold-miss refetch.
        fjall.faults().fail_puts().store(true, Ordering::Relaxed);
        if error_probe {
            fjall.faults().fail_reads().store(true, Ordering::Relaxed);
        }

        // Park the refetch's fill after the target's durable read lands, advance
        // the clock past the entry's floor expiry while parked, then resume: the
        // warm entry expires DURING the delayed lower read.
        holds.read().arm(1);
        let batch = batch_of(coords.iter().copied())?;
        let task = tokio::spawn({
            let cached = cached.clone();
            let id = id.clone();
            async move {
                CellRead::<Values>::read_many(&cached, &id, SECTION, &batch.as_ref())
                    .await
                    .map(|cells| {
                        cells
                            .into_iter()
                            .map(|(committed, _)| committed)
                            .collect::<CellBuffer<Committed>>()
                    })
                    .map_err(|error| eyre!("{error:?}"))
            }
        });
        holds.read().entered().await;
        now.store(AFTER_EXPIRY, Ordering::Relaxed);
        holds.read().release();
        let out = task.await??;
        assert_eq!(
            out[0].get(),
            Some(&bytes(2)),
            "the degraded refetch serves durable V2, never the stale warm V1"
        );

        // Heal the faults, then assert before any further write that the failed
        // publish neither deleted nor re-stamped the surviving entry: it is
        // still V1's original floor stamp (6_000), independent of its payload.
        // A degrade that re-stamped it live, or deleted it, makes this test fail here.
        fjall.faults().fail_puts().store(false, Ordering::Relaxed);
        fjall.faults().fail_reads().store(false, Ordering::Relaxed);
        assert_eq!(
            fjall.stored_expiry(&id, &cell_at(0)).await?,
            Some(6_000),
            "the failed publish left the stale entry's original stamp untouched"
        );

        // The surviving (now-expired) V1 entry must not be served: the point
        // read re-classifies it Expired and refetches V2.
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(2)),
            "the surviving expired entry is never served; the point read refetches V2"
        );
        Ok(())
    }

    TEST_RUNTIME.block_on(async {
        // (i) sampled Hit at coord 0 discarded because cold coord 1 misses.
        degrade_case("batch-degrade-hit", &[0, 1], false).await?;
        // (ii) an injected read fault errors the probe over the live entry.
        degrade_case("batch-degrade-err", &[0], true).await?;
        Ok(())
    })
}

/// T-h negative caching: an Absent entry is published only from a fully
/// successful batch. The positive arm proves absence IS cached (one durable
/// read for two reads); the erroring arm proves a batch that errors mid-fill
/// publishes NOTHING (A, read successfully before B errored, is not cached).
///
/// Filtering absent positions out of the publish makes this test fail the
/// positive arm (the second read misses). A mixed-merge that publishes
/// per-position as results arrive would red the erroring arm (A cached before B
/// errored).
#[test]
fn batch_get_publishes_absence_only_from_successful_batch() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        // ---- Positive arm: absence is cached from a successful batch. -------
        let (cached, counting, id) = counting_cached("batch-neg-ok")?;
        counting.reset();
        let out = CellRead::<Values>::read_many(&cached, &id, SECTION, &batch_of([0])?.as_ref())
            .await
            .map(|cells| {
                cells
                    .into_iter()
                    .map(|(committed, _)| committed)
                    .collect::<CellBuffer<Committed>>()
            })?;
        assert_eq!(out[0].get(), None, "the never-written cell is absent");
        assert_eq!(
            counting.batch_cache_reads(),
            1,
            "the first batch paid one cache-fill read"
        );
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            None,
            "the second read serves the cached Absent tag"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "absence was cached: the point get reads nothing"
        );

        // ---- Erroring arm: a mid-fill error publishes nothing. --------------
        let counting_b = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let failing = FailingCellStore::failing_read(
            counting_b.clone(),
            BTreeMap::from([(1u8, ErrorCategory::Transient)]),
        );
        let cached_b = Cached::new(test_db::cache("batch-neg-err")?, failing.clone());
        let id_b = collection("batch-neg-err")?;

        // A (coord 0) is absent; B (coord 1) is poisoned. The default fill loops
        // read, reads A, then errors on B — so put_batch never runs.
        let err =
            CellRead::<Values>::read_many(&cached_b, &id_b, SECTION, &batch_of([0, 1])?.as_ref())
                .await
                .map(|cells| {
                    cells
                        .into_iter()
                        .map(|(committed, _)| committed)
                        .collect::<CellBuffer<Committed>>()
                });
        assert!(
            err.is_err(),
            "a poisoned fill position fails the whole batch"
        );

        // Disarm and reset: if A had been cached by the errored batch its point
        // get would read nothing; it was NOT, so it refetches once.
        failing.set_poison(None);
        counting_b.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached_b, &id_b, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            None,
            "A resolves absent on the healed read"
        );
        assert_eq!(
            counting_b.lower_reads(),
            1,
            "the errored batch cached nothing: A is refetched once"
        );
        Ok(())
    })
}

/// T-c co-anchor property: for any within-second start offset and any delay ≥
/// 1s on the lower response, the expiry a batch fill stamps equals `floor(T0) +
/// remaining(T0)` and never overhangs the durable row death — even though the
/// clock advances while the fill's response is parked. The stamp is anchored
/// before the lower read, so a slow resolution can only stamp early.
///
/// Moving the anchor to after `read_many` makes this test fail:
/// for a delay crossing a second, `floor(T0 + delay) + remaining > death`.
#[test]
fn prop_batch_fill_expiry_never_overhangs() {
    #[derive(Clone, Copy, Debug)]
    struct Timing {
        t0_offset: u16,
        delay_ms: u16,
    }

    impl Arbitrary for Timing {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                t0_offset: u16::arbitrary(g),
                // ≥ one full second so the delay can cross a floor boundary.
                delay_ms: 1_000 + (u16::arbitrary(g) % 11_000),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let base = *self;
            Box::new(
                self.t0_offset
                    .shrink()
                    .map(move |t0_offset| Timing { t0_offset, ..base }),
            )
        }
    }

    fn property(timing: Timing) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            const START: u64 = 1_000;
            // A far, second-aligned death so `remaining` is always positive.
            const DEATH: u64 = 10_000_000;
            let t0 = START + u64::from(timing.t0_offset);
            let delay = u64::from(timing.delay_ms);
            let now = Arc::new(AtomicU64::new(t0));
            let clock = Clock::Fixed(now.clone());
            let lower = HoldingCellStore::new(TtlAwareCellStore::new(
                CountingCellStore::new(MemoryCellStore::new(MemoryCells::new())),
                clock.clone(),
                DEATH,
            ));
            let holds = lower.holds();
            let cached = Cached::new(
                test_db::cache_with_clock("batch-anchor", clock)?,
                lower.clone(),
            );
            let id = collection("batch-anchor")?;
            let cref = CollectionRef::new(id.clone(), None);

            // A present, durable, and COLD (written through the lower store, so
            // the cache probes a miss and refetches).
            lower
                .write_resolved(&cref, &[(cell_at(7), Some(bytes(7)))], &[])
                .await?;

            // Spawn a singleton batch fill; park its lower response.
            holds.read().arm(1);
            let task = tokio::spawn({
                let cached = cached.clone();
                let id = id.clone();
                async move {
                    let batch = batch_of([7])?;
                    CellRead::<Values>::read_many(&cached, &id, SECTION, &batch.as_ref())
                        .await
                        .map(|cells| {
                            cells
                                .into_iter()
                                .map(|(committed, _)| committed)
                                .collect::<CellBuffer<Committed>>()
                        })
                        .map_err(|error| eyre!("{error:?}"))
                }
            });
            holds.read().entered().await;
            // Advance the clock while the fill's response is parked, then resume.
            now.store(t0 + delay, Ordering::Relaxed);
            holds.read().release();
            task.await??;

            // The remaining computed at T0, floored to Cassandra's second grain.
            let remaining_ms = ((DEATH - t0) / 1_000) * 1_000;
            let want = (t0 - t0 % 1_000) + remaining_ms;
            let got = cached.stored_expiry(&id, &cell_at(7)).await?;
            Ok(got == Some(want) && got.is_some_and(|e| e != 0 && e <= DEATH))
        })
    }

    QuickCheck::new().quickcheck(property as fn(Timing) -> Result<bool>);
}
