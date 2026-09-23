//! Cache reads: expiry, one-hop batch probes, and corrupt-frame repair.

use super::*;

/// An expired present entry reads back as a miss (`None`) under a clock
/// advanced past its stamped expiry; the same entry with a `0`-never expiry, or
/// read at a time before expiry, stays a hit. Drives the read-side TTL check
/// with a deterministic [`Clock::Fixed`], no sleep.
#[test]
fn expired_entry_reads_as_miss() -> Result<()> {
    let now = Arc::new(AtomicU64::new(1_000));
    let cache = test_db::cache_with_clock("ttl_value", Clock::Fixed(now.clone()))?;
    let c = fresh_collection("ttl")?;
    let cell = value_cell();
    let payload = Committed::<Values>::new(Some(Bytes::from_static(b"v")));

    TEST_RUNTIME.block_on(async {
        // Stamp an entry that expires at 2_000ms.
        cache
            .put::<Values>(&c, cell.as_ref(), payload.clone(), 2_000)
            .await?;
        // Before expiry: a hit.
        assert!(
            matches!(
                cache.get::<Values>(&c, cell.as_ref()).await?,
                CacheRead::Hit(_)
            ),
            "live entry must hit"
        );
        // At/after expiry: reported Expired (an entry exists, floor-expired).
        now.store(2_000, Ordering::Relaxed);
        assert!(
            matches!(
                cache.get::<Values>(&c, cell.as_ref()).await?,
                CacheRead::Expired
            ),
            "expired entry must read as Expired"
        );
        // A `never` (0) expiry never expires, even far in the future.
        cache
            .put::<Values>(&c, cell.as_ref(), payload.clone(), 0)
            .await?;
        now.store(u64::MAX, Ordering::Relaxed);
        assert!(
            matches!(
                cache.get::<Values>(&c, cell.as_ref()).await?,
                CacheRead::Hit(_)
            ),
            "a never-expiry entry must always hit"
        );
        Ok::<_, Report>(())
    })?;
    Ok(())
}

/// Single-hop probe: a full `CELL_BATCH`-wide `get_batch` over 16 warm cells
/// launches EXACTLY ONE blocking closure and returns every value present.
///
/// The `probe_hops` counter is bumped INSIDE `get_batch`'s `spawn_blocking`
/// closure (the seam, not the method boundary), so a rewrite that loops
/// `read_cell` per key runs 16 closures and reddens `probe_hops() == 1` — a
/// method-boundary counter would false-pass that per-key loop.
#[test]
fn get_batch_probes_the_whole_batch_in_one_blocking_hop() -> Result<()> {
    let cache = test_db::cache("get_batch_hop")?;
    let c = fresh_collection("one-hop")?;

    TEST_RUNTIME.block_on(async {
        // Each coordinate holds a DISTINCT payload (its own byte), so a
        // scattered/reversed result reddens the index-aligned asserts below —
        // identical payloads would false-pass a reorder bug.
        for b in 0..u8::try_from(CELL_BATCH.get()).unwrap_or(u8::MAX) {
            cache
                .put::<Values>(
                    &c,
                    batch_cell(b).as_ref(),
                    Committed::new(Some(bytes(b))),
                    0,
                )
                .await?;
        }
        let batch = batch_of(0..u8::try_from(CELL_BATCH.get()).unwrap_or(u8::MAX))?;
        let hits = cache
            .get_batch::<Values>(&c, Section::new(0), &batch.as_ref())
            .await?;
        assert_eq!(hits.len(), CELL_BATCH.get(), "every position answered");
        for (i, hit) in hits.iter().enumerate() {
            let want = bytes(u8::try_from(i).unwrap_or(u8::MAX));
            let CacheRead::Hit((committed, _)) = hit else {
                return Err(eyre!("every warm position must hit"));
            };
            assert_eq!(
                committed.get(),
                Some(&want),
                "position {i} serves its own coordinate's value, index-aligned"
            );
        }
        assert_eq!(
            cache.faults().probe_hops(),
            1,
            "the whole batch cost exactly one blocking hop"
        );
        Ok::<_, Report>(())
    })?;
    Ok(())
}

/// Batch probes preserve each position's value, presence, expiry, and error.
#[test]
fn get_batch_classifies_hits_misses_expiry_and_errors() -> Result<()> {
    let now = Arc::new(AtomicU64::new(1_000));
    let cache = test_db::cache_with_clock("get_batch_classify", Clock::Fixed(now.clone()))?;
    let c = fresh_collection("classify")?;
    let present = Committed::<Values>::new(Some(Bytes::from_static(b"v")));

    TEST_RUNTIME.block_on(async {
        // (1) Two present coordinates with DISTINCT payloads: an all-hit batch,
        // each position index-aligned to its own coordinate's value.
        for index in [0, 1] {
            cache
                .put::<Values>(
                    &c,
                    batch_cell(index).as_ref(),
                    Committed::new(Some(bytes(index))),
                    0,
                )
                .await?;
        }
        let hits = cache
            .get_batch::<Values>(&c, Section::new(0), &batch_of([0, 1])?.as_ref())
            .await?;
        assert_eq!(hits.len(), 2, "both positions answered");
        for (i, hit) in hits.iter().enumerate() {
            let CacheRead::Hit((committed, _)) = hit else {
                return Err(eyre!("every warm position must hit"));
            };
            assert_eq!(committed.get(), Some(&bytes(u8::try_from(i)?)));
        }

        cache
            .put::<Presence>(&c, batch_cell(5).as_ref(), Committed::new(Some(())), 3_000)
            .await?;
        assert!(matches!(
            cache.get::<Values>(&c, batch_cell(5).as_ref()).await?,
            CacheRead::Miss
        ));
        let CacheRead::Hit((presence, ttl)) =
            cache.get::<Presence>(&c, batch_cell(5).as_ref()).await?
        else {
            return Err(eyre!("presence frame must answer presence"));
        };
        assert_eq!(presence.get(), Some(&()));
        assert_eq!(ttl, Some(CompactDuration::new(2)));
        assert_eq!(cache.stored_expiry(&c, &batch_cell(5)).await?, Some(3_000));
        let hits = cache
            .get_batch::<Presence>(&c, Section::new(0), &batch_of([0, 5])?.as_ref())
            .await?;
        assert!(
            hits.iter().all(
                |hit| matches!(hit, CacheRead::Hit((committed, _)) if committed.get().is_some())
            )
        );

        // (2) A coordinate that was never put: the batch misses.
        assert!(
            cache
                .get_batch::<Values>(&c, Section::new(0), &batch_of([0, 2])?.as_ref())
                .await?
                .iter()
                .any(|hit| matches!(hit, CacheRead::Miss)),
            "an unwritten coordinate makes the batch a miss"
        );

        // (3) A floor-expired entry (stamped at 500, clock at 1_000): refetch.
        cache
            .put::<Values>(&c, batch_cell(3).as_ref(), present.clone(), 500)
            .await?;
        assert!(
            cache
                .get_batch::<Values>(&c, Section::new(0), &batch_of([0, 3])?.as_ref())
                .await?
                .iter()
                .any(|hit| matches!(hit, CacheRead::Expired)),
            "a floor-expired entry makes the batch a miss, never a stale hit"
        );

        // (4) The injected read fault errors the whole hop.
        cache.faults().fail_reads().store(true, Ordering::Relaxed);
        assert!(
            cache
                .get_batch::<Values>(&c, Section::new(0), &batch_of([0, 1])?.as_ref())
                .await
                .is_err(),
            "an engine read fault degrades the batch to Err"
        );
        cache.faults().fail_reads().store(false, Ordering::Relaxed);

        check_corrupt_repair(&cache, &c).await?;

        now.store(3_000, Ordering::Relaxed);
        assert!(matches!(
            cache.get::<Values>(&c, batch_cell(5).as_ref()).await?,
            CacheRead::Expired
        ));
        let probes = cache
            .get_batch::<Values>(&c, Section::new(0), &batch_of([0, 5])?.as_ref())
            .await?;
        assert!(matches!(
            probes.as_slice(),
            [CacheRead::Hit(_), CacheRead::Expired]
        ));
        Ok::<_, Report>(())
    })?;
    Ok(())
}

/// A corrupt position does not change its neighbors. A fill repairs it.
async fn check_corrupt_repair(cache: &FjallCellCache, c: &CollectionId) -> Result<()> {
    cache
        .inner
        .handle()
        .insert(cell_key(c, batch_cell(4).as_ref()).as_slice(), [0x05; 9])?;
    assert!(matches!(
        cache.get::<Values>(c, batch_cell(4).as_ref()).await?,
        CacheRead::Corrupt
    ));
    let probes = cache
        .get_batch::<Values>(c, Section::new(0), &batch_of([0, 4, 2, 3])?.as_ref())
        .await?;
    assert!(matches!(
        probes.as_slice(),
        [
            CacheRead::Hit(_),
            CacheRead::Corrupt,
            CacheRead::Miss,
            CacheRead::Expired
        ]
    ));

    let cached = Cached::new(cache.clone(), MemoryCellStore::new(MemoryCells::new()));
    CellRead::<Values>::read(&cached, c, batch_cell(4).as_ref()).await?;
    assert!(matches!(
        cache.get::<Values>(c, batch_cell(4).as_ref()).await?,
        CacheRead::Hit((value, _)) if value.get().is_none()
    ));
    cache
        .inner
        .handle()
        .insert(cell_key(c, batch_cell(4).as_ref()).as_slice(), [0x05; 9])?;
    CellRead::<Presence>::read_many(&cached, c, Section::new(0), &batch_of([0, 4])?.as_ref())
        .await?;
    assert!(matches!(
        cache.get::<Values>(c, batch_cell(4).as_ref()).await?,
        CacheRead::Hit((value, _)) if value.get().is_none()
    ));
    assert!(matches!(
        cache.get::<Values>(c, batch_cell(0).as_ref()).await?,
        CacheRead::Hit((value, _)) if value.get() == Some(&bytes(0))
    ));

    Ok(())
}
