//! Promote publication and the commit transform of staged cells.

use super::*;

/// Stages three cells with `data` = 100+c over committed base `c`.
/// Returns the staged writes and their marker.
pub(super) async fn stage_committed_marker<L>(
    cached: &Cached<L>,
    cref: &CollectionRef,
    dedup: u128,
) -> Result<(Vec<(CellKey, ProvisionalWrite)>, EventMarker)>
where
    L: CellStore,
{
    let id = cref.id();
    let event = probe(dedup);
    for c in [1u8, 2, 3] {
        cached
            .write_resolved(cref, &[(cell_at(c), Some(bytes(c)))], &[])
            .await?;
    }
    let mut writes = Vec::new();
    for c in [1u8, 2, 3] {
        let prev = CellRead::<Values>::read(cached, id, cell_at(c).as_ref())
            .await?
            .0;
        writes.push((
            cell_at(c),
            ProvisionalWrite::new(Some(bytes(100 + c)), prev, event),
        ));
    }
    let marker = EventMarker::frozen(event, &writes, Vec::new(), &evidence([].into(), None));
    cached
        .write_provisional(cref, listed(&marker, &writes)?)
        .await?;

    Ok((writes, marker))
}

/// Proves that the cache publishes only after the durable response returns.
///
/// Failed and cancelled responses remove the affected cache entries.
#[test]
fn promote_publishes_after_durable_write() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        // ---- Window (a): the lower promote fails. --------------------------
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let handle: PoisonHandle = Arc::default();
        let cached = Cached::new(
            test_db::cache("d5_precall")?,
            FailingCellStore::with_handle(counting.clone(), handle.clone()),
        );
        let id = collection("d5-precall")?;
        let cref = CollectionRef::new(id.clone(), None);
        let (writes, marker) = stage_committed_marker(&cached, &cref, 1).await?;

        *handle.lock() = Some(Poison::Collection(
            id.name().clone(),
            ErrorCategory::Transient,
        ));
        let result = cached.commit_provisional(&cref, &marker, &writes).await;
        assert!(result.is_err(), "the poisoned lower promote must surface");
        *handle.lock() = None;

        // The failed promote evicts each entry. Lower reads return the base.
        counting.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, cell_at(c).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(c)),
                "a failed promote preserves the committed base"
            );
        }
        assert_eq!(
            counting.lower_reads(),
            3,
            "the failed promote evicts every staged cell"
        );

        // ---- Window (b): the settle future is DROPPED after the lower batch
        // landed (response withheld, then the task aborted). -----------------
        let counting_b = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let holding = HoldingCellStore::new(counting_b.clone());
        let holds = holding.holds();
        let cached_b = Cached::new(test_db::cache("d5_drop")?, holding);
        let id_b = collection("d5-drop")?;
        let cref_b = CollectionRef::new(id_b.clone(), None);
        let (writes_b, marker_b) = stage_committed_marker(&cached_b, &cref_b, 2).await?;

        holds.commit_provisional().arm(1);
        let landed_before = holds.commit_provisional().landed();
        let task = tokio::spawn({
            let cached_b = cached_b.clone();
            let cref_b = cref_b.clone();
            let writes_b = writes_b.clone();
            async move {
                cached_b
                    .commit_provisional(&cref_b, &marker_b, &writes_b)
                    .await
            }
        });
        // Inspect the cache while the lower response remains blocked.
        holds.commit_provisional().entered().await;
        assert!(
            holds.commit_provisional().landed() > landed_before,
            "the lower batch landed before the drop"
        );
        counting_b.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                CellRead::<Values>::read(&cached_b, &id_b, cell_at(c).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(c)),
                "the cache preserves the prior projection until the response returns"
            );
        }
        assert_eq!(counting_b.lower_reads(), 0, "the prior values stay cached");

        task.abort();
        assert!(task.await.is_err(), "the settle future was dropped");

        counting_b.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                CellRead::<Values>::read(&cached_b, &id_b, cell_at(c).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(100 + c)),
                "cell {c} serves the committed data across the dropped settle"
            );
        }
        assert_eq!(
            counting_b.lower_reads(),
            3,
            "a dropped response cannot publish to the cache"
        );

        Ok(())
    })
}

/// Proves that a failed settlement cache update removes the affected entries.
///
/// The function must return the durable-store result.
#[test]
fn d5_transform_batch_failure_degrades_to_delete() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let handle: PoisonHandle = Arc::default();
        let fjall = test_db::cache("d5_fallback")?;
        let fail_puts = fjall.faults().fail_puts();
        let cached = Cached::new(
            fjall,
            FailingCellStore::with_handle(counting.clone(), handle.clone()),
        );
        let id = collection("d5-fallback")?;
        let cref = CollectionRef::new(id.clone(), None);
        let (writes, marker) = stage_committed_marker(&cached, &cref, 3).await?;

        // Poisoned lower + failed transform: the POISON surfaces, verbatim.
        fail_puts.store(true, Ordering::Relaxed);
        *handle.lock() = Some(Poison::Collection(
            id.name().clone(),
            ErrorCategory::Transient,
        ));
        let result = cached.commit_provisional(&cref, &marker, &writes).await;
        assert!(
            matches!(result, Err(ref e) if format!("{e}").contains("poison")),
            "the lower error returns verbatim — a fjall failure is never folded in"
        );
        *handle.lock() = None;

        // Healthy lower + still-failing transform: Ok, verbatim.
        let result = cached.commit_provisional(&cref, &marker, &writes).await;
        assert!(
            result.is_ok(),
            "a fjall transform failure never folds into the lower Ok"
        );
        fail_puts.store(false, Ordering::Relaxed);

        // The fallback delete left the cells COLD: each next get pays exactly
        // one durable read and yields the committed data (never stale prev),
        // then republishes (a cold miss, never a ghost of the deleted entry).
        counting.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, cell_at(c).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(100 + c)),
                "cell {c} resolves durably to the committed data"
            );
        }
        assert_eq!(
            counting.lower_reads(),
            3,
            "the fallback delete left every staged cell cold"
        );
        counting.reset();
        for c in [1u8, 2, 3] {
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, cell_at(c).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(100 + c)),
                "cell {c} re-warmed from the cold fall-through"
            );
        }
        assert_eq!(counting.lower_reads(), 0, "the republished cells are warm");
        Ok(())
    })
}

/// Proves that repeated settlement writes the same cache values and expiry.
#[test]
fn d5_transform_retry_is_byte_equivalent() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let now = Arc::new(AtomicU64::new(1_000));
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(
            test_db::cache_with_clock("d5_retry", Clock::Fixed(now.clone()))?,
            counting.clone(),
        );
        let id = collection("d5-retry")?;
        // A TTL'd collection, so the stage stamps a finite expiry the retry
        // must REUSE (a fresh now+ttl would differ after the clock advance).
        let cref = CollectionRef::new(id.clone(), Some(CompactDuration::new(60)));
        let (writes, marker) = stage_committed_marker(&cached, &cref, 4).await?;

        cached.commit_provisional(&cref, &marker, &writes).await?;
        let first: Vec<Option<u64>> = {
            let mut out = Vec::new();
            for c in [1u8, 2, 3] {
                out.push(cached.stored_expiry(&id, &cell_at(c)).await?);
            }
            out
        };

        // Advance the clock (a fresh now+ttl restamp would now differ), then
        // run the transform again — admission-retry shape.
        now.store(5_500, Ordering::Relaxed);
        cached.commit_provisional(&cref, &marker, &writes).await?;

        counting.reset();
        for (i, c) in [1u8, 2, 3].into_iter().enumerate() {
            assert_eq!(
                cached.stored_expiry(&id, &cell_at(c)).await?,
                first[i],
                "cell {c}'s expiry is unchanged by the retry (stage-anchored reuse)"
            );
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, cell_at(c).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(100 + c)),
                "cell {c}'s value is unchanged by the retry"
            );
        }
        assert_eq!(counting.lower_reads(), 0, "the retried entries stay warm");
        Ok(())
    })
}

/// The set equation: a commit whose event CLEARS the sections it repopulates
/// keeps the staged coordinates warm (S holds `data`) while every other cached
/// entry of the cleared sections is deleted (C ∖ S). Variable-length
/// coordinates across MULTIPLE sections make an exclusion-set encoding
/// mismatch (the index-key form's extra kind byte) unable to pass: a wrong
/// encoding would silently delete S ∩ C and the zero-lower-reads assert would
/// go red.
#[test]
fn d5_clear_and_repopulate_keeps_staged_cells_warm() -> Result<()> {
    fn cell(section: i8, coord: &[u8]) -> CellKey {
        CellKey {
            section: Section::new(section),
            coordinate: Coordinate::from_bytes(coord.to_vec()),
        }
    }

    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(test_db::cache("set_equation")?, counting.clone());
        let id = collection("set-equation")?;
        let cref = CollectionRef::new(id.clone(), None);
        let event = probe(5);

        // Pre-clear entries in both sections (the C ∖ S victims).
        let victims = [cell(0, &[0x01]), cell(0, &[0xAA, 0xBB]), cell(1, &[0x02])];
        for (i, victim) in victims.iter().enumerate() {
            cached
                .write_resolved(
                    &cref,
                    &[(victim.clone(), Some(bytes(u8::try_from(i)?)))],
                    &[],
                )
                .await?;
        }

        // Stage VARIABLE-LENGTH coordinates across BOTH sections, with the
        // event clearing both sections.
        let staged_cells = [
            cell(0, &[0x07]),
            cell(0, &[0x10, 0x20, 0x30]),
            cell(1, &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]),
        ];
        let mut writes = Vec::new();
        for (i, staged) in staged_cells.iter().enumerate() {
            let prev = CellRead::<Values>::read(&cached, &id, staged.as_ref())
                .await?
                .0;
            writes.push((
                staged.clone(),
                ProvisionalWrite::new(Some(bytes(200 + u8::try_from(i)?)), prev, event),
            ));
        }
        let clears = [
            SectionClear::frozen(Section::new(0), &writes),
            SectionClear::frozen(Section::new(1), &writes),
        ];
        let marker =
            EventMarker::frozen(event, &writes, clears.to_vec(), &evidence([].into(), None));
        cached
            .write_provisional(&cref, listed(&marker, &writes)?)
            .await?;

        cached.commit_provisional(&cref, &marker, &writes).await?;

        // S: every staged cell reads back WARM with `data` — zero lower reads.
        counting.reset();
        for (i, staged) in staged_cells.iter().enumerate() {
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, staged.as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(200 + u8::try_from(i)?)),
                "staged cell {i} keeps commit warmth through the clear"
            );
        }
        assert_eq!(
            counting.lower_reads(),
            0,
            "the scoped section delete excluded the staged coordinates"
        );
        // C ∖ S: the sections' other entries are gone (durably erased by the
        // clear; the cache pays the cold fall-through to absence).
        for victim in &victims {
            assert_eq!(
                CellRead::<Values>::read(&cached, &id, victim.as_ref())
                    .await?
                    .0
                    .get(),
                None,
                "the cleared sections' other entries are gone"
            );
        }
        Ok(())
    })
}
