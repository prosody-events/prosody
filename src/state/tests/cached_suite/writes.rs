//! Lower-store write failures, read-only scans, and section deletes.

use super::*;

/// Proves that a failed durable write does not cache the new value.
///
/// The next read loads the old durable value.
///
/// Example test by necessity: the crash and overlay properties observe values,
/// not the serving layer. The lower-read count is below the model's
/// abstraction, so it cannot join the generator.
#[test]
fn failed_lower_write_leaves_cache_serving_pre_write_value() -> Result<()> {
    use crate::state::StateName;

    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let handle: PoisonHandle = Arc::default();
        let cached = Cached::new(
            test_db::cache("establish_fault")?,
            FailingCellStore::with_handle(counting.clone(), handle.clone()),
        );
        let id = collection("establish-fault")?;
        let cref = CollectionRef::new(id.clone(), None);
        let name: StateName = id.name().clone();

        // Seed a base value; the write-through warms it, so a get serves
        // from fjall with zero lower reads — the arrange proof.
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(1)),
            "the seeded base serves warm"
        );
        assert_eq!(counting.lower_reads(), 0, "the arrange get is warm");

        // A clears-free lower write fault: the write must surface Err. The
        // drop-safe pre-call cell delete (F1) already evicted the entry, so the
        // follow-up serves the PRE-write value via a cold fall-through (one
        // lower read) — correct, never a warm stale hit, never a phantom
        // publish of the new value — then re-warms.
        *handle.lock() = Some(Poison::WriteResolved(
            name.clone(),
            ErrorCategory::Transient,
        ));
        assert!(
            cached
                .write_resolved(&cref, &[(cell_at(0), Some(bytes(2)))], &[])
                .await
                .is_err(),
            "the armed lower write must be rejected"
        );
        *handle.lock() = None;
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(1)),
            "the failed write serves the PRE-write value"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the drop-safe pre-call delete left the cell cold: one fall-through, never a phantom \
             publish of the new value"
        );
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(1)),
            "the fall-through re-warmed the pre-write value"
        );
        assert_eq!(counting.lower_reads(), 0, "the re-warmed get reads nothing");

        // Fail the durable write after the cache removes the section.
        let cells = [(cell_at(3), Some(bytes(9)))];
        let clear = SectionClear::frozen_resolved(SECTION, &cells);
        *handle.lock() = Some(Poison::WriteResolved(name, ErrorCategory::Transient));
        assert!(
            cached
                .write_resolved(&cref, &cells, slice::from_ref(&clear))
                .await
                .is_err(),
            "the armed lower write with clears must be rejected"
        );
        *handle.lock() = None;
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(1)),
            "delete-first on a failed apply degrades to a correct slow read, never a wrong one"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the deleted section falls through exactly once"
        );
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(1)),
            "the fall-through re-warmed the coordinate"
        );
        assert_eq!(counting.lower_reads(), 0, "the re-warmed get reads nothing");
        Ok(())
    })
}

/// Drop-safety (F1): `write_resolved` is the one user-droppable write path
/// (mid-handler `commit()` / `ReadUncommitted` finalize). If the future is
/// dropped between the durable write landing and the re-warming publish, the
/// written cell's OLD entry must not survive as a stale warm hit (KV1) — the
/// pre-call delete leaves it cold, so the next read falls through to the
/// durable NEW value. Reverting the pre-call delete freezes the stale `A` warm
/// and makes this test fail (the get would serve `A` with zero lower
/// reads).
#[test]
fn dropped_write_resolved_leaves_no_stale_entry() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let holding = HoldingCellStore::new(counting.clone());
        let holds = holding.holds();
        let cached = Cached::new(test_db::cache("drop_write")?, holding);
        let id = collection("drop-write")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Warm committed `A` = 1 through the cache (no charge armed, so the
        // hold passes through).
        cached
            .write_resolved(&cref, &[(cell_at(0), Some(bytes(1)))], &[])
            .await?;
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(1)),
            "A is warm"
        );
        assert_eq!(counting.lower_reads(), 0, "the arrange get is warm");

        // Arm the next `write_resolved` to park after its durable write lands,
        // then spawn a write of `B` = 2 and drop it mid-flight — the pre-call
        // delete has run and the durable write has landed, but the re-warming
        // publish never will.
        holds.write_resolved().arm(1);
        let landed_before = holds.write_resolved().landed();
        let task = tokio::spawn({
            let cached = cached.clone();
            let cref = cref.clone();
            async move {
                cached
                    .write_resolved(&cref, &[(cell_at(0), Some(bytes(2)))], &[])
                    .await
            }
        });
        holds.write_resolved().entered().await;
        assert!(
            holds.write_resolved().landed() > landed_before,
            "the durable write landed before the drop"
        );
        task.abort();
        assert!(task.await.is_err(), "the write future was dropped");

        // The cell is COLD (pre-call delete), so the next get falls through to
        // the durable NEW value `B` = 2 — never a warm stale `A`.
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(2)),
            "a dropped write_resolved must not leave the stale pre-write value warm"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the dropped write left the cell cold: exactly one fall-through"
        );
        Ok(())
    })
}

/// Proves that a scan does not change a provisional cell.
///
/// Admission can resolve the cell. Point reads also leave it unchanged.
#[test]
fn scan_resolution_is_read_only() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let cells = MemoryCells::new();
        let lower = MemoryCellStore::new(cells.clone());
        let id = collection("scan-readonly")?;
        let cref = CollectionRef::new(id.clone(), None);
        let prior_event = probe(7);

        // Seed a certified provisional clear. The read must preserve its row.
        let writes = [(
            cell_at(4),
            ProvisionalWrite::new(None, Committed::new(Some(bytes(1))), prior_event),
        )];
        let marker = EventMarker::frozen(prior_event, &writes, &[], &evidence([].into(), None));
        lower
            .write_provisional(&cref, listed(&marker, &writes)?)
            .await?;
        seed_commit_evidence(&lower, &cref).await?;
        assert_eq!(
            cells.provisional_coordinates(&id),
            vec![cell_at(4)],
            "the prior event committed provisional is seeded"
        );

        // Scan the section. The cell resolves to its
        // committed view — absent, since data = None — so the scan yields
        // nothing, but must leave the durable cell provisional.
        let seen = scan_forward(&lower, &id, 0, Bound::Included(255)).await?;
        assert!(
            seen.is_empty(),
            "the committed clear resolves to absent, so the scan yields nothing"
        );
        assert_eq!(
            cells.provisional_coordinates(&id),
            vec![cell_at(4)],
            "the scan must not durably resolve the prior event provisional (read-only): a scan \
             write-back could clobber a newer commit of the same cell"
        );
        Ok(())
    })
}

/// Proves that a section clear removes only entries in that section.
#[test]
fn delete_section_removes_exactly_the_cleared_section() -> Result<()> {
    /// The cell at coordinate `c` in section 1 (the surviving sibling; the
    /// shared [`cell_at`] addresses section 0, the cleared one).
    fn sect1_cell(c: u8) -> CellKey {
        CellKey {
            section: Section::new(1),
            coordinate: Coordinate::from_bytes(vec![c]),
        }
    }

    TEST_RUNTIME.block_on(async {
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(test_db::cache("clear_delete")?, counting.clone());
        let id = collection("clear-delete")?;
        let other = collection("clear-delete-other")?;
        let cref = CollectionRef::new(id.clone(), None);
        let other_ref = CollectionRef::new(other.clone(), None);

        // Warm entries in section 0 AND section 1, plus a sibling collection.
        cached
            .write_resolved(
                &cref,
                &[
                    (cell_at(0), Some(bytes(10))),
                    (sect1_cell(0), Some(bytes(20))),
                ],
                &[],
            )
            .await?;
        cached
            .write_resolved(&other_ref, &[(cell_at(0), Some(bytes(30)))], &[])
            .await?;

        // A committed clear of section 0 with one survivor write.
        let survivors = [(cell_at(5), Some(bytes(50)))];
        let clear = SectionClear::frozen_resolved(SECTION, &survivors);
        cached
            .write_resolved(&cref, &survivors, slice::from_ref(&clear))
            .await?;

        // The cleared section's pre-clear entry is gone: durable truth is
        // absent, and the read pays the one cold fall-through.
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            None,
            "the committed clear erased the non-survivor"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the cleared entry is cold — exactly one fall-through"
        );
        // The survivor re-warmed via the post-clear publish.
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, cell_at(5).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(50)),
            "the survivor serves the post-clear value"
        );
        assert_eq!(counting.lower_reads(), 0, "the survivor is warm");
        // The sibling section and the sibling collection stay warm.
        counting.reset();
        assert_eq!(
            CellRead::<Values>::read(&cached, &id, sect1_cell(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(20)),
            "the sibling section survives the clear"
        );
        assert_eq!(
            CellRead::<Values>::read(&cached, &other, cell_at(0).as_ref())
                .await?
                .0
                .get(),
            Some(&bytes(30)),
            "the sibling collection survives the clear"
        );
        assert_eq!(
            counting.lower_reads(),
            0,
            "nothing beyond the cleared section was evicted"
        );
        Ok(())
    })
}
