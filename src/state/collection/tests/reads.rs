//! Warm reads, aligned batch reads, and empty-plan fencing.

use super::*;

/// Steady-state I/O budget: a warm value read performs zero lower-store reads,
/// and opening a fresh operation does not change that — admission is not a
/// cache boundary.
#[test]
fn warm_reads_perform_no_additional_lower_reads() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let descriptor: ValueDescriptor<I64Codec> = value_state("warm-value");
        let registry = value_registry(&descriptor)?;
        let lower = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let cached = Cached::new(test_db::cache("collection-warm")?, lower.clone());
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("warm-key"));
        let session = session_over(MemoryLoader::new(), registry, state_key, cached);

        let handle = descriptor
            .bind(&session)
            .map_err(|e| eyre!("bind failed: {e}"))?;
        handle.set(7).await?;
        handle.commit().await?;

        assert_eq!(
            handle.get().await?,
            Some(7),
            "the committed value reads back"
        );
        let warm = lower.lower_reads();
        assert_eq!(handle.get().await?, Some(7), "the warm re-read");
        assert_eq!(
            lower.lower_reads(),
            warm,
            "a warm re-read performs no lower-store read"
        );

        let fresh = descriptor
            .bind(&session)
            .map_err(|e| eyre!("re-bind failed: {e}"))?;
        assert_eq!(fresh.get().await?, Some(7), "the fresh operation's read");
        assert_eq!(
            lower.lower_reads(),
            warm,
            "opening a new operation is not a cache boundary"
        );
        Ok(())
    })
}

/// A batch read stays index-aligned **across** the lower store's batch
/// boundary: a `CELL_BATCH`-crossing query answers every position, in input
/// order, with duplicates answered per position.
///
/// Deterministic because the generated property's key pool is tiny and its
/// queries never reach `CELL_BATCH`, so no random trace can cross the split
/// the sub-batching performs.
#[test]
fn batch_reads_stay_aligned_across_the_store_batch_boundary() -> Result<()> {
    // One past a full batch, so the query spans exactly two sub-batches and
    // lands on the 127/128/129 boundary.
    let populated = CELL_BATCH.get() as i64 + 1;
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, _dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key);
        let handle = bind_probe(&session)?;
        handle
            .cells
            .write(async |op| {
                for key in 0..populated {
                    op.set(PairLayout::LEFT.at(&key), key * 10)?;
                }
                Ok::<(), ProbeError>(())
            })
            .await?;

        // The boundary key at both ends, so a dropped or reordered sub-batch
        // cannot be masked by a palindromic query.
        let queries: Vec<i64> = once(CELL_BATCH.get() as i64)
            .chain(0..populated)
            .chain(once(CELL_BATCH.get() as i64))
            .collect();
        let answers = handle
            .cells
            .read(async |op| op.get_many(PairLayout::LEFT, &queries).await)
            .await?;

        let expected: Vec<Option<i64>> = queries.iter().map(|key| Some(key * 10)).collect();
        assert_eq!(
            answers.into_vec(),
            expected,
            "every position of a batch-crossing read answers its own key"
        );
        Ok(())
    })
}

/// A managed stream leaked past its attempt fences on **exhaustion**: an empty
/// coordinate plan errors `Terminated` at its first pull rather than reporting
/// a clean end. The plan is captured before the bump, so the error can only
/// come from the driver's per-emission fence.
#[test]
fn empty_coordinate_plan_fences_on_exhaustion() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, _dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key);
        let handle = bind_probe(&session)?;

        let plan = handle
            .cells
            .read(async |op| op.coordinates::<_, &[u8]>(PairLayout::LEFT, Vec::new()))
            .await;
        session.reset(RepinProof::for_test()).await;

        let stream = plan.projected::<Values>();
        futures::pin_mut!(stream);
        match stream.next().await {
            Some(Err(CellStateError::Access(StateAccessError::Terminated))) => Ok(()),
            other => Err(eyre!(
                "a leaked empty plan must fence Terminated on exhaustion, got ok={}",
                other.is_some()
            )),
        }
    })
}
