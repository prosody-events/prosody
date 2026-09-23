//! Crash cuts inside the stage and promote plans.

use super::*;

/// Executes a prefix of the production promote plan, then observes its commit
/// point.
pub(super) async fn promote_prefix<S: CellStore>(
    store: &S,
    collection: &CollectionRef,
    writes: &[(CellKey, ProvisionalWrite)],
    count: usize,
) -> Result<bool> {
    use crate::cassandra::BatchUnit;
    use crate::state::cassandra::crash_settle_batches;
    use crate::state::tests::support::seed_commit_evidence;
    let marker = store
        .marker_state(collection.id())
        .await?
        .staged
        .ok_or_else(|| eyre!("the promote prefix needs a stage"))?;
    // Weights identify the operations while the real planner determines their
    // order.
    let unit = |weight| BatchUnit::<()>::new(weight, smallvec::SmallVec::new());
    let (units, phases) = crash_settle_batches(Some(unit(1)), vec![unit(2)], unit(3), 1, 1);
    for index in phases.into_iter().flatten().take(count) {
        match units[index].weight() {
            1 => seed_commit_evidence(store, collection).await?,
            2 => {
                for (cell, write) in writes {
                    if write.data().is_some() {
                        store
                            .mark_resolved(collection, slice::from_ref(cell))
                            .await?;
                    } else {
                        store
                            .write_resolved(collection, &[(cell.clone(), None)], &[])
                            .await?;
                    }
                }
                store
                    .write_resolved(collection, &[], marker.clears())
                    .await?;
            }
            3 => store.abort_provisional(collection, &[]).await?,
            _ => return Err(eyre!("unknown promote operation")),
        }
    }
    Ok(store
        .marker_state(collection.id())
        .await?
        .committed
        .is_some_and(|evidence| evidence.certifies(&marker)))
}

/// Seeds a crash after ordered stage chunks with a virtual expiry clock.
/// The production planner selects each atomic mutation. Direct deletes
/// reproduce elapsed TTLs without wall-clock waits.
pub(super) async fn stage_clock_crash<S: CellStore>(
    store: &S,
    dedup: &MemoryDeduplicationStore,
    trace: &Trace,
) -> Result<()> {
    use crate::cassandra::BatchUnit;
    use crate::state::cassandra::{crash_stage_batches, crash_stage_chunk};

    let ttl = trace
        .ttl
        .map(|seconds| CompactDuration::new(u32::from(seconds) + 3600));
    let collection = CollectionRef::new(
        CollectionId::new(
            StateKey::new(Uuid::new_v4(), Arc::from("clock")),
            StateType::Application,
            StateName::try_new("clock")?,
        ),
        ttl,
    );
    let event = EventRef::Message {
        dedup_id: Uuid::new_v4(),
    };
    let writes: Vec<_> = (0..4_u8)
        .map(|coordinate| {
            (
                cell_in(0, coordinate),
                ProvisionalWrite::new(Some(bytes(coordinate)), Committed::new(None), event),
            )
        })
        .collect();
    let touched = vec![(StateType::Application, collection.id().name().clone())].into();
    let marker = EventMarker::frozen(
        event,
        &writes,
        &[],
        &EventEvidence {
            touched,
            evidence_ttl: CompactDuration::new(3600),
            dedup: None,
            attempt: AttemptId::new(),
        },
    );
    let marker_unit = BatchUnit::<()>::new(0, smallvec::SmallVec::new());
    let units: Vec<_> = (1..=writes.len())
        .map(|index| BatchUnit::<()>::new(index as u64, smallvec::SmallVec::new()))
        .collect();
    let mut now = 0_u32;
    let step = u32::from(trace.clock) * 100;
    let mut marker_expiry = 0_u32;
    let mut cell_expiry = [0_u32; 4];
    for range in
        crash_stage_batches(&marker_unit, &units, u64::MAX, 2).take(usize::from(trace.cut % 5))
    {
        let members: Vec<_> = crash_stage_chunk(&marker_unit, &units, range.clone())
            .map(BatchUnit::weight)
            .collect();
        ensure!(members.first() == Some(&0), "stage chunk lost its marker");
        let expiry = ttl.map_or(u32::MAX, |ttl| now + ttl.seconds());
        marker_expiry = expiry;
        for slot in range.clone() {
            cell_expiry[slot] = expiry;
        }
        store
            .write_provisional(&collection, listed(&marker, &writes[range.clone()])?)
            .await?;
        now += step;
    }
    let committed = !trace.cut.is_multiple_of(2) && marker_expiry > 0;
    if committed {
        seed_commit_evidence(store, &collection).await?;
    }
    now += step;
    for (slot, expiry) in cell_expiry.iter().enumerate() {
        if *expiry > 0 && *expiry <= now {
            store
                .write_resolved(&collection, &[(writes[slot].0.clone(), None)], &[])
                .await?;
        }
        if *expiry > now {
            ensure!(
                marker_expiry > now,
                "a live cell outlasted its discovery row"
            );
        }
    }
    if marker_expiry > 0 && marker_expiry <= now {
        store.abort_provisional(&collection, &[]).await?;
    }
    ensure!(admit_collection(store, dedup, &collection).await?);
    for (slot, expiry) in cell_expiry.iter().enumerate() {
        let expected = (committed && *expiry > now).then(|| bytes(slot as u8));
        ensure!(
            CellRead::<Values>::read(store, collection.id(), (writes[slot].0).as_ref())
                .await?
                .0
                .get()
                == expected.as_ref(),
            "expiry changed a committed value"
        );
    }
    ensure!(store.marker_state(collection.id()).await?.staged.is_none());
    Ok(())
}

pub(super) async fn assert_crash_state<S: CellStore, P: ShapeProbe>(
    store: &S,
    probe: &P,
    ids: &[CollectionId],
    model: &[BTreeMap<(u8, u8), Option<Bytes>>],
    certificates: &[Option<EventRef>],
) -> Result<()> {
    for (slot, id) in ids.iter().enumerate() {
        let marker = store.marker_state(id).await?;
        ensure!(marker.staged.is_none(), "admit left a stage");
        ensure!(marker.committed.as_ref().map(|marker| marker.event) == certificates[slot]);
        for section in 0..SECTIONS.len() as u8 {
            for coordinate in 0..CRASH_CELLS {
                let value = model[slot]
                    .get(&(section, coordinate))
                    .and_then(Option::as_ref);
                ensure!(
                    CellRead::<Values>::read(store, id, cell_in(section, coordinate).as_ref())
                        .await?
                        .0
                        .get()
                        == value
                );
            }
        }
        let expected: RowKeys = model[slot]
            .iter()
            .filter(|(_, value)| value.is_some())
            .map(|(&(section, coordinate), _)| row_key(&cell_in(section, coordinate)))
            .collect();
        ensure!(
            probe.cell_rows(id).await? == expected,
            "the durable row set differs from the model"
        );
        ensure!(probe.provisional_rows(id).await?.is_empty());
        ensure!(probe.unsettled_marker(id).await?.is_none());
    }
    Ok(())
}
