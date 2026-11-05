//! Crash-recovery equivalence: admission restores the last committed value.

use super::*;

/// One collection's stage-plan entry: the pool slot, the collapsed
/// `((section idx, coord), mutation)` cell set staged atomically, and the
/// section indices durably cleared.
type PlannedStage = (u8, Vec<((u8, u8), Mutation)>, Vec<u8>);

/// One event's stage plan, grouped by collection.
type StagePlan = Vec<PlannedStage>;

/// Groups an event's flat writes by collection (first-seen order, repeats of a
/// cell collapsed last-writer-wins) and merges in its durable clears — a
/// clear-touched collection with no writes becomes a clears-only entry.
pub(super) fn event_plan(event: &TraceEvent) -> StagePlan {
    let mut plan: StagePlan = Vec::new();
    for &(coll, s, c, mutation) in &event.writes {
        match plan.iter_mut().find(|(p, ..)| *p == coll) {
            Some((_, cells, _)) => collapse_cell_into(cells, (s, c), mutation),
            None => plan.push((coll, vec![((s, c), mutation)], Vec::new())),
        }
    }
    for &(coll, s) in &event.clears {
        match plan.iter_mut().find(|(p, ..)| *p == coll) {
            Some((_, _, cleared)) => {
                if !cleared.contains(&s) {
                    cleared.push(s);
                }
            }
            None => plan.push((coll, Vec::new(), vec![s])),
        }
    }
    plan
}

/// Inserts `(cell, mutation)` into a collection's cell set, overwriting any
/// existing mutation for that cell (last-writer-wins) and preserving order.
fn collapse_cell_into<K: PartialEq>(cells: &mut Vec<(K, Mutation)>, cell: K, mutation: Mutation) {
    match cells.iter_mut().find(|(c, _)| *c == cell) {
        Some(slot) => slot.1 = mutation,
        None => cells.push((cell, mutation)),
    }
}

/// Collapses one collection's cell writes to last-writer-wins per cell,
/// preserving first-seen order (an event stages each cell at most once).
pub(super) fn collapse_cells(cells: Vec<(u8, Mutation)>) -> Vec<(u8, Mutation)> {
    let mut out: Vec<(u8, Mutation)> = Vec::new();
    for (cell, mutation) in cells {
        collapse_cell_into(&mut out, cell, mutation);
    }
    out
}

/// The collection ids / refs / pool for a crash or overwrite trace.
pub(super) fn pooled_collections() -> Result<(Vec<CollectionId>, Vec<CollectionRef>)> {
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let ids: Vec<CollectionId> = (0..POOL)
        .map(|c| {
            Ok(CollectionId::new(
                state_key.clone(),
                StateType::Application,
                StateName::try_new(format!("c{c}"))?,
            ))
        })
        .collect::<Result<_>>()?;
    let refs = ids
        .iter()
        .map(|id| CollectionRef::new(id.clone(), None))
        .collect();
    Ok((ids, refs))
}

/// The per-collection staged state an event produces: for each touched
/// collection, its pool index, the `(cell, write)` set staged atomically, and
/// the frozen [`SectionClear`]s its marker carries.
type StagedWrites = Vec<(u8, Vec<(CellKey, ProvisionalWrite)>, Vec<SectionClear>)>;

/// Stages every collection in the plan under `event`, checking each cell's
/// committed base against the model first (returning `None` on a mismatch — a
/// property violation the caller surfaces) and returning the per-collection
/// staged writes + frozen clears for the settle. Every stage passes the
/// event's frozen union marker; when `split` is set, a ≥2-cell collection is
/// staged in two sequential same-event `write_provisional` calls (prefix, then
/// the rest), both carrying that union marker. The second call's unsettled
/// marker is the event's OWN; the stage boundary must overwrite it, never
/// resolve it. This is the only path that exercises the same-event marker
/// overwrite, so treating an own marker as prior event strands the prefix and
/// fails convergence. A clears-only collection stages `writes = []` with a
/// marker whose `staged()` is empty and `clears()` non-empty.
///
/// `stale_prev_ok[i]` accepts a stale prev-read on collection `i`: a restage
/// over a **warm** stage with an unsettled clear (a settle failure with the
/// in-process cache intact) may legitimately read the warm pre-settle value
/// — the accepted bounded window. The staged prev still feeds the write, so a
/// later rollback restores exactly what was read (mirrored in the model by the
/// rollback-restores-staged-prev rule).
async fn stage_event<S>(
    store: &S,
    refs: &[CollectionRef],
    staged: &[PlannedStage],
    model: &[BTreeMap<(u8, u8), Option<Bytes>>],
    event: EventRef,
    split: bool,
    stale_prev_ok: &[bool],
) -> Result<Option<StagedWrites>>
where
    S: CellStore,
{
    let mut touched: Vec<_> = staged
        .iter()
        .map(|(slot, ..)| {
            let id = refs[usize::from(*slot)].id();
            (id.state_type(), id.name().clone())
        })
        .collect();
    touched.sort_unstable();
    let dedup = match event {
        EventRef::Message { dedup_id } => Some(dedup_id),
        EventRef::Timer(_) => None,
    };
    let evidence = evidence(touched.into(), dedup);
    let mut staged_writes = Vec::with_capacity(staged.len());
    for (coll, cells, cleared) in staged {
        let mut cell_writes: Vec<(CellKey, ProvisionalWrite)> = Vec::with_capacity(cells.len());
        for &((s, c), mutation) in cells {
            let key = cell_in(s, c);
            let prev = CellRead::<Values>::read(store, refs[*coll as usize].id(), key.as_ref())
                .await?
                .0;
            if !stale_prev_ok[*coll as usize]
                && prev.get().cloned() != model[*coll as usize].get(&(s, c)).cloned().flatten()
            {
                return Ok(None);
            }
            cell_writes.push((key, ProvisionalWrite::new(mutation.value(), prev, event)));
        }
        // Survivors frozen from the staged set (the single survivor
        // definition: the cleared section's present-data staged cells).
        let clears: Vec<SectionClear> = cleared
            .iter()
            .map(|&s| SectionClear::frozen(SECTIONS[s as usize], &cell_writes))
            .collect();
        let marker = EventMarker::frozen(event, &cell_writes, clears.clone(), &evidence);
        let collection = &refs[*coll as usize];
        if split && cell_writes.len() >= 2 {
            let mid = cell_writes.len() / 2;
            store
                .write_provisional(collection, listed(&marker, &cell_writes[..mid])?)
                .await?;
            store
                .write_provisional(collection, listed(&marker, &cell_writes[mid..])?)
                .await?;
        } else {
            store
                .write_provisional(collection, listed(&marker, &cell_writes)?)
                .await?;
        }
        staged_writes.push((*coll, cell_writes, clears));
    }
    Ok(Some(staged_writes))
}

/// A crash preserves the last committed value across every collection.
/// Admission resolves each stage from durable evidence before the next event.
pub(crate) async fn run_crash_equivalence_trace<S, F, P>(
    make_store: F,
    dedup: MemoryDeduplicationStore,
    trace: Trace,
    probe: &P,
) -> Result<bool>
where
    S: CellStore,
    F: Fn(&PoisonHandle) -> Result<S>,
    P: ShapeProbe,
{
    use crate::state::manager::Admission;
    use crate::state::tests::support::admit_registered;
    let (ids, refs) = pooled_collections()?;
    let lower: PoisonHandle = Arc::default();
    let mut store = make_store(&lower)?;
    stage_clock_crash(&store, &dedup, &trace).await?;
    let mut model = vec![BTreeMap::new(); POOL as usize];
    let mut certificates = vec![None; POOL as usize];

    for (index, ev) in trace.events.into_iter().enumerate() {
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(index as u128),
        };
        ensure!(admit_registered(&store, &dedup, &refs).await? == Admission::Fresh);
        for &(collection, section, coordinate, mutation) in &ev.blind {
            let slot = usize::from(collection);
            store
                .write_resolved(
                    &refs[slot],
                    &[(cell_in(section, coordinate), mutation.value())],
                    &[],
                )
                .await?;
            model[slot].insert((section, coordinate), mutation.value());
        }
        let mut planned = event_plan(&ev);
        if ev.outcome.mid_fan_out() {
            if let Some((_, cells, _)) = planned.last_mut() {
                cells.pop();
            }
            planned.retain(|(_, cells, clears)| !cells.is_empty() || !clears.is_empty());
        }
        if ev.stage_fault
            && let Some((collection, ..)) = planned.first()
        {
            *lower.lock() = Some(Poison::WriteProvisional(
                refs[usize::from(*collection)].id().name().clone(),
                ErrorCategory::Transient,
            ));
            let stage = stage_event(
                &store,
                &refs,
                &planned[..1],
                &model,
                event,
                ev.split,
                &[false; POOL as usize],
            )
            .await;
            *lower.lock() = None;
            ensure!(
                stage.is_err(),
                "the stage fault did not reach the lower store"
            );
            planned.clear();
        }
        let staged = stage_event(
            &store,
            &refs,
            &planned,
            &model,
            event,
            ev.split,
            &[false; POOL as usize],
        )
        .await?
        .ok_or_else(|| eyre!("stage read differs from the committed model"))?;
        let mut committed = false;
        if ev.outcome.marker_flushed()
            && let Some((collection, writes, _)) = staged.first()
        {
            committed = promote_prefix(
                &store,
                &refs[usize::from(*collection)],
                writes,
                usize::from(ev.split) + 1,
            )
            .await?;
        }
        if committed {
            commit_model(&planned, &mut model, &mut certificates, event);
        }
        if ev.outcome.is_crash() {
            store = make_store(&lower)?;
        }
        ensure!(admit_registered(&store, &dedup, &refs).await? == Admission::Fresh);
        assert_crash_state(&store, probe, &ids, &model, &certificates).await?;
        if committed {
            ensure!(
                dedup.exists(Uuid::from_u128(index as u128)).await?,
                "admit did not retire the committed message"
            );
        }
    }
    Ok(true)
}

/// Applies one committed event to the reference model.
fn commit_model(
    planned: &[PlannedStage],
    model: &mut [BTreeMap<(u8, u8), Option<Bytes>>],
    certificates: &mut [Option<EventRef>],
    event: EventRef,
) {
    for (collection, cells, clears) in planned {
        let slot = usize::from(*collection);
        for ((section, _), value) in &mut model[slot] {
            if clears.contains(section) {
                *value = None;
            }
        }
        for &((section, coordinate), mutation) in cells {
            model[slot].insert((section, coordinate), mutation.value());
        }
        certificates[slot] = Some(event);
    }
}

/// Proves that a resolved write does not settle a marker without section
/// clears.
pub(crate) async fn run_blind_write_leaves_clears_free_marker<S, P>(
    store: S,
    probe: &P,
) -> Result<()>
where
    S: CellStore,
    P: ShapeProbe,
{
    let (_ids, refs) = pooled_collections()?;
    let id = refs[0].id();
    let event_a = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };

    // Stage a clears-FREE marker; the commit is deliberately NOT recorded (a
    // clears-free marker is never consulted, so the verdict is irrelevant).
    let staged = cell_in(0, 0);
    let prev = CellRead::<Values>::read(&store, id, staged.as_ref())
        .await?
        .0;
    let writes = vec![(
        staged.clone(),
        ProvisionalWrite::new(Some(bytes(1)), prev, event_a),
    )];
    let marker = EventMarker::frozen(event_a, &writes, Vec::new(), &evidence([].into(), None));
    store
        .write_provisional(&refs[0], listed(&marker, &writes)?)
        .await?;

    // Blind-write a different coordinate.
    let blind = cell_in(0, 5);
    store
        .write_resolved(&refs[0], &[(blind.clone(), Some(bytes(9)))], &[])
        .await?;

    let (expected_staged, expected_clears) = probed_parts(&marker);
    ensure!(
        probe.unsettled_marker(id).await?
            == Some((event_a, expected_staged.clone(), expected_clears.clone())),
        "a blind write must leave a clears-free marker unsettled"
    );
    ensure!(
        probe
            .provisional_rows(id)
            .await?
            .contains(&row_key(&staged)),
        "the staged provisional row must survive the blind write"
    );

    // The blind cell reads back, and the marker STILL stands after the read
    // (clear resolution leaves clears-free markers unsettled too — parity with
    // reads).
    ensure!(
        CellRead::<Values>::read(&store, id, blind.as_ref())
            .await?
            .0
            .into_inner()
            == Some(bytes(9)),
        "the blind write did not read back"
    );
    ensure!(
        probe.unsettled_marker(id).await? == Some((event_a, expected_staged, expected_clears)),
        "reading a clears-free marker's collection must leave it unsettled"
    );
    Ok(())
}
