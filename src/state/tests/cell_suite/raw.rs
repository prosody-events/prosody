//! Raw provisional batch parity.

use super::*;

/// Generated input for the raw-provisional batch property. Like
/// [`BatchReadTrace`] but reads are capped at ONE chunk (`provisional_many` is
/// a single-batch verb) and there is no commit verdict — the raw verbs never
/// resolve, so a staged cell stays provisional regardless of commit evidence.
#[derive(Clone, Debug)]
pub(crate) struct RawBatchTrace {
    /// `(section idx, coord byte, state)`; later writers win per coordinate.
    population: Vec<(u8, u8, BatchCellState)>,
    /// The section the read list scans.
    read_section: u8,
    /// The coordinate bytes to batch-read, in order (duplicates + unknowns
    /// included). `≤ CELL_BATCH`, so exactly one [`CoordinateBatch`].
    reads: Vec<u8>,
}

impl Arbitrary for RawBatchTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let reads: Vec<u8> = capped_vec::<u8>(g, CELL_BATCH.get())
            .into_iter()
            .map(|b| b % CELLS)
            .collect();
        Self {
            population: capped_vec(g, MAX_TRACE_OPS),
            read_section: section_idx(u8::arbitrary(g)),
            reads,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let base = self.clone();
        let by_pop = {
            let base = base.clone();
            base.population
                .shrink()
                .map(move |population| RawBatchTrace {
                    population,
                    ..base.clone()
                })
        };
        let by_reads = base.reads.shrink().map(move |reads| RawBatchTrace {
            reads,
            ..base.clone()
        });
        Box::new(by_pop.chain(by_reads))
    }
}

/// The resolved (`write_resolved`) and provisional (`write_provisional`) seed
/// lists a batch population collapses into.
type BatchSeed = (Vec<(CellKey, Option<Bytes>)>, Vec<(CellKey, u8)>);

/// Collapses a batch population last-writer-wins into the resolved +
/// provisional seed lists — shared by the raw-batch runners.
pub(super) fn collapse_population(population: &[(u8, u8, BatchCellState)]) -> BatchSeed {
    let mut final_state: BTreeMap<(u8, u8), BatchCellState> = BTreeMap::new();
    for &(s, c, state) in population {
        final_state.insert((section_idx(s), c % CELLS), state);
    }
    let mut resolved: Vec<(CellKey, Option<Bytes>)> = Vec::new();
    let mut provisional: Vec<(CellKey, u8)> = Vec::new();
    for (&(s, c), state) in &final_state {
        match *state {
            BatchCellState::Present(v) => resolved.push((cell_in(s, c), Some(bytes(v)))),
            BatchCellState::Absent => {}
            BatchCellState::Provisional(v) => provisional.push((cell_in(s, c), v)),
        }
    }
    (resolved, provisional)
}

/// Backend-generic raw-provisional batch parity: `provisional_many` over one
/// collection returns exactly the survivors the sequential
/// [`CellStore::provisional_cell_at`] point loop does over the same distinct
/// coordinates, in ascending order. ONE collection suffices — unlike
/// `get_many`, the raw verbs never mutate, so a point-loop reference does not
/// pre-resolve what the batch would read.
pub(crate) async fn run_raw_batch_parity_trace<S: CellStore>(
    store: S,
    trace: RawBatchTrace,
) -> Result<bool> {
    if trace.reads.is_empty() {
        return Ok(true); // A `CoordinateBatch` is non-empty; nothing to compare.
    }
    let (resolved, provisional) = collapse_population(&trace.population);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(0x5EED),
    };
    let collection = CollectionRef::new(
        CollectionId::new(
            StateKey::new(Uuid::new_v4(), Arc::from("key")),
            StateType::Application,
            StateName::try_new("entries")?,
        ),
        None,
    );
    seed_batch(&store, &collection, &resolved, &provisional, event).await?;

    let section = SECTIONS[trace.read_section as usize % SECTIONS.len()];
    // Reference: the survivors among the distinct read coordinates, ascending.
    let mut distinct: Vec<u8> = trace.reads.clone();
    distinct.sort_unstable();
    distinct.dedup();
    let mut reference: Vec<(Coordinate, ProvisionalCell)> = Vec::new();
    for &b in &distinct {
        let cell = cell_in(trace.read_section, b);
        if let Some(provisional) = store.provisional_cell_at(collection.id(), &cell).await? {
            reference.push((cell.coordinate.clone(), provisional));
        }
    }

    let batch = batch_of(trace.reads.iter().copied())?;
    let actual = store
        .provisional_many(collection.id(), section, &batch)
        .await?;
    Ok(actual.into_vec() == reference)
}

/// Deterministic ascending-output test: provisional cells staged and read in a
/// NON-ascending byte order still come back strictly ascending by coordinate,
/// each carrying its staged data.
pub(crate) async fn run_raw_batch_ascending_output<S: CellStore>(store: S) -> Result<()> {
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(0x5EED),
    };
    let collection = CollectionRef::new(
        CollectionId::new(
            StateKey::new(Uuid::new_v4(), Arc::from("key")),
            StateType::Application,
            StateName::try_new("entries")?,
        ),
        None,
    );
    // Stage in the non-ascending order [3, 1, 2] with values 30/10/20.
    let provisional = vec![
        (cell_in(0, 3), 30u8),
        (cell_in(0, 1), 10),
        (cell_in(0, 2), 20),
    ];
    seed_batch(&store, &collection, &[], &provisional, event).await?;

    // Read in the same non-ascending order.
    let batch = batch_of([3u8, 1, 2])?;
    let out = store
        .provisional_many(collection.id(), SECTIONS[0], &batch)
        .await?;
    let coords: Vec<u8> = out.iter().map(|(c, _)| c.as_bytes()[0]).collect();
    assert_eq!(
        coords,
        vec![1, 2, 3],
        "provisional_many output is strictly ascending by coordinate"
    );
    for (coord, provisional) in &out {
        let expected = coord.as_bytes()[0] * 10;
        assert_eq!(
            provisional.data(),
            Some(&bytes(expected)),
            "each survivor carries its staged data"
        );
    }
    Ok(())
}

/// Proves that provisional batch reads do not change state or check events.
pub(crate) async fn run_raw_batch_no_side_effects<S: CellStore>(store: S) -> Result<()> {
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(0x5EED),
    };
    let collection = CollectionRef::new(
        CollectionId::new(
            StateKey::new(Uuid::new_v4(), Arc::from("key")),
            StateType::Application,
            StateName::try_new("entries")?,
        ),
        None,
    );
    // A mix: resolved 5, provisional 2 and 8. Read list includes absent 4.
    let resolved = vec![(cell_in(0, 5), Some(bytes(50)))];
    let provisional = vec![(cell_in(0, 2), 20u8), (cell_in(0, 8), 80)];
    seed_batch(&store, &collection, &resolved, &provisional, event).await?;

    let read_bytes = [2u8, 5, 8, 4];
    let before = raw_snapshot(&store, collection.id(), &read_bytes).await?;

    let batch = batch_of(read_bytes)?;
    let result = store
        .provisional_many(collection.id(), SECTIONS[0], &batch)
        .await?;

    let after = raw_snapshot(&store, collection.id(), &read_bytes).await?;
    assert!(
        before == after,
        "provisional_many mutated the raw provisional set or the unsettled marker"
    );

    // The survivors are exactly the provisional cells among the read coords,
    // ascending (coords 2 and 8; 5 resolved and 4 absent are dropped).
    let expected: Vec<(Coordinate, ProvisionalCell)> = before
        .0
        .iter()
        .zip(read_bytes)
        .filter_map(|(cell, b)| {
            cell.clone()
                .map(|provisional| (cell_in(0, b).coordinate.clone(), provisional))
        })
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect();
    assert_eq!(
        result.into_vec(),
        expected,
        "provisional_many returns exactly the surviving provisional cells, ascending"
    );
    Ok(())
}

/// Reads each coordinate's RAW provisional cell plus the unsettled marker — the
/// no-side-effects test's before/after snapshot (never a resolving read, so a
/// skipped resolve is visible).
async fn raw_snapshot<S: CellStore>(
    store: &S,
    id: &CollectionId,
    read_bytes: &[u8],
) -> Result<(Vec<Option<ProvisionalCell>>, Option<EventMarker>), S::Error> {
    let mut cells: Vec<Option<ProvisionalCell>> = Vec::with_capacity(read_bytes.len());
    for &b in read_bytes {
        cells.push(store.provisional_cell_at(id, &cell_in(0, b)).await?);
    }
    let marker = store.marker_state(id).await?.staged;
    Ok((cells, marker))
}
