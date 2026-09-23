//! Batch-read parity against sequential point reads.

use super::*;

/// One seeded cell's committed state for the batch-read parity generator.
#[derive(Clone, Copy, Debug)]
pub(super) enum BatchCellState {
    /// A committed-present resolved cell (`write_resolved(Some)`).
    Present(u8),
    /// A committed-absent cell — no row (the parity oracle answers
    /// `Committed(None)`).
    Absent,
    /// A provisional cell that reads through the trace's commit evidence.
    Provisional(u8),
}

impl Arbitrary for BatchCellState {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Self::Present(u8::arbitrary(g)),
            1 => Self::Absent,
            _ => Self::Provisional(u8::arbitrary(g)),
        }
    }
}

/// Generated input for the batch-read parity property: a small cell population
/// over the [`SECTIONS`] pool (collisions dedupe last-writer-wins), the single
/// staging event's verdict, and a read list in ONE sampled section whose
/// coordinate bytes deliberately include duplicates and coordinates absent from
/// the population.
#[derive(Clone, Debug)]
pub(crate) struct BatchReadTrace {
    /// `(section idx, coord byte, state)`; later writers win per coordinate.
    population: Vec<(u8, u8, BatchCellState)>,
    /// The staging event's commit decision (`true` ⇒ its provisional cells
    /// resolve to their staged data, `false` ⇒ to their `prev`).
    event_committed: bool,
    /// The section the read list scans.
    read_section: u8,
    /// The coordinate bytes to batch-read, in order (duplicates + unknowns
    /// included).
    reads: Vec<u8>,
}

impl Arbitrary for BatchReadTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        // Read lists up to ~3× CELL_BATCH so multi-chunk fan-out is exercised.
        let reads: Vec<u8> = capped_vec::<u8>(g, CELL_BATCH.get() * 3)
            .into_iter()
            .map(|b| b % CELLS)
            .collect();
        Self {
            population: capped_vec(g, MAX_TRACE_OPS),
            event_committed: bool::arbitrary(g),
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
                .map(move |population| BatchReadTrace {
                    population,
                    ..base.clone()
                })
        };
        let by_reads = base.reads.shrink().map(move |reads| BatchReadTrace {
            reads,
            ..base.clone()
        });
        Box::new(by_pop.chain(by_reads))
    }
}

/// Seeds one collection from a batch population: committed-present cells as one
/// `write_resolved`, then the staging event's provisional cells as one
/// `write_provisional` (each `prev` read from the store, never minted). Absent
/// cells write nothing (row-absence). The two collections a parity run compares
/// call this with identical arguments.
pub(super) async fn seed_batch<S: CellStore>(
    store: &S,
    collection: &CollectionRef,
    resolved: &[(CellKey, Option<Bytes>)],
    provisional: &[(CellKey, u8)],
    event: EventRef,
) -> Result<()> {
    if !resolved.is_empty() {
        store.write_resolved(collection, resolved, &[]).await?;
    }
    if !provisional.is_empty() {
        let mut writes = Vec::with_capacity(provisional.len());
        for (cell, data) in provisional {
            let prev = CellRead::<Values>::read(store, collection.id(), cell.as_ref())
                .await?
                .0;
            writes.push((
                cell.clone(),
                ProvisionalWrite::new(Some(bytes(*data)), prev, event),
            ));
        }
        let marker = EventMarker::frozen(event, &writes, &[], &evidence([].into(), None));
        store
            .write_provisional(collection, listed(&marker, &writes)?)
            .await?;
    }
    Ok(())
}

/// Compares batch reads with point reads over equivalent collections.
/// Both reads must agree across duplicates, absent rows, and provisional cells.
/// Each collection keeps its own rows and cache entries.
pub(crate) async fn run_batch_read_parity_trace<S: CellStore>(
    store: S,
    trace: BatchReadTrace,
) -> Result<bool> {
    let (resolved, provisional) = collapse_population(&trace.population);

    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(0x5EED),
    };

    // Fresh, distinct segments per invocation: the two collections isolate on
    // both backends (per-key row isolation on the shared Cassandra keyspace,
    // TESTING.md) and never collide across quickcheck iterations.
    let mk = || -> Result<CollectionRef> {
        Ok(CollectionRef::new(
            CollectionId::new(
                StateKey::new(Uuid::new_v4(), Arc::from("key")),
                StateType::Application,
                StateName::try_new("entries")?,
            ),
            None,
        ))
    };
    let expected_coll = mk()?;
    let batch_coll = mk()?;
    let presence_coll = mk()?;
    seed_batch(&store, &expected_coll, &resolved, &provisional, event).await?;
    seed_batch(&store, &batch_coll, &resolved, &provisional, event).await?;
    seed_batch(&store, &presence_coll, &resolved, &provisional, event).await?;
    if trace.event_committed && !provisional.is_empty() {
        seed_commit_evidence(&store, &expected_coll).await?;
        seed_commit_evidence(&store, &batch_coll).await?;
        seed_commit_evidence(&store, &presence_coll).await?;
    }

    let section = SECTIONS[trace.read_section as usize % SECTIONS.len()];
    let mut expected: Vec<Committed> = Vec::with_capacity(trace.reads.len());
    for &b in &trace.reads {
        expected.push(
            CellRead::<Values>::read(
                &store,
                expected_coll.id(),
                cell_in(trace.read_section, b).as_ref(),
            )
            .await?
            .0,
        );
    }
    let coords = trace.reads.iter().map(|&b| Coordinate::from_bytes(vec![b]));
    let mut got: Vec<Committed> = Vec::with_capacity(trace.reads.len());
    let mut presence = Vec::with_capacity(trace.reads.len());
    for batch in CoordinateBatch::chunks(coords) {
        got.extend(
            CellRead::<Values>::read_many(&store, batch_coll.id(), section, &batch.as_ref())
                .await
                .map(|cells| {
                    cells
                        .into_iter()
                        .map(|(committed, _)| committed)
                        .collect::<CommittedBatch>()
                })?,
        );
        presence.extend(
            CellRead::<Presence>::read_many(&store, presence_coll.id(), section, &batch.as_ref())
                .await
                .map(|cells| {
                    cells
                        .into_iter()
                        .map(|(committed, _)| committed.get().is_some())
                        .collect::<CellBuffer<bool>>()
                })?,
        );
    }
    Ok(got.len() == trace.reads.len()
        && got == expected
        && presence
            == expected
                .iter()
                .map(|cell| cell.get().is_some())
                .collect::<Vec<_>>())
}

/// Proves that duplicate batch positions return the same cell value.
pub(crate) async fn run_batch_duplicate_co_observation<S: CellStore>(store: S) -> Result<()> {
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("key")),
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let collection = CollectionRef::new(id.clone(), None);
    store
        .write_resolved(
            &collection,
            &[
                (cell_in(0, 5), Some(bytes(42))),
                (cell_in(0, 9), Some(bytes(99))),
            ],
            &[],
        )
        .await?;
    let batch = batch_of([5, 9, 5])?;
    let got = CellRead::<Values>::read_many(&store, &id, SECTIONS[0], &batch.as_ref())
        .await
        .map(|cells| {
            cells
                .into_iter()
                .map(|(committed, _)| committed)
                .collect::<CommittedBatch>()
        })?;
    assert_eq!(got.len(), 3, "every position answered");
    assert_eq!(got[0], got[2], "duplicate coordinate co-observes one value");
    assert_eq!(
        got[0],
        Committed::new(Some(bytes(42))),
        "coordinate 5 carries its own value"
    );
    assert_eq!(
        got[1],
        Committed::new(Some(bytes(99))),
        "coordinate 9 carries its own value"
    );
    Ok(())
}

/// Deterministic alignment test: a read list spanning two chunks that mixes
/// present, absent, and duplicate coordinates — the concatenated `get_many`
/// output has exactly the input length and each position matches the point
/// `get` (a dropped no-row position makes this test fail both the length check
/// and the per-position compare).
pub(crate) async fn run_batch_alignment<S: CellStore>(store: S) -> Result<()> {
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("key")),
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let collection = CollectionRef::new(id.clone(), None);
    store
        .write_resolved(
            &collection,
            &[
                (cell_in(0, 2), Some(bytes(20))),
                (cell_in(0, 7), Some(bytes(70))),
                (cell_in(0, 10), Some(bytes(100))),
            ],
            &[],
        )
        .await?;
    // 20 positions across two chunks: present (2,7,10), absent (0,1,3..), and
    // duplicates (2, 7 repeated).
    let read_bytes: Vec<u8> = vec![
        2, 7, 10, 0, 1, 2, 3, 4, 7, 5, 6, 8, 10, 2, 11, 7, 0, 10, 2, 1,
    ];
    let mut expected: Vec<Committed> = Vec::with_capacity(read_bytes.len());
    for &b in &read_bytes {
        expected.push(
            CellRead::<Values>::read(&store, &id, cell_in(0, b).as_ref())
                .await?
                .0,
        );
    }
    let coords = read_bytes.iter().map(|&b| Coordinate::from_bytes(vec![b]));
    let mut got: Vec<Committed> = Vec::new();
    for batch in CoordinateBatch::chunks(coords) {
        got.extend(
            CellRead::<Values>::read_many(&store, &id, SECTIONS[0], &batch.as_ref())
                .await
                .map(|cells| {
                    cells
                        .into_iter()
                        .map(|(committed, _)| committed)
                        .collect::<CommittedBatch>()
                })?,
        );
    }
    assert_eq!(got.len(), read_bytes.len(), "every input position answered");
    assert_eq!(got, expected, "each position matches the point-get dedup");
    Ok(())
}
