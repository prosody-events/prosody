use super::*;

/// Stage a set, observe it provisional, promote, read back resolved — the
/// hot-path round-trip — then a direct resolved clear reads back absent. A fast
/// deterministic smoke of shapes the crash-equivalence property and its
/// physical oracle (`assert_physical`) reach organically over generated traces.
#[tokio::test]
async fn provisional_set_promote_and_resolved_clear_round_trip() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("cart")?;
    let cell = value_cell();
    let data = Bytes::from_static(b"v1");

    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
    )];
    let marker = EventMarker::frozen(event(1), &writes, &[]);
    store.write_provisional(&c, &writes, Some(&marker)).await?;
    let staged = provisional_cells(&store, c.id()).await?;
    let (key, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected a provisional cell after stage"))?;
    assert_eq!(key, cell);
    assert_eq!(prov.data(), Some(&data));
    assert_eq!(prov.prev(), None);
    assert_eq!(prov.event(), event(1));

    store.mark_resolved(&c, slice::from_ref(&cell)).await?;
    assert_eq!(
        store.get(c.id(), &cell, event(2)).await?,
        Committed::new(Some(data))
    );
    assert!(provisional_cells(&store, c.id()).await?.is_empty());

    store
        .write_resolved(&c, &[(cell.clone(), None)], &[])
        .await?;
    assert_eq!(
        store.get(c.id(), &cell, event(2)).await?,
        Committed::new(None)
    );
    Ok(())
}

/// A section-0 cell at a distinct 4-byte coordinate, so a sized test can place
/// thousands of non-colliding committed and provisional cells.
pub(super) fn cell_i(i: u32) -> CellKey {
    CellKey {
        section: Section::new(0),
        coordinate: Coordinate::from_bytes(i.to_be_bytes().to_vec()),
    }
}

/// Drains a whole-section-0 forward scan (a concrete edge pair dominating
/// every 4-byte [`cell_i`] coordinate, which all begin `0x00`), returning the
/// yield count.
async fn drain_section_scan<S: CellStore>(store: &S, id: &CollectionId) -> Result<u32> {
    let low = Coordinate::empty();
    let high = Coordinate::from_bytes(vec![0xFF, 0xFF, 0xFF, 0xFF]);
    let scan = Scan {
        section: Section::new(0),
        start: ScanEdge::Included(&low),
        dir: Direction::Forward,
        end: ScanEdge::Included(&high),
        limit: None,
    };
    let stream = store.scan_cells(id, scan, event(1));
    futures::pin_mut!(stream);
    let mut scanned = 0_u32;
    while let Some(item) = stream.next().await {
        item?;
        scanned += 1;
    }
    Ok(scanned)
}

/// After a clean, fully-settled event, the recovery sweep issues **zero**
/// Cassandra queries: the stage's boundary check paid the one durable
/// event-marker point read (a cold memo miss), the settle recorded the marker
/// known-absent in the marker memo, so both the cold sweep (marker memo hit,
/// nothing listed) and the warm sweep (fjall short-circuit) touch nothing
/// durable. The zeros are meaningful: the same counter provably incremented
/// at the stage first.
#[tokio::test]
async fn warm_quiescence_issues_zero_queries() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    // Keep a clone of the bottom store so we can read its recovery counters; the
    // clone shares the same `Arc` counters as the one inside `Cached`.
    let bottom = fx.bottom_store(ScriptedOracle::default());
    let counts = bottom.recovery_reads();
    let store = Cached::new(test_db::cache("cassandra_warm")?, bottom);
    let c = collection("warm-quiescence")?;
    let cell = value_cell();

    // A clean event: stage, then settle through `commit_provisional` (the
    // production settle that deletes the event marker), leaving nothing
    // provisional and no unsettled marker.
    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(
            Some(Bytes::from_static(b"v")),
            Committed::new(None),
            event(1),
        ),
    )];
    let marker = EventMarker::frozen(event(1), &writes, &[]);
    store.write_provisional(&c, &writes, Some(&marker)).await?;
    store.commit_provisional(&c, &writes, &[]).await?;
    let staged_marker_reads = counts.marker_point_reads.load(Ordering::Relaxed);
    assert_eq!(
        staged_marker_reads, 1,
        "the stage boundary pays the one durable marker read on a cold memo"
    );

    // Cold sweep: `Cached` finds the collection unseeded and drives the
    // bottom store's seed — whose marker leg answers from the marker memo
    // (settled ⇒ known-absent), so no durable read of either kind.
    assert!(provisional_cells(&store, c.id()).await?.is_empty());
    // Warm sweep: the seeded, empty warm index short-circuits before the
    // bottom store entirely.
    assert!(provisional_cells(&store, c.id()).await?.is_empty());
    assert_eq!(
        counts.marker_point_reads.load(Ordering::Relaxed),
        staged_marker_reads,
        "a quiescent sweep issues no durable marker read"
    );
    assert_eq!(
        counts.cell_point_reads.load(Ordering::Relaxed),
        0,
        "a quiescent sweep issues no recovery point read"
    );
    assert_eq!(
        counts.provisional_in_queries.load(Ordering::Relaxed),
        0,
        "a quiescent sweep issues no raw batch IN query"
    );

    // A section clear does not add another durable marker read.
    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(
            Some(Bytes::from_static(b"w")),
            Committed::new(Some(Bytes::from_static(b"v"))),
            event(2),
        ),
    )];
    let clears = [SectionClear::frozen(cell.section, &writes)];
    let marker = EventMarker::frozen(event(2), &writes, &clears);
    store.write_provisional(&c, &writes, Some(&marker)).await?;
    store.commit_provisional(&c, &writes, &clears).await?;
    assert!(provisional_cells(&store, c.id()).await?.is_empty());
    assert!(provisional_cells(&store, c.id()).await?.is_empty());
    assert_eq!(
        counts.marker_point_reads.load(Ordering::Relaxed),
        staged_marker_reads,
        "the clear leg's stage boundary and sweeps ride the memo — no new durable marker read"
    );
    assert_eq!(
        counts.cell_point_reads.load(Ordering::Relaxed),
        0,
        "the clear leg adds no recovery point read"
    );
    assert_eq!(
        counts.provisional_in_queries.load(Ordering::Relaxed),
        0,
        "the clear leg adds no raw batch IN query"
    );
    Ok(())
}

/// Proves that recovery cost depends on provisional cells.
///
/// Each collection needs one durable marker read per assignment.
/// Committed cells do not increase this cost.
#[tokio::test]
async fn bounded_recovery_is_size_independent() -> Result<()> {
    const PROVISIONAL: u32 = 4;
    /// Provisional coordinates, disjoint from the committed range below.
    const PROV_BASE: u32 = 0xFFFF_0000;

    init_test_logging();
    let fx = fixture().await?;
    for committed in [32u32, 512] {
        let store = fx.bottom_store(ScriptedOracle::default());
        let c = collection(&format!("bounded-{committed}"))?;

        // `committed` resolved cells: the write never writes the marker slice,
        // but its write-side boundary is the first marker consumer of the
        // assignment, so it pays the one durable seed read and seeds the memo.
        let counts = store.recovery_reads();
        let resolved: Vec<(CellKey, Option<Bytes>)> = (0..committed)
            .map(|i| (cell_i(i), Some(Bytes::from(i.to_be_bytes().to_vec()))))
            .collect();
        store.write_resolved(&c, &resolved, &[]).await?;
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "the write boundary pays the one durable seed read on a cold memo"
        );

        // The first read rides the memo the write already seeded — no further
        // durable marker read.
        assert!(
            store
                .get(c.id(), &cell_i(0), event(1))
                .await?
                .get()
                .is_some(),
            "the committed cell reads back present",
        );
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "the first read rides the memo the write seeded — still one durable read"
        );
        // A whole-section scan (its clear resolution rides the memo: still one
        // durable marker read).
        let scanned = drain_section_scan(&store, c.id()).await?;
        assert_eq!(scanned, committed, "the scan yields every committed cell");
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "the scan's clear resolution rides the memo — still one durable marker read"
        );
        assert_eq!(
            counts.cell_point_reads.load(Ordering::Relaxed),
            0,
            "a committed get/scan never issues a recovery point read"
        );

        // A fixed handful of provisional cells staged by one event, listed by
        // its event marker. The stage's boundary check rides the same memo.
        let staged: Vec<(CellKey, ProvisionalWrite)> = (0..PROVISIONAL)
            .map(|i| {
                (
                    cell_i(PROV_BASE + i),
                    ProvisionalWrite::new(
                        Some(Bytes::from_static(b"p")),
                        Committed::new(None),
                        event(1),
                    ),
                )
            })
            .collect();
        let marker = EventMarker::frozen(event(1), &staged, &[]);
        store.write_provisional(&c, &staged, Some(&marker)).await?;
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "the stage boundary rides the memo — still one durable marker read"
        );

        // A cold sweep: the marker leg answers from the memo (zero durable
        // marker reads) + one raw `IN` query for the single section-0 chunk,
        // and NO per-coordinate point reads, independent of `committed` (the
        // same `counts` handle).
        let found = provisional_cells(&store, c.id()).await?;
        assert_eq!(found.len(), PROVISIONAL as usize);
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "the cold sweep rides the memo — still one durable marker read {committed}"
        );
        assert_eq!(
            counts.provisional_in_queries.load(Ordering::Relaxed),
            1,
            "one IN query for the single section-0 chunk, not #committed {committed}"
        );
        assert_eq!(
            counts.cell_point_reads.load(Ordering::Relaxed),
            0,
            "the batched sweep issues no per-coordinate point reads {committed}"
        );

        // A second sweep re-reads the listed cells but STILL pays no durable
        // marker read — the "at most one per collection per assignment" test.
        let again = provisional_cells(&store, c.id()).await?;
        assert_eq!(again.len(), PROVISIONAL as usize);
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "a second sweep pays no durable marker read {committed}"
        );
        assert_eq!(
            counts.provisional_in_queries.load(Ordering::Relaxed),
            2,
            "each sweep pays exactly one IN query per section {committed}"
        );
        assert_eq!(
            counts.cell_point_reads.load(Ordering::Relaxed),
            0,
            "no sweep issues a per-coordinate point read {committed}"
        );
    }
    Ok(())
}

/// Proves that a lost marker check causes one durable marker read.
///
/// The durable read restores the check and finds the unsettled marker.
#[tokio::test]
async fn presence_loss_forces_one_recheck_and_reseeds() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    // Both helpers open the same named index keyspace.
    let (_db, _cache, index) = test_db::keyspace_pair("cassandra_presence_degrade")?;
    index.clear()?;
    let store = fx.bottom_store_with(
        ScriptedOracle::default(),
        test_db::marker_checks("cassandra_presence_degrade")?,
    );
    let counts = store.recovery_reads();
    let c = collection("presence-degrade")?;
    let cell = value_cell();

    // Stage a provisional marker: the boundary pays the one cold durable marker
    // read and seeds unsettled + presence.
    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(
            Some(Bytes::from_static(b"v")),
            Committed::new(None),
            event(1),
        ),
    )];
    let marker = EventMarker::frozen(event(1), &writes, &[]);
    store.write_provisional(&c, &writes, Some(&marker)).await?;
    let after_stage = counts.marker_point_reads.load(Ordering::Relaxed);
    assert_eq!(
        after_stage, 1,
        "the stage boundary pays one cold durable marker read"
    );

    // A consult while seeded: presence hit, no durable read.
    store.unsettled_marker(c.id()).await?;
    assert_eq!(
        counts.marker_point_reads.load(Ordering::Relaxed),
        after_stage,
        "a seeded marker check answers from the unsettled map — no durable read",
    );

    // Lose the latch mid-assignment.
    index.clear()?;

    // The next consult pays exactly one durable re-check and still sees the
    // unsettled durable marker.
    let recovered = store.unsettled_marker(c.id()).await?;
    assert_eq!(
        counts.marker_point_reads.load(Ordering::Relaxed),
        after_stage + 1,
        "presence loss forces exactly one durable re-check",
    );
    assert!(
        recovered.is_some_and(|m| m.event() == event(1)),
        "the re-check reads the still-unsettled durable marker",
    );

    // And it re-seeded the latch: a further consult rides RAM again.
    store.unsettled_marker(c.id()).await?;
    assert_eq!(
        counts.marker_point_reads.load(Ordering::Relaxed),
        after_stage + 1,
        "the re-check re-seeds the marker check — no further durable read",
    );
    Ok(())
}

/// Committing a staged clear over a present base **deletes the row** (the
/// row-absence invariant): the cell reads back absent, and no residue row
/// lingers — a stale `encoding`/`version` would still be selected. Settles
/// through the routed `commit_provisional` path (the promote arm that owns
/// clear→delete). A deterministic falsifier of the row-absence shape
/// `assert_physical` asserts for every model-absent coordinate on the crash
/// traces; it isolates the committed-clear delete leg at the bottom store.
#[tokio::test]
async fn committed_clear_deletes_the_row() -> Result<()> {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;

    init_test_logging();
    let fx = fixture().await?;
    let oracle = ScriptedOracle::default();
    let store = fx.bottom_store(oracle.clone());
    let c = collection("clear-deletes")?;
    let cell = value_cell();
    let old = Bytes::from_static(b"old");

    // Committed base present, then stage a clear over it and settle committed.
    store
        .write_resolved(&c, &[(cell.clone(), Some(old.clone()))], &[])
        .await?;
    let write = ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(2));
    let writes = [(cell.clone(), write.clone())];
    let marker = EventMarker::frozen(event(2), &writes, &[]);
    store.write_provisional(&c, &writes, Some(&marker)).await?;
    let staged = provisional_cells(&store, c.id()).await?;
    let (_, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected a provisional cell after clear-over-present"))?;
    assert_eq!(prov.data(), None);
    assert_eq!(prov.prev(), Some(&old));

    oracle.record_message(Uuid::from_u128(2)).await?;
    store
        .commit_provisional(&c, &[(cell.clone(), write)], &[])
        .await?;

    assert_eq!(
        store.get(c.id(), &cell, event(3)).await?,
        Committed::new(None)
    );

    // The residue row would still be selected by its live `encoding`/`version`;
    // its absence proves the commit deleted the row rather than nulling columns.
    let cql = format!(
        "SELECT encoding, version FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} WHERE segment_id \
         = ? AND key = ? AND state_type = ? AND name = ? AND kind = 0 AND section = ? AND \
         coordinate = ?"
    );
    let id = c.id();
    let residue = fx
        .cassandra
        .session()
        .query_unpaged(
            cql,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
                i8::from(cell.section),
                cell.coordinate.as_bytes(),
            ),
        )
        .await?
        .into_rows_result()?
        .maybe_first_row::<(Option<i16>, Option<i32>)>()?;
    assert!(
        residue.is_none(),
        "committed clear must delete the row, leaving no residue: {residue:?}"
    );
    Ok(())
}
