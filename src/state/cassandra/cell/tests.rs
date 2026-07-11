//! Live-cluster tests for the Cassandra [`CassandraStore`] cell backend.
//!
//! These run against the local Cassandra node and the shared `prosody_test`
//! keyspace (migrated on driver connect). They exercise the part the pure
//! decoder test cannot: `prepare`/`bind`/round-trip of every cell statement,
//! including a committed clear deleting its row and the legacy null-null
//! residue read back live. The backend-generic
//! property suites ([`crate::state::tests::cell_suite`]) run here over the
//! production assembly `Cached<CassandraStore>` and `Overlay<Cached<…>>`, so
//! memory and Cassandra prove identical invariants. Each test mints a fresh
//! `segment_id` (and the property suites mint one per iteration) so rows never
//! collide across runs.

use super::decode::try_decode_marker;
use super::{
    CassandraStore, CellAddr, CellBatchRow, CellBlobs, CellKind, CellQueries, KeyRow,
    MarkerWriteRow, ResolvedRow, RowShape, StageRow, encode_cell_blobs, fits_one_batch,
};
use crate::cassandra::{BatchUnit, CassandraStore as CassandraSession};
use crate::state::cached::Cached;
use crate::state::cassandra::udt::RawEventRef;
use crate::state::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::fjall::test_db;
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::{
    ApplyTrace, FailingCellStore, OverlayTrace, OverwriteTrace, PoisonHandle, ProbedMarker,
    ScanTrace, ScriptedOracle, ShapeProbe, Trace, probed_parts, run_apply_idempotence,
    run_bottom_scan_trace, run_crash_equivalence_trace, run_overlay_trace, run_overwrite_trace,
    value_cell,
};
use crate::state::tests::support::{fresh_collection, probe as event};
use crate::state::{
    CollectionId, CollectionRef, SHARD_FANOUT_CONCURRENCY, StateKey, StateName, StateType,
};
use crate::test_util::{
    TEST_KEYSPACE, TEST_RUNTIME, integration_test_count, test_cassandra_config,
};
use crate::tracing::init_test_logging;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use quickcheck::{QuickCheck, TestResult};
use std::collections::BTreeSet;
use std::iter;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use uuid::Uuid;

/// [`ShapeProbe`] over the live cluster, read by raw CQL against the trace's
/// own partition key only (the isolation rule):
///
/// * `cell_rows` — the physically stored `kind=Cell` rows as `(section,
///   coordinate byte)`. A residue row (an absent value left with live
///   `encoding`/`version` columns) is returned; a `cell_delete`d or gap-erased
///   row is not — so exact-set equality against the model's present set catches
///   residue and lost rows alike.
/// * `standing_marker` — the whole `kind=Marker` slice, asserting the
///   structural shape (at most ONE row, at the fixed address `(0, empty)` — the
///   zero-per-coordinate-rows postcondition) before decoding the frozen payload
///   (staged set AND clear half) through the production decoder.
/// * `provisional_rows` — `kind=Cell` rows whose `event` is populated (filtered
///   in code; CQL cannot filter a regular column without ALLOW FILTERING, and
///   the partition is the trace's own — bounded).
struct CassandraShapeProbe {
    session: CassandraSession,
}

/// The six-column raw shape of one `kind=Marker` slice row the probe reads.
type MarkerSliceRow = (
    i8,
    Vec<u8>,
    Option<Vec<u8>>,
    Option<i16>,
    Option<i32>,
    Option<RawEventRef>,
);

/// The four partition-key binds of `id`'s collection.
fn pk_binds(id: &CollectionId) -> (&Uuid, &str, i8, &str) {
    (
        &id.state_key().segment_id,
        id.state_key().key.as_ref(),
        i8::from(id.state_type()),
        id.name().as_str(),
    )
}

impl ShapeProbe for CassandraShapeProbe {
    async fn cell_rows(&self, id: &CollectionId) -> Result<BTreeSet<(i8, u8)>> {
        use crate::cassandra::TABLE_KEYED_STATE_CELL;

        let cql = format!(
            "SELECT section, coordinate FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} WHERE \
             segment_id = ? AND key = ? AND state_type = ? AND name = ? AND kind = 0"
        );
        let result = self
            .session
            .session()
            .query_unpaged(cql, pk_binds(id))
            .await?
            .into_rows_result()?;
        let mut out = BTreeSet::new();
        for row in result.rows::<(i8, Vec<u8>)>()? {
            let (section, coordinate) = row?;
            if let Some(&byte) = coordinate.first() {
                out.insert((section, byte));
            }
        }
        Ok(out)
    }

    async fn standing_marker(&self, id: &CollectionId) -> Result<Option<ProbedMarker>> {
        use crate::cassandra::TABLE_KEYED_STATE_CELL;

        let cql = format!(
            "SELECT section, coordinate, data, encoding, version, event FROM \
             {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} WHERE segment_id = ? AND key = ? AND \
             state_type = ? AND name = ? AND kind = 1"
        );
        let result = self
            .session
            .session()
            .query_unpaged(cql, pk_binds(id))
            .await?
            .into_rows_result()?;
        let mut rows: Vec<MarkerSliceRow> = Vec::new();
        for row in result.rows::<MarkerSliceRow>()? {
            rows.push(row?);
        }
        // The structural postcondition: the whole marker slice is at most ONE
        // row, at the fixed address — zero per-coordinate rows exist.
        if rows.len() > 1 {
            return Err(eyre!(
                "marker slice holds {} rows, expected ≤ 1",
                rows.len()
            ));
        }
        let Some((section, coordinate, data, encoding, version, raw_event)) = rows.pop() else {
            return Ok(None);
        };
        if section != 0 || !coordinate.is_empty() {
            return Err(eyre!(
                "marker row off the fixed address: section {section}, coordinate {coordinate:?}"
            ));
        }
        let marker = try_decode_marker((data, encoding, version, raw_event))?;
        let (staged, clears) = probed_parts(&marker);
        Ok(Some((marker.event(), staged, clears)))
    }

    async fn provisional_rows(&self, id: &CollectionId) -> Result<BTreeSet<(i8, u8)>> {
        use crate::cassandra::TABLE_KEYED_STATE_CELL;

        let cql = format!(
            "SELECT section, coordinate, event FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? AND kind = 0"
        );
        let result = self
            .session
            .session()
            .query_unpaged(cql, pk_binds(id))
            .await?
            .into_rows_result()?;
        let mut out = BTreeSet::new();
        for row in result.rows::<(i8, Vec<u8>, Option<RawEventRef>)>()? {
            let (section, coordinate, event) = row?;
            if event.is_some()
                && let Some(&byte) = coordinate.first()
            {
                out.insert((section, byte));
            }
        }
        Ok(out)
    }
}

/// The production committed bottom assembly: fjall write-through over the
/// resolving Cassandra cell store.
type Bottom = Cached<CassandraStore<ScriptedOracle>>;

/// [`Bottom`] with the crash trace's lower fault seam between the cache and
/// the resolving store, so generated lower-store faults fire beneath the
/// cache.
type FaultyBottom = Cached<FailingCellStore<CassandraStore<ScriptedOracle>>>;

/// The shared driver session and prepared cell statements — the
/// partition-independent half both the bottom store and the property assemblies
/// are built from.
struct Fixture {
    cassandra: CassandraSession,
    queries: Arc<CellQueries>,
    registry: Arc<CollectionDefRegistry>,
}

async fn fixture() -> Result<Fixture> {
    let config = test_cassandra_config();
    let cassandra = CassandraSession::new(&config).await?;
    let queries = Arc::new(CellQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(Fixture {
        cassandra,
        queries,
        registry: Arc::new(CollectionDefRegistry::default()),
    })
}

impl Fixture {
    /// The bare resolving Cassandra cell store over `oracle`.
    fn bottom_store(&self, oracle: ScriptedOracle) -> CassandraStore<ScriptedOracle> {
        CassandraStore::new(
            self.cassandra.clone(),
            self.queries.clone(),
            oracle,
            self.registry.clone(),
        )
    }
}

/// A fresh-segment collection ref (no TTL) so concurrent runs never collide.
fn collection(name: &str) -> Result<CollectionRef> {
    Ok(CollectionRef::new(fresh_collection(name)?, None))
}

/// The still-provisional cells of a collection — the public, non-resolving way
/// to observe staged state (`get` would resolve and mutate it).
async fn provisional_cells<S>(
    store: &S,
    id: &CollectionId,
) -> Result<Vec<(CellKey, ProvisionalCell)>>
where
    S: CellStore,
{
    let stream = store.provisional_cells(id);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Stage a set, observe it provisional, promote, read back resolved — the
/// hot-path round-trip — then a direct resolved clear reads back absent.
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
fn cell_i(i: u32) -> CellKey {
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
/// known-absent in the RAM memo, so both the cold sweep (marker memo hit,
/// nothing listed) and the warm sweep (fjall short-circuit) touch nothing
/// durable. The zeros are non-vacuous: the same counter provably incremented
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
    // provisional and no standing marker.
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
    // bottom store's seed — whose marker leg answers from the RAM memo
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

    // The clear leg adds NO steady-state queries: a second event stages with
    // a section clear and settles through `commit_provisional(…, clears)` —
    // the Cov-Clr punch is fjall/coverage-only and the boundary rides the RAM
    // memo, so the durable marker-read count never moves again and both
    // post-settle sweeps stay at zero durable reads.
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
    Ok(())
}

/// Recovery cost is bounded by **#provisional, never #committed**: a cold
/// sweep over collections with wildly different committed-cell counts issues
/// at most ONE durable event-marker point read per collection per assignment
/// **total** (the shared memo, seeded by whichever consumer fires first —
/// pinned by staying at 1 across the first read's read-help seed, the second
/// read, the stage's boundary check, the cold sweep, AND a second sweep) plus
/// exactly one cell point read per provisional coordinate per sweep. The
/// committed cells live in the `kind=Cell` range recovery never touches, so
/// they cost nothing.
///
/// This also pins who pays the seed: a committed `write_resolved` is
/// marker-free by design (no counter moves), the FIRST read pays the one
/// durable marker read (read-help's cold-memo seed), and every later consumer
/// rides the memo. The fixed values are non-vacuous: the same counters stay
/// exactly 1 across five more marker consumers below.
///
/// Sizes are kept modest (not large production scale) so the live test stays
/// fast; 16× is ample to distinguish an O(#cells) regression, which would
/// read 32 vs 512 rather than a fixed 4.
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

        // `committed` resolved cells: marker-free (no event marker written) —
        // the counters must not move on the write itself.
        let counts = store.recovery_reads();
        let resolved: Vec<(CellKey, Option<Bytes>)> = (0..committed)
            .map(|i| (cell_i(i), Some(Bytes::from(i.to_be_bytes().to_vec()))))
            .collect();
        store.write_resolved(&c, &resolved, &[]).await?;
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            0,
            "a committed write never issues a durable marker read"
        );

        // The FIRST read pays the one durable marker read of the whole
        // assignment: read-help seeds the cold memo alongside the point read.
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
            "the first read seeds the marker memo with one durable read"
        );
        // A whole-section scan (its read-help rides the memo: still one
        // durable marker read).
        let scanned = drain_section_scan(&store, c.id()).await?;
        assert_eq!(scanned, committed, "the scan yields every committed cell");
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "the scan's read-help rides the memo — still one durable marker read"
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
        // marker reads) + one point read per listed coordinate, independent of
        // `committed` (the same `counts` handle).
        let found = provisional_cells(&store, c.id()).await?;
        assert_eq!(found.len(), PROVISIONAL as usize);
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "the cold sweep rides the memo — still one durable marker read {committed}"
        );
        assert_eq!(
            counts.cell_point_reads.load(Ordering::Relaxed),
            PROVISIONAL as usize,
            "recovery point reads bounded by #provisional, not #committed {committed}"
        );

        // A second sweep re-reads the listed cells but STILL pays no durable
        // marker read — the "at most one per collection per assignment" pin.
        let again = provisional_cells(&store, c.id()).await?;
        assert_eq!(again.len(), PROVISIONAL as usize);
        assert_eq!(
            counts.marker_point_reads.load(Ordering::Relaxed),
            1,
            "a second sweep pays no durable marker read {committed}"
        );
        assert_eq!(
            counts.cell_point_reads.load(Ordering::Relaxed),
            2 * PROVISIONAL as usize,
            "each sweep pays exactly #provisional cell point reads {committed}"
        );
    }
    Ok(())
}

/// Committing a staged clear over a present base **deletes the row** (the
/// row-absence invariant): the cell reads back absent, and no residue row
/// lingers — a stale `encoding`/`version` would still be selected. Settles
/// through the routed `commit_provisional` path (the promote arm that owns
/// clear→delete).
#[tokio::test]
async fn committed_clear_deletes_the_row() -> Result<()> {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;
    use crate::state::oracle::CommitOracle;

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

/// Legacy decode tolerance: the null-null-with-encoding residue shape is no
/// longer produced by any statement (a committed-absent cell deletes its row),
/// but rows written by earlier builds may still carry it. Seeded directly via
/// raw CQL, it must still read `Committed(None)`, never corruption — the
/// decoder's tolerance kept honest now that no code path produces the shape.
#[tokio::test]
async fn legacy_null_null_residue_reads_committed_none() -> Result<()> {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("legacy-residue")?;
    let cell = value_cell();
    let id = c.id();

    // Both blobs and `event` absent, `encoding` = 4 (RawZstdV1), `version` = 1
    // (INITIAL_VERSION) — the legacy promote-of-clear residue shape.
    let insert = format!(
        "INSERT INTO {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} (segment_id, key, state_type, name, \
         kind, section, coordinate, encoding, version) VALUES (?, ?, ?, ?, 0, ?, ?, 4, 1)"
    );
    fx.cassandra
        .session()
        .query_unpaged(
            insert,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
                i8::from(cell.section),
                cell.coordinate.as_bytes(),
            ),
        )
        .await?;

    assert_eq!(
        store.get(id, &cell, event(1)).await?,
        Committed::new(None),
        "the decoder must read the legacy residue as committed-absence"
    );
    Ok(())
}

/// The durable Cassandra `data` column stays zstd-compressed (`RawZstdV1`),
/// unlike the fjall cache which stores raw and lets fjall block-compress on
/// disk. Reads the column with a raw CQL `SELECT` so the store's transparent
/// decompression cannot mask a regression to raw storage.
#[tokio::test]
async fn cassandra_data_column_is_zstd_compressed() -> Result<()> {
    use super::encoding::{Encoding, decode_payload};
    use crate::cassandra::TABLE_KEYED_STATE_CELL;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("cart")?;
    let cell = value_cell();
    // A long, repetitive payload so the zstd frame is unmistakably smaller than
    // the raw bytes — a regression to raw storage fails both assertions.
    let payload = Bytes::from(vec![0xAB_u8; 4096]);
    store
        .write_resolved(&c, &[(cell, Some(payload.clone()))], &[])
        .await?;

    let cql = format!(
        "SELECT data FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} WHERE segment_id = ? AND key = \
         ? AND state_type = ? AND name = ?"
    );
    let id = c.id();
    let raw = fx
        .cassandra
        .session()
        .query_unpaged(
            cql,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
            ),
        )
        .await?
        .into_rows_result()?
        .maybe_first_row::<(Option<Vec<u8>>,)>()?
        .and_then(|(data,)| data)
        .ok_or_else(|| eyre!("data column missing"))?;

    assert_ne!(
        raw.as_slice(),
        payload.as_ref(),
        "durable data column must be compressed, not stored raw"
    );
    assert!(
        raw.len() < payload.len(),
        "zstd frame ({} bytes) should be smaller than the {} raw bytes",
        raw.len(),
        payload.len()
    );
    assert_eq!(
        decode_payload(&raw, Encoding::RawZstdV1)?,
        payload,
        "zstd frame must decompress to the payload"
    );
    Ok(())
}

/// A cell whose `event_ref` UDT carries a `timer_type` outside `{0,1,2,3}`
/// (external CQL corruption, or a forward-compat cross-version writer) must
/// reject the **one row** as `CorruptUdt`/`Permanent`, not tear the partition
/// down with scylla's `Terminal` `DeserializationError`. Injects via raw CQL so
/// the bad byte travels the real wire/materialization path — the unit test
/// cannot, since it bypasses scylla deserialization — and reads through the
/// **bare** `bottom_store` so no fjall cache hit serves the read without
/// decoding the row.
#[tokio::test]
async fn corrupt_timer_type_is_permanent_not_terminal() -> Result<()> {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::state::cassandra::CassandraCellStoreError;
    use crate::state::resolve::ResolveCellError;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("corrupt-timer")?;
    let id = c.id();

    // Stage one healthy provisional cell through the real store (so the event
    // marker lists its coordinate on the real wire path), then corrupt the
    // cell row's `event` UDT by raw CQL: the Timer arm rejects the unknown
    // `timer_type: 99` in the literal (whose own `kind: 1` field means Timer —
    // distinct from the clustering `kind` column). Recovery point-reads the
    // marker-listed coordinate and must reject the ONE row, not the partition.
    let cell = value_cell();
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
    let corrupt_cell = format!(
        "UPDATE {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} SET event = {{kind: 1, msg_dedup_id: \
         null, timer_type: 99, time: 0, tag: 0}} WHERE segment_id = ? AND key = ? AND state_type \
         = ? AND name = ? AND kind = 0 AND section = ? AND coordinate = ?"
    );
    let binds = (
        id.state_key().segment_id,
        id.state_key().key.as_ref(),
        i8::from(id.state_type()),
        id.name().as_str(),
        0_i8,
        b"" as &[u8],
    );
    fx.cassandra
        .session()
        .query_unpaged(corrupt_cell, binds)
        .await?;

    let stream = store.provisional_cells(id);
    futures::pin_mut!(stream);
    let err = loop {
        match stream.next().await {
            Some(Ok(_)) => {}
            Some(Err(e)) => break e,
            None => return Err(eyre!("expected a CorruptUdt error, got a clean scan")),
        }
    };
    assert_eq!(err.classify_error(), ErrorCategory::Permanent);
    assert!(
        matches!(
            err,
            ResolveCellError::Store(CassandraCellStoreError::CorruptUdt(_))
        ),
        "expected Store(CorruptUdt), got {err:?}"
    );
    Ok(())
}

/// Read-path uniqueness invariant: a present cell read back from the Cassandra
/// decode path is **uniquely owned** (`try_into_mut().is_ok()`), the production
/// fast path `CellView::get` relies on. Run over random non-empty payloads.
#[test]
fn prop_cassandra_present_cell_is_uniquely_owned() {
    async fn check(payload: Vec<u8>) -> Result<bool> {
        let fx = fixture().await?;
        let store = fx.bottom_store(ScriptedOracle::default());
        let c = collection("uniq")?;
        let cell = value_cell();
        let data = Bytes::from(payload);
        store
            .write_resolved(&c, &[(cell.clone(), Some(data))], &[])
            .await?;
        let Some(bytes) = store.get(c.id(), &cell, event(1)).await?.into_inner() else {
            return Err(eyre!("expected a present committed value"));
        };
        Ok(bytes.try_into_mut().is_ok())
    }

    fn prop(payload: Vec<u8>) -> TestResult {
        if payload.is_empty() {
            return TestResult::discard();
        }
        match TEST_RUNTIME.block_on(check(payload)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("present cell was a shared clone, not uniquely owned"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

/// Co-anchoring regression-prover: every cell of one multi-cell
/// write under a collection TTL must share a single write timestamp **and**
/// TTL. One same-partition `UNLOGGED BATCH` carries one batch write timestamp
/// and one coordinator TTL anchor, so `WRITETIME(data)` and `TTL(data)` are
/// identical across the cells; the old per-cell `execute()` loop stamped each
/// statement with a *distinct* monotonic client timestamp, so this fails
/// **deterministically** against it — the discriminator is the timestamp, not
/// wall-clock TTL drift, so there is no second-boundary flakiness. Run over
/// multi-cell writes of varying cardinality and payload sizes.
#[test]
fn prop_multi_cell_write_co_anchors_writetime_and_ttl() {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;
    use crate::state::cell_key::Coordinate;
    use crate::timers::duration::CompactDuration;

    async fn check(payloads: Vec<Vec<u8>>) -> Result<bool> {
        let fx = fixture().await?;
        let store = fx.bottom_store(ScriptedOracle::default());
        let id = CollectionId::new(
            StateKey::new(Uuid::new_v4(), Arc::from("k")),
            StateType::Application,
            StateName::try_new("co-anchor")?,
        );
        // A collection TTL so the `USING TTL` path is exercised; the batch must
        // apply one coordinator anchor across every cell.
        let c = CollectionRef::new(id.clone(), Some(CompactDuration::new(3_600)));
        let cells: Vec<(CellKey, Option<Bytes>)> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let cell = CellKey {
                    section: Section::new(0),
                    coordinate: Coordinate::from_bytes(vec![i as u8]),
                };
                (cell, Some(Bytes::from(p.clone())))
            })
            .collect();
        store.write_resolved(&c, &cells, &[]).await?;

        // `WRITETIME`/`TTL` are read functions (no schema change); both are
        // non-null because every cell wrote a present `data`.
        let cql = format!(
            "SELECT WRITETIME(data), TTL(data) FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? AND kind = 0 AND \
             section = ?"
        );
        let result = fx
            .cassandra
            .session()
            .query_unpaged(
                cql,
                (
                    id.state_key().segment_id,
                    id.state_key().key.as_ref(),
                    i8::from(id.state_type()),
                    id.name().as_str(),
                    0_i8,
                ),
            )
            .await?
            .into_rows_result()?;
        let mut writetimes: Vec<Option<i64>> = Vec::new();
        let mut ttls: Vec<Option<i32>> = Vec::new();
        for row in result.rows::<(Option<i64>, Option<i32>)>()? {
            let (writetime, ttl) = row?;
            writetimes.push(writetime);
            ttls.push(ttl);
        }
        // One batch ⇒ every cell shares the batch timestamp and the TTL anchor.
        let writetime_equal = writetimes.windows(2).all(|w| w[0] == w[1]);
        let ttl_equal = ttls.windows(2).all(|w| w[0] == w[1]);
        Ok(writetime_equal && ttl_equal)
    }

    fn prop(payloads: Vec<Vec<u8>>) -> TestResult {
        // ≥2 cells for "equal across cells" to discriminate; ≤256 so the
        // index-as-coordinate-byte stays unique.
        if payloads.len() < 2 || payloads.len() > 256 {
            return TestResult::discard();
        }
        match TEST_RUNTIME.block_on(check(payloads)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(
                "cells of one multi-cell write had differing WRITETIME/TTL — not one batch",
            ),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(prop as fn(Vec<Vec<u8>>) -> TestResult);
}

/// Builds the mixed-statement batch for the binding-order test: five one-row
/// units whose single flatten interleaves every [`RowShape`] across five
/// distinct prepared statements — stage A, promote B, resolved-write C,
/// `cell_delete` D, and the `marker_write` of the collection's event marker.
/// (`marker_delete` cannot join this batch: it targets the same fixed-address
/// row as the marker write, and within one batch the shared timestamp's
/// delete-wins tie-break would erase the write it is meant to prove — so the
/// delete is proven in a follow-up batch.) The blobs, cells, and `id` outlive
/// the returned borrows (the caller holds them).
fn mixed_binding_batch<'a>(
    q: &'a CellQueries,
    id: &'a CollectionId,
    blob_a: &'a CellBlobs,
    blob_c: &'a CellBlobs,
    marker_blob: &'a Bytes,
    cells: [&'a CellKey; 4],
) -> Vec<BatchUnit<CellBatchRow<'a>>> {
    use super::Pk;
    use smallvec::smallvec;

    let [cell_a, cell_b, cell_c, cell_d] = cells;
    let pk = Pk::of(id);
    let (addr_a, addr_b, addr_c, addr_d) = (
        CellAddr::new(pk, cell_a),
        CellAddr::new(pk, cell_b),
        CellAddr::new(pk, cell_c),
        CellAddr::new(pk, cell_d),
    );
    vec![
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.write_provisional_no_ttl,
                row: RowShape::Stage(StageRow {
                    ttl: None,
                    data: blob_a.data.as_deref(),
                    prev_data: None,
                    encoding: blob_a.encoding,
                    version: blob_a.version,
                    event: event(2),
                    addr: addr_a,
                }),
            }],
        ),
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.mark_resolved,
                row: RowShape::Key(KeyRow {
                    kind: CellKind::Cell,
                    addr: addr_b,
                }),
            }],
        ),
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.write_resolved_no_ttl,
                row: RowShape::Resolved(ResolvedRow {
                    ttl: None,
                    data: blob_c.data.as_deref(),
                    encoding: blob_c.encoding,
                    version: blob_c.version,
                    addr: addr_c,
                }),
            }],
        ),
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.cell_delete,
                row: RowShape::Key(KeyRow {
                    kind: CellKind::Cell,
                    addr: addr_d,
                }),
            }],
        ),
        BatchUnit::new(
            1_024,
            smallvec![CellBatchRow {
                statement: &q.marker_write_no_ttl,
                row: RowShape::MarkerWrite(MarkerWriteRow {
                    ttl: None,
                    payload: marker_blob,
                    event: event(2),
                    addr: CellAddr::marker(pk),
                }),
            }],
        ),
    ]
}

/// Positional binding-order proof (the one silent-failure surface):
/// `scylla::Batch` binds its statement list 1:1 with the value list, and on a
/// count/order mismatch scylla falls back to an **empty** context with no
/// error — a misordered `unzip` flatten would bind a row against the wrong
/// statement's columns silently. Build ONE batch whose single flatten
/// interleaves every [`RowShape`] across **five distinct prepared
/// statements** — stage a cell, promote a pre-provisioned cell, write a fresh
/// resolved value, row-delete a pre-seeded cell, and write the event marker —
/// with **distinct payloads** so any cross-binding corrupts an observable
/// value. Read everything back through a FRESH store (a cold marker memo, so
/// the recovery read decodes the batch-written marker): each value landing
/// correctly proves its statement bound its own columns. A follow-up batch
/// proves `marker_delete` (same-row timestamp ties bar it from the first
/// batch — see [`mixed_binding_batch`]).
#[tokio::test]
async fn mixed_statement_batch_binds_each_statement_to_its_own_columns() -> Result<()> {
    use super::encoding::encode_payload;
    use super::{Pk, VALUE_ENCODING, marker_delete_unit};
    use crate::state::cell_key::Coordinate;
    use crate::state::marker::encode_marker_payload;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("mixed-batch")?;
    let id = c.id().clone();

    let cell = |b: u8| CellKey {
        section: Section::new(0),
        coordinate: Coordinate::from_bytes(vec![b]),
    };
    let (cell_a, cell_b, cell_c, cell_d) = (cell(1), cell(2), cell(3), cell(4));
    let (data_a, data_b, data_c, data_d) = (
        Bytes::from_static(b"aaa"),
        Bytes::from_static(b"bbb"),
        Bytes::from_static(b"ccc"),
        Bytes::from_static(b"ddd"),
    );

    // Pre-provision B so the mixed batch can promote it.
    let writes_b = [(
        cell_b.clone(),
        ProvisionalWrite::new(Some(data_b.clone()), Committed::new(None), event(1)),
    )];
    let marker_b = EventMarker::frozen(event(1), &writes_b, &[]);
    store
        .write_provisional(&c, &writes_b, Some(&marker_b))
        .await?;
    // Pre-seed D resolved so the batch's `cell_delete` has a row to remove.
    store
        .write_resolved(&c, &[(cell_d.clone(), Some(data_d.clone()))], &[])
        .await?;

    // Owned blobs the bound rows borrow into; must outlive the awaited batch.
    let blob_a = encode_cell_blobs(Some(&data_a), None)?;
    let blob_c = encode_cell_blobs(Some(&data_c), None)?;
    // The batch's event marker lists exactly A (the one cell it stages),
    // encoded through the production payload + blob conventions.
    let staged_a = [(
        cell_a.clone(),
        ProvisionalWrite::new(Some(data_a.clone()), Committed::new(None), event(2)),
    )];
    let marker_blob = encode_payload(
        &encode_marker_payload(&EventMarker::frozen(event(2), &staged_a, &[]))?,
        VALUE_ENCODING,
    )?;
    // One batch, one flatten, five distinct statements interleaved.
    let units = mixed_binding_batch(
        &fx.queries,
        &id,
        &blob_a,
        &blob_c,
        &marker_blob,
        [&cell_a, &cell_b, &cell_c, &cell_d],
    );
    fx.cassandra
        .execute_unlogged_batches(&units, 1 << 20, 4_096, SHARD_FANOUT_CONCURRENCY)
        .await?;

    // Read back through a FRESH store: its cold memo forces the durable
    // marker read, so recovery decodes the batch-written marker (proving the
    // `marker_write` row bound its own columns), finds it lists exactly A,
    // and reads A back provisional with A's payload (the stage row bound its
    // columns). B was promoted out of the provisional set by the key-only
    // promote row.
    let reader = fx.bottom_store(ScriptedOracle::default());
    let staged = provisional_cells(&reader, &id).await?;
    assert_eq!(staged.len(), 1, "only A stays provisional: {staged:?}");
    let (key, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected A provisional"))?;
    assert_eq!(key, cell_a);
    assert_eq!(prov.data(), Some(&data_a));

    // B promoted to its own payload (the key-only promote row bound its
    // columns); C written fresh resolved to its own payload (the resolved-write
    // row bound its columns).
    assert_eq!(
        reader.get(&id, &cell_b, event(3)).await?,
        Committed::new(Some(data_b))
    );
    assert_eq!(
        reader.get(&id, &cell_c, event(3)).await?,
        Committed::new(Some(data_c))
    );
    // D's row was deleted (the `cell_delete` bound its own `kind=Cell` key
    // columns, not the marker slice's `kind=Marker`), so it reads absent.
    assert_eq!(
        reader.get(&id, &cell_d, event(3)).await?,
        Committed::new(None)
    );

    // Follow-up batch: `marker_delete` removes the fixed-address marker row —
    // a second fresh store's cold recovery then finds no marker at all.
    let delete = [marker_delete_unit(Pk::of(&id), &fx.queries)];
    fx.cassandra
        .execute_unlogged_batches(&delete, 1 << 20, 4_096, SHARD_FANOUT_CONCURRENCY)
        .await?;
    let reader = fx.bottom_store(ScriptedOracle::default());
    assert!(
        provisional_cells(&reader, &id).await?.is_empty(),
        "marker_delete removed the marker row, so cold recovery lists nothing"
    );
    Ok(())
}

/// Converts a property body's `Result<bool>` into a `TestResult`, surfacing the
/// error on failure (a store/setup error is a broken environment, not a
/// shrinkable property failure).
fn finish(result: Result<bool>) -> TestResult {
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::failed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}

/// The crash-recovery-equivalence property over the production
/// `Cached<CassandraStore>` assembly at the full clears-bearing alphabet
/// (crash-recovery equivalence and oracle-correctness properties). A "crash"
/// rebuilds the cache cold over the same durable Cassandra rows and oracle
/// set, so every durable clear leg — gap range deletes, the indivisible
/// gaps+marker-delete settle unit, the marker payload's clear half, read-help
/// — runs beneath the production cache with its Cov-Clr punches, and the
/// lower fault seam (`FaultDepth::Lower` settle failures + directed
/// post-failure reads, stage faults) fires beneath the cache against live
/// CQL. The tempdir and fjall client outlive every store the `make` closure
/// mints (the await completes before they drop).
#[test]
fn prop_cassandra_cell_crash_equivalence() {
    async fn run(trace: Trace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        // Each `make` is a crash: a cold fjall cache over the same durable
        // Cassandra rows, with the runner's lower fault seam between them.
        // `cold_cache` clears the shared `cassandra_crash` keyspace pair (a
        // cheap journal marker, no keyspace-creation fsync) instead of
        // minting a fresh workspace per make; distinct v4 segments per
        // iteration keep the shared keyspace disjoint.
        let make = |handle: &PoisonHandle| -> Result<FaultyBottom> {
            Ok(Cached::new(
                test_db::cold_cache("cassandra_crash")?,
                FailingCellStore::with_handle(fx.bottom_store(oracle.clone()), handle.clone()),
            ))
        };
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_crash_equivalence_trace(make, oracle.clone(), trace, &probe).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck((|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(Trace) -> TestResult);
}

/// Apply idempotence over the bare live store: any generated interleaving of
/// marker resolution, verdict-matching settle re-applies, and per-cell
/// first-touches over one staged set with durable section clears converges to
/// the verdict state — no marker, no provisional residue, exact row shape.
#[test]
fn prop_cassandra_apply_idempotence() {
    async fn run(input: ApplyTrace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_apply_idempotence(fx.bottom_store(oracle.clone()), oracle, input, &probe).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|input| finish(TEST_RUNTIME.block_on(run(input)))) as fn(ApplyTrace) -> TestResult,
        );
}

/// Implicit-overwrite soundness over `Cached<CassandraStore>`: each overwrite
/// resolves its predecessor's provisional cell through the oracle on read, with
/// no explicit promote or rollback.
#[test]
fn prop_cassandra_cell_implicit_overwrite() {
    async fn run(trace: OverwriteTrace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        // Each op reads its committed base through a fresh COLD store, so
        // `make` clears the shared `cassandra_overwrite` keyspace pair (no
        // keyspace-creation fsync); distinct v4 segments per iteration keep it
        // disjoint.
        let make = || -> Result<Bottom> {
            Ok(Cached::new(
                test_db::cold_cache("cassandra_overwrite")?,
                fx.bottom_store(oracle.clone()),
            ))
        };
        run_overwrite_trace(make, oracle.clone(), trace).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(OverwriteTrace) -> TestResult,
        );
}

/// A single `Cached<CassandraStore>` over the shared `cassandra_overlay`
/// fjall keyspace pair (warm-reuse; distinct v4 segments keep iterations
/// disjoint).
fn assembly(fx: &Fixture) -> Result<Bottom> {
    Ok(Cached::new(
        test_db::cache("cassandra_overlay")?,
        fx.bottom_store(ScriptedOracle::default()),
    ))
}

/// Unified view soundness over `Overlay<Cached<CassandraStore>>`: point `get`s,
/// range `scan`s (bounds, direction, limit, early-stop), dirty buffering, and
/// committed writes intermixed in one trace, all vs the sorted-map oracle
/// (unified-view soundness and oracle-correctness properties).
#[test]
fn prop_cassandra_overlay_view() {
    async fn run(trace: OverlayTrace) -> Result<bool> {
        let fx = fixture().await?;
        // Box the future: the assembly + trace exceed clippy's large-future
        // threshold on the stack.
        Box::pin(run_overlay_trace(assembly(&fx)?, trace)).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(OverlayTrace) -> TestResult,
        );
}

/// Scan correctness directly over `CassandraStore::scan_cells` — the live
/// `ORDER BY ASC/DESC`, clustering-range bounds, and `LIMIT`/in-code `end` the
/// overlay merge delegates to.
#[test]
fn prop_cassandra_bottom_scan() {
    async fn run(trace: ScanTrace) -> Result<bool> {
        let fx = fixture().await?;
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_bottom_scan_trace(fx.bottom_store(ScriptedOracle::default()), trace, &probe).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(ScanTrace) -> TestResult,
        );
}

/// `TTL(data)` surfacing for the co-expiry stamp (no cluster needed — pure
/// boundary cases). A NULL means the cell has no TTL → never expires (`None`).
/// A present value is the whole remaining seconds and must round-trip —
/// crucially `0` (sub-second remaining) maps to an *immediate* expiry, never
/// `None`, or a fjall entry would outlive a durable row that dies within the
/// second.
#[test]
fn ttl_seconds_surfacing_distinguishes_no_ttl_from_sub_second() {
    use super::ttl_seconds_to_duration;
    use crate::timers::duration::CompactDuration;

    assert_eq!(ttl_seconds_to_duration(None), None, "NULL ⇒ no TTL (never)");
    assert_eq!(
        ttl_seconds_to_duration(Some(0_i32)),
        Some(CompactDuration::new(0)),
        "0 ⇒ sub-second remaining, an immediate expiry — never None"
    );
    assert_eq!(
        ttl_seconds_to_duration(Some(42_i32)),
        Some(CompactDuration::new(42))
    );
    assert_eq!(
        ttl_seconds_to_duration(Some(-1_i32)),
        Some(CompactDuration::new(0)),
        "a defensive negative also stamps an immediate expiry, not never"
    );
}

/// The cache-fill co-expiry matches the value actually returned, not the
/// pre-resolution `TTL(data)`. A staged clear over a present base (`data`
/// NULL, `prev_data` present, finite stage TTL) whose event the oracle never
/// committed rolls back to `prev` on read — and both cache-fill paths must
/// report a finite co-expiry no later than the stage TTL. Reporting `None`
/// ("never expires", the old `TTL(data)`-only read) stamped the fjall entry
/// to strictly outlive the durable row, serving the value after the row died.
#[tokio::test]
async fn rolled_back_staged_clear_reports_finite_co_expiry() -> Result<()> {
    use crate::timers::duration::CompactDuration;
    use futures::TryStreamExt;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let ttl = CompactDuration::new(3_600);
    let old = Bytes::from_static(b"old");

    // One collection per cache-fill path — resolution mutates the cell, so the
    // two paths each need their own rolled-back read.
    for path in ["get_for_cache", "scan_for_cache"] {
        let c = CollectionRef::new(
            collection(&format!("co-expiry-{path}"))?.id().clone(),
            Some(ttl),
        );
        let cell = value_cell();
        store
            .write_resolved(&c, &[(cell.clone(), Some(old.clone()))], &[])
            .await?;
        // `event(1)` is never recorded in the oracle, so resolution rolls the
        // staged clear back to `prev`.
        let writes = [(
            cell.clone(),
            ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(1)),
        )];
        let marker = EventMarker::frozen(event(1), &writes, &[]);
        store.write_provisional(&c, &writes, Some(&marker)).await?;

        let (value, co_expiry) = if path == "get_for_cache" {
            let (committed, co_expiry) = store.get_for_cache(c.id(), &cell, event(2)).await?;
            (committed.into_inner(), co_expiry)
        } else {
            // A concrete edge pair covering the single value cell (empty
            // coordinate) — a whole-section scan with `ScanEdge`.
            let low = Coordinate::empty();
            let high = Coordinate::from_bytes(vec![0xFF, 0xFF, 0xFF, 0xFF]);
            let scan = Scan {
                section: Section::new(0),
                start: ScanEdge::Included(&low),
                dir: Direction::Forward,
                end: ScanEdge::Included(&high),
                limit: None,
            };
            let stream = store.scan_for_cache(c.id(), scan, event(2));
            futures::pin_mut!(stream);
            let (_, bytes, co_expiry) = stream
                .try_next()
                .await?
                .ok_or_else(|| eyre!("{path}: the rolled-back cell must scan back present"))?;
            (Some(bytes), co_expiry)
        };

        assert_eq!(value.as_ref(), Some(&old), "{path}: rollback returns prev");
        let co_expiry = co_expiry.ok_or_else(|| {
            eyre!("{path}: a rolled-back staged clear must report a finite co-expiry, not never")
        })?;
        assert!(
            co_expiry <= ttl,
            "{path}: co-expiry {co_expiry:?} must not exceed the stage TTL {ttl:?}"
        );
    }
    Ok(())
}

/// Marker TTL co-expiry pin: staging on a TTL'd collection stamps the
/// event-marker row with the collection TTL, so the marker dies with the
/// newest staged cell. Structurally untestable by the trace suites (their
/// collection pool is TTL-less), so pinned directly with a raw-CQL
/// `TTL(data)` read at the fixed marker address.
#[tokio::test]
async fn event_marker_co_expires_with_collection_ttl() -> Result<()> {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;
    use crate::timers::duration::CompactDuration;

    const TTL: u32 = 3_600;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = CollectionRef::new(
        collection("marker-ttl")?.id().clone(),
        Some(CompactDuration::new(TTL)),
    );
    let cell = value_cell();
    let writes = [(
        cell,
        ProvisionalWrite::new(
            Some(Bytes::from_static(b"v")),
            Committed::new(None),
            event(1),
        ),
    )];
    let marker = EventMarker::frozen(event(1), &writes, &[]);
    store.write_provisional(&c, &writes, Some(&marker)).await?;

    let cql = format!(
        "SELECT TTL(data) FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} WHERE segment_id = ? AND \
         key = ? AND state_type = ? AND name = ? AND kind = 1 AND section = 0 AND coordinate = ?"
    );
    let id = c.id();
    let remaining = fx
        .cassandra
        .session()
        .query_unpaged(
            cql,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
                b"" as &[u8],
            ),
        )
        .await?
        .into_rows_result()?
        .maybe_first_row::<(Option<i32>,)>()?
        .and_then(|(ttl,)| ttl)
        .ok_or_else(|| eyre!("the event-marker row or its TTL is missing"))?;
    assert!(
        remaining > 0_i32 && remaining <= TTL as i32,
        "marker TTL {remaining} must lie in (0, {TTL}]"
    );
    assert!(
        remaining > TTL as i32 - 60_i32,
        "marker TTL {remaining} must be freshly stamped (60s slack for elapsed wall time)"
    );
    Ok(())
}

/// The `Cached` stage-boundary punch: event A stages two coordinates through
/// the cached assembly (fjall covers A's stage-time `prev`s as the committed
/// projections), A's commit marker is recorded but the settle never runs (the
/// skipped-settle window); event B then stages ONE overlapping coordinate
/// through the same assembly. The lower store's stage boundary resolves A's
/// event marker *beneath* the cache, so `Cached::write_provisional` must punch
/// A's marker-listed coordinates BEFORE forwarding down — without the punch,
/// A's untouched coordinate keeps serving the stale covered `prev` verbatim
/// forever.
#[tokio::test]
async fn stage_boundary_punches_foreign_marker_coverage() -> Result<()> {
    use crate::state::oracle::CommitOracle;

    init_test_logging();
    let fx = fixture().await?;
    let oracle = ScriptedOracle::default();
    let store = Cached::new(
        test_db::cache("cassandra_boundary_punch")?,
        fx.bottom_store(oracle.clone()),
    );
    let c = collection("boundary-punch")?;
    let id = c.id().clone();
    let (cell0, cell1) = (cell_i(0), cell_i(1));
    let (base0, base1) = (Bytes::from_static(b"base0"), Bytes::from_static(b"base1"));

    // Committed bases, covered by the write-through publish.
    store
        .write_resolved(
            &c,
            &[
                (cell0.clone(), Some(base0.clone())),
                (cell1.clone(), Some(base1.clone())),
            ],
            &[],
        )
        .await?;

    // Event A stages over both coordinates; its stage re-publishes the prevs
    // (the bases) as the covered committed projections. A's commit marker is
    // recorded — A is committed — but the settle is never attempted.
    let writes_a = [
        (
            cell0.clone(),
            ProvisionalWrite::new(
                Some(Bytes::from_static(b"a0")),
                Committed::new(Some(base0)),
                event(1),
            ),
        ),
        (
            cell1.clone(),
            ProvisionalWrite::new(
                Some(Bytes::from_static(b"a1")),
                Committed::new(Some(base1)),
                event(1),
            ),
        ),
    ];
    let marker_a = EventMarker::frozen(event(1), &writes_a, &[]);
    store
        .write_provisional(&c, &writes_a, Some(&marker_a))
        .await?;
    oracle.record_message(Uuid::from_u128(1)).await?;

    // Event B stages the overlapping coordinate 1. Its prev-read may serve
    // the covered pre-settle value (the accepted bounded window — nothing is
    // asserted on it); the stage's boundary then resolves A's marker beneath
    // the cache and the punch drops A's listed coordinates from coverage.
    let prev_b = store.get(&id, &cell1, event(2)).await?;
    let writes_b = [(
        cell1.clone(),
        ProvisionalWrite::new(Some(Bytes::from_static(b"b1")), prev_b, event(2)),
    )];
    let marker_b = EventMarker::frozen(event(2), &writes_b, &[]);
    store
        .write_provisional(&c, &writes_b, Some(&marker_b))
        .await?;

    // A's untouched coordinate 0 must read A's committed data: the punch
    // uncovered it, so the read falls through to the boundary-promoted row
    // instead of serving the stale covered prev.
    assert_eq!(
        store.get(&id, &cell0, event(3)).await?,
        Committed::new(Some(Bytes::from_static(b"a0"))),
        "the stage-boundary punch must drop the stale covered prev"
    );
    Ok(())
}

/// The pure single-batch packing decision `write_provisional` bases its
/// marker-first ordering on: a unit set fits one batch iff the weight sum is
/// within the byte budget AND the unit count is within the statement budget.
/// (The intra-call tear of an over-budget stage cannot be injected through
/// the trait; the ordering is enforced by the two sequential awaits plus this
/// decision, and the staged-coverage postcondition guards the durable shape
/// on every generated trace.)
#[test]
fn fits_one_batch_decides_on_both_budgets() {
    // Strictly under both budgets, and exactly at both boundaries.
    assert!(fits_one_batch([1, 2].into_iter(), 5, 3));
    assert!(fits_one_batch([2, 3].into_iter(), 5, 2));
    // Over the byte budget by one.
    assert!(!fits_one_batch([3, 3].into_iter(), 5, 8));
    // Over the count budget by one.
    assert!(!fits_one_batch([1, 1, 1].into_iter(), 100, 2));
    // Empty always fits.
    assert!(fits_one_batch(iter::empty(), 0, 0));
}
