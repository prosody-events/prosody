//! Live-cluster tests for the Cassandra [`CassandraStore`] cell backend.
//!
//! These run against the local Cassandra node and the shared `prosody_test`
//! keyspace (migrated on driver connect). They exercise the part the pure
//! decoder test cannot: `prepare`/`bind`/round-trip of every cell statement,
//! including the promote-of-clear residue read back live. The backend-generic
//! property suites ([`crate::state::tests::cell_suite`]) run here over the
//! production assembly `Cached<CassandraStore>` and `Overlay<Cached<…>>`, so
//! memory and Cassandra prove identical invariants. Each test mints a fresh
//! `segment_id` (and the property suites mint one per iteration) so rows never
//! collide across runs.

use super::{
    CassandraStore, CellAddr, CellBatchRow, CellBlobs, CellKind, CellQueries, IndexUpsertRow,
    KeyRow, ResolvedRow, RowShape, StageRow, encode_cell_blobs,
};
use crate::cassandra::{BatchUnit, CassandraStore as CassandraSession};
use crate::state::cached::Cached;
use crate::state::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use crate::state::fjall::test_db;
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::{
    OverlayTrace, OverwriteTrace, ScanTrace, ScriptedOracle, Trace, run_bottom_scan_trace,
    run_crash_equivalence_trace, run_overlay_trace, run_overwrite_trace, value_cell,
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
use std::ops::Bound;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use uuid::Uuid;

/// The production committed bottom assembly: fjall write-through over the
/// resolving Cassandra cell store.
type Bottom = Cached<CassandraStore<ScriptedOracle>>;

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

    store
        .write_provisional(
            &c,
            &[(
                cell.clone(),
                ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
            )],
        )
        .await?;
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

    store.write_resolved(&c, &[(cell.clone(), None)]).await?;
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

/// After a clean, fully-resolved event, the recovery sweep issues **zero**
/// Cassandra queries once its collection is seeded: the first (cold) sweep runs
/// the one bounded `kind=Index` seed read and marks the collection seeded in
/// the disk-backed warm index; the second (warm) sweep short-circuits on the
/// local fjall index. The warm gate lives on `Cached` (which owns the fjall
/// cache), so this runs the production `Cached<CassandraStore>` assembly
/// and reads the `CassandraStore`'s `RecoveryReadCounts` — which `Cached`
/// touches only on a cold sweep, so the "zero" is non-vacuous (the cold sweep
/// provably incremented them first).
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

    // A clean event: stage then promote, leaving nothing provisional and no
    // live `kind=Index` marker.
    store
        .write_provisional(
            &c,
            &[(
                cell.clone(),
                ProvisionalWrite::new(
                    Some(Bytes::from_static(b"v")),
                    Committed::new(None),
                    event(1),
                ),
            )],
        )
        .await?;
    store.mark_resolved(&c, slice::from_ref(&cell)).await?;

    // Cold sweep: `Cached` finds the collection unseeded, so it drives the
    // bottom store's bounded seed read (finds no live marker) and seeds the warm
    // index.
    assert!(provisional_cells(&store, c.id()).await?.is_empty());
    let seed_index = counts.index_range_reads.load(Ordering::Relaxed);
    let seed_points = counts.cell_point_reads.load(Ordering::Relaxed);
    assert!(
        seed_index >= 1,
        "the cold sweep must issue the kind=Index seed read"
    );

    // Warm sweep: the seeded, empty warm index short-circuits with no further
    // Cassandra query — `Cached` never enters the bottom store's cold seed.
    assert!(provisional_cells(&store, c.id()).await?.is_empty());
    assert_eq!(
        counts.index_range_reads.load(Ordering::Relaxed),
        seed_index,
        "a warm quiescence issues no kind=Index read"
    );
    assert_eq!(
        counts.cell_point_reads.load(Ordering::Relaxed),
        seed_points,
        "a warm quiescence issues no point read"
    );
    Ok(())
}

/// Recovery cost is bounded by **#provisional, never #committed**: a cold sweep
/// over collections with wildly different committed-cell counts issues the same
/// one `kind=Index` range read plus exactly one point read per provisional
/// coordinate. The committed cells live in the `kind=Cell` range recovery never
/// scans, so they cost nothing.
///
/// This also pins the **set-gated read-skip**: before any cell is staged,
/// a committed `get` and full-section `scan` over the clean cells touch only
/// the `kind=Cell` range — **neither** recovery counter moves — so reads never
/// consult the provisional index (byte-for-byte today's cost). The zeros are
/// non-vacuous: the very same counters provably increment on the sweep below.
///
/// Sizes are kept modest (not large production scale) so the live
/// test stays fast and its committed-cell `index_delete` tombstones stay under
/// Cassandra's scan-warn threshold; 16× is ample to distinguish an O(#cells)
/// regression, which would read 32 vs 512 rather than a fixed 4.
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

        // `committed` resolved cells: no live `kind=Index` marker.
        let resolved: Vec<(CellKey, Option<Bytes>)> = (0..committed)
            .map(|i| (cell_i(i), Some(Bytes::from(i.to_be_bytes().to_vec()))))
            .collect();
        store.write_resolved(&c, &resolved).await?;

        // Read-skip: a committed get + full-section scan over the clean cells
        // consult only the `kind=Cell` range — no recovery read of either kind.
        let counts = store.recovery_reads();
        assert!(
            store
                .get(c.id(), &cell_i(0), event(1))
                .await?
                .get()
                .is_some(),
            "the committed cell reads back present",
        );
        let scan = Scan {
            section: Section::new(0),
            start: Bound::Unbounded,
            dir: Direction::Forward,
            end: Bound::Unbounded,
            limit: None,
        };
        let stream = store.scan_cells(c.id(), scan, event(1));
        futures::pin_mut!(stream);
        let mut scanned = 0_u32;
        while let Some(item) = stream.next().await {
            item?;
            scanned += 1;
        }
        assert_eq!(scanned, committed, "the scan yields every committed cell");
        assert_eq!(
            counts.index_range_reads.load(Ordering::Relaxed),
            0,
            "a committed get/scan never issues a kind=Index read"
        );
        assert_eq!(
            counts.cell_point_reads.load(Ordering::Relaxed),
            0,
            "a committed get/scan never issues a recovery point read"
        );

        // A fixed handful of provisional cells: each a live marker.
        let staged: Vec<(CellKey, ProvisionalWrite)> = (0..PROVISIONAL)
            .map(|i| {
                (
                    cell_i(PROV_BASE + i),
                    ProvisionalWrite::new(
                        Some(Bytes::from_static(b"p")),
                        Committed::new(None),
                        event(u128::from(i)),
                    ),
                )
            })
            .collect();
        store.write_provisional(&c, &staged).await?;

        // A cold sweep: one bounded seed read + one point read per provisional
        // coordinate, independent of `committed` (the same `counts` handle).
        let found = provisional_cells(&store, c.id()).await?;
        assert_eq!(found.len(), PROVISIONAL as usize);
        assert_eq!(
            counts.index_range_reads.load(Ordering::Relaxed),
            1,
            "one bounded kind=Index seed read regardless of committed size {committed}"
        );
        assert_eq!(
            counts.cell_point_reads.load(Ordering::Relaxed),
            PROVISIONAL as usize,
            "recovery point reads bounded by #provisional, not #committed {committed}"
        );
    }
    Ok(())
}

/// Stage a clear over a present base, observe it provisional (`data` null,
/// `prev` present), promote, and read back absent — the promote-of-clear
/// residue decoded live (encoding/version linger, both blobs null).
#[tokio::test]
async fn provisional_clear_over_present_promotes_to_absent() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("cart")?;
    let cell = value_cell();
    let old = Bytes::from_static(b"old");

    store
        .write_provisional(
            &c,
            &[(
                cell.clone(),
                ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(2)),
            )],
        )
        .await?;
    let staged = provisional_cells(&store, c.id()).await?;
    let (_, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected a provisional cell after clear-over-present"))?;
    assert_eq!(prov.data(), None);
    assert_eq!(prov.prev(), Some(&old));

    store.mark_resolved(&c, slice::from_ref(&cell)).await?;
    assert_eq!(
        store.get(c.id(), &cell, event(3)).await?,
        Committed::new(None)
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
        .write_resolved(&c, &[(cell, Some(payload.clone()))])
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

    // The corrupt `kind=0` (Cell) row: `event` non-NULL alone reaches the
    // validator — with data/prev/encoding/version all NULL the decoder skips
    // straight to `try_into_event`, where the Timer arm rejects the unknown
    // `timer_type: 99` in the `event_ref` UDT literal (whose own `kind: 1` field
    // means Timer — distinct from the clustering `kind` column).
    let insert_cell = format!(
        "INSERT INTO {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} (segment_id, key, state_type, name, \
         kind, section, coordinate, event) VALUES (?, ?, ?, ?, 0, ?, ?, {{kind: 1, msg_dedup_id: \
         null, timer_type: 99, time: 0, tag: 0}})"
    );
    // Its `kind=1` (Index) marker, so the index-based recovery scan discovers
    // the coordinate and point-reads the corrupt cell (recovery no longer scans
    // the whole partition).
    let insert_marker = format!(
        "INSERT INTO {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} (segment_id, key, state_type, name, \
         kind, section, coordinate) VALUES (?, ?, ?, ?, 1, ?, ?)"
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
        .query_unpaged(insert_cell, binds)
        .await?;
    fx.cassandra
        .session()
        .query_unpaged(insert_marker, binds)
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
/// fast path `StateHandle::get` relies on. Run over random non-empty payloads.
#[test]
fn prop_cassandra_present_cell_is_uniquely_owned() {
    async fn check(payload: Vec<u8>) -> Result<bool> {
        let fx = fixture().await?;
        let store = fx.bottom_store(ScriptedOracle::default());
        let c = collection("uniq")?;
        let cell = value_cell();
        let data = Bytes::from(payload);
        store
            .write_resolved(&c, &[(cell.clone(), Some(data))])
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
        store.write_resolved(&c, &cells).await?;

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

/// Builds the mixed-statement batch for the binding-order test: three units
/// whose single flatten interleaves every row shape and all five cell
/// statements — stage+index for A, promote+index-delete for B,
/// resolved-write+index-delete for C. The blobs, cells, and `id` outlive the
/// returned borrows (the caller holds them).
fn mixed_binding_batch<'a>(
    q: &'a CellQueries,
    id: &'a CollectionId,
    blob_a: &'a CellBlobs,
    blob_c: &'a CellBlobs,
    cell_a: &'a CellKey,
    cell_b: &'a CellKey,
    cell_c: &'a CellKey,
) -> Vec<BatchUnit<CellBatchRow<'a>>> {
    use super::Pk;
    use smallvec::smallvec;

    let pk = Pk::of(id);
    let (addr_a, addr_b, addr_c) = (
        CellAddr::new(pk, cell_a),
        CellAddr::new(pk, cell_b),
        CellAddr::new(pk, cell_c),
    );
    vec![
        BatchUnit::new(
            1_024,
            smallvec![
                CellBatchRow {
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
                },
                CellBatchRow {
                    statement: &q.index_insert_no_ttl,
                    row: RowShape::IndexUpsert(IndexUpsertRow {
                        ttl: None,
                        addr: addr_a,
                    }),
                },
            ],
        ),
        BatchUnit::new(
            1_024,
            smallvec![
                CellBatchRow {
                    statement: &q.mark_resolved,
                    row: RowShape::Key(KeyRow {
                        kind: CellKind::Cell,
                        addr: addr_b,
                    }),
                },
                CellBatchRow {
                    statement: &q.index_delete,
                    row: RowShape::Key(KeyRow {
                        kind: CellKind::Index,
                        addr: addr_b,
                    }),
                },
            ],
        ),
        BatchUnit::new(
            1_024,
            smallvec![
                CellBatchRow {
                    statement: &q.write_resolved_no_ttl,
                    row: RowShape::Resolved(ResolvedRow {
                        ttl: None,
                        data: blob_c.data.as_deref(),
                        encoding: blob_c.encoding,
                        version: blob_c.version,
                        addr: addr_c,
                    }),
                },
                CellBatchRow {
                    statement: &q.index_delete,
                    row: RowShape::Key(KeyRow {
                        kind: CellKind::Index,
                        addr: addr_c,
                    }),
                },
            ],
        ),
    ]
}

/// Positional binding-order proof (the one silent-failure surface):
/// `scylla::Batch` binds its statement list 1:1 with the value list, and on a
/// count/order mismatch scylla falls back to an **empty** context with no
/// error — a misordered `unzip` flatten would bind a row against the wrong
/// statement's columns silently. Build ONE batch whose single flatten
/// interleaves every [`RowShape`] across **five distinct prepared
/// statements** — stage a cell, upsert its index marker, promote a
/// pre-provisioned cell, delete markers, and write a fresh resolved value —
/// with **distinct payloads** so any cross-binding corrupts an observable
/// value. Read every cell back: each landing correctly proves its statement
/// bound its own columns.
#[tokio::test]
async fn mixed_statement_batch_binds_each_statement_to_its_own_columns() -> Result<()> {
    use crate::state::cell_key::Coordinate;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("mixed-batch")?;
    let id = c.id().clone();

    let cell = |b: u8| CellKey {
        section: Section::new(0),
        coordinate: Coordinate::from_bytes(vec![b]),
    };
    let (cell_a, cell_b, cell_c) = (cell(1), cell(2), cell(3));
    let (data_a, data_b, data_c) = (
        Bytes::from_static(b"aaa"),
        Bytes::from_static(b"bbb"),
        Bytes::from_static(b"ccc"),
    );

    // Pre-provision B (a provisional cell + its index marker) so the mixed batch
    // can promote it.
    store
        .write_provisional(
            &c,
            &[(
                cell_b.clone(),
                ProvisionalWrite::new(Some(data_b.clone()), Committed::new(None), event(1)),
            )],
        )
        .await?;

    // Owned blobs the bound rows borrow into; must outlive the awaited batch.
    let blob_a = encode_cell_blobs(Some(&data_a), None)?;
    let blob_c = encode_cell_blobs(Some(&data_c), None)?;
    // One batch, one flatten, five distinct statements interleaved.
    let units = mixed_binding_batch(
        &fx.queries,
        &id,
        &blob_a,
        &blob_c,
        &cell_a,
        &cell_b,
        &cell_c,
    );
    fx.cassandra
        .execute_unlogged_batches(&units, 1 << 20, 4_096, SHARD_FANOUT_CONCURRENCY)
        .await?;

    // A is provisional with its own payload (the stage + index-upsert rows
    // bound their columns; the index-based recovery scan reads it back), and it
    // is the ONLY provisional cell (the index-delete cleared B's marker, C
    // never had one).
    let staged = provisional_cells(&store, &id).await?;
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
        store.get(&id, &cell_b, event(3)).await?,
        Committed::new(Some(data_b))
    );
    assert_eq!(
        store.get(&id, &cell_c, event(3)).await?,
        Committed::new(Some(data_c))
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
/// `Cached<CassandraStore>` assembly (crash-recovery equivalence and
/// oracle-correctness properties). A "crash" rebuilds the cache cold over the
/// same durable Cassandra rows and oracle set. The tempdir and fjall client
/// outlive every store the `make` closure mints (the await completes before
/// they drop).
#[test]
fn prop_cassandra_cell_crash_equivalence() {
    async fn run(trace: Trace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        // Each `make` is a crash: a cold fjall cache over the same durable
        // Cassandra rows. `cold_cache` clears the shared `cassandra_crash`
        // keyspace pair (a cheap journal marker, no keyspace-creation fsync)
        // instead of minting a fresh workspace per make; distinct v4 segments
        // per iteration keep the shared keyspace disjoint.
        let make = || -> Result<Bottom> {
            Ok(Cached::new(
                test_db::cold_cache("cassandra_crash")?,
                fx.bottom_store(oracle.clone()),
            ))
        };
        run_crash_equivalence_trace(make, oracle.clone(), trace).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck((|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(Trace) -> TestResult);
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
        run_bottom_scan_trace(fx.bottom_store(ScriptedOracle::default()), trace).await
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
            .write_resolved(&c, &[(cell.clone(), Some(old.clone()))])
            .await?;
        // `event(1)` is never recorded in the oracle, so resolution rolls the
        // staged clear back to `prev`.
        store
            .write_provisional(
                &c,
                &[(
                    cell.clone(),
                    ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(1)),
                )],
            )
            .await?;

        let (value, co_expiry) = if path == "get_for_cache" {
            let (committed, co_expiry) = store.get_for_cache(c.id(), &cell, event(2)).await?;
            (committed.into_inner(), co_expiry)
        } else {
            let scan = Scan {
                section: Section::new(0),
                start: Bound::Unbounded,
                dir: Direction::Forward,
                end: Bound::Unbounded,
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
