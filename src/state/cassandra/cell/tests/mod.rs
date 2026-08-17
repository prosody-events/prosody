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

mod batch_bind;
mod batch_order;
mod batch_reads;
mod compatibility;
mod lifecycle;
mod properties;
mod query_shape;
mod repair;
mod ttl_marker;

use super::decode::try_decode_marker;
use super::{
    CassandraStore, CellAddr, CellBatchRow, CellBlobs, CellCorruptReason, CellKind, CellQueries,
    KeyRow, MarkerBlob, MarkerWriteRow, Pk, ResolvedRow, RowShape, StageRow, blob_weight,
    decode_rows_for_coordinates, encode_cell_blobs, fits_one_batch, marker_delete_unit,
    marker_last_split, sorted_unique_coordinates, ttl_seconds_to_duration,
};
use super::{decode, encoding};
use crate::cassandra::{BatchUnit, CassandraStore as CassandraSession};
use crate::state::cached::Cached;
use crate::state::cassandra::udt::RawEventRef;
use crate::state::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::fjall::{MarkerPresence, test_db};
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::sweep_provisional;
use crate::state::store::CellStore;
use crate::state::store::CoordinateBatch;
use crate::state::tests::cell_suite::{
    ApplyTrace, BatchReadTrace, FailingCellStore, OverlayTrace, OverwriteTrace, PoisonHandle,
    ProbedMarker, RawBatchTrace, SECTIONS, ScanTrace, ScriptedOracle, ShapeProbe, Trace, bytes,
    cell_in, probed_parts, run_apply_idempotence, run_batch_alignment,
    run_batch_duplicate_co_observation, run_batch_read_parity_trace,
    run_blind_write_leaves_clears_free_marker, run_blind_write_survives_stale_clear,
    run_bottom_scan_trace, run_crash_equivalence_trace, run_overlay_trace, run_overwrite_trace,
    run_raw_batch_ascending_output, run_raw_batch_no_side_effects, run_raw_batch_parity_trace,
    run_repair_after_marker_abort_converges, run_repair_defers_beneath_stale_clear, value_cell,
};
use crate::state::tests::support::{CountingOracle, fresh_collection, probe as event};
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
use scylla::client::session::Session;
use std::collections::BTreeSet;
use std::iter;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use uuid::Uuid;

use compatibility::mixed_binding_batch;
use lifecycle::cell_i;
use properties::finish;

#[test]
fn row_encoding_uses_the_larger_present_payload() -> Result<()> {
    use super::encoding::{CASSANDRA_COMPRESSION_BLOCK_BYTES, Encoding};

    let small = Bytes::from_static(b"small");
    let large = Bytes::from(vec![0x5A; CASSANDRA_COMPRESSION_BLOCK_BYTES + 1]);
    let blobs = encode_cell_blobs(Some(&small), Some(&large))?;
    assert_eq!(blobs.encoding(), Some(Encoding::Zstd));
    assert_eq!(
        encoding::decode_payload(
            blobs.data().ok_or_else(|| eyre!("data must exist"))?,
            Encoding::Zstd,
        )?,
        small
    );
    assert_eq!(
        encoding::decode_payload(
            blobs
                .prev_data()
                .ok_or_else(|| eyre!("previous data must exist"))?,
            Encoding::Zstd,
        )?,
        large
    );
    Ok(())
}

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
    presence: MarkerPresence,
}

async fn fixture() -> Result<Fixture> {
    let config = test_cassandra_config();
    let cassandra = CassandraSession::new(&config).await?;
    let queries = Arc::new(CellQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(Fixture {
        cassandra,
        queries,
        registry: Arc::new(CollectionDefRegistry::default()),
        presence: test_db::presence("cassandra_cell_presence")?,
    })
}

impl Fixture {
    /// The bare resolving Cassandra cell store over `oracle`, using the
    /// fixture's shared warm presence keyspace. Safe while a test builds ONE
    /// store per collection (fresh v4 segments keep tests disjoint); a test
    /// minting a second store over the same collection models a fresh
    /// assignment and must pass a cold presence handle via
    /// [`bottom_store_with`](Self::bottom_store_with).
    fn bottom_store(&self, oracle: ScriptedOracle) -> CassandraStore<ScriptedOracle> {
        self.bottom_store_with(oracle, self.presence.clone())
    }

    /// A bare store over an explicit presence handle and an arbitrary
    /// [`CommitOracle`] — for assemblies that model reassignment
    /// (crash/overwrite rebuilds, fresh cold readers), the
    /// presence-degradation test, and pins that need a
    /// non-[`ScriptedOracle`] oracle (e.g. the [`CountingOracle`]
    /// no-side-effects pin).
    fn bottom_store_with<O: CommitOracle>(
        &self,
        oracle: O,
        presence: MarkerPresence,
    ) -> CassandraStore<O> {
        CassandraStore::new(
            self.cassandra.clone(),
            self.queries.clone(),
            oracle,
            self.registry.clone(),
            presence,
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
