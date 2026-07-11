//! Cassandra-backed uniform cell store.
//!
//! [`CassandraStore`] implements the untyped [`CellStore`] over the
//! `keyed_state_cell` table provisioned by migration
//! `20260522_create_keyed_state.cql`. Every durable mutation writes one
//! self-consistent cell-column shape in a single statement.
//!
//! It is the **bottom** store: it owns the commit oracle (via the composed
//! [`Resolver`]) and oracle-resolves any in-flight provisional cell inside
//! `get`/`scan_cells` before yielding, so the layers above it
//! ([`Cached`](crate::state::cached::Cached),
//! [`Overlay`](crate::state::overlay::Overlay)) are oracle-free.
//!
//! # Cell rows and the event marker
//!
//! The partition's leading clustering [`CellKind`] splits it into two disjoint
//! ranges. A `kind=Cell` row is one cell over the columns `data | prev_data |
//! encoding | version | event`, addressed by `(section, coordinate)`. One
//! `kind=Marker` row per collection, at the **fixed address**
//! `(section = 0, coordinate = empty)`, is the durable recovery handle: its
//! `event` column names the staging event and its `data` column carries the
//! frozen marker payload — the event's full staged coordinate list
//! ([`crate::state::marker`]). Recovery
//! ([`provisional_cells`](CellStore::provisional_cells)) point-reads the
//! marker, then point-reads each listed cell, so its cost is proportional to
//! the number of provisional cells, never the partition size — and because
//! every marker write and delete lands at the one fixed position, marker churn
//! compacts to a single entry instead of accumulating a tombstone field.
//!
//! Marker lifecycle ownership is stated once, on
//! [`write_provisional`](CellStore::write_provisional). A stage that fits the
//! batch budget carries the marker row **in** the atomic batch; an over-budget
//! stage writes the marker first, alone, so a torn stage is always
//! marker-without-cells (over-report-safe), never cells-without-marker (a
//! strand). The per-assignment RAM memo ([`MarkerMemo`]) bounds durable marker
//! reads to at most one per collection per assignment.
//!
//! The marker also carries the stage's **section clears** (each cleared
//! section with its frozen survivor list). A committed clear is applied as the
//! n+1 **gap range deletes** between sorted survivors ([`gaps_unit`]) —
//! survivors are excluded positionally, never temporally. On the settle path
//! the gaps and the marker delete ride one indivisible batch unit (a marker
//! delete landing without its gaps would lose the committed clear forever);
//! [`write_resolved`](CellStore::write_resolved) applies its direct clears the
//! same way, marker-free. Until the gaps land, reads are defended by
//! **read-help**: `get`/`scan` (and their cache-fill twins) resolve a standing
//! foreign clears-bearing marker through the sweep path before serving — the
//! committed-unapplied read-window contract stated on
//! [`get`](CellStore::get) — riding the same memo, so the fast path stays a
//! RAM check.
//!
//! The three cell mutators write exactly one cell-column shape each:
//!
//! * [`write_provisional`](CellStore::write_provisional) — *stage*: `data`,
//!   `prev_data`, `event`, and the shared `encoding`/`version` in one `UPDATE`.
//!   The encoding/version flags key on **either** blob being present (a
//!   clear-over-present stages `data = null` with a non-null `prev_data`, which
//!   still needs an encoding).
//! * [`write_resolved`](CellStore::write_resolved) — writes a committed value
//!   with `prev_data`/`event` nulled, **or deletes the row** when the value is
//!   absent (the `ReadUncommitted` direct write/clear, the mid-handler
//!   checkpoint, and rollback resolution).
//! * [`mark_resolved`](CellStore::mark_resolved) — *promote*: nulls `prev_data`
//!   and `event` only, keeping `data` and its TTL. O(1) bytes; reserved for
//!   present data.
//!
//! # Committed absence is row absence
//!
//! Every path that resolves a cell to absent **deletes** the `kind=Cell` row
//! (`cell_delete`) rather than nulling its columns — the row-absence invariant
//! owned by [`CellStore`]. An absent-data promote therefore routes through
//! `write_resolved(cell, None)`; [`mark_resolved`](CellStore::mark_resolved)
//! never resolves a cell to absent. No statement in this build produces the
//! legacy null-null-with-encoding residue shape; the decoder's tolerance of it
//! (for rows written by earlier builds) is documented at the decoder.
//!
//! # Concurrency
//!
//! The framework guarantees one handler per key system-wide (Kafka partition
//! ownership + in-process per-key serialization), so this store never needs
//! LWTs or distributed locks.

mod decode;
mod encoding;

#[cfg(test)]
mod tests;

pub(in crate::state::cassandra) use encoding::Encoding;
pub use encoding::EncodingError;

use crate::cassandra::CassandraStore as CassandraSession;
use crate::cassandra::TABLE_KEYED_STATE_CELL;
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{BatchRow, BatchUnit};
use crate::cassandra_queries;
use crate::state::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge};
use crate::state::event_ref::EventRef;
use crate::state::marker::{EventMarker, SectionClear, encode_marker_payload};
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::{
    ResolveCellError, Resolver, flatten_resolve, help_read_window, resolve_marker, resolve_read,
};
use crate::state::store::CellStore;
use crate::state::{CollectionId, CollectionRef, SHARD_FANOUT_CONCURRENCY, StateType};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use decode::{CellTtlRow, KeyedCellRow, RawCellRow};
use encoding::encode_payload;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut, stream};
use scylla::client::session::Session;
use scylla::deserialize::row::DeserializeRow;
use scylla::serialize::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::writers::RowWriter;
use scylla::statement::prepared::PreparedStatement;
use smallvec::{SmallVec, smallvec};
use std::error::Error;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::task::coop::cooperative;

pub use crate::state::cassandra::error::CassandraCellStoreError;
pub use decode::CellCorruptReason;

/// Payload encoding for cell blobs written by this build.
const VALUE_ENCODING: Encoding = Encoding::RawZstdV1;

/// Soft byte ceiling for one same-partition `UNLOGGED BATCH`.
///
/// A single-partition batch is one replica mutation, bounded by Cassandra's
/// `max_mutation_size` — half of `commitlog_segment_size` (16 MiB at the 5.0
/// default 32 MiB segment). 5 MiB keeps a generous margin even if an operator
/// halves the commitlog to an 8 MiB ceiling; promote to
/// [`CassandraConfiguration`](crate::cassandra::CassandraConfiguration) only
/// for a deployment with a tighter commitlog.
const MAX_BATCH_BYTES: u64 = 5 * 1_024 * 1_024;

/// Soft ceiling on the number of batch units in one batch. Each unit is one
/// row (a stage batch adds at most one extra marker row total), far under the
/// protocol u16 max the driver enforces client-side, so the byte budget
/// dominates for any non-trivial value.
const MAX_BATCH_STATEMENTS: usize = 4_096;

/// Per-statement size the row weight adds on top of its blob bytes, covering
/// the partition/clustering key, the `event` UDT, and column metadata the blob
/// count omits — so the estimate over-counts rather than under-counts.
const PER_STATEMENT_OVERHEAD: u64 = 512;

/// The only cell `version` stamp this build writes or accepts.
///
/// Every authoritative cell stamps the version its bytes were written under;
/// this build writes version 1 and rejects any other at decode
/// ([`decode::validate_version`]). Per-key identity migration is future work —
/// the stamp is the dormant hook it would build on.
const INITIAL_VERSION: i32 = 1;

/// The leading clustering discriminator that splits a collection's partition
/// into two disjoint front-to-back ranges.
///
/// [`Cell`](Self::Cell) rows carry the full cell columns;
/// [`Marker`](Self::Marker) is the collection's **one** event-marker row at the
/// fixed address `(section = 0, coordinate = empty)`, so recovery is a single
/// point read at a compaction-merged position — never a range over a tombstone
/// field. It is **always bound as a constant clustering predicate**, never
/// decoded back into a value — hence serialize-only ([`SerializeValue`] in
/// [`super::serialize`]), with no `TryFrom`/`DeserializeValue`.
///
/// # Reserved-`kind` safety
///
/// `kind` is **cell-store-internal**: it splits the physical partition into the
/// data slice and the event-marker slice, and no collection may address the
/// marker slice. This is enforced structurally, not by a runtime check:
///
/// * `CellKind` and its `Marker` variant are private to the Cassandra state
///   backend — never re-exported, never reachable from a collection.
/// * A collection addresses a cell only through a [`CellKey`], which carries
///   **only** `(section, coordinate)` — it has no `kind` field, so the marker
///   slice is unnameable from the collection layer.
/// * This store binds `kind` itself, as the compile-time constant
///   `CellKind::Cell` on every data read/write and `CellKind::Marker` only on
///   the marker statements.
///
/// So "a collection reads or writes the marker slice" is uncompilable, and no
/// assertion or property test is needed to defend it.
///
/// [`CellKey`]: crate::state::cell_key::CellKey
/// [`SerializeValue`]: scylla::serialize::value::SerializeValue
#[repr(i8)]
#[derive(Clone, Copy, Debug)]
pub(super) enum CellKind {
    /// A cell row: the full `data | prev_data | encoding | version | event`
    /// column shape.
    Cell = 0,

    /// The collection's fixed-address event-marker row: `event` names the
    /// staging event, `data` carries the frozen marker payload
    /// ([`crate::state::marker`]). Wire value `1` unchanged from the
    /// per-coordinate design this row replaced.
    Marker = 1,
}

impl From<CellKind> for i8 {
    fn from(kind: CellKind) -> Self {
        kind as i8
    }
}

/// The bottom store's resolving read/sweep error: a raw store failure or an
/// oracle consult failure.
pub type CellStoreError<OracleErr> = ResolveCellError<CassandraCellStoreError, OracleErr>;

/// The session + prepared statements a [`CassandraStore`] is built from, shared
/// across partitions. The per-partition oracle and registry are supplied at
/// [`CassandraStateBackendFactory::for_partition`] time, so the resolving cell
/// store cannot be pre-built — this holds the partition-independent pieces.
///
/// [`CassandraStateBackendFactory::for_partition`]: crate::state::production::CassandraStateBackendFactory
#[derive(Clone)]
pub struct CassandraCellResources {
    pub(crate) session: CassandraSession,
    pub(crate) queries: Arc<CellQueries>,
}

impl CassandraCellResources {
    /// Bundles the shared session and prepared cell statements.
    #[must_use]
    pub fn new(session: CassandraSession, queries: Arc<CellQueries>) -> Self {
        Self { session, queries }
    }
}

/// Test-only recovery-read counters. `marker_point_reads` increments only in
/// [`standing_marker`](CellStore::standing_marker)'s durable-read arm — a
/// [`MarkerMemo`] hit must not count — which is what makes the "at most one
/// durable marker read per collection per assignment" pin non-vacuous.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct RecoveryReadCounts {
    /// Durable event-marker point reads — memo misses only.
    pub(crate) marker_point_reads: AtomicUsize,
    /// `kind=Cell` point reads issued during recovery — bounded by
    /// #provisional.
    pub(crate) cell_point_reads: AtomicUsize,
}

/// Per-assignment RAM event-marker state — the shared memo all three marker
/// consumers ride (the cold recovery seed, the stage-boundary check, and
/// read-help): `checked` records the collections whose durable marker has been
/// consulted this assignment, `standing` the markers known to stand.
///
/// **Memo invariant:** the RAM state may **over-report** a marker (list one
/// that never durably landed — resolving a phantom marker is a harmless no-op
/// settle) but must never **under-report** (`checked` true while `standing`
/// misses a durable marker would strand that marker's cells from the sweep
/// until the next assignment). Therefore `standing` is updated **before** the
/// durable stage attempt, and `checked` is set only by the durable read path,
/// the stage path, and the settle path. Per-key serialization means no two ops
/// race on one collection's entry; scc handles cross-collection concurrency.
///
/// Minted fresh per [`CassandraStore`] (fresh store per partition acquisition
/// ⇒ cold memo per assignment); clones share the `Arc`.
#[derive(Debug, Default)]
struct MarkerMemo {
    checked: scc::HashSet<CollectionId, RandomState>,
    standing: scc::HashMap<CollectionId, EventMarker, RandomState>,
}

/// Cassandra-backed uniform cell store.
#[derive(Clone, Debug)]
pub struct CassandraStore<O> {
    session: CassandraSession,
    queries: Arc<CellQueries>,
    resolver: Resolver<O>,
    memo: Arc<MarkerMemo>,
    #[cfg(test)]
    counters: Arc<RecoveryReadCounts>,
}

impl<O> CassandraStore<O> {
    /// Creates a Cassandra cell store over an existing session, a prepared
    /// [`CellQueries`] set, the commit oracle it resolves provisional cells
    /// through, and the registry that supplies per-collection TTLs.
    #[must_use]
    pub(crate) fn new(
        session: CassandraSession,
        queries: Arc<CellQueries>,
        oracle: O,
        registry: Arc<CollectionDefRegistry>,
    ) -> Self {
        Self {
            session,
            queries,
            resolver: Resolver::new(oracle, registry),
            memo: Arc::default(),
            #[cfg(test)]
            counters: Arc::default(),
        }
    }

    /// Test handle on the recovery-read counters (shared across clones), for
    /// the zero-query and bounded-recovery assertions.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn recovery_reads(&self) -> Arc<RecoveryReadCounts> {
        self.counters.clone()
    }

    fn cql(&self) -> &Session {
        self.session.session()
    }

    /// Point-reads one `kind=Cell` row with `statement`, generic over the
    /// selected row shape — `read_cell` ([`RawCellRow`]) and `read_cell_ttl`
    /// ([`CellTtlRow`]) share this one bind tuple.
    async fn point_read<R>(
        &self,
        statement: &PreparedStatement,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<R>, CassandraCellStoreError>
    where
        R: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>,
    {
        let pk = Pk::of(id);
        let row = self
            .cql()
            .execute_unpaged(
                statement,
                (
                    pk.segment_id,
                    pk.key,
                    pk.state_type,
                    pk.name,
                    CellKind::Cell,
                    i8::from(cell.section),
                    cell.coordinate.as_bytes(),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<R>()
            .map_err(CassandraStoreError::from)?;
        Ok(row)
    }

    /// Executes the packed same-partition `UNLOGGED BATCH`es for a multi-cell
    /// mutation — the shared tail of the cell mutators. Each [`BatchUnit`] is
    /// one row (a cell mutation, or the marker row), packed into the fewest
    /// batches under the byte and statement budgets; every batch is
    /// row-disjoint by construction (the marker address is disjoint from every
    /// cell row by `kind`), so no same-batch timestamp tie can pit a delete
    /// against a write of one row.
    async fn run_batches(
        &self,
        units: &[BatchUnit<CellBatchRow<'_>>],
    ) -> Result<(), CassandraCellStoreError> {
        self.session
            .execute_unlogged_batches(
                units,
                MAX_BATCH_BYTES,
                MAX_BATCH_STATEMENTS,
                SHARD_FANOUT_CONCURRENCY,
            )
            .await?;
        Ok(())
    }

    /// Builds the resolved-write batch units for `cells` — the shared unit
    /// construction of `write_resolved` and `abort_provisional`. A present
    /// value binds the resolved-value shape; an absent value **deletes** the
    /// `kind=Cell` row (the row-absence invariant — no null-blob residue).
    fn resolved_units<'u>(
        &'u self,
        pk: Pk<'u>,
        ttl: Option<i32>,
        blobs: &'u [CellBlobs],
        cells: &'u [(CellKey, Option<Bytes>)],
    ) -> Vec<BatchUnit<CellBatchRow<'u>>> {
        let cell_stmt = if ttl.is_some() {
            &self.queries.write_resolved
        } else {
            &self.queries.write_resolved_no_ttl
        };
        blobs
            .iter()
            .zip(cells)
            .map(|(blob, (cell, _))| {
                let addr = CellAddr::new(pk, cell);
                let row = match blob.data {
                    Some(_) => CellBatchRow {
                        statement: cell_stmt,
                        row: RowShape::Resolved(ResolvedRow {
                            ttl,
                            data: blob.data.as_deref(),
                            encoding: blob.encoding,
                            version: blob.version,
                            addr,
                        }),
                    },
                    None => CellBatchRow {
                        statement: &self.queries.cell_delete,
                        row: RowShape::Key(KeyRow {
                            kind: CellKind::Cell,
                            addr,
                        }),
                    },
                };
                BatchUnit::new(blob_weight(blob), smallvec![row])
            })
            .collect()
    }

    /// Mirrors a successful settle into the [`MarkerMemo`]: the marker is now
    /// durably deleted, so the collection is known marker-absent for the rest
    /// of the assignment.
    async fn settle_memo(&self, collection: &CollectionId) {
        self.memo.standing.remove_async(collection).await;
        let _ = self.memo.checked.insert_async(collection.clone()).await;
    }
}

impl<O> CassandraStore<O>
where
    O: CommitOracle,
{
    /// The stage's marker half, ahead of any row building: the stage-boundary
    /// resolve, the memo mirror, and the frozen payload's encoding. Returns
    /// the marker row's blob.
    ///
    /// The boundary resolves any standing FOREIGN marker (a different event)
    /// before overwriting it, establishing marker uniqueness per collection. A
    /// same-event marker (a retry attempt re-running finalize, or the later
    /// chunk of a split stage) is overwritten, never resolved. A resolution
    /// failure fails the stage (retry middleware). The memo is updated BEFORE
    /// the durable attempt (the over-report-safe direction — see the
    /// [`MarkerMemo`] invariant).
    async fn stage_marker(
        &self,
        collection: &CollectionRef,
        marker: &EventMarker,
    ) -> Result<Bytes, CellStoreError<O::Error>> {
        if let Some(standing) = self.standing_marker(collection.id()).await?
            && standing.event() != marker.event()
        {
            resolve_marker(self, self.resolver.oracle(), collection, &standing)
                .await
                .map_err(flatten_resolve)?;
        }
        self.memo
            .standing
            .upsert_async(collection.id().clone(), marker.clone())
            .await;
        let _ = self
            .memo
            .checked
            .insert_async(collection.id().clone())
            .await;
        // The frozen payload rides the store-wide blob convention: the same
        // zstd `encode_payload` and `encoding`/`version` stamps as every cell
        // blob, decoded through `decode_blob`.
        let payload = encode_marker_payload(marker)
            .map_err(CassandraCellStoreError::from)
            .map_err(ResolveCellError::Store)?;
        encode_payload(&payload, VALUE_ENCODING)
            .map_err(CassandraCellStoreError::from)
            .map_err(ResolveCellError::Store)
    }

    /// The single resolving section scan, yielding each present cell's
    /// committed bytes **and** its cache-fill co-expiry TTL (the remaining TTL
    /// of whichever blob the row carries — [`decode`]'s `blob_ttl`).
    /// [`scan_cells`](CellStore::scan_cells) drops the TTL;
    /// [`scan_for_cache`](CellStore::scan_for_cache) keeps it.
    fn scan_inner<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<
        Item = Result<(CellKey, Bytes, Option<CompactDuration>), CellStoreError<O::Error>>,
    > + Send
    + 'a {
        let section = i8::from(scan.section);
        let dir = scan.dir;
        let limit = scan.limit;
        // The start edge goes into CQL (the comparator is chosen by selecting
        // one of the per-edge prepared statements). The end edge is enforced
        // in-code by `past_end`, so it needs no statement variant. Both edges
        // are held as owned `Coordinate`s across the stream's awaits — O(1)
        // refcount bumps (`Coordinate` is `Bytes`), never byte copies.
        let statement = match (dir, &scan.start) {
            (Direction::Forward, ScanEdge::Included(_)) => self.queries.scan_forward_incl.clone(),
            (Direction::Forward, ScanEdge::Excluded(_)) => self.queries.scan_forward_excl.clone(),
            (Direction::Backward, ScanEdge::Included(_)) => self.queries.scan_backward_incl.clone(),
            (Direction::Backward, ScanEdge::Excluded(_)) => self.queries.scan_backward_excl.clone(),
        };
        let start = scan.start.cloned();
        let end = scan.end.cloned();
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            // Read-help once before the pager opens (`help_read_window`): a
            // standing foreign clears-bearing marker is resolved so the scan
            // pages post-clear truth. Memo-backed — RAM after the seed read.
            let standing = self.standing_marker(collection).await?;
            help_read_window(self, self.resolver.oracle(), &collection_ref, standing.as_ref(), own)
                .await
                .map_err(flatten_resolve)?;
            let pk = Pk::of(collection);
            // The start edge always binds its coordinate; the comparator was
            // fixed by the statement selected above.
            let pager = self
                .cql()
                .execute_iter(
                    statement,
                    (pk.segment_id, pk.key, pk.state_type, pk.name, CellKind::Cell, section, start.coordinate().as_bytes()),
                )
                .await;
            let stream = pager
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O::Error>)?
                .rows_stream::<KeyedCellRow>()
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O::Error>)?;
            pin_mut!(stream);

            let mut yielded = 0usize;
            // Deliberately sequential, not an oversight: the common `resolve_read`
            // is a free no-op (own-event provisional / already-resolved cells
            // consult no oracle and write nothing), so steady-state scans gain
            // nothing from fan-out; the only payoff is mid-recovery across many
            // foreign provisional cells. And because `limit` counts *present*
            // yields — knowable only post-resolve — a buffered pipeline would
            // resolve up to N−1 foreign-provisional cells past the boundary, each
            // a durable write-back: read-path write amplification we won't pay for
            // a recovery-only win on a hot read path.
            while let Some(row) = cooperative(stream.try_next())
                .await
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O::Error>)?
            {
                // The limit bounds *yielded* (present) cells; check it before
                // processing the next row so `Some(0)` yields nothing (an absent
                // cell never consumes a slot — only a present yield does).
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let (key, raw, ttl) =
                    decode::try_decode_keyed_cell_ttl(row).map_err(ResolveCellError::Store)?;
                if past_end(dir, &key, end.as_ref()) {
                    break;
                }
                let committed =
                    resolve_read(self, self.resolver.oracle(), &collection_ref, &key, own, raw)
                        .await
                        .map_err(flatten_resolve)?;
                if let Some(bytes) = committed.into_inner() {
                    yield (key, bytes, ttl_seconds_to_duration(ttl));
                    yielded += 1;
                }
            }
        }
    }
}

impl<O> CellStore for CassandraStore<O>
where
    O: CommitOracle,
{
    type Error = CellStoreError<O::Error>;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        // The committed value is exactly the cache-fill read minus its co-expiry
        // TTL; production only ever calls this via `Cached` (which uses
        // `get_for_cache`), so `get` is a thin convenience for direct callers.
        Ok(self.get_for_cache(collection, cell, own).await?.0)
    }

    async fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        let collection_ref = self.resolver.collection_ref(collection);
        // The committed-unapplied read window (`help_read_window`): consult
        // the standing marker — memo-backed, so RAM after the one seed read —
        // CONCURRENTLY with the cell point read, keeping the ~always
        // marker-free fast path free of serial latency. If the marker was
        // resolved (foreign + clears), the pre-help row may hold a now-erased
        // value, so the point read is re-issued.
        let (row, standing) = futures::join!(
            self.point_read::<CellTtlRow>(&self.queries.read_cell_ttl, collection, cell),
            self.standing_marker(collection),
        );
        let mut row = row.map_err(ResolveCellError::Store)?;
        if help_read_window(
            self,
            self.resolver.oracle(),
            &collection_ref,
            standing?.as_ref(),
            own,
        )
        .await
        .map_err(flatten_resolve)?
        {
            row = self
                .point_read::<CellTtlRow>(&self.queries.read_cell_ttl, collection, cell)
                .await
                .map_err(ResolveCellError::Store)?;
        }
        let (raw, ttl) = match row {
            Some(row) => {
                let (cell, ttl) =
                    decode::try_decode_cell_ttl(row).map_err(ResolveCellError::Store)?;
                (cell, ttl)
            }
            None => (Cell::Resolved(Committed::new(None)), None),
        };
        let committed = resolve_read(
            self,
            self.resolver.oracle(),
            &collection_ref,
            cell,
            own,
            raw,
        )
        .await
        .map_err(flatten_resolve)?;
        Ok((committed, ttl_seconds_to_duration(ttl)))
    }

    fn scan_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        self.scan_inner(collection, scan, own)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        // Same scan, dropping the per-cell TTL the cache-fill variant keeps.
        self.scan_inner(collection, scan, own)
            .map_ok(|(key, bytes, _ttl)| (key, bytes))
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        try_stream! {
            // The **cold seed** is the standing event marker (memoized — the
            // one durable marker point read per collection per assignment
            // happens in `standing_marker`, wherever it fires first): its
            // frozen payload lists the staged coordinates, so recovery cost is
            // ∝ #provisional, never #cells, with no range read anywhere. The
            // warm short-circuit that skips this on a quiescent sweep lives
            // one layer up on `Cached` (which owns the fjall warm index).
            let Some(marker) = self.standing_marker(collection).await? else {
                return;
            };
            // Bounded by one event's staged set, sized once.
            let coords: Vec<CellKey> = marker.staged().to_vec();

            // Point-read each listed coordinate's `kind=Cell` row to rebuild
            // its `ProvisionalCell`, pipelined on the partition's shard
            // fan-out — the reads are independent and both consumers are
            // order-free (the sweep resolves `buffer_unordered`; the warm
            // index records a set). A listed coordinate whose row is absent
            // (cell and marker share one TTL) or decodes `Cell::Resolved`
            // (first-touch or concurrently resolved) is silently dropped —
            // the marker's over-report is safe.
            let reads = stream::iter(coords)
                .map(|key| {
                    // `cooperative` adds a per-cell coop-budget checkpoint;
                    // `buffered` keeps full concurrency (and the index order).
                    cooperative(async move {
                        #[cfg(test)]
                        self.counters.cell_point_reads.fetch_add(1, Ordering::Relaxed);
                        let raw = self
                            .point_read::<RawCellRow>(&self.queries.read_cell, collection, &key)
                            .await
                            .map_err(ResolveCellError::Store)?;
                        Ok::<_, CellStoreError<O::Error>>((key, raw))
                    })
                })
                .buffered(SHARD_FANOUT_CONCURRENCY);
            pin_mut!(reads);
            while let Some((key, raw)) = reads.try_next().await? {
                let Some(raw) = raw else {
                    continue;
                };
                let cell = decode::try_decode_cell(raw).map_err(ResolveCellError::Store)?;
                if let Cell::Provisional(provisional) = cell {
                    yield (key, provisional);
                }
            }
        }
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        // Point-read the `kind=Cell` row and keep only a genuinely provisional
        // shape; an absent or resolved coordinate reads `None` (over-report-safe
        // — a coordinate the warm index over-reports is dropped here).
        #[cfg(test)]
        self.counters
            .cell_point_reads
            .fetch_add(1, Ordering::Relaxed);
        let Some(raw) = self
            .point_read::<RawCellRow>(&self.queries.read_cell, collection, cell)
            .await
            .map_err(ResolveCellError::Store)?
        else {
            return Ok(None);
        };
        match decode::try_decode_cell(raw).map_err(ResolveCellError::Store)? {
            Cell::Provisional(provisional) => Ok(Some(provisional)),
            Cell::Resolved(_) => Ok(None),
        }
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        // `None` ⇒ the explicit empty-stage no-op: no marker, no boundary
        // check (nothing to strand). A clears-only stage passes a marker with
        // empty `staged()` and runs the boundary like any stage.
        debug_assert!(
            marker.is_some() || writes.is_empty(),
            "a markerless stage must write nothing"
        );
        debug_assert!(
            marker.is_none_or(|marker| writes
                .iter()
                .all(|(cell, _)| marker.staged().binary_search(cell).is_ok())),
            "every staged write must be listed by the event marker"
        );
        let pk = Pk::of(collection.id());
        let marker_blob: Option<(Bytes, EventRef)> = match marker {
            None => None,
            Some(marker) => Some((self.stage_marker(collection, marker).await?, marker.event())),
        };

        // Encode every cell's blobs up front (this Vec owns the `Bytes`); the
        // bound rows borrow into it and into each input cell's coordinate slice,
        // so the whole batch is one `blobs` allocation with no per-cell copy.
        let mut blobs = Vec::with_capacity(writes.len());
        for (_, write) in writes {
            blobs.push(
                encode_cell_blobs(write.data(), write.prev()).map_err(ResolveCellError::Store)?,
            );
        }

        // The collection TTL is uniform, so the with-TTL vs no-TTL choice — and
        // hence which statement each row carries — is made once for the whole
        // batch, never per cell. The marker row shares the TTL (co-expiry with
        // the newest staged cell).
        let ttl = collection.ttl().map(ttl_to_i32);
        let (cell_stmt, marker_stmt) = if ttl.is_some() {
            (&self.queries.write_provisional, &self.queries.marker_write)
        } else {
            (
                &self.queries.write_provisional_no_ttl,
                &self.queries.marker_write_no_ttl,
            )
        };
        // The marker unit leads; each cell unit is one row.
        let mut units: Vec<BatchUnit<CellBatchRow>> = Vec::with_capacity(writes.len() + 1);
        if let Some((blob, event)) = &marker_blob {
            units.push(BatchUnit::new(
                blob.len() as u64 + PER_STATEMENT_OVERHEAD,
                smallvec![CellBatchRow {
                    statement: marker_stmt,
                    row: RowShape::MarkerWrite(MarkerWriteRow {
                        ttl,
                        payload: blob,
                        event: *event,
                        addr: CellAddr::marker(pk),
                    }),
                }],
            ));
        }
        units.extend(blobs.iter().zip(writes).map(|(blob, (cell, write))| {
            let addr = CellAddr::new(pk, cell);
            BatchUnit::new(
                blob_weight(blob),
                smallvec![CellBatchRow {
                    statement: cell_stmt,
                    row: RowShape::Stage(StageRow {
                        ttl,
                        data: blob.data.as_deref(),
                        prev_data: blob.prev_data.as_deref(),
                        encoding: blob.encoding,
                        version: blob.version,
                        event: write.event(),
                        addr,
                    }),
                }],
            )
        }));

        // Marker-first ordering. Within one batch the marker rides atomically;
        // an over-budget stage MUST await the marker batch to completion
        // before issuing the cell batches, because `execute_unlogged_batches`
        // runs its chunks `buffer_unordered` (chunk order is NOT guaranteed).
        // Marker-without-cells is the over-report-safe crash shape;
        // cells-without-marker would strand them from recovery.
        if marker_blob.is_none()
            || fits_one_batch(
                units.iter().map(BatchUnit::weight),
                MAX_BATCH_BYTES,
                MAX_BATCH_STATEMENTS,
            )
        {
            self.run_batches(&units)
                .await
                .map_err(ResolveCellError::Store)
        } else {
            self.run_batches(&units[..1])
                .await
                .map_err(ResolveCellError::Store)?;
            self.run_batches(&units[1..])
                .await
                .map_err(ResolveCellError::Store)
        }
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // A marker-free primitive (the marker lifecycle belongs to the staged
        // verbs — see `CellStore::write_provisional`), so a fresh committed
        // write never touches the marker slice. A cleared section's gap
        // deletes lead as one unit (NO marker delete — resolved writes are
        // marker-free); survivors are the present-data `cells`, excluded from
        // the gaps positionally, so every batch row stays disjoint.
        let pk = Pk::of(collection.id());
        // Encode each cell's committed `data` up front (owns the `Bytes`); no
        // `prev`, so the blobs carry only `data` + its encoding/version.
        let mut blobs = Vec::with_capacity(cells.len());
        for (_, data) in cells {
            blobs.push(encode_cell_blobs(data.as_ref(), None).map_err(ResolveCellError::Store)?);
        }
        let ttl = collection.ttl().map(ttl_to_i32);
        let mut units = Vec::with_capacity(cells.len() + 1);
        if !clears.is_empty() {
            units.push(gaps_unit(&self.queries, pk, clears, GapsMarker::MarkerFree));
        }
        units.extend(self.resolved_units(pk, ttl, &blobs, cells));
        self.run_batches(&units)
            .await
            .map_err(ResolveCellError::Store)
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        let pk = Pk::of(collection.id());
        // A marker-free single-row primitive. Promotes carry no blob — only
        // key columns — so every unit weighs the fixed overhead and the count
        // budget alone splits an enormous promote set.
        let units: Vec<BatchUnit<CellBatchRow>> = cells
            .iter()
            .map(|cell| {
                let addr = CellAddr::new(pk, cell);
                BatchUnit::new(
                    PER_STATEMENT_OVERHEAD,
                    smallvec![CellBatchRow {
                        statement: &self.queries.mark_resolved,
                        row: RowShape::Key(KeyRow {
                            kind: CellKind::Cell,
                            addr,
                        }),
                    }],
                )
            })
            .collect();
        self.run_batches(&units)
            .await
            .map_err(ResolveCellError::Store)
    }

    async fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        // Memo hit: the durable marker was consulted this assignment, so the
        // RAM state is at least durable truth — zero durable reads.
        if self.memo.checked.contains_async(collection).await {
            return Ok(self
                .memo
                .standing
                .read_async(collection, |_, marker| marker.clone())
                .await);
        }
        // Memo miss: the one durable point read at the fixed marker address,
        // seeding the memo for the rest of the assignment.
        #[cfg(test)]
        self.counters
            .marker_point_reads
            .fetch_add(1, Ordering::Relaxed);
        let pk = Pk::of(collection);
        let addr = CellAddr::marker(pk);
        let row = self
            .cql()
            .execute_unpaged(
                &self.queries.marker_read,
                (
                    pk.segment_id,
                    pk.key,
                    pk.state_type,
                    pk.name,
                    CellKind::Marker,
                    addr.section,
                    addr.coordinate,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)
            .map_err(into_store_err::<O::Error>)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)
            .map_err(into_store_err::<O::Error>)?
            .maybe_first_row::<decode::MarkerRow>()
            .map_err(CassandraStoreError::from)
            .map_err(into_store_err::<O::Error>)?;
        let marker = row
            .map(decode::try_decode_marker)
            .transpose()
            .map_err(ResolveCellError::Store)?;
        if let Some(marker) = &marker {
            self.memo
                .standing
                .upsert_async(collection.clone(), marker.clone())
                .await;
        }
        let _ = self.memo.checked.insert_async(collection.clone()).await;
        Ok(marker)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // The routing `route_commit` defines — present data promotes in place,
        // a staged clear deletes its row (the row-absence invariant) — packed
        // natively with the clear gaps and the marker delete into ONE
        // `run_batches` call, so the settle stays one batched round-trip set.
        // Every row disjoint (an event stages each cell at most once; gaps
        // exclude survivors positionally and each other; the marker address is
        // kind-disjoint; a `cell_delete` of a staged clear inside a gap range
        // is a delete/delete tie — harmless), individually idempotent,
        // order-free.
        let pk = Pk::of(collection.id());
        let mut units: Vec<BatchUnit<CellBatchRow>> = Vec::with_capacity(writes.len() + 1);
        units.extend(writes.iter().map(|(cell, write)| {
            let addr = CellAddr::new(pk, cell);
            let statement = if write.data().is_some() {
                &self.queries.mark_resolved
            } else {
                &self.queries.cell_delete
            };
            BatchUnit::new(
                PER_STATEMENT_OVERHEAD,
                smallvec![CellBatchRow {
                    statement,
                    row: RowShape::Key(KeyRow {
                        kind: CellKind::Cell,
                        addr,
                    }),
                }],
            )
        }));
        // Gaps + marker delete ride ONE indivisible unit: the marker must
        // never be deleted in a batch that lands without the gap tombstones,
        // or a committed clear is lost forever. The clears-free settle keeps
        // the standalone marker-delete unit.
        if clears.is_empty() {
            units.push(marker_delete_unit(pk, &self.queries));
        } else {
            units.push(gaps_unit(
                &self.queries,
                pk,
                clears,
                GapsMarker::DeleteMarker,
            ));
        }
        self.run_batches(&units)
            .await
            .map_err(ResolveCellError::Store)?;
        self.settle_memo(collection.id()).await;
        Ok(())
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // Rollback: write each staged cell's committed base `prev` back as
        // resolved (`prev = None` restores exact row absence — the routing
        // `route_abort` defines) plus the marker delete, in one `run_batches`
        // packing.
        let pk = Pk::of(collection.id());
        let cells: Vec<(CellKey, Option<Bytes>)> = writes
            .iter()
            .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
            .collect();
        let mut blobs = Vec::with_capacity(cells.len());
        for (_, data) in &cells {
            blobs.push(encode_cell_blobs(data.as_ref(), None).map_err(ResolveCellError::Store)?);
        }
        let ttl = collection.ttl().map(ttl_to_i32);
        let mut units = self.resolved_units(pk, ttl, &blobs, &cells);
        units.push(marker_delete_unit(pk, &self.queries));
        self.run_batches(&units)
            .await
            .map_err(ResolveCellError::Store)?;
        self.settle_memo(collection.id()).await;
        Ok(())
    }
}

/// The four partition-key column values of a collection's Cassandra partition.
#[derive(Clone, Copy)]
struct Pk<'a> {
    segment_id: &'a crate::SegmentId,
    key: &'a str,
    state_type: StateType,
    name: &'a str,
}

impl<'a> Pk<'a> {
    fn of(id: &'a CollectionId) -> Self {
        Self {
            segment_id: &id.state_key().segment_id,
            key: id.state_key().key.as_ref(),
            state_type: id.state_type(),
            name: id.name().as_str(),
        }
    }
}

/// The cell column values bound by the stage / resolved-write paths.
///
/// `encoding` and `version` are shared by `data` and `prev_data` and present
/// iff **either** blob is present — a clear-over-present stage carries a null
/// `data` with a non-null `prev_data` and still needs an encoding to decode it.
struct CellBlobs {
    data: Option<Bytes>,
    prev_data: Option<Bytes>,
    encoding: Option<Encoding>,
    version: Option<i32>,
}

/// The key + clustering columns addressing one cell in its partition: the four
/// partition-key columns and the cell's `section`/`coordinate`. `kind` is
/// **not** carried — each [`RowShape`] binds its own `kind` (`Cell` vs
/// `Marker`), so one address type serves both a cell row and the marker row.
#[derive(Clone, Copy)]
struct CellAddr<'a> {
    pk: Pk<'a>,
    section: i8,
    coordinate: &'a [u8],
}

impl<'a> CellAddr<'a> {
    fn new(pk: Pk<'a>, cell: &'a CellKey) -> Self {
        Self {
            pk,
            section: i8::from(cell.section),
            coordinate: cell.coordinate.as_bytes(),
        }
    }

    /// The collection's **fixed marker address**: `(section = 0,
    /// coordinate = empty)`. Every marker statement binds this one position
    /// (with `kind = Marker`), so marker churn compacts to a single entry.
    fn marker(pk: Pk<'a>) -> Self {
        Self {
            pk,
            section: 0,
            coordinate: &[],
        }
    }
}

/// One durable row bound into a same-partition `UNLOGGED BATCH`: the prepared
/// statement it targets and the [`RowShape`] that binds exactly that
/// statement's columns. [`scylla::statement::batch::Batch`] binds its
/// statement list 1:1 with the value list, so each row must serialize
/// precisely the columns of the statement [`BatchRow::statement`] returns —
/// kept consistent at the construction sites, which pair each shape with its
/// own statement.
struct CellBatchRow<'a> {
    statement: &'a PreparedStatement,
    row: RowShape<'a>,
}

/// The column shape a [`CellBatchRow`] binds — one variant per distinct bind
/// tuple. A cell promote, a cell delete, and a marker delete share the
/// key-only [`Key`](Self::Key) shape: they differ only in statement and
/// constant `kind`, both carried as data. The two one-coordinate gap deletes
/// (`gap_below`/`gap_above`) share [`GapEdge`](Self::GapEdge) the same way.
enum RowShape<'a> {
    /// Stage a provisional cell (`kind=Cell`): the full `data | prev_data |
    /// event` shape plus shared `encoding`/`version`.
    Stage(StageRow<'a>),
    /// Write a resolved value (`kind=Cell`): committed `data` +
    /// encoding/version, nulling `prev_data`/`event`.
    Resolved(ResolvedRow<'a>),
    /// Upsert the collection's event-marker row (`kind=Marker`) at the fixed
    /// address, at the collection TTL so it co-expires with the staged cells.
    MarkerWrite(MarkerWriteRow<'a>),
    /// Key columns only, binding the carried [`CellKind`]: a cell promote
    /// (`kind=Cell`, nulling `prev_data`/`event` while keeping `data` and its
    /// TTL), a `cell_delete` (`kind=Cell`), or a `marker_delete`
    /// (`kind=Marker`).
    Key(KeyRow<'a>),
    /// Whole-section gap delete (`gap_section`): a cleared section with no
    /// survivors — pk + `kind=Cell` + section, no coordinate predicate.
    GapSection(GapSectionRow<'a>),
    /// One-edge gap delete (`gap_below` / `gap_above`): the open range below
    /// the first or above the last survivor — one bound coordinate, borrowed
    /// from the frozen survivor list.
    GapEdge(GapEdgeRow<'a>),
    /// Open-interval gap delete (`gap_between`): the range between two
    /// adjacent survivors — two bound coordinates.
    GapBetween(GapBetweenRow<'a>),
}

/// The `write_provisional[_no_ttl]` bind shape. `ttl` selects the with-/no-TTL
/// statement **and** the bound column count — kept consistent with the carried
/// statement at the single construction site.
struct StageRow<'a> {
    ttl: Option<i32>,
    data: Option<&'a [u8]>,
    prev_data: Option<&'a [u8]>,
    encoding: Option<Encoding>,
    version: Option<i32>,
    event: EventRef,
    addr: CellAddr<'a>,
}

/// The `write_resolved[_no_ttl]` bind shape (committed `data` +
/// encoding/version; `prev_data`/`event` nulled by the statement).
struct ResolvedRow<'a> {
    ttl: Option<i32>,
    data: Option<&'a [u8]>,
    encoding: Option<Encoding>,
    version: Option<i32>,
    addr: CellAddr<'a>,
}

/// The `marker_write[_no_ttl]` bind shape: the encoded marker payload with its
/// encoding/version, the staging event, and the fixed marker address. `ttl`
/// selects the with-/no-TTL statement and the bound column count, exactly like
/// [`StageRow`].
struct MarkerWriteRow<'a> {
    ttl: Option<i32>,
    payload: &'a [u8],
    event: EventRef,
    addr: CellAddr<'a>,
}

/// The key-only bind shape shared by `mark_resolved`, `cell_delete`, and
/// `marker_delete`: the four PK columns, the constant `kind`, and the row's
/// `section`/`coordinate`.
struct KeyRow<'a> {
    kind: CellKind,
    addr: CellAddr<'a>,
}

/// The `gap_section` bind shape: pk + `kind=Cell` + the cleared section.
struct GapSectionRow<'a> {
    pk: Pk<'a>,
    section: i8,
}

/// The `gap_below`/`gap_above` bind shape: [`GapSectionRow`]'s columns plus
/// the one bound survivor coordinate.
struct GapEdgeRow<'a> {
    pk: Pk<'a>,
    section: i8,
    coordinate: &'a [u8],
}

/// The `gap_between` bind shape: [`GapSectionRow`]'s columns plus the two
/// adjacent survivor coordinates bounding the open interval.
struct GapBetweenRow<'a> {
    pk: Pk<'a>,
    section: i8,
    low: &'a [u8],
    high: &'a [u8],
}

impl BatchRow for CellBatchRow<'_> {
    fn statement(&self) -> &PreparedStatement {
        self.statement
    }
}

impl SerializeRow for CellBatchRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        match &self.row {
            RowShape::Stage(row) => row.serialize(ctx, writer),
            RowShape::Resolved(row) => row.serialize(ctx, writer),
            RowShape::MarkerWrite(row) => row.serialize(ctx, writer),
            RowShape::Key(row) => row.serialize(ctx, writer),
            RowShape::GapSection(row) => row.serialize(ctx, writer),
            RowShape::GapEdge(row) => row.serialize(ctx, writer),
            RowShape::GapBetween(row) => row.serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for StageRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        // The `ttl` arms differ only by the leading `USING TTL ?` column the
        // no-TTL statement omits; `kind` leads the clustering key.
        match self.ttl {
            Some(ttl) => (
                ttl,
                self.data,
                self.prev_data,
                self.encoding,
                self.version,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
            None => (
                self.data,
                self.prev_data,
                self.encoding,
                self.version,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for ResolvedRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        match self.ttl {
            Some(ttl) => (
                ttl,
                self.data,
                self.encoding,
                self.version,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
            None => (
                self.data,
                self.encoding,
                self.version,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for MarkerWriteRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        // The `ttl` arms differ only by the leading `USING TTL ?` column the
        // no-TTL statement omits (as `StageRow`). The payload always carries
        // this build's encoding/version stamps.
        match self.ttl {
            Some(ttl) => (
                ttl,
                self.payload,
                VALUE_ENCODING,
                INITIAL_VERSION,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Marker,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
            None => (
                self.payload,
                VALUE_ENCODING,
                INITIAL_VERSION,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Marker,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for KeyRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        (
            a.pk.segment_id,
            a.pk.key,
            a.pk.state_type,
            a.pk.name,
            self.kind,
            a.section,
            a.coordinate,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for GapSectionRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        (
            self.pk.segment_id,
            self.pk.key,
            self.pk.state_type,
            self.pk.name,
            CellKind::Cell,
            self.section,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for GapEdgeRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        (
            self.pk.segment_id,
            self.pk.key,
            self.pk.state_type,
            self.pk.name,
            CellKind::Cell,
            self.section,
            self.coordinate,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for GapBetweenRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        (
            self.pk.segment_id,
            self.pk.key,
            self.pk.state_type,
            self.pk.name,
            CellKind::Cell,
            self.section,
            self.low,
            self.high,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

/// Whether [`gaps_unit`] appends the marker-delete row to the gap rows.
#[derive(Clone, Copy)]
enum GapsMarker {
    /// The commit-settle path: gaps + marker delete are one indivisible unit,
    /// so the marker can never die in a batch that lands without its gaps.
    DeleteMarker,
    /// The resolved-write path: marker-free by design, gaps only.
    MarkerFree,
}

/// One **indivisible** batch unit erasing every cleared section: per section,
/// the n+1 gap range deletes around its sorted, deduped survivors
/// (`< k₁`, `(k₁,k₂)`, …, `> kₙ`; `gap_section` alone when no survivor) —
/// plus, on the commit path, the marker-delete row. Sized once
/// (`Σ survivors + 1` rows per section); bound coordinates borrow from the
/// frozen [`SectionClear`]s, no copies. An open interval between adjacent
/// distinct survivors is never empty-inverted, and a `< k₁` on an empty
/// coordinate matches nothing — harmless.
fn gaps_unit<'u>(
    queries: &'u CellQueries,
    pk: Pk<'u>,
    clears: &'u [SectionClear],
    marker: GapsMarker,
) -> BatchUnit<CellBatchRow<'u>> {
    let count = clears
        .iter()
        .map(|clear| clear.survivors().len() + 1)
        .sum::<usize>()
        + usize::from(matches!(marker, GapsMarker::DeleteMarker));
    let mut rows: SmallVec<[CellBatchRow<'u>; 2]> = SmallVec::with_capacity(count);
    let mut weight = count as u64 * PER_STATEMENT_OVERHEAD;
    for clear in clears {
        let section = i8::from(clear.section());
        let survivors = clear.survivors();
        let (Some(first), Some(last)) = (survivors.first(), survivors.last()) else {
            rows.push(CellBatchRow {
                statement: &queries.gap_section,
                row: RowShape::GapSection(GapSectionRow { pk, section }),
            });
            continue;
        };
        weight += (first.as_bytes().len() + last.as_bytes().len()) as u64;
        rows.push(CellBatchRow {
            statement: &queries.gap_below,
            row: RowShape::GapEdge(GapEdgeRow {
                pk,
                section,
                coordinate: first.as_bytes(),
            }),
        });
        for pair in survivors.windows(2) {
            weight += (pair[0].as_bytes().len() + pair[1].as_bytes().len()) as u64;
            rows.push(CellBatchRow {
                statement: &queries.gap_between,
                row: RowShape::GapBetween(GapBetweenRow {
                    pk,
                    section,
                    low: pair[0].as_bytes(),
                    high: pair[1].as_bytes(),
                }),
            });
        }
        rows.push(CellBatchRow {
            statement: &queries.gap_above,
            row: RowShape::GapEdge(GapEdgeRow {
                pk,
                section,
                coordinate: last.as_bytes(),
            }),
        });
    }
    if matches!(marker, GapsMarker::DeleteMarker) {
        rows.push(CellBatchRow {
            statement: &queries.marker_delete,
            row: RowShape::Key(KeyRow {
                kind: CellKind::Marker,
                addr: CellAddr::marker(pk),
            }),
        });
    }
    BatchUnit::new(weight, rows)
}

/// The one-row batch unit deleting a collection's event-marker row at its
/// fixed address — appended by the clears-free settle verbs.
fn marker_delete_unit<'u>(pk: Pk<'u>, queries: &'u CellQueries) -> BatchUnit<CellBatchRow<'u>> {
    BatchUnit::new(
        PER_STATEMENT_OVERHEAD,
        smallvec![CellBatchRow {
            statement: &queries.marker_delete,
            row: RowShape::Key(KeyRow {
                kind: CellKind::Marker,
                addr: CellAddr::marker(pk),
            }),
        }],
    )
}

/// Whether `weights` pack into a **single** batch under the byte and count
/// budgets — the pure marker-first ordering decision: `chunk_boundaries`
/// provably yields one chunk iff the weight sum fits `max_bytes` and the count
/// fits `max_count`, so a stage passing this check may carry its marker in the
/// atomic batch; otherwise the marker must be awaited alone first.
fn fits_one_batch(weights: impl Iterator<Item = u64>, max_bytes: u64, max_count: usize) -> bool {
    let (mut total, mut count) = (0_u64, 0_usize);
    for weight in weights {
        total = total.saturating_add(weight);
        count += 1;
    }
    total <= max_bytes && count <= max_count
}

/// Maps a raw Cassandra error into the resolving store error, generic only over
/// the oracle error type `E` the caller's stream carries.
fn into_store_err<E: Error + 'static>(error: CassandraStoreError) -> CellStoreError<E> {
    ResolveCellError::Store(CassandraCellStoreError::from(error))
}

/// Whether `key` has walked past the in-code `end` edge for the scan
/// direction. An `Excluded` edge also stops *on* the endpoint (the exclusive
/// variant for coverage gap fall-through).
fn past_end(dir: Direction, key: &CellKey, end: ScanEdge<&Coordinate>) -> bool {
    let coordinate = key.coordinate.as_bytes();
    match (dir, end) {
        (Direction::Forward, ScanEdge::Included(end)) => coordinate > end.as_bytes(),
        (Direction::Forward, ScanEdge::Excluded(end)) => coordinate >= end.as_bytes(),
        (Direction::Backward, ScanEdge::Included(end)) => coordinate < end.as_bytes(),
        (Direction::Backward, ScanEdge::Excluded(end)) => coordinate <= end.as_bytes(),
    }
}

/// Encodes a cell's `data`/`prev` payloads into their bound columns, computing
/// the shared encoding/version flags off whether **either** blob is present.
fn encode_cell_blobs(
    data: Option<&Bytes>,
    prev: Option<&Bytes>,
) -> Result<CellBlobs, CassandraCellStoreError> {
    let data = data
        .map(|p| encode_payload(p, VALUE_ENCODING))
        .transpose()?;
    let prev_data = prev
        .map(|p| encode_payload(p, VALUE_ENCODING))
        .transpose()?;
    let any = data.is_some() || prev_data.is_some();
    Ok(CellBlobs {
        data,
        prev_data,
        encoding: any.then_some(VALUE_ENCODING),
        version: any.then_some(INITIAL_VERSION),
    })
}

/// The batch-packing weight of a cell row: its blob bytes plus the fixed
/// [`PER_STATEMENT_OVERHEAD`]. Over-counts rather than under-counts, so a
/// packed batch never exceeds the byte budget it was sized against.
fn blob_weight(blob: &CellBlobs) -> u64 {
    let blob_bytes = blob.data.as_ref().map_or(0_u64, |b| b.len() as u64)
        + blob.prev_data.as_ref().map_or(0_u64, |b| b.len() as u64);
    PER_STATEMENT_OVERHEAD + blob_bytes
}

/// Converts a per-write TTL to the `i32` the driver binds to `USING TTL ?`.
/// The input is pre-validated against Cassandra's ceiling at registration, so
/// the saturating conversion is only a defensive floor.
fn ttl_to_i32(ttl: CompactDuration) -> i32 {
    ttl.seconds().try_into().unwrap_or(i32::MAX)
}

/// Converts a blob-TTL read (`decode`'s `blob_ttl`) into the cache-fill
/// remaining duration. A NULL (`None`) means the cell has no TTL — it never
/// expires. A present value is the whole remaining seconds (a FLOOR), so a
/// fjall entry stamped `now + remaining` never outlives the durable row —
/// including `0`, which means *sub-second remaining* and must stamp an
/// (almost) immediate expiry, never "never" (a negative is treated the same,
/// defensively). Collapsing `0` into `None` would let the fjall entry outlive
/// a durable row that dies within the second.
fn ttl_seconds_to_duration(ttl: Option<i32>) -> Option<CompactDuration> {
    ttl.map(|s| CompactDuration::new(u32::try_from(s).unwrap_or(0)))
}

cassandra_queries! {
    /// Container for the prepared CQL statements used by [`CassandraStore`].
    ///
    /// Every statement binds the leading clustering `kind` as a constant
    /// (`CellKind::Cell` for the cell statements, `CellKind::Marker` for the
    /// marker statements) — a clustering-prefix column cannot be skipped.
    /// Each cell mutation is one `UPDATE`/`INSERT`/`DELETE` of one row; a
    /// multi-cell collection write binds these once per cell into one
    /// same-partition `UNLOGGED BATCH` (via `execute_unlogged_batches`), so all
    /// its cells share one write timestamp and TTL anchor. TTL/no-TTL pairs
    /// exist because Cassandra cannot bind `NULL` to `USING TTL ?`. The scans
    /// are single-section clustering ranges within the `kind=Cell` slice: the
    /// `ORDER BY` direction cannot be bound (forward/backward), and the
    /// **start-side comparator** cannot be bound either, so each direction
    /// carries two start variants — inclusive (`>=`/`<=`) and exclusive
    /// (`>`/`<`, for coverage gap fall-through). Every scan binds a concrete
    /// start coordinate ([`ScanEdge`] has no unbounded variant). The end bound
    /// is enforced in code (`past_end`), so it needs no statement variant. The
    /// `marker_*` statements maintain and point-read the one fixed-address
    /// event-marker row that bounds recovery. The `gap_*` statements are the
    /// section-clear range deletes (`gaps_unit`) — writes, never reads, so
    /// no statement can scan a tombstone field. None use `ALLOW FILTERING`.
    pub struct CellQueries {
        /// Reads one cell's columns (Resolved/Provisional/Corrupt shapes).
        read_cell: (
            "SELECT data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Reads one cell's columns plus each blob's remaining TTL, for the
        /// cache-fill point read. `TTL(column)` is a read function (no schema
        /// change); it returns NULL when the column is NULL or the row has no
        /// TTL. Both blobs are selected so the co-expiry can follow whichever
        /// blob resolution returns (the `decode` module's `blob_ttl`).
        read_cell_ttl: (
            "SELECT data, prev_data, encoding, version, event, TTL(data), TTL(prev_data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an inclusive `coordinate` anchor.
        scan_forward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data), \
             TTL(prev_data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate >= ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an exclusive `coordinate` anchor.
        scan_forward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data), \
             TTL(prev_data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an inclusive `coordinate` anchor.
        scan_backward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data), \
             TTL(prev_data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate <= ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an exclusive `coordinate` anchor.
        scan_backward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data), \
             TTL(prev_data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate < ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell with TTL (the full `data | prev_data |
        /// event` shape plus the shared encoding/version columns).
        write_provisional: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell without TTL.
        write_provisional_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell with TTL: the committed `data` plus its
        /// encoding/version, nulling `prev_data` and `event`.
        write_resolved: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell without TTL.
        write_resolved_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Promotes a provisional cell: nulls `prev_data` and `event`, keeping
        /// `data` (and its original TTL). O(1) bytes; no TTL clause — the
        /// retained `data` keeps the TTL set at its provisional write.
        mark_resolved: (
            "UPDATE $keyspace.{} \
             SET prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Row-level delete of one `kind=Cell` row: the committed-absent shape
        /// (see the `CellStore` row-absence invariant). One row tombstone that
        /// also covers any future columns — strictly better than nulling every
        /// column. No TTL clause (deletes carry none). Its CQL text matches
        /// `marker_delete`; the two are kept separate because they die
        /// separately and bind a different constant `kind`.
        cell_delete: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Upserts the collection's event-marker row with TTL (co-expiry with
        /// the staged cells): the frozen payload in `data`/`encoding`/`version`
        /// and the staging event in `event`. Deliberately does NOT touch
        /// `prev_data` — a marker row never carries one, and binding an
        /// explicit null would write a needless column tombstone at the fixed
        /// address on every stage.
        marker_write: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Upserts the event-marker row without TTL.
        marker_write_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Point-reads the event-marker row at its fixed address — the cold
        /// recovery seed (cost: one point read at a compaction-merged
        /// position, never a range over a tombstone field).
        marker_read: (
            "SELECT data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Row-level delete of the event-marker row (on settle — the whole
        /// stage resolved). Deleting an absent marker is a harmless no-op.
        marker_delete: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Whole-section gap delete: erases a cleared section with no
        /// survivors as one clustering-range tombstone (`kind` bound
        /// `CellKind::Cell`; a write, never a read — no TTL clause).
        gap_section: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Gap delete below the first survivor (`coordinate < ?`).
        gap_below: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate < ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Gap delete between two adjacent survivors
        /// (`coordinate > ? AND coordinate < ?` — both exclusive, so the
        /// survivors themselves are never covered).
        gap_between: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ? AND coordinate < ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Gap delete above the last survivor (`coordinate > ?`).
        gap_above: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ?",
            TABLE_KEYED_STATE_CELL
        ),
    }
}
