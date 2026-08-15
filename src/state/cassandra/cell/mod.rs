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
//! strand). The per-assignment marker memo — the standing RAM map
//! ([`MarkerMemo`]) plus the fjall presence latch ([`MarkerPresence`]) — bounds
//! durable marker reads to at most one per collection per assignment.
//!
//! The marker also carries the stage's **section clears** (each cleared
//! section with its frozen survivor list). A committed clear is applied as the
//! n+1 **gap range deletes** between sorted survivors ([`extend_gap_units`]) —
//! survivors are excluded positionally, never temporally. Settle applies the
//! lifecycle invariant marker-**last** through the shared
//! [`issue_marker_last`](CassandraStore::issue_marker_last) tail (used by both
//! [`commit_provisional`](CellStore::commit_provisional) and its abort twin
//! [`abort_provisional`](CellStore::abort_provisional));
//! [`write_resolved`](CellStore::write_resolved) applies its direct clears
//! marker-free. Until the gaps land, reads are defended by **read-help** and
//! blind committed writes by its **write twin**: `get`/`scan` (and `get`'s
//! cache-fill twin) resolve a standing foreign clears-bearing marker through
//! the sweep path before serving — the committed-unapplied read-window
//! contract stated on [`get`](CellStore::get) — and `write_resolved` resolves a
//! standing clears-bearing marker before landing (the committed-unapplied
//! write-window contract stated on
//! [`write_resolved`](CellStore::write_resolved)); both ride the same memo, so
//! the fast path pays no durable marker read.
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
//!   `commit()`, and rollback resolution).
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
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::event_ref::EventRef;
use crate::state::fjall::MarkerPresence;
use crate::state::marker::{EventMarker, SectionClear, encode_marker_payload};
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::{
    ResolveCellError, Resolver, flatten_resolve, help_read_window, help_write_window, peek_read,
    resolve_marker, resolve_read,
};
use crate::state::store::{
    CacheBatch, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, dedupe, section_batches,
};
use crate::state::{CollectionId, CollectionRef, SHARD_FANOUT_CONCURRENCY, StateType};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use decode::{BorrowedKeyedCellTtlRow, FramedKeyedCellRow, split_keyed_cell_ttl};
use encoding::{EncodedPayload, encode_payload, select_encoding};
use futures::{Stream, TryStreamExt, pin_mut};
use scylla::client::session::Session;
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
/// (`decode::validate_version`). Per-key identity migration is future work —
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
/// `CassandraStateBackendFactory::for_partition` time, so the resolving cell
/// store cannot be pre-built — this holds the partition-independent parts.
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

    /// Reads one cell's committed value without consulting the oracle. This is
    /// the read path a standalone reader uses, not the owner. It point-reads
    /// the `kind=Cell` row and projects [`Cell::project_committed`]. It
    /// never returns an in-flight provisional value and never runs
    /// owner-side repair: no `help_read_window`, no oracle. An absent row
    /// reads `None`. It decodes the borrowed row before it drops the response.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraCellStoreError`] on a store failure or a corrupt row
    /// shape.
    pub(crate) async fn read_committed(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, CassandraCellStoreError> {
        let Some(cell) =
            fetch_and_decode_cell(&self.session, &self.queries.read_cell, id, cell).await?
        else {
            return Ok(None);
        };
        Ok(cell.project_committed().cloned())
    }

    /// The batch form of [`Self::read_committed`]. Reads one section's
    /// coordinates in one `IN` query. The result is index-aligned to `batch`,
    /// so `result[i]` answers `batch[i]`. Duplicate coordinates share one
    /// lookup, and an absent coordinate reads `None`. Only the committed value
    /// is projected. The TTL column is ignored: the reader has no write-through
    /// cache to mirror it into.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraCellStoreError`] on a store failure or a corrupt row
    /// shape.
    pub(crate) async fn read_committed_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, CassandraCellStoreError> {
        let (uniques, plan) = dedupe(batch);
        let mut rows =
            fetch_cells_batch(&self.session, &self.queries, id, section, &uniques).await?;
        let mut answers: CellBuffer<Option<Bytes>> = SmallVec::with_capacity(uniques.len());
        for &coordinate in &uniques {
            let committed = match rows.iter().position(|(found, ..)| found == coordinate) {
                Some(pos) => {
                    let (_, cell, _) = rows.swap_remove(pos);
                    cell.project_committed().cloned()
                }
                None => None,
            };
            answers.push(committed);
        }
        let out: CellBuffer<Option<Bytes>> = plan.iter().map(|&i| answers[i].clone()).collect();
        debug_assert_eq!(
            out.len(),
            batch.len(),
            "batch read must answer every input position"
        );
        Ok(out)
    }

    /// Scans a section's committed values without consulting the oracle. This
    /// is the scan path a standalone reader uses, not the owner. It drives
    /// the shared [`page_cells`] pager and yields each present cell's
    /// [`Cell::project_committed`] in `coordinate` order. The scan's `limit`
    /// counts only present yields. It skips `help_read_window`, the owner-side
    /// durable repair a reader cannot and may not run.
    ///
    /// The projection is sound without that repair. A provisional row's `prev`
    /// is committed by construction, and a resolved row's `data` was committed
    /// at some earlier point. So a resolved row written before a committed but
    /// not-yet-applied section clear reads a value that was once committed but
    /// is now stale, until the owner applies the clear. That staleness is
    /// bounded (see
    /// [`Cell::project_committed`](crate::state::cell::Cell::project_committed)).
    /// It is never an uncommitted read.
    pub(crate) fn scan_committed<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), CassandraCellStoreError>> + Send + 'a {
        let limit = scan.limit;
        try_stream! {
            let pages = page_cells(&self.session, &self.queries, id, scan);
            pin_mut!(pages);
            let mut yielded = 0usize;
            while let Some((key, cell)) = pages.try_next().await? {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                if let Some(bytes) = cell.project_committed().cloned() {
                    yield (key, bytes);
                    yielded += 1;
                }
            }
        }
    }
}

/// Test-only recovery-read counters. `marker_point_reads` increments only in
/// [`standing_marker`](CellStore::standing_marker)'s durable-read arm — a memo
/// hit (presence latch + standing map) must not count — which is what makes the
/// "at most one durable marker read per collection per assignment" pin
/// non-vacuous.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct RecoveryReadCounts {
    /// Durable event-marker point reads — memo misses only.
    pub(crate) marker_point_reads: AtomicUsize,
    /// `kind=Cell` point reads issued during recovery — bounded by
    /// #provisional.
    pub(crate) cell_point_reads: AtomicUsize,
    /// `provisional_many` IN queries — exactly one per non-empty chunk.
    /// Distinct from `cell_point_reads` so the query-count pin proves the verb
    /// BATCHED (one IN query) rather than point-looped.
    pub(crate) provisional_in_queries: AtomicUsize,
}

/// Per-assignment RAM record of the markers known to stand: one standing
/// [`EventMarker`] per collection with an outstanding marker. Self-draining —
/// [`settle_memo`](CassandraStore::settle_memo) removes the entry on
/// commit/abort — so its population tracks collections with live markers, never
/// the whole key space.
///
/// It is the RAM half of the marker memo the three marker consumers ride (the
/// cold recovery seed, the stage-boundary check, and read-help). The
/// **checked** half — "has this collection's durable marker been consulted this
/// assignment?" — lives on disk in the per-assignment fjall index keyspace
/// ([`MarkerPresence`]): a RAM checked-set is insert-only and unbounded over a
/// weeks-long assignment.
///
/// **Memo invariant:** the pair may **over-report** a marker (list one that
/// never durably landed — resolving a phantom marker is a harmless no-op
/// settle) but must never **under-report** (presence-checked true while
/// `standing` misses a durable marker would strand that marker's cells from the
/// sweep until the next assignment). Two rules preserve it:
/// * `standing` is upserted **before** the presence set and before the durable
///   stage attempt (see [`stage_marker`](CassandraStore::stage_marker)), so a
///   presence hit never finds `standing` behind durable truth.
/// * A presence fjall error reads as **unchecked** ([`MarkerPresence`] is
///   infallible by design), degrading to one redundant durable marker read —
///   never a false checked-true. `standing` is pure RAM, untouched by presence
///   failures.
///
/// **Ownership invariant:** `standing` and the presence latch are ONE memo with
/// one lifecycle — a presence keyspace may only ever be read by stores sharing
/// this very `standing` map (clones of one store). Production enforces this by
/// construction: one store per partition acquisition, its latch minted from the
/// same fresh-per-assignment fjall workspace. Test fixtures must preserve it —
/// a re-minted store models a fresh assignment and gets a cold latch.
///
/// Per-key serialization means no two ops race on one collection's entry; scc
/// handles cross-collection concurrency. Minted fresh per [`CassandraStore`]
/// (fresh store per partition acquisition ⇒ cold memo per assignment); clones
/// share the `Arc`.
///
/// One field, still a named type: it anchors the memo and ownership invariants
/// the module, stage, and settle docs cite.
#[derive(Debug, Default)]
struct MarkerMemo {
    standing: scc::HashMap<CollectionId, EventMarker, RandomState>,
}

/// Cassandra-backed uniform cell store.
#[derive(Clone, Debug)]
pub struct CassandraStore<O> {
    session: CassandraSession,
    queries: Arc<CellQueries>,
    resolver: Resolver<O>,
    memo: Arc<MarkerMemo>,
    presence: MarkerPresence,
    #[cfg(test)]
    counters: Arc<RecoveryReadCounts>,
}

impl<O> CassandraStore<O> {
    /// Creates a Cassandra cell store over an existing session, a prepared
    /// [`CellQueries`] set, the commit oracle it resolves provisional cells
    /// through, the registry that supplies per-collection TTLs, and the
    /// per-assignment [`MarkerPresence`] latch minted from the partition's
    /// fjall workspace.
    #[must_use]
    pub(crate) fn new(
        session: CassandraSession,
        queries: Arc<CellQueries>,
        oracle: O,
        registry: Arc<CollectionDefRegistry>,
        presence: MarkerPresence,
    ) -> Self {
        Self {
            session,
            queries,
            resolver: Resolver::new(oracle, registry),
            memo: Arc::default(),
            presence,
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

    async fn point_read_cell(
        &self,
        statement: &PreparedStatement,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<Cell>, CassandraCellStoreError> {
        fetch_and_decode_cell(&self.session, statement, id, cell).await
    }

    async fn point_read_cell_ttl(
        &self,
        statement: &PreparedStatement,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<(Cell, Option<i32>)>, CassandraCellStoreError> {
        fetch_and_decode_cell_ttl(&self.session, statement, id, cell).await
    }

    /// Reads and decodes a section's coordinates in one `IN` query.
    /// The output follows `uniques` order and omits absent coordinates.
    ///
    /// `uniques` is caller-deduped and non-empty by [`CoordinateBatch`]
    /// construction, so the `IN` list has no repeats and is never empty; and
    /// bounded by `CELL_BATCH`, so the [`CellBuffer`] result stays bounded —
    /// inline for small reads, a single heap spill for larger ones.
    async fn batch_read(
        &self,
        id: &CollectionId,
        section: Section,
        uniques: &[&Coordinate],
    ) -> Result<CellBuffer<(Coordinate, Cell, Option<i32>)>, CassandraCellStoreError> {
        fetch_cells_batch(&self.session, &self.queries, id, section, uniques).await
    }

    /// Executes the packed same-partition `UNLOGGED BATCH`es for a multi-cell
    /// mutation — the shared tail of the cell mutators. Each [`BatchUnit`] is
    /// one row (a cell mutation, or the marker row), packed into the fewest
    /// batches under the byte and statement budgets; every batch is
    /// row-disjoint by construction (the marker address is disjoint from every
    /// cell row by `kind`), so no same-batch timestamp tie can pit a delete
    /// against a write of one row.
    ///
    /// Allocation ruling (write-path buffer audit): every mutator's `units`
    /// buffer — and the `blobs` its rows borrow — stays a `Vec`, never a
    /// [`CellBuffer`]/`SmallVec`.
    /// `BatchUnit<CellBatchRow>` is 320 B and `CellBlobs` 80 B, and both live
    /// across this `.await`; an inline capacity would embed hundreds of bytes
    /// to kilobytes in every stage/settle future the way `StagedCollection`
    /// tripped clippy `large_futures` (`crate::state::session`). Write sets are
    /// not `CELL_BATCH`-bounded (the packer splits by byte/statement budget
    /// downstream), and each build site is already exactly-sized
    /// `Vec::with_capacity`, so a conversion removes at most one allocation and
    /// cannot earn the footprint.
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
    /// Returns a borrowing iterator the callers extend into their pre-sized
    /// `units` — no intermediate buffer; see [`Self::run_batches`] for why the
    /// callers' `units` is a `Vec` rather than a [`CellBuffer`].
    fn resolved_units<'u>(
        &'u self,
        pk: Pk<'u>,
        ttl: Option<i32>,
        blobs: &'u [CellBlobs],
        cells: &'u [(CellKey, Option<Bytes>)],
    ) -> impl Iterator<Item = BatchUnit<CellBatchRow<'u>>> + 'u {
        let cell_stmt = if ttl.is_some() {
            &self.queries.write_resolved
        } else {
            &self.queries.write_resolved_no_ttl
        };
        blobs.iter().zip(cells).map(move |(blob, (cell, _))| {
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
    }

    /// Mirrors a successful settle into the marker memo ([`MarkerMemo`]'s
    /// standing map plus the presence latch): the marker is now durably
    /// deleted, so the collection is known marker-absent for the rest of the
    /// assignment.
    async fn settle_memo(&self, collection: &CollectionId) {
        self.memo.standing.remove_async(collection).await;
        self.presence.set(collection).await;
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
    ) -> Result<MarkerBlob, CellStoreError<O::Error>> {
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
        self.presence.set(collection.id()).await;
        let payload = encode_marker_payload(marker)
            .map_err(CassandraCellStoreError::from)
            .map_err(ResolveCellError::Store)?;
        let encoding = select_encoding(payload.len());
        let payload = encode_payload(&payload, encoding)
            .map_err(CassandraCellStoreError::from)
            .map_err(ResolveCellError::Store)?;
        Ok(MarkerBlob {
            payload,
            encoding,
            event: marker.event(),
        })
    }

    /// The single resolving section scan, yielding each present cell's
    /// committed bytes — the body behind [`scan_cells`](CellStore::scan_cells).
    fn scan_inner<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), CellStoreError<O::Error>>> + Send + 'a {
        let limit = scan.limit;
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            // Read-help once before the pager opens (`help_read_window`): a
            // standing foreign clears-bearing marker is resolved so the scan
            // pages post-clear truth. Memo-backed — no durable marker read
            // after the seed read. The reader-only scan
            // ([`CassandraCellResources::scan_committed`]) skips this: it
            // observes `prev`, which is committed by construction.
            let standing = self.standing_marker(collection).await?;
            help_read_window(self, self.resolver.oracle(), &collection_ref, standing.as_ref(), own)
                .await
                .map_err(flatten_resolve)?;
            // The shared paging core (`page_cells`): it selects the per-bound
            // statement, decodes each row, and applies `past_end`. It applies
            // no resolution and no limit.
            let pages = page_cells(&self.session, &self.queries, collection, scan);
            pin_mut!(pages);

            let mut yielded = 0usize;
            // Deliberately sequential, not an oversight: the common `peek_read`
            // is a free no-op (own-event provisional / already-resolved cells
            // consult no oracle), so steady-state scans gain nothing from
            // fan-out; the only payoff is mid-recovery across many foreign
            // provisional cells. And because `limit` counts *present* yields —
            // knowable only post-resolve — a buffered pipeline would resolve up
            // to N−1 foreign-provisional cells past the boundary, each an extra
            // oracle read: a recovery-only win we won't pay for on a hot read
            // path. `peek_read` is read-only — it never writes a resolution
            // back durably (a scan write-back could clobber a newer `commit()`
            // of the same cell), so this posture costs no write amplification.
            while let Some((key, raw)) = pages.try_next().await.map_err(ResolveCellError::Store)? {
                // The limit bounds *yielded* (present) cells; check it before
                // processing the next row so `Some(0)` yields nothing (an absent
                // cell never consumes a slot — only a present yield does).
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let committed = peek_read(self.resolver.oracle(), &collection_ref, own, raw)
                    .await
                    .map_err(ResolveCellError::Oracle)?;
                if let Some(bytes) = committed.into_inner() {
                    yield (key, bytes);
                    yielded += 1;
                }
            }
        }
    }

    /// Issues a settle's `units` marker-LAST: appends the collection's marker
    /// delete, then runs one atomic batch when everything fits the budget, else
    /// awaits the recovery prefix to completion BEFORE issuing the marker
    /// alone. Owning the append, the split, and the ordered await here
    /// makes marker misplacement and await reversal unrepresentable at the
    /// call sites — the coupling [`marker_last_split`]'s positional index
    /// alone cannot enforce.
    async fn issue_marker_last<'u>(
        &'u self,
        pk: Pk<'u>,
        mut units: Vec<BatchUnit<CellBatchRow<'u>>>,
    ) -> Result<(), CellStoreError<O::Error>> {
        units.push(marker_delete_unit(pk, &self.queries));
        let split = marker_last_split(&units, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS);
        self.run_batches(&units[..split])
            .await
            .map_err(ResolveCellError::Store)?;
        if split < units.len() {
            self.run_batches(&units[split..])
                .await
                .map_err(ResolveCellError::Store)?;
        }
        Ok(())
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
        // the standing marker — memo-backed, so no durable read after the one
        // seed read —
        // CONCURRENTLY with the cell point read, keeping the ~always
        // marker-free fast path free of serial latency. If the marker was
        // resolved (foreign + clears), the pre-help row may hold a now-erased
        // value, so the point read is re-issued.
        let (row, standing) = futures::join!(
            self.point_read_cell_ttl(&self.queries.read_cell_ttl, collection, cell),
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
                .point_read_cell_ttl(&self.queries.read_cell_ttl, collection, cell)
                .await
                .map_err(ResolveCellError::Store)?;
        }
        let (raw, ttl) = match row {
            Some(decoded) => decoded,
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

    async fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> Result<CommittedBatch, Self::Error> {
        // Mirrors `get` → `get_for_cache`: the committed value is the batch
        // cache-fill read minus its co-expiry TTLs.
        Ok(self
            .get_many_for_cache(collection, section, batch, own)
            .await?
            .into_iter()
            .map(|(committed, _)| committed)
            .collect())
    }

    async fn get_many_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> Result<CacheBatch, Self::Error> {
        let collection_ref = self.resolver.collection_ref(collection);
        let (uniques, plan) = dedupe(batch);
        // The committed-unapplied read window, exactly as the point read
        // (`get_for_cache`): consult the standing marker — memo-backed, so no
        // durable read after the one seed read — CONCURRENTLY with the batch
        // query. If the marker was resolved (foreign + clears), the pre-help
        // rows may hold now-erased values, so the whole `IN` query is re-issued
        // once.
        let (rows, standing) = futures::join!(
            self.batch_read(collection, section, &uniques),
            self.standing_marker(collection),
        );
        let mut rows = rows.map_err(ResolveCellError::Store)?;
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
            rows = self
                .batch_read(collection, section, &uniques)
                .await
                .map_err(ResolveCellError::Store)?;
        }
        // Resolve coordinates in first-occurrence input order. This order
        // matches the point-read oracle. An absent row resolves as `None`.
        let mut answers: CacheBatch = SmallVec::with_capacity(uniques.len());
        for &coordinate in &uniques {
            let cell = CellKey {
                section,
                coordinate: Coordinate::clone(coordinate),
            };
            let (raw, ttl) = match rows.iter().position(|(found, ..)| found == coordinate) {
                Some(pos) => {
                    let (_, cell, ttl) = rows.swap_remove(pos);
                    (cell, ttl)
                }
                None => (Cell::Resolved(Committed::new(None)), None),
            };
            let committed = resolve_read(
                self,
                self.resolver.oracle(),
                &collection_ref,
                &cell,
                own,
                raw,
            )
            .await
            .map_err(flatten_resolve)?;
            answers.push((committed, ttl_seconds_to_duration(ttl)));
        }
        let out: CacheBatch = plan.iter().map(|&i| answers[i].clone()).collect();
        debug_assert_eq!(
            out.len(),
            batch.len(),
            "batch read must answer every input position"
        );
        Ok(out)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.scan_inner(collection, scan, own)
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

            // Rebuild each listed coordinate's `ProvisionalCell` through one
            // raw `IN` query per per-section `<=CELL_BATCH` chunk (the section
            // is reattached to each survivor, since coordinates repeat across
            // sections). A listed coordinate whose row is absent (cell and
            // marker share one TTL) or already resolved (first-touch or a
            // concurrent resolve) is dropped by `provisional_many` — the
            // marker's over-report is safe. Sub-batches run sequentially: real
            // `IN`-query I/O leaves drive the coop budget.
            for (section, batch) in section_batches(marker.staged()) {
                // `Box::pin` keeps the large per-chunk batch-read future off
                // this generator's state so it stays small across the yield
                // (bounded per-chunk alloc on a cold recovery path).
                let survivors =
                    Box::pin(self.provisional_many(collection, section, &batch)).await?;
                for (coordinate, provisional) in survivors {
                    yield (CellKey { section, coordinate }, provisional);
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
            .point_read_cell(&self.queries.read_cell, collection, cell)
            .await
            .map_err(ResolveCellError::Store)?
        else {
            return Ok(None);
        };
        match raw {
            Cell::Provisional(provisional) => Ok(Some(provisional)),
            Cell::Resolved(_) => Ok(None),
        }
    }

    async fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error> {
        #[cfg(test)]
        self.counters
            .provisional_in_queries
            .fetch_add(1, Ordering::Relaxed);
        // Sorted-distinct IN list (no scatter plan — the output is survivor-only,
        // not index-aligned).
        let mut uniques: CellBuffer<&Coordinate> = SmallVec::with_capacity(batch.len());
        uniques.extend(batch.iter());
        uniques.sort_unstable();
        uniques.dedup();
        // One IN query, reusing the TTL-bearing batch read; TTL is discarded in
        // the decoder. Never consults the oracle, never resolves, never writes —
        // no read-window marker resolve, exactly as `provisional_cell_at`.
        let rows = self
            .batch_read(collection, section, &uniques)
            .await
            .map_err(ResolveCellError::Store)?;
        Ok(decode_provisional_batch(rows))
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
        let marker_blob: Option<MarkerBlob> = match marker {
            None => None,
            Some(marker) => Some(self.stage_marker(collection, marker).await?),
        };

        // Encode every cell's blobs up front (this Vec owns the `Bytes`); the
        // bound rows borrow into it and into each input cell's coordinate slice,
        // so the whole batch is one `blobs` allocation with no per-cell copy.
        // Stays a `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
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
        // The marker unit leads; each cell unit is one row. `units` stays a
        // `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
        let mut units: Vec<BatchUnit<CellBatchRow>> = Vec::with_capacity(writes.len() + 1);
        if let Some(blob) = &marker_blob {
            units.push(BatchUnit::new(
                blob.payload.len() as u64 + PER_STATEMENT_OVERHEAD,
                smallvec![CellBatchRow {
                    statement: marker_stmt,
                    row: RowShape::MarkerWrite(MarkerWriteRow {
                        ttl,
                        payload: &blob.payload,
                        encoding: blob.encoding,
                        event: blob.event,
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
        // The write-side committed-unapplied boundary (`help_write_window`):
        // resolve any standing clears-bearing event marker before this blind
        // write lands, ordering the write after that resolution so a stale
        // clear's positional replay cannot erase it (modulo the
        // concurrent-resolver residual documented on `help_write_window`).
        // Memo-backed (`standing_marker`): one RAM/presence check steady-state,
        // and the first marker consumer per collection per assignment pays the
        // one durable seed read. The verb still never *writes* the marker slice
        // (the marker lifecycle belongs to the staged verbs — see
        // `CellStore::write_provisional`); the resolution runs through this
        // store's own `commit_provisional`/`abort_provisional`, keeping the
        // memo coherent.
        let standing = self.standing_marker(collection.id()).await?;
        help_write_window(self, self.resolver.oracle(), collection, standing.as_ref())
            .await
            .map_err(flatten_resolve)?;
        // Survivors are the present-data `cells`, excluded from the gaps
        // positionally, so every batch row stays disjoint and may be packed
        // independently.
        let pk = Pk::of(collection.id());
        // Encode each cell's committed `data` up front (owns the `Bytes`); no
        // `prev`, so the blobs carry only `data` + its encoding/version. Both
        // `blobs` and `units` stay a `Vec` — see the `run_batches` ruling.
        let mut blobs = Vec::with_capacity(cells.len());
        for (_, data) in cells {
            blobs.push(encode_cell_blobs(data.as_ref(), None).map_err(ResolveCellError::Store)?);
        }
        let ttl = collection.ttl().map(ttl_to_i32);
        let mut units = Vec::with_capacity(cells.len() + gap_count(clears));
        extend_gap_units(&mut units, &self.queries, pk, clears);
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
        // budget alone splits an enormous promote set. `units` stays a `Vec`
        // (not a `CellBuffer`) — see the `run_batches` ruling.
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
        // Memo hit: the presence latch says the durable marker was consulted
        // this assignment, so the RAM standing map is at least durable truth —
        // zero durable reads. A presence fjall error reads as unchecked (a
        // redundant durable re-check, never an under-report).
        if self.presence.contains(collection).await {
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
        let result = self
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
            .map_err(into_store_err::<O::Error>)?;
        let row = result
            .maybe_first_row::<decode::BorrowedMarkerRow<'_>>()
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
        self.presence.set(collection).await;
        Ok(marker)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Commit applies natively — present data promotes in place, a staged
        // clear deletes its row (the row-absence invariant).
        // Cell and gap rows are disjoint and idempotent: gaps exclude survivors
        // and one another positionally, while a cell delete inside a gap is a
        // harmless delete/delete tie.
        let pk = Pk::of(collection.id());
        // `units` stays a `Vec` (not a `CellBuffer`) — see the `run_batches` ruling.
        let mut units: Vec<BatchUnit<CellBatchRow>> =
            Vec::with_capacity(writes.len() + gap_count(clears) + 1);
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
        extend_gap_units(&mut units, &self.queries, pk, clears);
        self.issue_marker_last(pk, units).await?;
        self.settle_memo(collection.id()).await;
        Ok(())
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // Rollback: write each staged cell's committed base `prev` back as
        // resolved natively (`prev = None` restores exact row absence), then
        // delete the marker.
        let pk = Pk::of(collection.id());
        // Synthesized (cell, prev) pairs for the shared `resolved_units` helper;
        // kept a `Vec` (the clones are O(1) `Arc`/`Bytes` refcount bumps) —
        // deleting it would force `resolved_units` onto a less-clear iterator
        // signature on the common `write_resolved` path, for the rare rollback.
        let cells: Vec<(CellKey, Option<Bytes>)> = writes
            .iter()
            .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
            .collect();
        // `blobs` and `units` stay a `Vec` — see the `run_batches` ruling.
        let mut blobs = Vec::with_capacity(cells.len());
        for (_, data) in &cells {
            blobs.push(encode_cell_blobs(data.as_ref(), None).map_err(ResolveCellError::Store)?);
        }
        let ttl = collection.ttl().map(ttl_to_i32);
        let mut units = Vec::with_capacity(cells.len() + 1);
        units.extend(self.resolved_units(pk, ttl, &blobs, &cells));
        self.issue_marker_last(pk, units).await?;
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
    data: Option<EncodedPayload>,
    prev_data: Option<EncodedPayload>,
    encoding: Option<Encoding>,
    version: Option<i32>,
}

struct MarkerBlob {
    payload: EncodedPayload,
    encoding: Encoding,
    event: EventRef,
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
    encoding: Encoding,
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
                self.encoding,
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
                self.encoding,
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

/// The number of gap rows needed to erase `clears` while excluding survivors.
fn gap_count(clears: &[SectionClear]) -> usize {
    clears.iter().map(|clear| clear.survivors().len() + 1).sum()
}

/// Appends one bounded batch unit per gap around each cleared section's sorted,
/// deduplicated survivors (`< k₁`, `(k₁,k₂)`, …, `> kₙ`; one whole-section
/// delete when empty). Coordinates borrow from the frozen [`SectionClear`]s.
fn extend_gap_units<'u>(
    units: &mut Vec<BatchUnit<CellBatchRow<'u>>>,
    queries: &'u CellQueries,
    pk: Pk<'u>,
    clears: &'u [SectionClear],
) {
    for clear in clears {
        let section = i8::from(clear.section());
        let survivors = clear.survivors();
        let (Some(first), Some(last)) = (survivors.first(), survivors.last()) else {
            units.push(BatchUnit::new(
                PER_STATEMENT_OVERHEAD,
                smallvec![CellBatchRow {
                    statement: &queries.gap_section,
                    row: RowShape::GapSection(GapSectionRow { pk, section }),
                }],
            ));
            continue;
        };
        units.push(BatchUnit::new(
            first.as_bytes().len() as u64 + PER_STATEMENT_OVERHEAD,
            smallvec![CellBatchRow {
                statement: &queries.gap_below,
                row: RowShape::GapEdge(GapEdgeRow {
                    pk,
                    section,
                    coordinate: first.as_bytes(),
                }),
            }],
        ));
        for pair in survivors.windows(2) {
            units.push(BatchUnit::new(
                (pair[0].as_bytes().len() + pair[1].as_bytes().len()) as u64
                    + PER_STATEMENT_OVERHEAD,
                smallvec![CellBatchRow {
                    statement: &queries.gap_between,
                    row: RowShape::GapBetween(GapBetweenRow {
                        pk,
                        section,
                        low: pair[0].as_bytes(),
                        high: pair[1].as_bytes(),
                    }),
                }],
            ));
        }
        units.push(BatchUnit::new(
            last.as_bytes().len() as u64 + PER_STATEMENT_OVERHEAD,
            smallvec![CellBatchRow {
                statement: &queries.gap_above,
                row: RowShape::GapEdge(GapEdgeRow {
                    pk,
                    section,
                    coordinate: last.as_bytes(),
                }),
            }],
        ));
    }
}

/// The one-row batch unit deleting a collection's event-marker row at its
/// fixed address, appended last by [`issue_marker_last`], the shared tail of
/// both settle verbs.
///
/// [`issue_marker_last`]: CassandraStore::issue_marker_last
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

/// Returns the boundary between settle's cell/gap work and its final marker
/// delete. Everything returns in the first slice when one batch can carry all
/// rows atomically; otherwise the marker is the second slice's sole unit, so it
/// is issued only after every recovery-relevant mutation has completed.
///
/// **Precondition:** the marker delete is the LAST unit — the split is
/// positional (`units.len() - 1`), so a marker placed elsewhere, or a caller
/// awaiting the tail before the prefix, would issue the marker before the
/// recovery-relevant rows.
/// [`issue_marker_last`](CassandraStore::issue_marker_last) owns that ordering;
/// this function only decides where the split falls.
fn marker_last_split<R>(units: &[BatchUnit<R>], max_bytes: u64, max_count: usize) -> usize {
    if fits_one_batch(units.iter().map(BatchUnit::weight), max_bytes, max_count) {
        units.len()
    } else {
        units.len().saturating_sub(1)
    }
}

/// Whether `weights` pack into a **single** batch under the byte and count
/// budgets: `chunk_boundaries` provably yields one chunk iff the weight sum
/// fits `max_bytes` and the count fits `max_count`. This one predicate
/// underlies both marker-ordering decisions — the stage's marker-FIRST choice
/// (`write_provisional`) and the settle's marker-LAST split
/// ([`marker_last_split`]): when everything fits one atomic batch the marker
/// rides along; otherwise it is isolated to its own batch — issued first at
/// stage, last at settle.
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

async fn fetch_and_decode_cell(
    session: &CassandraSession,
    statement: &PreparedStatement,
    id: &CollectionId,
    cell: &CellKey,
) -> Result<Option<Cell>, CassandraCellStoreError> {
    let pk = Pk::of(id);
    let result = session
        .session()
        .execute_unpaged(
            statement,
            (
                pk.segment_id,
                pk.key,
                pk.state_type,
                pk.name,
                CellKind::Cell,
                i8::from(cell.section),
                &cell.coordinate,
            ),
        )
        .await
        .map_err(CassandraStoreError::from)?
        .into_rows_result()
        .map_err(CassandraStoreError::from)?;
    result
        .maybe_first_row::<decode::BorrowedRawCellRow<'_>>()
        .map_err(CassandraStoreError::from)?
        .map(decode::try_decode_cell)
        .transpose()
}

async fn fetch_and_decode_cell_ttl(
    session: &CassandraSession,
    statement: &PreparedStatement,
    id: &CollectionId,
    cell: &CellKey,
) -> Result<Option<(Cell, Option<i32>)>, CassandraCellStoreError> {
    let pk = Pk::of(id);
    let result = session
        .session()
        .execute_unpaged(
            statement,
            (
                pk.segment_id,
                pk.key,
                pk.state_type,
                pk.name,
                CellKind::Cell,
                i8::from(cell.section),
                &cell.coordinate,
            ),
        )
        .await
        .map_err(CassandraStoreError::from)?
        .into_rows_result()
        .map_err(CassandraStoreError::from)?;
    result
        .maybe_first_row::<decode::BorrowedCellTtlRow<'_>>()
        .map_err(CassandraStoreError::from)?
        .map(decode::try_decode_cell_ttl)
        .transpose()
}

/// Reads and decodes one bounded `IN` query in input resolution order.
/// The result owns no reference to the Scylla response frame.
async fn fetch_cells_batch(
    session: &CassandraSession,
    queries: &CellQueries,
    id: &CollectionId,
    section: Section,
    uniques: &[&Coordinate],
) -> Result<CellBuffer<(Coordinate, Cell, Option<i32>)>, CassandraCellStoreError> {
    let pk = Pk::of(id);
    let result = session
        .session()
        .execute_unpaged(
            &queries.read_cells_batch,
            (
                pk.segment_id,
                pk.key,
                pk.state_type,
                pk.name,
                CellKind::Cell,
                i8::from(section),
                uniques,
            ),
        )
        .await
        .map_err(CassandraStoreError::from)?
        .into_rows_result()
        .map_err(CassandraStoreError::from)?;
    // At most one row per unique coordinate, so size once at the IN-list upper
    // bound rather than growing an inline buffer up to `CELL_BATCH`.
    let mut rows: CellBuffer<(Coordinate, decode::BorrowedCellTtlRow<'_>)> =
        SmallVec::with_capacity(uniques.len());
    for row in result
        .rows::<BorrowedKeyedCellTtlRow<'_>>()
        .map_err(CassandraStoreError::from)?
    {
        rows.push(split_keyed_cell_ttl(
            row.map_err(CassandraStoreError::from)?,
        ));
    }

    decode_batch_rows(rows, uniques)
}

fn decode_batch_rows(
    mut rows: CellBuffer<(Coordinate, decode::BorrowedCellTtlRow<'_>)>,
    uniques: &[&Coordinate],
) -> Result<CellBuffer<(Coordinate, Cell, Option<i32>)>, CassandraCellStoreError> {
    let mut out = CellBuffer::with_capacity(uniques.len());
    for &coordinate in uniques {
        let Some(pos) = rows.iter().position(|(found, _)| found == coordinate) else {
            continue;
        };
        let (coordinate, row) = rows.swap_remove(pos);
        let (cell, ttl) = decode::try_decode_cell_ttl(row)?;
        out.push((coordinate, cell, ttl));
    }
    Ok(out)
}

/// The shared section-scan row pager. It opens the prepared statement for the
/// scan bounds (the six-arm `(dir, start)` selection), builds the
/// [`FramedKeyedCellRow`] stream, and yields each decoded `(CellKey, Cell)`
/// with the in-code `past_end` cutoff applied. It applies no limit and no
/// resolution. The limit counts present cells after projection, so each
/// consumer keeps it in its own loop. Two callers consume this. The owner scan
/// ([`CassandraStore::scan_inner`]) then applies `peek_read`; the reader scan
/// ([`CassandraCellResources::scan_committed`]) then applies
/// `project_committed`. Sharing this pager keeps their physical paging from
/// drifting apart. Each `try_next` is wrapped in [`cooperative`] so a drain of
/// ready rows yields to the runtime every ~128 items.
fn page_cells<'a>(
    session: &'a CassandraSession,
    queries: &'a CellQueries,
    collection: &'a CollectionId,
    scan: Scan<'a>,
) -> impl Stream<Item = Result<(CellKey, Cell), CassandraCellStoreError>> + Send + 'a {
    let section = i8::from(scan.section);
    let dir = scan.dir;
    // Both edges are held as owned `Coordinate`s across the stream's awaits —
    // O(1) refcount bumps (`Coordinate` is `Bytes`), never byte copies.
    let start = scan.start.cloned();
    let end = scan.end.cloned();
    try_stream! {
        let pk = Pk::of(collection);
        // The section-prefix bind values every scan statement shares.
        let prefix = (pk.segment_id, pk.key, pk.state_type, pk.name, CellKind::Cell, section);
        let (seg, key, st, name, cell_kind, sect) = prefix;
        // Statement selection and binding are one match. A bounded arm appends
        // the anchor coordinate for a 7-tuple. An `Unbounded` arm binds only the
        // section prefix for a 6-tuple. Those are distinct Rust types, so the
        // pager must open inside each arm.
        let pager = match (dir, start.as_ref()) {
            (Direction::Forward, ScanEdge::Included(c)) => {
                session.session().execute_iter(queries.scan_forward_incl.clone(),
                    (seg, key, st, name, cell_kind, sect, c)).await
            }
            (Direction::Forward, ScanEdge::Excluded(c)) => {
                session.session().execute_iter(queries.scan_forward_excl.clone(),
                    (seg, key, st, name, cell_kind, sect, c)).await
            }
            (Direction::Backward, ScanEdge::Included(c)) => {
                session.session().execute_iter(queries.scan_backward_incl.clone(),
                    (seg, key, st, name, cell_kind, sect, c)).await
            }
            (Direction::Backward, ScanEdge::Excluded(c)) => {
                session.session().execute_iter(queries.scan_backward_excl.clone(),
                    (seg, key, st, name, cell_kind, sect, c)).await
            }
            (Direction::Forward, ScanEdge::Unbounded) => {
                session.session().execute_iter(queries.scan_forward_all.clone(), prefix).await
            }
            (Direction::Backward, ScanEdge::Unbounded) => {
                session.session().execute_iter(queries.scan_backward_all.clone(), prefix).await
            }
        };
        let stream = pager
            .map_err(CassandraStoreError::from)?
            .rows_stream::<FramedKeyedCellRow>()
            .map_err(CassandraStoreError::from)?;
        pin_mut!(stream);
        while let Some(row) = cooperative(stream.try_next())
            .await
            .map_err(CassandraStoreError::from)?
        {
            let (key, cell) = decode::try_decode_keyed_cell(row)?;
            if past_end(dir, &key, end.as_ref()) {
                break;
            }
            yield (key, cell);
        }
    }
}

/// Whether `key` has walked past the in-code `end` edge for the scan
/// direction. An `Excluded` edge also stops *on* the endpoint (the exclusive
/// variant for exclusive scan anchors); an `Unbounded` end never stops the
/// walk (the section-only fallback).
fn past_end(dir: Direction, key: &CellKey, end: ScanEdge<&Coordinate>) -> bool {
    let coordinate = key.coordinate.as_bytes();
    match (dir, end) {
        (Direction::Forward, ScanEdge::Included(end)) => coordinate > end.as_bytes(),
        (Direction::Forward, ScanEdge::Excluded(end)) => coordinate >= end.as_bytes(),
        (Direction::Backward, ScanEdge::Included(end)) => coordinate < end.as_bytes(),
        (Direction::Backward, ScanEdge::Excluded(end)) => coordinate <= end.as_bytes(),
        (_, ScanEdge::Unbounded) => false,
    }
}

/// Encodes a cell's `data`/`prev` payloads into their bound columns, computing
/// the shared encoding/version flags off whether **either** blob is present.
fn encode_cell_blobs(
    data: Option<&Bytes>,
    prev: Option<&Bytes>,
) -> Result<CellBlobs, CassandraCellStoreError> {
    let payload_len = data
        .into_iter()
        .chain(prev)
        .map(Bytes::len)
        .max()
        .unwrap_or(0);
    let encoding = select_encoding(payload_len);
    let data = data
        .map(|payload| encode_payload(payload, encoding))
        .transpose()?;
    let prev_data = prev
        .map(|payload| encode_payload(payload, encoding))
        .transpose()?;
    let any = data.is_some() || prev_data.is_some();
    Ok(CellBlobs {
        data,
        prev_data,
        encoding: any.then_some(encoding),
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

/// Keeps provisional cells from a recovery batch and discards their TTLs.
/// The input already follows ascending coordinate order.
fn decode_provisional_batch(
    rows: CellBuffer<(Coordinate, Cell, Option<i32>)>,
) -> CellBuffer<(Coordinate, ProvisionalCell)> {
    let mut out: CellBuffer<(Coordinate, ProvisionalCell)> = SmallVec::with_capacity(rows.len());
    for (coordinate, cell, _) in rows {
        match cell {
            Cell::Provisional(provisional) => out.push((coordinate, provisional)),
            Cell::Resolved(_) => {}
        }
    }
    out
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
    /// (`>`/`<`, for exclusive anchors). A start edge is therefore either a
    /// bound coordinate (the four incl/excl statements) or
    /// [`Unbounded`](ScanEdge::Unbounded) (the two section-only `_all`
    /// statements, which carry no start comparator). The end bound is enforced
    /// in code (`past_end`), so it needs no statement variant. The `marker_*`
    /// statements maintain and point-read the one fixed-address event-marker row
    /// that bounds recovery. The `gap_*` statements are the section-clear range
    /// deletes (`extend_gap_units`) — writes, never reads. Scan issuance is gated: the
    /// four cell mutators each write exactly one row shape, so the only reader
    /// that walks a whole section (and thus can meet a tombstone field) is an
    /// `_all` scan, reached solely by the map's degraded full-section fallback —
    /// the accepted degraded cost. None use `ALLOW FILTERING`.
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

        /// Batch twin of [`read_cell_ttl`](Self::read_cell_ttl): one section's
        /// cells for a bounded (`1..=CELL_BATCH`) coordinate list, plus each
        /// blob's remaining TTL. `IN` returns matching clustering rows in
        /// coordinate order (not input order) and omits absent coordinates, so
        /// the reader carries the `coordinate` column to re-key each row to its
        /// input position and treats a missing coordinate as an absent row.
        /// One same-partition, single-shard query — never a cross-partition
        /// `IN` (the partition key is fully bound).
        read_cells_batch: (
            "SELECT coordinate, data, prev_data, encoding, version, event, \
             TTL(data), TTL(prev_data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate IN ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an inclusive `coordinate` anchor.
        scan_forward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate >= ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an exclusive `coordinate` anchor.
        scan_forward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an inclusive `coordinate` anchor.
        scan_backward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate <= ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an exclusive `coordinate` anchor.
        scan_backward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate < ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Section-only forward scan (an [`Unbounded`](ScanEdge::Unbounded)
        /// start edge): no start comparator, walks the whole `kind=Cell` slice
        /// of the section in ascending `coordinate` order.
        scan_forward_all: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Section-only backward scan (an [`Unbounded`](ScanEdge::Unbounded)
        /// start edge): no start comparator, walks the whole `kind=Cell` slice
        /// of the section in descending `coordinate` order.
        scan_backward_all: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? \
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
        /// also includes any future columns — strictly better than nulling every
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
        /// survivors themselves are never inside a gap range).
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
