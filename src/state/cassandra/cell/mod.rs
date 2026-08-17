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

mod batch;
mod cell_store;
mod decode;
mod encoding;
mod helpers;
mod queries;
mod read;
mod resources;
mod rows;
mod serialization;
mod store;
mod write;

use batch::{extend_gap_units, fits_one_batch, gap_count, marker_delete_unit, marker_last_split};
use helpers::{
    blob_weight, decode_provisional_batch, encode_cell_blobs, ttl_seconds_to_duration, ttl_to_i32,
};
pub use queries::CellQueries;
#[cfg(test)]
use read::decode_rows_for_coordinates;
use read::{
    ScanStatements, decode_batch_rows, decode_cell_ttl_result, decode_presence_batch_rows,
    fetch_and_decode_cell, fetch_cell_rows_result, fetch_cells_batch, fetch_cells_batch_result,
    fetch_presence_batch_result, into_store_err, match_batch_rows_to_coordinates, page_cells,
};
use rows::{
    CellAddr, CellBatchRow, CellBlobs, GapBetweenRow, GapEdgeRow, GapSectionRow, KeyRow,
    MarkerBlob, MarkerWriteRow, Pk, ResolvedRow, RowShape, StageRow,
};
use write::write_provisional;
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
    CacheBatch, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, PresenceBatch, dedupe,
    expand_to_input_order, section_batches, sorted_unique_coordinates,
};
use crate::state::{CollectionId, CollectionRef, SHARD_FANOUT_CONCURRENCY, StateType};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use decode::{BorrowedKeyedCellTtlRow, split_keyed_cell_ttl};
use encoding::{EncodedBlob, encode, encode_payload, select_encoding};
use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use scylla::client::session::Session;
use scylla::deserialize::row::DeserializeRow;
use scylla::response::query_result::QueryRowsResult;
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
