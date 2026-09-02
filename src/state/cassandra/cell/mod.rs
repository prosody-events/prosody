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
//! strand). [`MarkerMemo`] keeps unsettled markers in memory.
//! [`MarkerCheckSet`] stores completed checks on disk.
//! Together, they permit one durable marker read per assignment and collection.
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
//! A read resolves a prior event's section clear before it returns data.
//! A resolved write resolves an unsettled section clear before it writes data.
//! These operations use the same marker memo.
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
    decode_batch_rows, decode_cell_ttl_result, fetch_and_decode_cell, fetch_cell_rows_result,
    fetch_cells_batch, fetch_cells_batch_result, into_store_err, match_batch_rows_to_coordinates,
    page_cells,
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
use crate::state::fjall::MarkerCheckSet;
use crate::state::marker::{EventMarker, SectionClear, encode_marker_payload};
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::{
    ReadPreparation, ResolveCellError, Resolver, flatten_resolve, peek_read, resolve_event_marker,
    resolve_prior_clear_before_read, resolve_read, resolve_unsettled_clear_before_write,
};
use crate::state::store::{
    CacheBatch, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, dedupe,
    expand_to_input_order, section_batches, sorted_unique_coordinates,
};
use crate::state::{CollectionId, CollectionRef, SHARD_FANOUT_CONCURRENCY, StateType};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use decode::{BorrowedKeyedCellTtlRow, FramedKeyedCellRow, split_keyed_cell_ttl};
use encoding::{EncodedBlob, encode, encode_payload, select_encoding};
use futures::{Stream, TryStreamExt, pin_mut};
use scylla::client::session::Session;
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

/// Counts durable reads during recovery tests.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct RecoveryReadCounts {
    /// Durable event-marker point reads — memo misses only.
    pub(crate) marker_point_reads: AtomicUsize,
    /// `kind=Cell` point reads issued during recovery — bounded by
    /// #provisional.
    pub(crate) cell_point_reads: AtomicUsize,
    /// `provisional_many` IN queries — exactly one per non-empty chunk.
    /// Distinct from `cell_point_reads` so the query-count test proves the verb
    /// BATCHED (one IN query) rather than point-looped.
    pub(crate) provisional_in_queries: AtomicUsize,
}

/// Tracks marker state for one partition assignment.
///
/// `unsettled` can over-report, but it must not under-report after `checks` is
/// set. Update `unsettled` before `checks` to enforce this invariant.
/// A settle removes the marker and keeps `checks` set.
/// The disk-backed check set prevents unbounded keyed RAM use.
///
/// A check-set row is valid only for a store whose `unsettled` map saw that
/// collection's marker read. Production gives each partition assignment one
/// store and one check set from that assignment's new workspace. A store that
/// models a new assignment must start with a cold check set for the collections
/// it reads. A check-set error reads as unchecked and causes one extra durable
/// marker read.
#[derive(Debug)]
struct MarkerMemo {
    unsettled: scc::HashMap<CollectionId, EventMarker, RandomState>,
    checks: MarkerCheckSet,
}

impl MarkerMemo {
    fn new(checks: MarkerCheckSet) -> Self {
        Self {
            unsettled: scc::HashMap::default(),
            checks,
        }
    }
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
