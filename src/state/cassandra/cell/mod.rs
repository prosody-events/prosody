//! Stores keyed-state cells and collection commit evidence in Cassandra.
//! Admission resolves residue before owner dispatch. Readers project values
//! through collection evidence.

mod batch;
mod cell_store;
mod decode;
mod encoding;
mod helpers;
mod projection;
mod queries;
mod read;
mod resources;
mod rows;
mod serialization;
mod store;
mod write;

use batch::{extend_gap_units, gap_count};
use helpers::{blob_weight, decode_provisional_batch, encode_cell_blobs, ttl_seconds_to_duration};
pub use queries::CellQueries;
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
use crate::cassandra::{BatchRow, BatchUnit, bind_ttl};
use crate::cassandra_queries;
use crate::state::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use crate::state::event_ref::EventRef;
use crate::state::marker::{EventMarker, SectionClear, encode_marker_payload};
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::{EvidenceLookup, ResolveCellError};
use crate::state::store::{
    CacheBatch, CellBuffer, CellStore, CoordinateBatch, distinct, repeated,
    sorted_unique_coordinates,
};
use crate::state::{CollectionId, CollectionRef, SHARD_FANOUT_CONCURRENCY, StateType};
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use encoding::{EncodedBlob, encode, encode_payload, select_encoding};
use futures::{Stream, TryStreamExt, pin_mut};
use scylla::serialize::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::writers::RowWriter;
use scylla::statement::prepared::PreparedStatement;
use smallvec::{SmallVec, smallvec};
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
/// [`Cell`](Self::Cell) rows carry values. [`Marker`](Self::Marker) selects
/// the two addresses owned by [`MarkerRow`](crate::state::marker::MarkerRow).
/// Statements bind this discriminator; reads never decode it.
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

    /// The collection's Staged and Committed slice.
    Marker = 1,
}

impl From<CellKind> for i8 {
    fn from(kind: CellKind) -> Self {
        kind as i8
    }
}

/// Classifies cell reads through their underlying store error.
pub type CellStoreError = ResolveCellError<CassandraCellStoreError>;

/// Shares a Cassandra session and prepared cell statements across partitions.
#[derive(Clone)]
pub struct CassandraCellResources {
    pub(crate) session: CassandraSession,
    pub(crate) queries: Arc<CellQueries>,
}

/// Counts cell, marker, and batch reads inside one store method, which no
/// wrapper can observe.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct CellReadCounts {
    /// Point reads in `kind=Cell`, bounded by the provisional cell count.
    pub(crate) cell_point_reads: AtomicUsize,
    /// Marker reads from `marker_state`, absent from raw provisional reads.
    pub(crate) marker_point_reads: AtomicUsize,
    /// IN queries from `provisional_many`, exactly one per non-empty chunk.
    pub(crate) provisional_in_queries: AtomicUsize,
}

/// Cassandra-backed uniform cell store.
#[derive(Clone, Debug)]
pub struct CassandraStore {
    session: CassandraSession,
    queries: Arc<CellQueries>,
    registry: Arc<CollectionDefRegistry>,
    #[cfg(test)]
    counters: Arc<CellReadCounts>,
}

#[cfg(test)]
pub(in crate::state) use batch::{
    settle_batches as crash_settle_batches, stage_batches as crash_stage_batches,
    stage_chunk as crash_stage_chunk,
};
