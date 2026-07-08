//! Cassandra infrastructure for all Prosody components.
//!
//! This module provides the foundational Cassandra connectivity, configuration,
//! and migration system used by all stateful components in Prosody. It manages
//! a single session and unified migration system.

use crate::propagator::new_propagator;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentVersion;
use futures::stream::{self, StreamExt, TryStreamExt};
use opentelemetry::propagation::TextMapCompositePropagator;
use scylla::_macro_internal::{CellWriter, ColumnType, WrittenCellProof};
use scylla::client::Compression;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::metadata::NativeType;
use scylla::deserialize::value::DeserializeValue;
use scylla::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::retry::DefaultRetryPolicy;
use scylla::policies::timestamp_generator::MonotonicTimestampGenerator;
use scylla::serialize::SerializationError;
use scylla::serialize::row::SerializeRow;
use scylla::serialize::value::SerializeValue;
use scylla::statement::Consistency;
use scylla::statement::batch::{Batch, BatchStatement, BatchType};
use scylla::statement::prepared::PreparedStatement;
use smallvec::SmallVec;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

pub mod config;
pub mod errors;
pub mod macros;
pub mod migrator;

#[cfg(test)]
mod tests;

pub use config::CassandraConfiguration;
use errors::CassandraStoreError;
pub use migrator::CassandraMigrator;

/// Table name for storing segment metadata and slab IDs.
pub const TABLE_SEGMENTS: &str = "timer_segments";

/// Table name for storing timer triggers organized by time slabs.
pub const TABLE_SLABS: &str = "timer_slabs";

/// Table name for storing timer triggers indexed by key for efficient key-based
/// lookups.
pub const TABLE_KEYS: &str = "timer_keys";

/// Table name for storing v2 timer triggers with timer type support, organized
/// by time slabs.
pub const TABLE_TYPED_SLABS: &str = "timer_typed_slabs";

/// Table name for storing v2 timer triggers with timer type support, indexed by
/// key for efficient key-based lookups.
pub const TABLE_TYPED_KEYS: &str = "timer_typed_keys";

/// Table name for tracking applied database migrations.
pub const TABLE_SCHEMA_MIGRATIONS: &str = "schema_migrations";

/// Table name for distributed migration locking.
pub const TABLE_LOCKS: &str = "locks";

/// Table name for storing defer segment metadata.
pub const TABLE_DEFERRED_SEGMENTS: &str = "deferred_segments";

/// Table name for storing deferred offsets awaiting retry.
pub const TABLE_DEFERRED_OFFSETS: &str = "deferred_offsets";

/// Table name for storing deferred timers awaiting retry.
pub const TABLE_DEFERRED_TIMERS: &str = "deferred_timers";

/// Table name for message deduplication records.
pub const TABLE_DEDUPLICATION: &str = "deduplication";

/// Table name for keyed-state cell-store collection partitions.
pub const TABLE_KEYED_STATE_CELL: &str = "keyed_state_cell";

/// Table for the frozen group-global keyed-state descriptor identities.
pub const TABLE_KEYED_STATE_IDENTITY: &str = "keyed_state_identity";

/// Cassandra's maximum TTL in seconds (~20 years).
pub const MAX_CASSANDRA_TTL_SECS: i64 = 630_720_000;

/// Unified Cassandra store providing session and infrastructure for all
/// components.
///
/// This store manages the single Cassandra session, runs migrations for all
/// components, and provides common utilities like TTL calculation and
/// OpenTelemetry propagation.
#[derive(Clone, Debug)]
pub struct CassandraStore {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    session: Session,
    propagator: TextMapCompositePropagator,
    base_ttl: CompactDuration,
}

impl CassandraStore {
    /// Creates a new Cassandra store with the given configuration.
    ///
    /// Initializes the connection to Cassandra and runs schema migrations
    /// for all registered components.
    ///
    /// # Arguments
    ///
    /// * `config` - Cassandra connection and TTL configuration
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] if:
    /// - Connection to Cassandra fails
    /// - Schema migration fails
    pub async fn new(config: &CassandraConfiguration) -> Result<Self, CassandraStoreError> {
        let session = Box::pin(create_session(config)).await?;

        // Run all migrations
        let migrator = CassandraMigrator::new(&session, &config.keyspace).await?;
        migrator.migrate().await?;

        let base_ttl = config.retention.try_into()?;

        Ok(Self {
            inner: Arc::new(Inner {
                session,
                propagator: new_propagator(),
                base_ttl,
            }),
        })
    }

    /// Returns a reference to the Cassandra session.
    ///
    /// This session is shared across all components and should be used
    /// for all database operations.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.inner.session
    }

    /// Returns a reference to the OpenTelemetry propagator.
    ///
    /// Used for distributed tracing context propagation in stored spans.
    #[must_use]
    pub fn propagator(&self) -> &TextMapCompositePropagator {
        &self.inner.propagator
    }

    /// Returns the base TTL duration for this store.
    ///
    /// This is the retention period configured for the store, used as
    /// the base for TTL calculations.
    #[must_use]
    pub fn base_ttl(&self) -> CompactDuration {
        self.inner.base_ttl
    }

    /// Calculates an appropriate TTL value for Cassandra.
    ///
    /// Computes a TTL by adding the base retention period to the time
    /// remaining until the target time, with overflow protection for
    /// Cassandra's maximum TTL limit.
    ///
    /// # Arguments
    ///
    /// * `target_time` - The target time for the data
    ///
    /// # Returns
    ///
    /// * `Some(ttl_seconds)` if a valid TTL can be calculated
    /// * `None` if the TTL would exceed Cassandra's limits or calculation fails
    #[must_use]
    pub fn calculate_ttl(&self, target_time: CompactDateTime) -> Option<i32> {
        let Ok(duration) = target_time.compact_duration_from_now() else {
            // Return the base TTL if the time is in the past
            return self.base_ttl().seconds().try_into().ok();
        };

        duration
            .checked_add(self.base_ttl())
            .ok()?
            .seconds()
            .try_into()
            .ok()
            .filter(|&ttl: &i32| i64::from(ttl) < MAX_CASSANDRA_TTL_SECS)
    }

    /// Executes an unpaged mutation and discards the result.
    ///
    /// Fire-and-forget wrapper for mutations that need only error
    /// propagation; used by the Cassandra timer store for single-row writes.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] when the driver fails to execute.
    pub async fn execute_unpaged_discard<P>(
        &self,
        query: &PreparedStatement,
        params: P,
    ) -> Result<(), CassandraStoreError>
    where
        P: SerializeRow,
    {
        self.session()
            .execute_unpaged(query, params)
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }

    /// Executes `query_with_ttl` when `ttl` is `Some`, otherwise
    /// `query_no_ttl`, building each statement's params on demand.
    ///
    /// `ttl` is the already-resolved TTL in seconds; `None` means indefinite
    /// retention and routes to the `*_no_ttl` query variant. The two param
    /// builders let each query carry a different parameter shape — the
    /// `with_ttl` tuple leads with the TTL, the `no_ttl` tuple omits it.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] when the driver fails to execute.
    pub async fn execute_with_optional_ttl<P1, P2, F1, F2>(
        &self,
        ttl: Option<i32>,
        query_with_ttl: &PreparedStatement,
        query_no_ttl: &PreparedStatement,
        params_with_ttl: F1,
        params_no_ttl: F2,
    ) -> Result<(), CassandraStoreError>
    where
        P1: SerializeRow,
        P2: SerializeRow,
        F1: FnOnce(i32) -> P1,
        F2: FnOnce() -> P2,
    {
        match ttl {
            Some(ttl) => {
                self.execute_unpaged_discard(query_with_ttl, params_with_ttl(ttl))
                    .await
            }
            None => {
                self.execute_unpaged_discard(query_no_ttl, params_no_ttl())
                    .await
            }
        }
    }

    /// Executes `units` as the **fewest** same-partition `UNLOGGED BATCH`es
    /// that fit the backend budget. Each unit is an atomic group of rows that
    /// must never be split across batches; a unit's rows may target
    /// **different** prepared statements ([`BatchRow::statement`]).
    ///
    /// Each unit carries its own size estimate ([`BatchUnit`]);
    /// [`chunk_boundaries`] greedily packs contiguous **whole** units into
    /// chunks within `max_bytes` and `max_count` (a unit heavier than
    /// `max_bytes` takes its own chunk), and up to `concurrency` chunks are in
    /// flight at once — the caller supplies its own fan-out bound alongside its
    /// batch budget. Callers pass units sharing **one** partition key, so each
    /// batch is a single atomic replica mutation. An empty `units` is a no-op.
    ///
    /// The batch is marked idempotent — these are last-write-wins full-column
    /// `UPDATE`s / `INSERT`s / `DELETE`s, and batch idempotency does **not**
    /// inherit from the member statements — so the `DefaultRetryPolicy` may
    /// retry it on timeout.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] when the driver fails to execute a
    /// batch.
    pub(crate) async fn execute_unlogged_batches<R>(
        &self,
        units: &[BatchUnit<R>],
        max_bytes: u64,
        max_count: usize,
        concurrency: usize,
    ) -> Result<(), CassandraStoreError>
    where
        R: BatchRow + Sync,
    {
        // Collect the chunk ranges eagerly: `BatchUnit::weight` is a *function
        // item*, not a closure, so the boundary iterator is higher-ranked over
        // `R`'s borrow, but draining it into a `SmallVec` up front (heap-free in
        // the common single-chunk case) lets each chunk's future own its `range`
        // without holding the lazy iterator across an await — sidestepping the
        // "implementation of FnOnce is not general enough" the original `for`
        // loop was shaped to avoid. Ranges are over **units**; whole cells never
        // straddle a chunk. The chunks partition this collection's cells into
        // disjoint sub-slices, so concurrent batches never touch the same cell;
        // `MonotonicTimestampGenerator` keeps last-write-wins correct regardless
        // of submission order. `session.batch().await` is a tokio I/O leaf, so
        // the coop budget decrements naturally — no `cooperative` wrap.
        let ranges: SmallVec<[Range<usize>; 1]> =
            chunk_boundaries(units.iter().map(BatchUnit::weight), max_bytes, max_count).collect();
        stream::iter(ranges)
            .map(|range| async move {
                // One flatten pass over the chunk's units builds the statement
                // list and the value list in lockstep via `unzip`, so
                // `batch.statements[i]` binds `values[i]` — each row's own
                // statement against its own columns. A misaligned flatten would
                // bind against the wrong statement's columns *silently* (scylla
                // falls back to an empty context on a count/order mismatch), so
                // the single-pass lockstep is load-bearing.
                let (statements, values): (Vec<BatchStatement>, Vec<&R>) = units[range]
                    .iter()
                    .flat_map(|unit| unit.rows.iter())
                    .map(|row| (BatchStatement::from(row.statement().clone()), row))
                    .unzip();
                let mut batch = Batch::new_with_statements(BatchType::Unlogged, statements);
                batch.set_is_idempotent(true);
                self.session()
                    .batch(&batch, &values)
                    .await
                    .map(drop)
                    .map_err(CassandraStoreError::from)
            })
            .buffer_unordered(concurrency)
            .try_collect::<()>()
            .await
    }
}

/// A single prepared-statement + bound-values pair for one row of an
/// `UNLOGGED BATCH`. Decouples [`CassandraStore::execute_unlogged_batches`]
/// from the concrete row type: the executor packs and submits, the row names
/// its own statement — so a batch can carry rows targeting **different**
/// statements without the executor knowing any of them.
pub(crate) trait BatchRow: SerializeRow {
    /// The prepared statement this row's columns bind against.
    fn statement(&self) -> &PreparedStatement;
}

/// An indivisible group of [`BatchRow`]s that must land in the **same** batch —
/// a caller-defined atom (e.g. a record's mutation and its marker row).
/// [`chunk_boundaries`] packs whole units and never splits one, so a unit's
/// rows are always one same-partition atomic mutation — "split a unit across
/// batches" is unrepresentable. Two rows fit inline in the `SmallVec`.
pub(crate) struct BatchUnit<R> {
    weight: u64,
    rows: SmallVec<[R; 2]>,
}

impl<R> BatchUnit<R> {
    /// Groups `rows` as one atomic unit weighing `weight` (the unit's payload
    /// bytes plus a per-row overhead estimate, supplied by the caller).
    pub(crate) fn new(weight: u64, rows: SmallVec<[R; 2]>) -> Self {
        Self { weight, rows }
    }

    /// The packing weight. A function item (not a closure), so
    /// `units.iter().map(BatchUnit::weight)` stays higher-ranked over `R`'s
    /// borrow.
    fn weight(&self) -> u64 {
        self.weight
    }
}

/// Greedy next-fit packing of weighed rows into the **fewest contiguous**
/// chunks whose rows stay within `max_bytes` and `max_count`. A row heavier
/// than `max_bytes` takes its own chunk (the unavoidable break).
/// Order-preserving and allocation-free (lazy [`std::iter::from_fn`]); pure
/// over the weights, so it tests with plain numbers and no cluster.
///
/// Because cells keep their order (the partition is partitioned contiguously),
/// greedily extending each chunk to a limit yields the provably minimal chunk
/// count: no contiguous partition into fewer parts can exist.
fn chunk_boundaries(
    weights: impl Iterator<Item = u64>,
    max_bytes: u64,
    max_count: usize,
) -> impl Iterator<Item = Range<usize>> {
    let mut weights = weights.peekable();
    let mut start = 0_usize;
    iter::from_fn(move || {
        // The first row always joins the chunk, even if it alone exceeds
        // `max_bytes` — the unavoidable oversized-row break.
        let first = weights.next()?;
        let mut end = start + 1;
        let mut acc = first;
        while end - start < max_count {
            let Some(&next) = weights.peek() else { break };
            let combined = acc.saturating_add(next);
            if combined > max_bytes {
                break;
            }
            acc = combined;
            weights.next();
            end += 1;
        }
        let range = start..end;
        start = end;
        Some(range)
    })
}

/// Creates and configures a Cassandra session with the given configuration.
async fn create_session(config: &CassandraConfiguration) -> Result<Session, CassandraStoreError> {
    let mut lb_policy = DefaultPolicy::builder()
        .token_aware(true)
        .permit_dc_failover(true);

    if let Some(dc) = &config.datacenter {
        lb_policy = match &config.rack {
            None => lb_policy.prefer_datacenter(dc.clone()),
            Some(rack) => lb_policy.prefer_datacenter_and_rack(dc.clone(), rack.clone()),
        }
    }

    let profile = ExecutionProfile::builder()
        .consistency(Consistency::LocalQuorum)
        .load_balancing_policy(lb_policy.build())
        .retry_policy(Arc::new(DefaultRetryPolicy::new()))
        .build();

    let mut session = SessionBuilder::new()
        .known_nodes(&config.nodes)
        .compression(Some(Compression::Lz4))
        .default_execution_profile_handle(profile.into_handle())
        // Client-side monotonic timestamps: one handler per key and one
        // PartitionManager per partition route every write to a partition
        // through this single session, so these timestamps increase in issue
        // order and make last-write-wins lost-write-free. Never override a
        // write timestamp by hand (`USING TIMESTAMP`) — it bypasses this.
        .timestamp_generator(Arc::new(MonotonicTimestampGenerator::new()));

    if let Some(user) = &config.user {
        session = session.user(user.clone(), config.password.clone().unwrap_or_default());
    }

    Ok(Box::pin(session.build()).await?)
}

// Scylla trait implementations for Prosody types

impl SerializeValue for CompactDuration {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i32::from(*self).serialize(typ, writer)
    }
}

impl SerializeValue for CompactDateTime {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i32::from(*self).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CompactDuration {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(CassandraStoreError::IntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(CompactDuration::from(i32::deserialize(typ, v)?))
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CompactDateTime {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(CassandraStoreError::IntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(CompactDateTime::from(i32::deserialize(typ, v)?))
    }
}

impl SerializeValue for TimerType {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i8::from(*self).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for TimerType {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::TinyInt) => Ok(()),
            _ => Err(TypeCheckError::new(CassandraStoreError::TinyIntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let value = i8::deserialize(typ, v)?;
        TimerType::try_from(value).map_err(DeserializationError::new)
    }
}

impl SerializeValue for SegmentVersion {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i8::from(*self).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for SegmentVersion {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::TinyInt) => Ok(()),
            _ => Err(TypeCheckError::new(CassandraStoreError::TinyIntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let value = i8::deserialize(typ, v)?;
        SegmentVersion::try_from(value).map_err(DeserializationError::new)
    }
}
