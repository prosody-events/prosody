//! Keyed application state protocol types.
//!
//! This module defines the shared collection identity and cell shapes used by
//! the keyed-state cell store. The cell layer is **uniform and
//! untyped** — it addresses cells by [`CellKey`] and names no collection
//! family; typed collection handles (Value, Map, Deque) are built
//! atop it in [`descriptor`].
//!
//! The shapes themselves live in leaf-to-root submodules and are
//! re-exported flat below, so consumers keep importing `crate::state::X`:
//!
//! * [`identity`] — collection identity ([`CollectionId`], [`CollectionRef`],
//!   [`StateKey`], [`CollectionKindId`], …).
//! * [`cell_key`] — intra-collection cell addressing ([`CellKey`], [`Section`],
//!   [`Coordinate`], [`Scan`], [`ScanEdge`]).
//! * [`event_ref`] — event identity and verdicts ([`EventRef`],
//!   [`CommitDecision`], [`StoreOutcome`], …).
//! * [`cell`] — the provisional-cell durability model ([`Cell`], [`Committed`],
//!   [`ProvisionalCell`], [`ProvisionalWrite`]).
//! * `store` — the uniform `CellStore` backend trait (crate-internal).
//! * [`collection`] — the collection-operation core: one bound [`Collection`],
//!   one scoped operation per public invocation, and the two sealed engines
//!   (owner session and published reader) behind it.
//! * [`descriptor`] — typed collection handles bound over the raw byte cells
//!   the stores persist.
//! * [`registry`] — per-collection operational settings ([`CommitMode`],
//!   [`registry::CollectionDef`], …).
//!
//! Three things here are named *identity*; keep them distinct. [`identity`]
//! **addresses** a collection ([`CollectionId`], [`CollectionRef`],
//! [`StateKey`]) — pure in-process routing, local and cheap. A collection's
//! [`StructuralIdentity`](descriptor::StructuralIdentity) is the `(kind,
//! format_id, …)` **shape** a descriptor asserts. The [`descriptor_identity`]
//! durable table **freezes** that shape group-globally on first use, validated
//! against once at partition acquisition so a later redeploy cannot silently
//! change it.
//!
//! The cross-cutting backend abstraction — the `StateBackend` bundle trait, its
//! one concrete `PartitionBackend`, and the `StateBackendFactory` that mints it
//! per partition — belongs to no leaf and lives in the crate-internal `backend`
//! module.
//!
//! # Operational notes
//!
//! Accepted-inherent costs and deployment assumptions, recorded so an operator
//! meets no surprises.
//!
//! **Partition width is the application's data model.** One collection is one
//! Cassandra partition — required for transactional semantics (one batch, one
//! timestamp, one recovery scope), so the cell layer cannot bound it without a
//! per-mutation count, which would force a read-before-write and break the
//! blind `remove` of [`descriptor::map`]. A [`descriptor::deque`] is naturally
//! bounded by its window; a [`descriptor::map`] should be kept comfortably
//! under Cassandra's wide-partition pain — on the order of 100 MB, low-millions
//! of cells. A hard cardinality bound, if ever needed, belongs in
//! collection-owned `Meta` bookkeeping (as the deque window already is), never
//! as a cell-layer feature.
//!
//! **TTL mass-expiry is transient and self-healing.** On a TTL'd
//! [`descriptor::map`] the keyset cell rides the same TTL-refresh rule as every
//! entry (each `set` rewrites it), so it outlives the newest entry. When the
//! map's cells expire the keyset expires with them, and the next `stream` reads
//! it absent and yields nothing with **zero scans** — no tombstone wave at all
//! on the fast path. A degraded (`Overflowed`) map instead falls back to a
//! full-section ([`Unbounded`](ScanEdge::Unbounded)-edged) scan that *can* meet
//! a one-time tombstone wave, which self-heals as those rows compact — the
//! accepted degraded cost. A [`descriptor::deque`] scan keeps concrete
//! [`ScanEdge`] bounds pinned to its live window, so its range collapses as
//! those bounds expire.
//!
//! **Cross-assignment clock skew is a standard Cassandra assumption, not a new
//! hazard.** Last-write-wins ordering *across* assignments — a new assignee's
//! write or delete versus a prior assignee's write of the same coordinate —
//! rests on wall clocks, because the monotonic timestamp generator is
//! per-session. Row-absence and section-clear deletes make deletes more common
//! but do not change this pre-existing class. Assume standard Cassandra
//! operations: NTP with bounded skew well below the failover interval.
//!
//! [`Cell`]: cell::Cell
//! [`Committed`]: cell::Committed
//! [`ProvisionalCell`]: cell::ProvisionalCell
//! [`ProvisionalWrite`]: cell::ProvisionalWrite

pub mod access;
pub(crate) mod backend;
pub(crate) mod cached;
pub mod cassandra;
pub mod cell;
pub mod cell_key;
pub mod collection;
pub(crate) mod commit;
pub mod config;
pub mod descriptor;
pub mod descriptor_identity;
pub(crate) mod dirty;
pub mod event_ref;
pub(crate) mod first_write;
pub(crate) mod fjall;
pub mod identity;
pub mod manager;
pub(crate) mod marker;
pub mod memory;
pub mod oracle;
pub mod order_codec;
pub(crate) mod overlay;
pub(crate) mod production;
pub mod publication;
pub mod registry;
pub mod resolve;
pub mod session;
pub(crate) mod store;

#[cfg(test)]
pub(crate) mod tests;

pub use access::StateAccessError;
pub use cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
pub use collection::{Collection, StateSession, WritableStateSession};
pub use event_ref::{CommitDecision, EventRef, StoreOutcome, TimerEventRef};
pub use identity::{
    CollectionId, CollectionKindId, CollectionRef, StateKey, StateName, StateNameError, StateType,
};
pub use order_codec::{
    I64KeyCodec, KeyCodecError, OrderedKeyCodec, U64KeyCodec, UnitKey, Utf8KeyCodec,
    order_preserving_i64, order_preserving_i64_decode,
};
pub use registry::{CommitMode, ReadCachePolicy, StateVisibility};

// The backend cluster is crate-internal (module-capped in [`backend`]); these
// re-exports keep every in-crate `crate::state::X` import resolving without
// exposing any of it from the crate root (`pub(crate) use` is not a `pub use`).
#[cfg(test)]
pub(crate) use backend::SharedStateBackend;
pub(crate) use backend::{PartitionBackend, StateBackend, StateBackendFactory};

/// Maximum concurrent per-collection durable operations in the keyed-state
/// lifecycle (finalize stage, commit promote, rollback, recovery sweep).
/// Each collection is its own Cassandra partition, so the fan-out is safe.
pub(crate) const STATE_FANOUT_CONCURRENCY: usize = 16;

/// Maximum concurrent in-flight requests within a *single* collection
/// (one Cassandra partition → one Scylla shard): the batch-chunk submission
/// of one durable write, the recovery sweep's per-cell resolution, a
/// stage's committed-base reads, and the ordered resolution window of a typed
/// cell scan (each scanned cell is decoded and its resolver/loader fan-out run
/// up to this many items ahead of the consumer). Same shard, so this bounds
/// round-trip / oracle-consult *overlap* (latency), not throughput; kept modest
/// because it nests inside the per-collection `STATE_FANOUT_CONCURRENCY`
/// fan-out, so the product is the per-shard in-flight depth.
///
/// Ruling: retained at eight pending a benchmark sweep over candidate values
/// `1, 2, 4, 8, 16, 32, 64`, exercised under cold multi-chunk stage reads,
/// cold and warm-index recovery spanning multiple chunks, oracle-resolving
/// provisional write-back, over-budget provisional/resolved/promote/abort
/// writes, and simultaneous events across many keys (to expose global shard
/// pressure, not one isolated shard). Batching moved what this bounds — it is
/// now concurrent batch chunks, recovery resolution, and over-budget write
/// batches against one shard, never point-query multiplication — so a value
/// picked before batching would have tuned the wrong thing. Not made
/// configurable speculatively: if the optimum proves strongly
/// deployment-dependent, a separately validated config field is the follow-up.
pub(crate) const SHARD_FANOUT_CONCURRENCY: usize = 8;

/// Maximum concurrent typed resolves in flight within one `CellView::get_many`
/// call — the loader (Kafka message) fan-out for a batch read. A resolve reads
/// the collection's source (a Kafka message for a loader-backed collection),
/// which does not contend on the
/// collection's Scylla shard, so it is not bounded by
/// [`SHARD_FANOUT_CONCURRENCY`] (that bounds same-shard round-trip overlap).
/// A batch's resolves fan out across the WHOLE call under this window, so the
/// resolves overlap rather than serialize per store sub-batch.
///
/// Currently matches [`store::CELL_BATCH`] so a full store batch resolves in
/// one wave; the two bounds remain independently tunable.
pub(crate) const RESOLVE_FANOUT: usize = 128;

/// Inline capacity for keyed-state buffers whose cardinality is commonly small.
/// Larger buffers spill to the heap instead of inflating nested async futures.
pub(crate) const CELLS_INLINE: usize = 8;

const _: () = assert!(RESOLVE_FANOUT > 0, "RESOLVE_FANOUT must be positive");
const _: () = assert!(
    CELLS_INLINE > 0 && CELLS_INLINE <= 8,
    "keyed-state inline buffers must stay small"
);
