//! What a collection *is*: its operational settings and the policy vocabulary
//! those settings are drawn from.
//!
//! The registry that stores these definitions lives in the parent module.

use crate::timers::duration::CompactDuration;
use std::num::NonZeroUsize;
use std::time::Duration;

/// Default Map keyset bound — the number of live distinct keys a map tracks
/// before it overflows to the full-section scan. Applied to any collection not
/// overriding it and to names absent from the registry.
pub(crate) const DEFAULT_KEYSET_LIMIT: usize = 128;

/// Persistence mode for a collection's state changes, chosen per collection at
/// registration
/// ([`StateDescriptor::read_uncommitted`](crate::state::descriptor::StateDescriptor::read_uncommitted)).
/// The default is [`Self::ReadCommitted`]. Both modes are named by the read
/// guarantee they give, not by the mechanism.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitMode {
    /// Atomic with the event, and crash-recoverable. On handler success the
    /// buffered write stages as a provisional cell beside the prior committed
    /// value, before the event's commit marker, then promotes to committed once
    /// that marker is durable. Crash recovery resolves the cell through the
    /// commit oracle. A handler that fails or redelivers never exposes its
    /// writes: readers observe committed values only.
    ReadCommitted,

    /// Cheaper, at-least-once. The buffered write applies straight to the
    /// committed value on handler success, and stays visible even if the event
    /// later fails. A crash between the apply and the event's commit re-runs
    /// the handler against already-applied state, so writes must be idempotent
    /// (last-writer-wins `set`s usually are). Choose it where re-application is
    /// harmless and the extra promote per event matters.
    ReadUncommitted,
}

/// Whether a collection's committed state is discoverable by cross-group
/// readers. Runtime-only policy: never part of the frozen
/// [`StructuralIdentity`](crate::state::descriptor::StructuralIdentity) and
/// never persisted. A collection can be published and un-published across
/// redeploys with no migration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StateVisibility {
    /// Not discoverable outside the owning consumer group (the default).
    #[default]
    Private,

    /// Discoverable by cross-group readers through the publication table.
    ///
    /// To retire a published collection, set `.published(false)` but keep both
    /// its registration and the consumer's `subsystem` for one stop-then-start
    /// deploy. Startup reconciliation only sweeps the routing rows of names
    /// that are still registered, now `Private`, and have a configured
    /// subsystem. Deleting the registration or dropping the `subsystem` config
    /// strands the `(subsystem, name)` routing row. With `TTL = None` that
    /// stranded row and its cells stay discoverable by cross-group readers
    /// indefinitely.
    Published,
}

/// How a standalone reader caches this collection's committed cells.
///
/// The client supplies the inherited TTL. A collection can accept that
/// default, disable caching, or replace it with its own TTL.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadCachePolicy {
    /// Use the reader client's default policy.
    #[default]
    Inherit,

    /// Read the durable store on every operation.
    Disabled,

    /// Cache reads for this duration.
    Ttl(Duration),
}

impl From<Duration> for ReadCachePolicy {
    fn from(ttl: Duration) -> Self {
        Self::Ttl(ttl)
    }
}

impl ReadCachePolicy {
    /// Resolves this collection policy against the reader client's default.
    pub(crate) fn resolve(self, inherited: Option<Duration>) -> Option<Duration> {
        match self {
            Self::Inherit => inherited,
            Self::Disabled => None,
            Self::Ttl(ttl) => Some(ttl),
        }
    }
}

/// Operational per-collection settings, pinned at registration time.
///
/// Deliberately separate from the collection's frozen
/// [`StructuralIdentity`](crate::state::descriptor::StructuralIdentity):
/// identity comes only from the descriptor, so a definition can never assert an
/// identity its descriptor does not have. Changing any setting here needs no
/// migration.
///
/// `recovery_within` is a reader-convergence bound, not a durability knob. It
/// only ever pulls the single per-key recovery backstop *sooner*: the effective
/// fire is `min(recovery_delay, tightest touched recovery_within)`. A value
/// above the always-on `recovery_delay` floor is clamped by it, and a value on
/// a [`CommitMode::ReadUncommitted`] collection is inert, since those writes
/// stage no provisional cell to converge. Being tightening-only, it needs no
/// ceiling: the floor already sits strictly below every collection's TTL.
#[derive(Clone, Copy, Debug)]
pub struct CollectionDef {
    /// Per-collection TTL. `None` is explicit indefinite retention. A value
    /// over Cassandra's `USING TTL` ceiling is rejected at registration, never
    /// silently collapsed to "no TTL".
    pub ttl: Option<CompactDuration>,

    /// Per-collection commit mode.
    pub commit_mode: CommitMode,

    /// Per-collection recovery-convergence bound (see the type doc).
    /// `None` uses the always-on `recovery_delay` floor.
    pub recovery_within: Option<CompactDuration>,

    /// Map keyset bound: the number of **live** distinct keys a map tracks in
    /// its keyset cell before overflowing to the full-section scan (`remove`
    /// subtracts, so this is current membership, not a running total).
    /// Meaningful for Map collections only; ignored by Value and Deque. `0`
    /// disables tracking, so every map overflows on its first `set`.
    pub keyset_limit: usize,

    /// Deque push cap: at most this many window slots, evicted opposite-end
    /// first (a `push_back` trims the front, a `push_front` the back), enforced
    /// **lazily on push only**. Reads, `len`, iteration, and `pop` never
    /// enforce it, and a persisted window need not respect the current cap —
    /// it may have changed across a redeploy. Meaningful for Deque collections
    /// only; ignored by Value and Map. `None` is unbounded. `NonZeroUsize`
    /// keeps `capacity = 0` unrepresentable.
    pub capacity: Option<NonZeroUsize>,

    /// Cross-group read visibility; see [`StateVisibility`]. A `Published`
    /// collection requires a configured subsystem, enforced at consumer build.
    pub visibility: StateVisibility,

    /// The read-only client's cache policy, consumed by the reader from its own
    /// descriptor. Inert on the owning consumer, and unrelated to the durable
    /// write [`ttl`](Self::ttl).
    pub read_cache: ReadCachePolicy,
}

impl CollectionDef {
    /// Creates a collection definition with the supplied TTL; commit
    /// mode defaults to [`CommitMode::ReadCommitted`] and the
    /// recovery-convergence bound to `None` (the `recovery_delay` floor).
    #[must_use]
    pub fn new(ttl: Option<CompactDuration>) -> Self {
        Self {
            ttl,
            commit_mode: CommitMode::ReadCommitted,
            recovery_within: None,
            keyset_limit: DEFAULT_KEYSET_LIMIT,
            capacity: None,
            visibility: StateVisibility::default(),
            read_cache: ReadCachePolicy::default(),
        }
    }
}
