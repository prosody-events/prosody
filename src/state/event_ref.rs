//! Event identity and store/oracle verdicts.
//!
//! [`EventRef`] is the durable reference to the upstream event that owns a
//! sealed WAL; [`EventScopeId`] distinguishes concurrent handler
//! invocations. [`CommitDecision`] and [`StoreOutcome`] are the two
//! distinct verdicts threaded through recovery: the oracle decides, the
//! store acts and reports.

use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use uuid::Uuid;

/// Per-event scope identity used by commit recovery.
///
/// The keyed-state middleware mints a fresh scope per handler invocation
/// (via [`Self::fresh`]) so dirty workspaces can be keyed by scope without
/// colliding across events. The Fjall dirty workspace will key on
/// [`EventScopeId`] in a later slice; today this identity is consumed by
/// the in-memory middleware workspace and is sufficient to distinguish
/// concurrent events at the type level.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct EventScopeId(u128);

impl EventScopeId {
    /// Creates an event scope identifier.
    #[must_use]
    pub fn new(id: u128) -> Self {
        Self(id)
    }

    /// Returns the raw identifier value.
    #[must_use]
    pub fn get(self) -> u128 {
        self.0
    }

    /// Mints a fresh random scope identifier. Used by the keyed-state
    /// middleware to scope per-event dirty workspaces.
    #[must_use]
    pub fn fresh() -> Self {
        Self(Uuid::new_v4().as_u128())
    }
}

/// Durable reference to the upstream event that owns a sealed WAL.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum EventRef {
    /// Kafka message event identified by its deduplication marker.
    Message {
        /// Deduplication row identifier written at the event commit point.
        dedup_id: Uuid,
    },

    /// Timer event identified by its durable timer row coordinates.
    Timer(TimerEventRef),
}

/// Durable timer identity stored in a sealed WAL.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TimerEventRef {
    /// Timer namespace.
    pub timer_type: TimerType,

    /// Scheduled fire time.
    pub time: CompactDateTime,

    /// Timer row tag observed when the WAL was sealed.
    pub tag: i32,
}

impl TimerEventRef {
    /// Creates a durable timer event reference.
    #[must_use]
    pub fn new(timer_type: TimerType, time: CompactDateTime, tag: i32) -> Self {
        Self {
            timer_type,
            time,
            tag,
        }
    }
}

/// Oracle verdict on a sealed WAL for one event.
///
/// Returned by the commit oracle when it resolves a
/// [`SealedWal`](super::SealedWal)'s [`EventRef`] against the upstream
/// commit source (deduplication store for messages, timer-row tag for
/// timers per `docs/keyed-state/design-summary.md` §"Recovery"). Distinct
/// from [`StoreOutcome`], which is the durable store's "did this call
/// mutate state" signal: the oracle decides, the store acts on the
/// decision.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitDecision {
    /// The sealed operations were committed.
    Committed,

    /// No sealed operations were committed.
    NotCommitted,
}

/// Did this store call mutate authoritative state.
///
/// Returned by store-side methods that may or may not have work to do:
/// [`apply_sealed`](super::value::DurableWalStore::apply_sealed) (WAL
/// present → folded),
/// [`rollback_sealed`](super::value::DurableWalStore::rollback_sealed)
/// (WAL present → cleared),
/// [`direct_apply`](super::value::DirectApplyStore::direct_apply) (ops
/// non-empty → folded), and the
/// [`TransactionValueStore`](super::value::TransactionValueStore) wrappers
/// around them.
///
/// Distinct from [`CommitDecision`]: the oracle decides whether a sealed
/// WAL should be committed, the store reports whether it actually
/// changed durable state when called. A second call with the same
/// arguments observes [`StoreOutcome::NoOp`].
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StoreOutcome {
    /// The call mutated authoritative state.
    Applied,

    /// No durable state changed (idempotent no-op).
    NoOp,
}
