//! Per-collection definitions and the collection registry.

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::descriptor::DescriptorIdentity;
use crate::state::descriptor::StructuralIdentity;
use crate::state::{StateName, StateNameError, StateType};
use crate::timers::duration::CompactDuration;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;
use thiserror::Error;

/// Default Map keyset bound — the number of live distinct keys a map tracks
/// before it overflows to the full-section scan. Applied to any collection not
/// overriding it and to names absent from the registry.
pub(crate) const DEFAULT_KEYSET_LIMIT: usize = 128;

/// Registration ceiling on the Map keyset bound: a larger limit is rejected at
/// build ([`RegisterStateError::KeysetLimit`]), capping the point-get fan-out
/// (and decode allocation) a single `stream` can issue — the byte ceiling
/// separately bounds the frame's wire size.
const MAX_KEYSET_LIMIT: usize = 4096;

/// Persistence mode for a collection's state changes, chosen per collection
/// at registration
/// ([`StateDescriptor::read_uncommitted`](crate::state::descriptor::StateDescriptor::read_uncommitted);
/// the default is [`Self::ReadCommitted`]).
///
/// The modes are named by the **read guarantee** they give, not the mechanism:
///
/// * **`ReadCommitted` — atomic with the event, crash-recoverable.** On handler
///   success the buffered write stages as a provisional cell (new value beside
///   the prior committed value) *before* the event's commit marker, then
///   promotes to committed after the marker is durable; crash recovery resolves
///   the cell through the commit oracle. A handler that fails or redelivers
///   never exposes its writes — internal and external readers only ever observe
///   committed values.
/// * **`ReadUncommitted` — cheaper, at-least-once.** The buffered write applies
///   straight to the committed value when the handler succeeds, visible even if
///   the event later fails. A crash between the apply and the event's commit
///   re-runs the handler against already-applied state, so writes must be
///   idempotent (last-writer-wins `set`s usually are). Choose it for state
///   where re-application is harmless and the extra promote per event matters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitMode {
    /// Stage the write provisionally before the event commit marker and
    /// promote it after — readers observe committed values only.
    ReadCommitted,

    /// Apply the write to committed state immediately — cheaper, with
    /// at-least-once, read-uncommitted semantics.
    ReadUncommitted,
}

/// Whether a collection's committed state is discoverable by cross-group
/// readers. Runtime-only policy: never part of the frozen
/// [`StructuralIdentity`] and never persisted. A collection can be published
/// and un-published across redeploys with no migration.
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

/// Operational per-collection settings.
///
/// Carries the collection's operational settings: TTL, [`CommitMode`],
/// recovery-convergence bound, and the runtime read policy (visibility and
/// cache TTL). Each is detailed on its field below. `ttl` is `None` for
/// "do not bind a TTL" (explicit indefinite retention); a `Some(ttl)` over
/// Cassandra's `USING TTL` ceiling is rejected at
/// `CollectionDefRegistry::register` time, never silently collapsed.
/// `commit_mode` decides whether handler-side dirty ops stage a provisional
/// cell that promotes to committed after the event commit
/// ([`CommitMode::ReadCommitted`]) or apply straight to the committed value
/// on handler success ([`CommitMode::ReadUncommitted`]).
///
/// `recovery_within` is a **reader-convergence bound**, not a durability knob:
/// `Some(d)` guarantees this collection's provisional cells are swept back to
/// committed within `d` of the commit, tightening how long an external
/// (non-owner) reader can observe the prior
/// committed value. `None` (the default) leaves the collection on the always-on
/// `recovery_delay` durability floor. The effective per-key fire is
/// `min(recovery_delay, tightest touched recovery_within)`, so this field only
/// ever pulls the single per-key backstop *sooner* — a value above the floor is
/// clamped by it, and a value on a `ReadUncommitted` collection is inert
/// (those writes never stage a provisional cell, so they have no convergence
/// window). Being tightening-only, it needs no ceiling or ordering validation:
/// the floor already sits strictly below every collection's TTL
/// ([`RegisterStateError::TtlBelowRecoveryDelay`]).
///
/// Operational settings are deliberately separate from the collection's
/// frozen [`StructuralIdentity`]: the identity comes only from the
/// descriptor at `CollectionDefRegistry::register` time, so a definition
/// cannot assert an identity its descriptor does not have.
///
/// A collection has one `CommitMode` while a handler is running — pinned
/// here at registration time, not at event scope creation time.
#[derive(Clone, Copy, Debug)]
pub struct CollectionDef {
    /// Per-collection TTL.
    pub ttl: Option<CompactDuration>,

    /// Per-collection commit mode.
    pub commit_mode: CommitMode,

    /// Per-collection recovery-convergence bound (see the type doc).
    /// `None` uses the always-on `recovery_delay` floor.
    pub recovery_within: Option<CompactDuration>,

    /// Map keyset bound: the number of **live** distinct keys a map tracks in
    /// its keyset cell before overflowing to the full-section scan (`remove`
    /// subtracts, so this is the current membership, not a running total).
    /// Meaningful for Map collections only; ignored
    /// by Value and Deque. `0` disables tracking (every map overflows on its
    /// first `set`). Operational, never part of the frozen
    /// [`StructuralIdentity`] — changing it needs no migration. Validated
    /// `<= 4096` at registration ([`RegisterStateError::KeysetLimit`]).
    pub keyset_limit: usize,

    /// Deque push cap: at most this many window slots, evicted opposite-end
    /// first (a `push_back` trims the front, a `push_front` the back), enforced
    /// **lazily on push only** — reads, `len`, iteration, and `pop` never
    /// enforce it, and a persisted window need not respect the current cap (it
    /// may have changed across a redeploy). Meaningful for Deque collections
    /// only; ignored by Value and Map. `None` is unbounded. Operational, never
    /// part of the frozen [`StructuralIdentity`] — a bounded and an unbounded
    /// deque share the same `Deque` kind, so changing it needs no migration.
    /// `NonZeroUsize` keeps `capacity = 0` unrepresentable.
    pub capacity: Option<NonZeroUsize>,

    /// Cross-group read visibility; see [`StateVisibility`]. A `Published`
    /// collection requires a configured subsystem, enforced at consumer build
    /// ([`RegisterStateError::PublishedWithoutSubsystem`]).
    pub visibility: StateVisibility,

    /// The read-only client's cache policy.
    ///
    /// The default inherits the reader client's TTL. The reader consumes this
    /// from its own descriptor.
    /// Runtime-only, like [`StateVisibility`]. It is inert on the owning
    /// consumer and unrelated to the durable write [`ttl`](Self::ttl).
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

/// A registered collection: the descriptor-derived frozen identity plus
/// the operational definition.
#[derive(Clone, Debug)]
pub(crate) struct RegisteredCollection {
    pub(crate) identity: StructuralIdentity,
    pub(crate) def: CollectionDef,
}

/// Registry of registered collections keyed by `(state_type, name)`.
///
/// A collection's name is unique only *within* its [`StateType`] namespace, so
/// the same name under two state types is two distinct entries and never an
/// identity conflict. See [`Self::collections`] for what the recovery sweep
/// enumerates.
#[derive(Clone, Debug, Default)]
pub(crate) struct CollectionDefRegistry {
    defs: HashMap<StateType, HashMap<StateName, RegisteredCollection>>,
}

impl CollectionDefRegistry {
    /// Registers `descriptor`'s collection with operational settings `def`.
    ///
    /// The frozen [`StructuralIdentity`] is derived from the descriptor —
    /// the single source of identity. A name already present is rejected
    /// loudly: [`RegisterStateError::IdentityConflict`] when the identity
    /// differs, [`RegisterStateError::Duplicate`] when it matches (one
    /// declaration per name per registry — never last-wins). Also rejects an
    /// empty name ([`RegisterStateError::Name`]) or a TTL over Cassandra's
    /// `USING TTL` ceiling ([`RegisterStateError::Ttl`]).
    ///
    /// Test-only: production builds the registry through `register_identity`
    /// (fed by the config's stored registrations); the typed descriptor form
    /// is a suite convenience.
    #[cfg(test)]
    pub(crate) fn register<D>(
        &mut self,
        descriptor: &D,
        def: CollectionDef,
    ) -> Result<(), RegisterStateError>
    where
        D: DescriptorIdentity,
    {
        self.register_identity(
            descriptor.state_type(),
            descriptor.name(),
            descriptor.structural_identity(),
            def,
        )
    }

    /// Registration body over a pre-extracted identity, shared with the
    /// middleware builder (which stores registrations untyped).
    pub(crate) fn register_identity(
        &mut self,
        state_type: StateType,
        name: &str,
        identity: StructuralIdentity,
        def: CollectionDef,
    ) -> Result<(), RegisterStateError> {
        let name = StateName::try_new(name)?;
        if let Some(ttl) = def.ttl
            && i64::from(ttl.seconds()) > MAX_CASSANDRA_TTL_SECS
        {
            return Err(RegisterStateError::Ttl {
                name,
                seconds: ttl.seconds(),
            });
        }
        if def.keyset_limit > MAX_KEYSET_LIMIT {
            return Err(RegisterStateError::KeysetLimit {
                name,
                limit: def.keyset_limit,
            });
        }
        // Policy: only Application collections may be published today. The
        // routing table and reader already carry `state_type`, so lifting this
        // is a matter of deleting the check.
        if def.visibility == StateVisibility::Published && state_type != StateType::Application {
            return Err(RegisterStateError::PublishedNonApplicationStateType { name });
        }
        let namespace = self.defs.entry(state_type).or_default();
        match namespace.get(&name) {
            // A differing identity is the more serious mistake — keep the
            // specific diagnostic.
            Some(existing) if existing.identity != identity => {
                Err(RegisterStateError::IdentityConflict {
                    name,
                    registered: Box::new(existing.identity.clone()),
                    requested: Box::new(identity),
                })
            }
            // A same-identity re-registration is a duplicate declaration:
            // reject it loudly rather than silently last-wins overwriting the
            // operational def. Consumers share the `Copy` `Registered` token
            // instead of registering a name twice.
            Some(_) => Err(RegisterStateError::Duplicate { name }),
            None => {
                namespace.insert(name, RegisteredCollection { identity, def });
                Ok(())
            }
        }
    }

    /// Looks up the registered collection for `(state_type, name)`, if any.
    pub(crate) fn lookup(
        &self,
        state_type: StateType,
        name: &str,
    ) -> Option<(&StateName, &RegisteredCollection)> {
        self.defs.get(&state_type)?.get_key_value(name)
    }

    /// Returns every registered `(state_type, name, identity)` triple — the set
    /// acquisition validates against the durable identity table.
    pub(crate) fn identities(
        &self,
    ) -> impl Iterator<Item = (StateType, &StateName, &StructuralIdentity)> {
        self.defs.iter().flat_map(|(state_type, namespace)| {
            namespace
                .iter()
                .map(move |(name, c)| (*state_type, name, &c.identity))
        })
    }

    /// Returns every live `(state_type, name)` collection — the
    /// registry-sourced name set the recovery sweep enumerates (the
    /// authoritative declared set; a collection whose descriptor was
    /// removed is dormant, not swept).
    pub(crate) fn collections(&self) -> impl Iterator<Item = (StateType, &StateName)> {
        self.defs.iter().flat_map(|(state_type, namespace)| {
            namespace.keys().map(move |name| (*state_type, name))
        })
    }

    /// Whether `(state_type, name)` is registered as `Published`. The
    /// first-write publisher consults this before upserting a routing row. An
    /// unregistered name is never published.
    #[must_use]
    pub(crate) fn is_published(&self, state_type: StateType, name: &StateName) -> bool {
        self.lookup_collection(state_type, name)
            .is_some_and(|c| c.def.visibility == StateVisibility::Published)
    }

    /// Whether any registered collection is `Published`. Gates the whole
    /// first-write publication subsystem: with no published collection there is
    /// nothing to advertise and nothing to reconcile.
    #[must_use]
    pub(crate) fn has_published(&self) -> bool {
        self.defs
            .values()
            .flat_map(HashMap::values)
            .any(|c| c.def.visibility == StateVisibility::Published)
    }

    /// Returns the TTL registered for `(state_type, name)`; an unregistered
    /// name yields `None`.
    #[must_use]
    pub(crate) fn ttl_for(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Option<CompactDuration> {
        self.lookup_collection(state_type, name)
            .and_then(|c| c.def.ttl)
    }

    /// Returns the Map keyset bound for `(state_type, name)`, falling back to
    /// [`DEFAULT_KEYSET_LIMIT`] for names not in the registry.
    #[must_use]
    pub(crate) fn keyset_limit_for(&self, state_type: StateType, name: &StateName) -> usize {
        self.lookup_collection(state_type, name)
            .map_or(DEFAULT_KEYSET_LIMIT, |c| c.def.keyset_limit)
    }

    /// Returns the Deque push capacity for `(state_type, name)` — the
    /// window-slot cap a bounded deque trims toward on push — or `None`
    /// (unbounded) for names not in the registry.
    #[must_use]
    pub(crate) fn capacity_for(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Option<NonZeroUsize> {
        self.lookup_collection(state_type, name)
            .and_then(|c| c.def.capacity)
    }

    /// Returns the commit mode bound to `(state_type, name)`, falling back to
    /// [`CommitMode::ReadCommitted`] for names not in the registry.
    #[must_use]
    pub(crate) fn commit_mode_for(&self, state_type: StateType, name: &StateName) -> CommitMode {
        self.lookup_collection(state_type, name)
            .map_or(CommitMode::ReadCommitted, |c| c.def.commit_mode)
    }

    /// Returns the recovery-convergence bound declared for `(state_type,
    /// name)`, or `None` for a name with no bound (or not in the registry).
    /// The durability boundary folds this against the `recovery_delay`
    /// floor, so `None` means "use the floor"; see [`CollectionDef`].
    #[must_use]
    pub(crate) fn recovery_within_for(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Option<CompactDuration> {
        self.lookup_collection(state_type, name)
            .and_then(|c| c.def.recovery_within)
    }

    fn lookup_collection(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Option<&RegisteredCollection> {
        self.defs.get(&state_type)?.get(name)
    }
}

/// Error returned by `CollectionDefRegistry::register`.
#[derive(Debug, Error)]
pub enum RegisterStateError {
    /// The descriptor's collection name was empty.
    #[error(transparent)]
    Name(#[from] StateNameError),

    /// The collection's TTL exceeds Cassandra's `USING TTL` ceiling
    /// (`630,720,000` seconds). Rejected at registration — the single choke
    /// point behind every `ttl_for` read — so a misconfigured static TTL
    /// fails fast at build time rather than failing every write for the
    /// collection at the coordinator. Rejection (not a silent collapse to
    /// "no TTL") avoids turning "expire in 25 years" into "persist forever".
    #[error(
        "state collection {name:?} TTL {seconds} seconds exceeds Cassandra maximum of 630,720,000 \
         seconds"
    )]
    Ttl {
        /// Collection name whose TTL is over the ceiling.
        name: StateName,

        /// The offending TTL, in seconds.
        seconds: u32,
    },

    /// The collection's TTL does not strictly exceed the keyed-state
    /// `recovery_delay`. A provisional cell carries this TTL; if the cell
    /// could expire before the `StateRecovery` sweep (scheduled
    /// `recovery_delay` after the last commit) resolves it, a committed
    /// write would be lost. Rejected at consumer build so the
    /// misconfiguration fails fast rather than silently losing state under
    /// crash recovery. Indefinite retention (`None`) always passes.
    #[error(
        "state collection {name:?} TTL {ttl_seconds} seconds must exceed the keyed-state recovery \
         delay of {recovery_seconds} seconds, or a provisional cell could expire before recovery"
    )]
    TtlBelowRecoveryDelay {
        /// Collection name whose TTL is at or below the recovery delay.
        name: StateName,

        /// The offending TTL, in seconds.
        ttl_seconds: u32,

        /// The configured recovery delay, in seconds.
        recovery_seconds: u32,
    },

    /// The collection's Map keyset limit exceeds the maximum of `4096`.
    /// Rejected at registration — the single choke point behind every
    /// `keyset_limit_for` read — so a misconfigured limit that would let one
    /// map hold an unbounded keyset cell fails fast at build time.
    #[error("state collection {name:?} keyset limit {limit} exceeds the maximum of 4096")]
    KeysetLimit {
        /// Collection name whose keyset limit is over the ceiling.
        name: StateName,

        /// The offending keyset limit.
        limit: usize,
    },

    /// The name is already registered with a different structural identity.
    #[error(
        "state collection {name:?} already registered with a different identity: registered \
         {registered:?}, requested {requested:?}"
    )]
    IdentityConflict {
        /// Collection name in conflict.
        name: StateName,

        /// Identity already held by the registry.
        registered: Box<StructuralIdentity>,

        /// Identity the new registration asserted.
        requested: Box<StructuralIdentity>,
    },

    /// The name is already registered (with the same identity). One
    /// declaration per name per registry — a duplicate is a configuration
    /// error, rejected rather than silently overwriting the prior
    /// operational settings.
    #[error("state collection {name:?} is already registered")]
    Duplicate {
        /// The duplicated collection name.
        name: StateName,
    },

    /// A collection declared `.published(true)` but the keyed-state
    /// configuration names no subsystem. A published collection must belong to
    /// a subsystem so readers can address it; rejected at consumer build.
    #[error("published state collection {name:?} requires a configured subsystem name")]
    PublishedWithoutSubsystem {
        /// The published collection lacking a subsystem.
        name: StateName,
    },

    /// A collection declared `.published(true)` under a [`StateType`] other
    /// than [`StateType::Application`]. Publishing internal state is not yet
    /// supported; this is a policy check, not a storage limitation.
    #[error(
        "published state collection {name:?} is not a StateType::Application collection; only \
         Application collections may be published"
    )]
    PublishedNonApplicationStateType {
        /// The published collection under a non-Application state type.
        name: StateName,
    },
}

impl ClassifyError for RegisterStateError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[cfg(test)]
mod tests {
    use super::{CollectionDef, CollectionDefRegistry, RegisterStateError, StateVisibility};
    use crate::codec::JsonCodec;
    use crate::state::StateType;
    use crate::state::descriptor::{DescriptorIdentity, value_state};
    use color_eyre::eyre::Result;

    /// A `Published` collection registered under a non-`Application` state type
    /// is rejected at registration. Publication routing rows carry no
    /// `state_type`, and the reader addresses `Application` cells only, so any
    /// other type would be unaddressable. The test uses the `#[cfg(test)]`
    /// `Framework` state type. Its `Application` arm shows the guard is
    /// specific to the state type, not a blanket ban on publishing.
    #[test]
    fn published_non_application_state_type_rejected() -> Result<()> {
        let identity = value_state::<JsonCodec>("cart").structural_identity();
        let mut def = CollectionDef::new(None);
        def.visibility = StateVisibility::Published;

        let mut registry = CollectionDefRegistry::default();
        registry.register_identity(StateType::Application, "cart", identity.clone(), def)?;

        let result = registry.register_identity(StateType::Framework, "cart", identity, def);
        assert!(
            matches!(
                result,
                Err(RegisterStateError::PublishedNonApplicationStateType { .. })
            ),
            "expected PublishedNonApplicationStateType, got {result:?}"
        );
        Ok(())
    }
}
