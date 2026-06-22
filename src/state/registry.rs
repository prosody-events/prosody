//! Per-collection definitions and the registry of middleware defaults.

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::{DescriptorIdentity, StructuralIdentity};
use crate::state::{CollectionKindId, CommitMode, StateName, StateNameError, StateType};
use crate::timers::duration::CompactDuration;
use std::collections::HashMap;
use thiserror::Error;

/// Operational per-collection settings that override middleware defaults.
///
/// Carries the collection's TTL and [`CommitMode`]. `ttl` is `None` for
/// "do not bind a TTL" (explicit indefinite retention); a `Some(ttl)` over
/// Cassandra's `USING TTL` ceiling is rejected at
/// [`CollectionDefRegistry::register`] time, never silently collapsed.
/// `commit_mode` decides whether handler-side dirty ops stage a provisional
/// cell that promotes to committed after the event commit
/// ([`CommitMode::ReadCommitted`]) or apply straight to the committed value
/// on handler success ([`CommitMode::ReadUncommitted`]).
///
/// Operational settings are deliberately separate from the collection's
/// frozen [`StructuralIdentity`]: the identity comes only from the
/// descriptor at [`CollectionDefRegistry::register`] time, so a definition
/// cannot assert an identity its descriptor does not have.
///
/// Core Invariant #6: "A collection has one `CommitMode` while a handler
/// is running" — pinned here at registration time, not at event scope
/// creation time.
#[derive(Clone, Copy, Debug)]
pub struct CollectionDef {
    /// Per-collection TTL override.
    pub ttl: Option<CompactDuration>,

    /// Per-collection commit mode.
    pub commit_mode: CommitMode,
}

impl CollectionDef {
    /// Creates a collection definition with the supplied TTL; commit
    /// mode defaults to [`CommitMode::ReadCommitted`].
    #[must_use]
    pub fn new(ttl: Option<CompactDuration>) -> Self {
        Self {
            ttl,
            commit_mode: CommitMode::ReadCommitted,
        }
    }

    /// Builder-style override for the commit mode.
    #[must_use]
    pub fn with_commit_mode(mut self, mode: CommitMode) -> Self {
        self.commit_mode = mode;
        self
    }
}

/// A registered collection: the descriptor-derived frozen identity plus
/// the operational definition.
#[derive(Clone, Debug)]
pub(crate) struct RegisteredCollection {
    pub(crate) identity: StructuralIdentity,
    pub(crate) def: CollectionDef,
}

/// Registry of registered collections plus middleware-wide defaults for
/// TTL / commit-mode lookups on names that are not registered.
///
/// Keyed by `(state_type, name)`: a collection's name is unique only *within*
/// its [`StateType`] namespace, so the same name under two state types is two
/// distinct entries and never an identity conflict. The recovery sweep
/// enumerates the registered collections for a kind
/// (`collections_for_kind`), the authoritative declared set — there is
/// no durable-name union, so a collection whose descriptor was removed is
/// simply dormant until it returns.
#[derive(Clone, Debug)]
pub struct CollectionDefRegistry {
    defs: HashMap<StateType, HashMap<StateName, RegisteredCollection>>,
    default_ttl: Option<CompactDuration>,
}

impl Default for CollectionDefRegistry {
    fn default() -> Self {
        Self::new(None)
    }
}

impl CollectionDefRegistry {
    /// Creates a registry with the supplied middleware-wide default TTL.
    #[must_use]
    pub fn new(default_ttl: Option<CompactDuration>) -> Self {
        Self {
            defs: HashMap::new(),
            default_ttl,
        }
    }

    /// Registers `descriptor`'s collection with operational settings `def`.
    ///
    /// The frozen [`StructuralIdentity`] is derived from the descriptor —
    /// the single source of identity. Re-registering the same name with the
    /// same identity is idempotent (the operational `def` is updated);
    /// a different identity for the same name is rejected.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterStateError::Name`] when the descriptor's name is
    /// empty, [`RegisterStateError::Ttl`] when its TTL exceeds
    /// Cassandra's `USING TTL` ceiling, or
    /// [`RegisterStateError::IdentityConflict`] when the name is already
    /// registered with a different structural identity.
    pub fn register<D>(
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
        let namespace = self.defs.entry(state_type).or_default();
        if let Some(existing) = namespace.get(&name)
            && existing.identity != identity
        {
            return Err(RegisterStateError::IdentityConflict {
                name,
                registered: Box::new(existing.identity.clone()),
                requested: Box::new(identity),
            });
        }
        namespace.insert(name, RegisteredCollection { identity, def });
        Ok(())
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

    /// Returns the live `(state_type, name)` collections registered under
    /// `kind` — the registry-sourced name set the recovery sweep enumerates for
    /// that kind's lane.
    pub(crate) fn collections_for_kind(
        &self,
        kind: CollectionKindId,
    ) -> impl Iterator<Item = (StateType, &StateName)> {
        self.defs.iter().flat_map(move |(state_type, namespace)| {
            namespace.iter().filter_map(move |(name, c)| {
                (c.identity.kind == kind).then_some((*state_type, name))
            })
        })
    }

    /// Returns the TTL bound to `(state_type, name)`, falling back to the
    /// middleware-wide default.
    #[must_use]
    pub fn ttl_for(&self, state_type: StateType, name: &StateName) -> Option<CompactDuration> {
        self.lookup_collection(state_type, name)
            .map_or(self.default_ttl, |c| c.def.ttl)
    }

    /// Returns the commit mode bound to `(state_type, name)`, falling back to
    /// [`CommitMode::ReadCommitted`] for names not in the registry.
    #[must_use]
    pub fn commit_mode_for(&self, state_type: StateType, name: &StateName) -> CommitMode {
        self.lookup_collection(state_type, name)
            .map_or(CommitMode::ReadCommitted, |c| c.def.commit_mode)
    }

    fn lookup_collection(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Option<&RegisteredCollection> {
        self.defs.get(&state_type)?.get(name)
    }
}

/// Error returned by [`CollectionDefRegistry::register`].
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
}

impl ClassifyError for RegisterStateError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
