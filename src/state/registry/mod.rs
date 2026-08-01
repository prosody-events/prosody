//! The collection registry: which collections a consumer declared, and the
//! frozen identity and operational settings each was registered with.
//!
//! The settings themselves, and the policy vocabulary they draw from, live in
//! `definition`.

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::descriptor::DescriptorIdentity;
use crate::state::descriptor::StructuralIdentity;
use crate::state::{StateName, StateNameError, StateType};
use crate::timers::duration::CompactDuration;
use std::collections::HashMap;
use thiserror::Error;

mod definition;

#[cfg(test)]
mod tests;

pub use definition::{CollectionDef, CommitMode, ReadCachePolicy, StateVisibility};

/// Registration ceiling on the Map keyset bound: a larger limit is rejected at
/// build ([`RegisterStateError::KeysetLimit`]), capping the point-get fan-out
/// (and decode allocation) a single `stream` can issue — the byte ceiling
/// separately bounds the frame's wire size.
pub(crate) const MAX_KEYSET_LIMIT: usize = 4096;

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
    /// Test-only: production builds the registry through
    /// [`Self::register_identity`], fed by the config's stored registrations.
    /// The typed descriptor form is a suite convenience.
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
    ///
    /// The frozen [`StructuralIdentity`] comes from the descriptor — the single
    /// source of identity. A name already present is rejected loudly:
    /// [`RegisterStateError::IdentityConflict`] when the identity differs,
    /// [`RegisterStateError::Duplicate`] when it matches. One declaration per
    /// name per registry, never last-wins.
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
            // Consumers share the `Copy` `Registered` token instead of
            // registering a name twice.
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

    /// Returns the operational settings registered for `(state_type, name)` —
    /// the whole definition a collection binding captures once, and the single
    /// lookup every per-setting accessor below reads its field from, so none of
    /// them can disagree about an unregistered name. An unregistered name
    /// yields the same defaults each setting carries on its own
    /// ([`CollectionDef::new`] with no TTL).
    #[must_use]
    pub(crate) fn def_for(&self, state_type: StateType, name: &StateName) -> CollectionDef {
        self.lookup_collection(state_type, name)
            .map_or_else(|| CollectionDef::new(None), |c| c.def)
    }

    /// Returns the TTL registered for `(state_type, name)`; an unregistered
    /// name yields `None`.
    #[must_use]
    pub(crate) fn ttl_for(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Option<CompactDuration> {
        self.def_for(state_type, name).ttl
    }

    /// Returns the commit mode bound to `(state_type, name)`, falling back to
    /// [`CommitMode::ReadCommitted`] for names not in the registry.
    #[must_use]
    pub(crate) fn commit_mode_for(&self, state_type: StateType, name: &StateName) -> CommitMode {
        self.def_for(state_type, name).commit_mode
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
        self.def_for(state_type, name).recovery_within
    }

    fn lookup_collection(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Option<&RegisteredCollection> {
        self.defs.get(&state_type)?.get(name)
    }
}

/// Error returned by `CollectionDefRegistry::register_identity` and by the
/// keyed-state configuration's build-time validation.
#[derive(Debug, Error)]
pub enum RegisterStateError {
    /// The descriptor's collection name was empty.
    #[error(transparent)]
    Name(#[from] StateNameError),

    /// The collection's TTL exceeds Cassandra's `USING TTL` ceiling. Rejecting
    /// it, rather than silently collapsing to "no TTL", avoids turning "expire
    /// in 25 years" into "persist forever".
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
    /// `recovery_delay`. A provisional cell carries this TTL, so if the cell
    /// could expire before the `StateRecovery` sweep resolves it, a committed
    /// write would be lost. Indefinite retention (`None`) always passes.
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

    /// The collection's Map keyset limit exceeds the maximum of `4096`, which
    /// would let one map hold an unbounded keyset cell.
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

    /// The name is already registered with the same identity. One declaration
    /// per name per registry, rejected rather than silently overwriting the
    /// prior operational settings.
    #[error("state collection {name:?} is already registered")]
    Duplicate {
        /// The duplicated collection name.
        name: StateName,
    },

    /// A collection declared `.published(true)` but the keyed-state
    /// configuration names no subsystem. A published collection must belong to
    /// a subsystem so readers can address it.
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
