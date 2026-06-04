//! Per-collection definitions and the registry of middleware defaults.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::{DescriptorIdentity, StructuralIdentity};
use crate::state::{CommitMode, StateName, StateNameError};
use crate::timers::duration::CompactDuration;
use std::collections::HashMap;
use thiserror::Error;

/// Operational per-collection settings that override middleware defaults.
///
/// Carries the collection's TTL and [`CommitMode`]. `ttl` is `None` for
/// "do not bind a TTL" (explicit opt-out / Cassandra over-20-year
/// overflow fallback). `commit_mode` decides whether handler-side dirty
/// ops produce a sealed WAL on success ([`CommitMode::Wal`]) or apply
/// straight to authoritative state ([`CommitMode::Direct`]).
///
/// Operational settings are deliberately separate from the collection's
/// frozen [`StructuralIdentity`]: the identity comes only from the
/// descriptor at [`CollectionDefRegistry::register`] time, so a definition
/// cannot assert an identity its descriptor does not have.
///
/// Core Invariant #6: "A collection has one `CommitMode` while a handler
/// is running" — pinned here at registration time, not at event scope
/// creation time.
#[derive(Clone, Debug)]
pub struct CollectionDef {
    /// Per-collection TTL override.
    pub ttl: Option<CompactDuration>,

    /// Per-collection commit mode.
    pub commit_mode: CommitMode,
}

impl CollectionDef {
    /// Creates a collection definition with the supplied TTL; commit
    /// mode defaults to [`CommitMode::Wal`].
    #[must_use]
    pub fn new(ttl: Option<CompactDuration>) -> Self {
        Self {
            ttl,
            commit_mode: CommitMode::Wal,
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
/// TTL / commit-mode lookups on names that are not registered (e.g.
/// recovery-sweep entries whose descriptor was since removed).
#[derive(Clone, Debug)]
pub struct CollectionDefRegistry {
    defs: HashMap<StateName, RegisteredCollection>,
    default_ttl: Option<CompactDuration>,
    default_commit_mode: CommitMode,
}

impl Default for CollectionDefRegistry {
    fn default() -> Self {
        Self::new(None)
    }
}

impl CollectionDefRegistry {
    /// Creates a registry with the supplied middleware-wide default TTL
    /// and [`CommitMode::Wal`] as the default commit mode.
    #[must_use]
    pub fn new(default_ttl: Option<CompactDuration>) -> Self {
        Self {
            defs: HashMap::new(),
            default_ttl,
            default_commit_mode: CommitMode::Wal,
        }
    }

    /// Overrides the default commit mode used for collections not in the
    /// registry.
    #[must_use]
    pub fn with_default_commit_mode(mut self, mode: CommitMode) -> Self {
        self.default_commit_mode = mode;
        self
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
    /// empty, or [`RegisterStateError::IdentityConflict`] when the name is
    /// already registered with a different structural identity.
    pub fn register<D>(
        &mut self,
        descriptor: &D,
        def: CollectionDef,
    ) -> Result<(), RegisterStateError>
    where
        D: DescriptorIdentity,
    {
        self.register_identity(descriptor.name(), descriptor.structural_identity(), def)
    }

    /// Registration body over a pre-extracted identity, shared with the
    /// middleware builder (which stores registrations untyped).
    pub(crate) fn register_identity(
        &mut self,
        name: &str,
        identity: StructuralIdentity,
        def: CollectionDef,
    ) -> Result<(), RegisterStateError> {
        let name = StateName::try_new(name)?;
        if let Some(existing) = self.defs.get(&name)
            && existing.identity != identity
        {
            return Err(RegisterStateError::IdentityConflict {
                name,
                registered: existing.identity.clone(),
                requested: identity,
            });
        }
        self.defs
            .insert(name, RegisteredCollection { identity, def });
        Ok(())
    }

    /// Looks up the registered collection for `name`, if any.
    pub(crate) fn lookup(&self, name: &str) -> Option<(&StateName, &RegisteredCollection)> {
        self.defs.get_key_value(name)
    }

    /// Returns every registered `(name, identity)` pair.
    pub(crate) fn identities(&self) -> impl Iterator<Item = (&StateName, &StructuralIdentity)> {
        self.defs.iter().map(|(name, c)| (name, &c.identity))
    }

    /// Returns the TTL bound to `name`, falling back to the
    /// middleware-wide default.
    #[must_use]
    pub fn ttl_for(&self, name: &StateName) -> Option<CompactDuration> {
        self.defs.get(name).map_or(self.default_ttl, |c| c.def.ttl)
    }

    /// Returns the commit mode bound to `name`, falling back to the
    /// middleware-wide default.
    #[must_use]
    pub fn commit_mode_for(&self, name: &StateName) -> CommitMode {
        self.defs
            .get(name)
            .map_or(self.default_commit_mode, |c| c.def.commit_mode)
    }
}

/// Error returned by [`CollectionDefRegistry::register`].
#[derive(Debug, Error)]
pub enum RegisterStateError {
    /// The descriptor's collection name was empty.
    #[error(transparent)]
    Name(#[from] StateNameError),

    /// The name is already registered with a different structural identity.
    #[error(
        "state collection {name:?} already registered with a different identity: registered \
         {registered:?}, requested {requested:?}"
    )]
    IdentityConflict {
        /// Collection name in conflict.
        name: StateName,

        /// Identity already held by the registry.
        registered: StructuralIdentity,

        /// Identity the new registration asserted.
        requested: StructuralIdentity,
    },
}

impl ClassifyError for RegisterStateError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
