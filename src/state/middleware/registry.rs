//! Per-collection definitions and the registry of middleware defaults.

use crate::state::{CommitMode, StateName};
use crate::timers::duration::CompactDuration;
use std::collections::HashMap;

/// Per-collection metadata that overrides middleware defaults.
///
/// Carries the collection's TTL and [`CommitMode`]. `ttl` is `None` for
/// "do not bind a TTL" (explicit opt-out / Cassandra over-20-year
/// overflow fallback). `commit_mode` decides whether handler-side dirty
/// ops produce a sealed WAL on success ([`CommitMode::Wal`]) or apply
/// straight to authoritative state ([`CommitMode::Direct`]).
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

/// Registry of [`CollectionDef`] entries plus middleware-wide defaults
/// for collections that are not explicitly registered.
#[derive(Clone, Debug)]
pub struct CollectionDefRegistry {
    pub(super) defs: HashMap<StateName, CollectionDef>,
    pub(super) default_ttl: Option<CompactDuration>,
    pub(super) default_commit_mode: CommitMode,
}

impl Default for CollectionDefRegistry {
    fn default() -> Self {
        Self {
            defs: HashMap::new(),
            default_ttl: None,
            default_commit_mode: CommitMode::Wal,
        }
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

    /// Registers a per-collection definition. Returns the previous value
    /// for the same name, if any.
    pub fn insert(&mut self, name: StateName, def: CollectionDef) -> Option<CollectionDef> {
        self.defs.insert(name, def)
    }

    /// Returns the TTL bound to `name`, falling back to the
    /// middleware-wide default.
    #[must_use]
    pub fn ttl_for(&self, name: &StateName) -> Option<CompactDuration> {
        self.defs.get(name).map_or(self.default_ttl, |def| def.ttl)
    }

    /// Returns the commit mode bound to `name`, falling back to the
    /// middleware-wide default.
    #[must_use]
    pub fn commit_mode_for(&self, name: &StateName) -> CommitMode {
        self.defs
            .get(name)
            .map_or(self.default_commit_mode, |def| def.commit_mode)
    }
}
