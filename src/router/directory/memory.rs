//! An in-process node directory.

use super::{NodeDirectory, NodeRegistration, RegistrationTtl};
use crate::router::{MAX_LABEL_BYTES, NodeId};
use quanta::{Clock, Instant};
use quick_cache::UnitWeighter;
use quick_cache::sync::{Cache, DefaultLifecycle};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// The number of registrations held by a default memory directory.
pub(crate) const MEMORY_DIRECTORY_CAPACITY: NonZeroUsize = match NonZeroUsize::new(16) {
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};

/// A node directory held in this process's memory.
///
/// Each call builds a fresh bounded map. A node resolves only registrations it
/// wrote to its own map. This serves one process that asks itself. It also
/// serves tests that do not use Docker.
///
/// Two bounds make it safe to key by a node id.
///
/// **Capacity** is fixed at construction and `quick_cache` evicts to stay
/// inside it. An entry leaves by an explicit `deregister`, by a later
/// `register` of the same node overwriting it, or by that eviction.
///
/// **Age** is the lease. An entry older than the lease reads as absent, which
/// is what makes a dead process stop resolving. Expiry is a visibility rule and
/// not a removal path: an expired entry stays in place rather than being
/// removed, because a remove would race a concurrent re-register and drop the
/// fresh entry that replaced it. The capacity bounds the memory either way.
#[derive(Clone)]
pub(crate) struct MemoryNodeDirectory {
    inner: Arc<Cache<NodeId, (Instant, NodeRegistration), UnitWeighter, ahash::RandomState>>,
    clock: Clock,
    ttl: RegistrationTtl,
}

impl MemoryNodeDirectory {
    /// Creates a bounded directory under `ttl` with the process clock.
    #[must_use]
    pub(crate) fn new(capacity: NonZeroUsize, ttl: RegistrationTtl) -> Self {
        memory_directory(capacity, ttl, Clock::new())
    }

    /// Creates a bounded directory with an injected clock.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn with_clock(capacity: NonZeroUsize, ttl: RegistrationTtl, clock: Clock) -> Self {
        memory_directory(capacity, ttl, clock)
    }

    /// How many registrations the directory holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }
}

fn memory_directory(
    capacity: NonZeroUsize,
    ttl: RegistrationTtl,
    clock: Clock,
) -> MemoryNodeDirectory {
    let inner = Cache::with(
        capacity.get(),
        capacity.get() as u64,
        UnitWeighter,
        ahash::RandomState::default(),
        DefaultLifecycle::default(),
    );
    MemoryNodeDirectory {
        inner: Arc::new(inner),
        clock,
        ttl,
    }
}

impl NodeDirectory for MemoryNodeDirectory {
    type Error = Infallible;

    fn ttl(&self) -> RegistrationTtl {
        self.ttl
    }

    async fn register(&self, registration: &NodeRegistration) -> Result<(), Self::Error> {
        self.inner
            .insert(registration.node, (self.clock.now(), registration.clone()));
        Ok(())
    }

    async fn read(&self, node: NodeId) -> Result<Option<NodeRegistration>, Self::Error> {
        let Some((issued, registration)) = self.inner.get(&node) else {
            return Ok(None);
        };
        if self.clock.now().duration_since(issued) >= self.ttl.duration()
            || !labels_are_bounded(&registration)
        {
            return Ok(None);
        }
        Ok(Some(registration))
    }

    async fn deregister(&self, registration: &NodeRegistration) -> Result<(), Self::Error> {
        drop(self.inner.remove(&registration.node));
        Ok(())
    }
}

/// Whether all six labels fit inside the directory's byte bound.
///
/// This backend filters a registration its own `register` accepted, and this
/// process wrote it. The filter is here for backend parity: the shared
/// directory suite asks both backends the same question, and Cassandra must
/// answer it because something other than this code can write a row.
fn labels_are_bounded(registration: &NodeRegistration) -> bool {
    registration.direct.host.len() <= MAX_LABEL_BYTES
        && registration
            .advertised
            .as_ref()
            .is_none_or(|endpoint| endpoint.host.len() <= MAX_LABEL_BYTES)
        && registration
            .network
            .as_ref()
            .is_none_or(|network| network.len() <= MAX_LABEL_BYTES)
        && registration.group.as_ref().is_none_or(|membership| {
            membership.cluster.len() <= MAX_LABEL_BYTES && membership.group.len() <= MAX_LABEL_BYTES
        })
        && registration.hostname.len() <= MAX_LABEL_BYTES
}
