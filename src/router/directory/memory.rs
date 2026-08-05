//! An in-process node directory for tests.

use super::{NodeDirectory, NodeRegistration, RegistrationTtl};
use crate::router::{MAX_LABEL_BYTES, NodeId};
use quanta::{Clock, Instant};
use quick_cache::UnitWeighter;
use quick_cache::sync::{Cache, DefaultLifecycle};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// A node directory held in this process's memory, for tests that need peer
/// behaviour without a Cassandra cluster.
///
/// **It cannot discover a peer in another process.** Two processes each hold
/// their own map and neither sees the other's registrations, so this is a test
/// double and never a deployment option.
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
        Self::with_clock(capacity, ttl, Clock::new())
    }

    /// Creates a bounded directory with an injected clock.
    #[must_use]
    pub(crate) fn with_clock(capacity: NonZeroUsize, ttl: RegistrationTtl, clock: Clock) -> Self {
        let inner = Cache::with(
            capacity.get(),
            capacity.get() as u64,
            UnitWeighter,
            ahash::RandomState::default(),
            DefaultLifecycle::default(),
        );
        Self {
            inner: Arc::new(inner),
            clock,
            ttl,
        }
    }

    /// How many registrations the directory holds.
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
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
