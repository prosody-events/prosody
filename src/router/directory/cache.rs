//! The read-through address cache in front of the node directory.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the response path and the process runtime are this module's production callers; \
                  the cache is exercised by this module's tests and the resolver by the router's"
    )
)]

use crate::cassandra::errors::CassandraStoreError;
use crate::router::NodeId;
use crate::router::directory::{NodeDirectory, NodeRegistration, RegistrationTtl};
use quanta::{Clock, Instant};
use quick_cache::UnitWeighter;
use quick_cache::sync::{Cache, DefaultLifecycle};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// One entry: when the read that produced it was issued, and what it found.
///
/// A known-absent node is cached too, so a burst of traffic for an id that the
/// directory does not hold issues one read rather than one per message. The
/// registration sits behind an [`Arc`], so serving a hit costs a reference
/// count and never an allocation.
type Entry = (Instant, Option<Arc<NodeRegistration>>);

/// The concrete `quick_cache` instance. Items are weighed by count: capacity
/// bounds how many registrations are held, and each one holds only what the
/// directory row held — a host, a machine name, and two optional labels.
type Inner = Cache<NodeId, Entry, UnitWeighter, ahash::RandomState>;

/// Node id to registration, read through to the directory.
///
/// Two bounds make it safe to key by something an outsider chooses.
///
/// **Capacity** is fixed at construction and `quick_cache` evicts to stay
/// inside it, so the entries it holds cannot grow with traffic. Eviction and
/// staleness are their removal paths. The capacity bounds resident entries,
/// never process RSS: a miss also holds a placeholder, which goes when its fill
/// inserts or drops. The callers in flight bound the placeholders, so traffic
/// does not.
///
/// **Age** is this process's own configured [`RegistrationTtl`], so there is no
/// second TTL to configure wrongly. It bounds how long an entry is served,
/// never how closely that entry tracks the row. The stamp is taken when the
/// read is issued, so an entry filled from a row that was about to expire
/// outlives that row by nearly a whole lease. A process configured with a
/// longer lease than the writer applies to its row keeps the entry longer
/// still. The address is then dialed and the response is dropped, which is what
/// the best-effort posture already accepts.
///
/// Its single-flight behaviour is what matters on the response path: every
/// caller after the first parks on the placeholder until the fill finishes, so
/// a burst for one cold node issues one directory read. **Retention and single
/// flight are both best-effort.** `quick_cache` may evict an entry it has just
/// admitted into a full cache, so a repeat request can miss. A fill that fails
/// inserts nothing and the next waiter reads again, so a burst against a
/// failing directory issues one read per waiter, one at a time.
#[derive(Clone)]
pub(crate) struct AddressCache {
    inner: Arc<Inner>,
    clock: Clock,
    ttl: Duration,
}

/// A node id resolved to what that node published, read through the bounded
/// cache.
///
/// One type, so every caller that needs an address holds one thing rather than
/// a cache and a directory it must remember to pair. It only reads, and every
/// read goes through the cache: it exposes no write and no direct row access.
#[derive(Clone)]
pub(crate) struct AddressResolver {
    cache: AddressCache,
    directory: NodeDirectory,
}

impl AddressCache {
    /// A cache holding up to `capacity` registrations, aged on the process
    /// monotonic clock.
    #[must_use]
    pub(crate) fn new(capacity: usize, ttl: RegistrationTtl) -> Self {
        Self::build(capacity, ttl, Clock::new())
    }

    /// A cache with an injected clock, for deterministic staleness tests. Pair
    /// it with [`quanta::Clock::mock`] and advance the returned handle.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_clock(capacity: usize, ttl: RegistrationTtl, clock: Clock) -> Self {
        Self::build(capacity, ttl, clock)
    }

    fn build(capacity: usize, ttl: RegistrationTtl, clock: Clock) -> Self {
        let inner = Cache::with(
            capacity,
            capacity as u64,
            UnitWeighter,
            ahash::RandomState::default(),
            DefaultLifecycle::default(),
        );
        Self {
            inner: Arc::new(inner),
            clock,
            ttl: ttl.duration(),
        }
    }

    /// How many registrations the cache currently holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    /// Serves a fresh entry, or fills single-flight through `fill`.
    ///
    /// A stale entry is removed only when it still carries the issue time this
    /// call observed, so a racing newer fill survives. The loop then re-reads
    /// and either serves that newer entry or takes the guard, which is what
    /// guarantees progress.
    ///
    /// # Errors
    ///
    /// Propagates the error `fill` returns.
    pub(crate) async fn resolve<F, Fut, E>(
        &self,
        node: NodeId,
        fill: F,
    ) -> Result<Option<Arc<NodeRegistration>>, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Option<NodeRegistration>, E>>,
    {
        loop {
            match self.inner.get_value_or_guard_async(&node).await {
                Ok((issued, registration)) => {
                    if self.clock.now().duration_since(issued) < self.ttl {
                        return Ok(registration);
                    }
                    // Equal clock readings are interchangeable observations.
                    // Removing either can only cause a refill.
                    self.inner
                        .remove_if(&node, |(observed, _)| *observed == issued);
                }
                Err(guard) => {
                    // Single-flight: this caller owns the fill. The issue time
                    // is taken before the read, so a slow fill enters already
                    // aged and cannot pass an old value off as fresh.
                    let issued = self.clock.now();
                    let registration = fill().await?.map(Arc::new);
                    // The directory's answer stays valid when admission loses
                    // a race.
                    drop(guard.insert((issued, registration.clone())));
                    return Ok(registration);
                }
            }
        }
    }
}

impl AddressResolver {
    /// Reads `directory` through `cache`.
    #[must_use]
    pub(crate) const fn new(cache: AddressCache, directory: NodeDirectory) -> Self {
        Self { cache, directory }
    }

    /// What `node` published, or `None` when the directory holds no row for it.
    ///
    /// # Errors
    ///
    /// Returns the directory's error when a cache miss cannot be filled.
    pub(crate) async fn resolve(
        &self,
        node: NodeId,
    ) -> Result<Option<Arc<NodeRegistration>>, CassandraStoreError> {
        self.cache.resolve(node, || self.directory.read(node)).await
    }
}
