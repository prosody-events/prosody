//! The read-through address cache in front of the node directory.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the destination fleet and the process runtime are this module's production \
                  callers; every item here is exercised by this module's tests"
    )
)]

use crate::router::NodeId;
use crate::router::directory::{NodeRegistration, RegistrationTtl};
use quanta::{Clock, Instant};
use quick_cache::UnitWeighter;
use quick_cache::sync::{Cache, DefaultLifecycle};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// One entry: when the read that produced it was issued, and what it found.
///
/// A known-absent node is cached too, so a burst of traffic for an id that the
/// directory does not hold issues one read rather than one per message.
type Entry = (Instant, Option<NodeRegistration>);

/// The concrete `quick_cache` instance. Entries are fixed-size — every string
/// is inline — so items are weighed by count rather than by bytes.
type Inner = Cache<NodeId, Entry, UnitWeighter, ahash::RandomState>;

/// Node id to registration, read through to the directory.
///
/// Two bounds make it safe to key by something an outsider chooses.
/// **Capacity** is fixed at construction and `quick_cache` evicts to stay
/// inside it, so the map cannot grow with traffic; eviction and staleness are
/// its removal paths. **Age** is bounded by the registration lease itself,
/// because the cache is built from the same [`RegistrationTtl`] the writer
/// uses. A cached address can therefore never outlive the row that justified
/// it, and there is no second TTL to configure wrongly.
///
/// Its single-flight behaviour is what matters on the response path: every
/// caller after the first parks on the placeholder until the winner inserts, so
/// a burst for one cold node issues one directory read. **Retention is
/// best-effort and single flight is not.** `quick_cache` may evict an entry it
/// has just admitted into a full cache, so a repeat request can miss; what the
/// cache guarantees is one read per burst, never that a value stays.
#[derive(Clone)]
pub(crate) struct AddressCache {
    inner: Arc<Inner>,
    clock: Clock,
    ttl: Duration,
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
    ) -> Result<Option<NodeRegistration>, E>
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
                    let registration = fill().await?;
                    // The directory's answer stays valid when admission loses
                    // a race.
                    drop(guard.insert((issued, registration.clone())));
                    return Ok(registration);
                }
            }
        }
    }
}
