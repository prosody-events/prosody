//! The read-through address cache in front of the peer directory.

use crate::peer::router::PeerId;
use crate::peer::router::directory::{PeerDirectory, PeerRegistration};
use quick_cache::sync::Cache;
use std::sync::Arc;

/// The concrete `quick_cache` instance. Items are weighed by count: capacity
/// bounds how many registrations are held, and each one holds only what the
/// directory entry held — a host, a machine name, and two optional labels.
type RegistrationCache = Cache<PeerId, Arc<PeerRegistration>>;

/// Peer id to registration, read through to the directory.
///
/// One bound makes it safe to key by something an outsider chooses.
///
/// **Capacity** is fixed at construction and `quick_cache` evicts to stay
/// inside it, so the entries it holds cannot grow with traffic. Eviction is
/// their removal path. A peer id names one router lifetime, and that router's
/// registration cannot change. A new router gets a new id. Successful entries
/// therefore need no age. Missing registrations are not cached because a read
/// can race with registration.
///
/// Its single-flight behaviour is what matters on the response path: every
/// caller after the first parks on the placeholder until the fill finishes, so
/// a burst for one cold peer issues one directory read. **Retention and single
/// flight are both best-effort.** `quick_cache` may evict an entry it has just
/// admitted into a full cache, so a repeat request can miss. A fill that fails
/// inserts nothing and the next waiter reads again, so a burst against a
/// missing or failing directory issues one read per waiter, one at a time.
/// A peer id resolved to what that peer published, read through the bounded
/// cache.
///
/// One type, so every caller that needs an address holds one thing rather than
/// a cache and a directory it must remember to pair. It only reads, and every
/// read goes through the cache: it exposes no write and no direct directory
/// access.
#[derive(Clone)]
pub(crate) struct AddressResolver<D> {
    registrations: Arc<RegistrationCache>,
    directory: D,
}

impl<D: PeerDirectory> AddressResolver<D> {
    /// Reads `directory` through a cache of up to `capacity` registrations.
    #[must_use]
    pub(crate) fn new(capacity: usize, directory: D) -> Self {
        Self {
            registrations: Arc::new(Cache::new(capacity)),
            directory,
        }
    }

    /// How many registrations the cache currently holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.registrations.len()
    }

    /// What `peer` published, or `None` when the directory holds no entry for
    /// it.
    ///
    /// # Errors
    ///
    /// Returns the directory's error when a cache miss cannot be filled.
    pub(crate) async fn resolve(
        &self,
        peer: PeerId,
    ) -> Result<Option<Arc<PeerRegistration>>, D::Error> {
        match self.registrations.get_value_or_guard_async(&peer).await {
            Ok(registration) => Ok(Some(registration)),
            Err(guard) => {
                let registration = self.directory.read(peer).await?.map(Arc::new);
                if let Some(registration) = &registration {
                    drop(guard.insert(Arc::clone(registration)));
                }
                Ok(registration)
            }
        }
    }
}
