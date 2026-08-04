//! What one cell of the destination fleet holds.

use super::Refusal;
use super::config::FleetConfiguration;
use super::rate::RateLimit;
use crate::router::Preference;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// One live destination: what bounds the work queued against it, what paces
/// that work, and which of its endpoints answered last.
///
/// It holds no transport channel and no queue on purpose. The channel sits
/// behind the [`ResponseSender`](crate::router::ResponseSender) seam, and the
/// queue and its worker belong to the typed sender. That is what keeps the
/// fleet untyped and free of transport vocabulary.
///
/// A destination lives only while it occupies a cell. Its pacing and its
/// remembered [`Preference`] live inside it and nowhere else. Eviction is
/// therefore the removal path for both. A destination admitted again starts
/// from the present and decides its endpoint again. What survives eviction is
/// the operator's ceiling: `max_destinations` multiplied by `sends_per_second`.
pub(crate) struct Destination {
    slots: Arc<Semaphore>,
    rate: RateLimit,
    /// Which endpoint answered last.
    ///
    /// A [`Mutex`] rather than an `AtomicU8`, examined and kept. The write
    /// happens once per delivered response, after a pacing sleep and a network
    /// round trip, so an atomic would buy a `u8` encoding and a fallible decode
    /// for nothing. Several senders share one fleet, so "one worker per cell"
    /// is not the reason.
    preferred: Mutex<Option<Preference>>,
}

impl Destination {
    /// Builds one destination under the fleet's per-destination limits.
    pub(super) fn new(config: FleetConfiguration) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(config.slots_each)),
            rate: RateLimit::new(config.sends_per_second),
            preferred: Mutex::new(None),
        }
    }

    /// Takes one of this destination's slots.
    ///
    /// # Errors
    ///
    /// Returns [`Refusal::NoSlot`] when every slot is already taken. That is
    /// the only refusal reachable here: the semaphore is never closed, only
    /// dropped with the cell it belongs to.
    pub(super) fn take_slot(&self) -> Result<OwnedSemaphorePermit, Refusal> {
        Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| Refusal::NoSlot)
    }

    /// How many of this destination's slots are free.
    pub(super) fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }

    /// Claims the instant this destination's next send may go at.
    pub(crate) fn next_send(&self) -> Instant {
        self.rate.claim()
    }

    /// Which endpoint answered last, if one has.
    pub(crate) fn preferred(&self) -> Option<Preference> {
        *self.preferred.lock()
    }

    /// Records which endpoint answered, or forgets the last one. A preference
    /// that no longer answers is not one.
    pub(crate) fn prefer(&self, preference: Option<Preference>) {
        *self.preferred.lock() = preference;
    }
}
