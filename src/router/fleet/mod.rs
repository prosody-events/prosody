//! The process-wide table of destinations a response may be sent to.
//!
//! One fleet serves every consumer in the process. A private allowance per
//! consumer would let one dead peer hold a fresh set of slots for each of them,
//! so the per-destination bound holds only because the table is shared.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the respond layer and the shutdown path are this module's production callers; \
                  the reservation accessors and `Destination::next_send` are exercised from the \
                  response sender's suites, and the rest from this module's own"
    )
)]

use crate::router::NodeId;
use crate::router::fleet::config::{FleetConfiguration, FleetConfigurationError};
use crate::router::fleet::gate::{AdmissionGate, GateTicket};
use crate::router::fleet::rate::RateLimit;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use validator::Validate;

pub(crate) mod config;
mod gate;
mod rate;

#[cfg(test)]
mod tests;

/// The destinations one process holds live, and what each of them is allowed.
///
/// The set of destinations is open — any node may be named — so the fleet is
/// bounded rather than preallocated. A new node takes an empty cell, else
/// evicts the least recently used cell whose destination has nothing in flight.
/// When every cell holds a busy destination, the fleet refuses the new one and
/// counts the refusal. The table never holds more than the configured maximum.
pub(crate) struct DestinationFleet {
    /// One cell per configured destination. The boxed slice has no `push`, so
    /// the length is fixed by the type and no reservation can grow it.
    ///
    /// One lock rather than a sharded map, because the bound and the eviction
    /// are global facts: "find or admit, evicting an idle cell" and "take one
    /// of that destination's slots" must be one atomic step against
    /// eviction. The scan is linear over 16-byte ids and bounded by the
    /// configured maximum.
    table: Mutex<Box<[Option<Arc<Destination>>]>>,
    gate: AdmissionGate,
    /// One monotonic source for both the use order and the generation, so a
    /// re-admitted node can never look like the occupant it replaced.
    stamp: AtomicU64,
    admitted: AtomicU64,
    evicted: AtomicU64,
    refused: AtomicU64,
    config: FleetConfiguration,
}

/// One live destination: what bounds its outstanding sends, what paces them,
/// and when it was last used.
///
/// It holds no transport channel and no drain state on purpose. The channel
/// sits behind the [`ResponseSender`](crate::router::ResponseSender) seam and
/// the drain is per lane in the typed sender. That is what keeps the fleet
/// untyped and free of transport vocabulary.
pub(crate) struct Destination {
    node: NodeId,
    /// Tells this occupant of a table cell from every earlier one. A sender
    /// holding work for an earlier occupant sees the difference and rebuilds.
    generation: u64,
    slots: Arc<Semaphore>,
    rate: RateLimit,
    last_used: AtomicU64,
}

/// One send slot, taken on one live destination.
///
/// Holding one means the fleet is open, the destination is live, and one of its
/// slots is taken. Dropping it releases the slot and leaves the admission gate.
/// [`Reservation::commit`] is the only other way out, and it leaves the gate
/// only after the slot has been handed on.
pub(crate) struct Reservation<'a> {
    slot: usize,
    generation: u64,
    destination: Arc<Destination>,
    permit: OwnedSemaphorePermit,
    ticket: GateTicket<'a>,
}

impl DestinationFleet {
    /// Validates `config` and builds the table at its configured length.
    ///
    /// This is the only way to make a fleet, so an unvalidated one does not
    /// exist. The table is allocated once and never grows. One destination
    /// record is allocated when a node is admitted and freed when its cell is
    /// evicted. No reservation on a live destination allocates.
    ///
    /// # Errors
    ///
    /// Returns [`FleetConfigurationError::Invalid`] when a field is outside its
    /// supported range, or when the slot total is.
    pub(crate) fn new(config: FleetConfiguration) -> Result<Self, FleetConfigurationError> {
        config.validate()?;
        Ok(Self {
            table: Mutex::new(vec![None; config.max_destinations].into_boxed_slice()),
            gate: AdmissionGate::new(),
            stamp: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            config,
        })
    }

    /// Takes one send slot on `node`, admitting the destination when the table
    /// has room.
    ///
    /// Never awaits: an apply hook calls this, and an apply hook that waits
    /// stalls the next event for the same key.
    ///
    /// # Errors
    ///
    /// Returns [`Refusal::ShuttingDown`] once admission is closed,
    /// [`Refusal::NoDestination`] when every cell holds a busy destination, and
    /// [`Refusal::NoSlot`] when this destination's slots are all taken. The
    /// last two are capacity refusals and are counted; a closed gate is
    /// not.
    pub(crate) fn reserve(&self, node: NodeId) -> Result<Reservation<'_>, Refusal> {
        let ticket = self.gate.enter().ok_or(Refusal::ShuttingDown)?;
        let mut table = self.table.lock();
        let reservation = self.take_slot(&mut table, node, ticket);
        if reservation.is_err() {
            self.refused.fetch_add(1, Relaxed);
        }
        reservation
    }

    /// Closes admission and returns once every caller already inside has left.
    ///
    /// Shutdown runs this before it stops the workers. Reversing the two would
    /// let a hook reserve a slot on a fleet that has stopped draining.
    pub(crate) async fn close(&self) {
        self.gate.close_and_drain().await;
    }

    /// What this fleet was built with.
    pub(crate) const fn config(&self) -> FleetConfiguration {
        self.config
    }

    /// How many destinations have been admitted since construction.
    pub(crate) fn admitted(&self) -> u64 {
        self.admitted.load(Relaxed)
    }

    /// How many destinations have been evicted to make room.
    pub(crate) fn evicted(&self) -> u64 {
        self.evicted.load(Relaxed)
    }

    /// How many reservations the fleet refused for want of capacity.
    pub(crate) fn refused(&self) -> u64 {
        self.refused.load(Relaxed)
    }

    /// The cell and generation `node` occupies, if it is live.
    #[cfg(test)]
    pub(crate) fn live(&self, node: NodeId) -> Option<(usize, u64)> {
        self.table
            .lock()
            .iter()
            .enumerate()
            .find_map(|(slot, cell)| {
                let destination = cell.as_ref()?;
                (destination.node == node).then_some((slot, destination.generation))
            })
    }

    /// How many cells hold `node`. Never more than one — see
    /// [`DestinationFleet::find_or_admit`].
    #[cfg(test)]
    pub(crate) fn cells_holding(&self, node: NodeId) -> usize {
        self.table
            .lock()
            .iter()
            .flatten()
            .filter(|destination| destination.node == node)
            .count()
    }

    /// How many cells the table holds. Constant for the fleet's whole life.
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.table.lock().len()
    }

    /// How many cells are occupied.
    #[cfg(test)]
    pub(crate) fn live_count(&self) -> usize {
        self.table.lock().iter().flatten().count()
    }

    /// How many of `node`'s slots are free, if it is live.
    #[cfg(test)]
    pub(crate) fn available(&self, node: NodeId) -> Option<usize> {
        self.table.lock().iter().find_map(|cell| {
            let destination = cell.as_ref()?;
            (destination.node == node).then(|| destination.slots.available_permits())
        })
    }

    /// Whether admission is closed.
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.gate.is_closed()
    }

    fn take_slot<'a>(
        &'a self,
        table: &mut [Option<Arc<Destination>>],
        node: NodeId,
        ticket: GateTicket<'a>,
    ) -> Result<Reservation<'a>, Refusal> {
        let (slot, destination) = self.find_or_admit(table, node)?;
        // Stamped before the permit attempt, so the stamp means "last reserved
        // against". A destination whose slots are all taken would otherwise
        // never refresh it, and would be the first eviction candidate the
        // moment it went idle — it would lose its cell under exactly the load
        // that wants it kept.
        destination.last_used.store(self.next_stamp(), Relaxed);
        // Taken under the table lock, so "nothing in flight" is an observation
        // eviction can trust: no slot can be taken on a cell being evicted.
        let permit = Arc::clone(&destination.slots)
            .try_acquire_owned()
            .map_err(|_| Refusal::NoSlot)?;
        Ok(Reservation {
            slot,
            generation: destination.generation,
            destination,
            permit,
            ticket,
        })
    }

    /// Finds `node`'s cell, or gives it one.
    ///
    /// At most one cell holds a given node. A second cell for the same node
    /// would hand it a second semaphore, and its slot bound would be twice what
    /// the operator configured. The search below runs before every admission,
    /// which is what keeps that true.
    fn find_or_admit(
        &self,
        table: &mut [Option<Arc<Destination>>],
        node: NodeId,
    ) -> Result<(usize, Arc<Destination>), Refusal> {
        let live = table.iter().enumerate().find_map(|(slot, cell)| {
            let destination = cell.as_ref()?;
            (destination.node == node).then(|| (slot, Arc::clone(destination)))
        });
        if let Some(found) = live {
            return Ok(found);
        }
        let slot = self.free_cell(table).ok_or(Refusal::NoDestination)?;
        let destination = Arc::new(Destination::new(node, self.next_stamp(), self.config));
        table[slot] = Some(Arc::clone(&destination));
        self.admitted.fetch_add(1, Relaxed);
        Ok((slot, destination))
    }

    /// An empty cell, else the least recently used idle one, which is evicted.
    ///
    /// A destination with a send in flight is never a candidate: evicting it
    /// would drop the table's reference while a worker still holds its slots,
    /// so a later admission of the same node would hand out a second set.
    fn free_cell(&self, table: &mut [Option<Arc<Destination>>]) -> Option<usize> {
        if let Some(slot) = table.iter().position(Option::is_none) {
            return Some(slot);
        }
        let (_, idle) = table
            .iter()
            .enumerate()
            .filter_map(|(slot, cell)| {
                let destination = cell.as_ref()?;
                (destination.slots.available_permits() == self.config.slots_each)
                    .then(|| (destination.last_used.load(Relaxed), slot))
            })
            .min()?;
        table[idle] = None;
        self.evicted.fetch_add(1, Relaxed);
        Some(idle)
    }

    fn next_stamp(&self) -> u64 {
        self.stamp.fetch_add(1, Relaxed) + 1
    }
}

impl Destination {
    fn new(node: NodeId, generation: u64, config: FleetConfiguration) -> Self {
        Self {
            node,
            generation,
            slots: Arc::new(Semaphore::new(config.slots_each)),
            rate: RateLimit::new(config.sends_per_second),
            last_used: AtomicU64::new(generation),
        }
    }

    /// Claims the instant this destination's next send may go at.
    pub(crate) fn next_send(&self) -> Instant {
        self.rate.claim()
    }
}

impl Reservation<'_> {
    /// The table cell this destination occupies. A sender indexes its own
    /// per-destination state by the same number.
    pub(crate) const fn slot(&self) -> usize {
        self.slot
    }

    /// Which occupant of that cell this reservation belongs to.
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    /// The destination itself, for a sender that must keep it alive.
    pub(crate) const fn destination(&self) -> &Arc<Destination> {
        &self.destination
    }

    /// Hands the slot to `queue`, then leaves the admission gate.
    ///
    /// The gate covers the whole hand-over, not the reservation alone. A
    /// shutdown that has seen the count reach zero must find nobody still about
    /// to queue work, and passing the slot through a closure is what makes that
    /// structural: the ticket cannot outlive the hand-over and cannot precede
    /// it. The slot itself stays taken until the permit `queue` was given
    /// drops, which is what keeps the destination unevictable while its send is
    /// in flight.
    pub(crate) fn commit<T, F>(self, queue: F) -> T
    where
        F: FnOnce(OwnedSemaphorePermit) -> T,
    {
        let Self { permit, ticket, .. } = self;
        let outcome = queue(permit);
        drop(ticket);
        outcome
    }
}

/// Why the fleet refused a reservation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum Refusal {
    /// Admission is closed: the process is shutting down.
    #[error("the destination fleet is not admitting")]
    ShuttingDown,

    /// Every cell holds a destination with sends in flight.
    #[error("every destination cell is busy")]
    NoDestination,

    /// This destination's slots are all taken.
    #[error("the destination has no free slot")]
    NoSlot,
}
