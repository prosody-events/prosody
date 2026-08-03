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
    /// of that destination's slots" must be one atomic step against eviction.
    table: Mutex<Box<[Cell]>>,
    gate: AdmissionGate,
    /// One monotonic source for both the use order and the admission stamp, so
    /// a re-admitted node can never look like the occupant it replaced.
    stamp: AtomicU64,
    admitted: AtomicU64,
    evicted: AtomicU64,
    refused: AtomicU64,
    config: FleetConfiguration,
}

/// One cell of the table: when it was last reserved against, and what occupies
/// it.
///
/// Both scans a reservation runs — find this node, choose an eviction — read
/// only the fields held here, so they walk the table's own memory. The
/// destination record behind the pointer is reached for the cell a scan
/// selects, and for no other.
struct Cell {
    last_used: u64,
    occupant: Option<Occupant>,
}

/// What a live cell holds: which node it stands for, which occupancy this is,
/// and the destination record itself.
struct Occupant {
    node: NodeId,
    /// The stamp this occupant was admitted at. It tells this occupancy of a
    /// cell from every earlier one.
    admitted_at: u64,
    destination: Arc<Destination>,
}

/// One live destination: what bounds the work queued against it, and what paces
/// that work.
///
/// It holds no transport channel and no queue on purpose. The channel sits
/// behind the [`ResponseSender`](crate::router::ResponseSender) seam, and the
/// queue and its worker belong to the typed sender. That is what keeps the
/// fleet untyped and free of transport vocabulary.
///
/// A destination lives only while it occupies a cell. Its pacing goes with it:
/// a destination evicted and admitted again starts from the present rather than
/// from the schedule it left. What survives eviction is the operator's real
/// ceiling — `max_destinations` multiplied by `sends_per_second` — because a
/// live destination is what a cell is.
pub(crate) struct Destination {
    slots: Arc<Semaphore>,
    rate: RateLimit,
}

/// One send slot, taken on one live destination.
///
/// Holding one means the destination is live, one of its slots is taken, and
/// the gate was open when the slot was taken. A close that has already begun
/// cannot end while one is held. Dropping it releases the slot and leaves the
/// admission gate. [`Reservation::commit`] is the only other way out, and it
/// leaves the gate only after the slot has been handed on.
///
/// Never hold one across an await. Shutdown waits for every live ticket, so a
/// reservation held across an await holds the whole process's shutdown with it.
pub(crate) struct Reservation<'a> {
    slot: usize,
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
            table: Mutex::new(
                (0..config.max_destinations)
                    .map(|_| Cell::empty())
                    .collect(),
            ),
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
    pub(in crate::router) async fn close(&self) {
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

    /// The cell `node` occupies and the stamp it was admitted at, if it is
    /// live. Both together tell one occupancy from every other, so an eviction
    /// and a re-admission into the same cell are observable.
    #[cfg(test)]
    pub(crate) fn live(&self, node: NodeId) -> Option<(usize, u64)> {
        self.table
            .lock()
            .iter()
            .enumerate()
            .find_map(|(slot, cell)| {
                let occupant = cell.occupant.as_ref()?;
                (occupant.node == node).then_some((slot, occupant.admitted_at))
            })
    }

    /// How many cells hold `node`. Never more than one — see
    /// [`DestinationFleet::find_or_admit`].
    #[cfg(test)]
    pub(crate) fn cells_holding(&self, node: NodeId) -> usize {
        self.table
            .lock()
            .iter()
            .filter(|cell| cell.holds(node))
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
        self.table
            .lock()
            .iter()
            .filter(|cell| cell.occupant.is_some())
            .count()
    }

    /// How many of `node`'s slots are free, if it is live.
    #[cfg(test)]
    pub(crate) fn available(&self, node: NodeId) -> Option<usize> {
        self.table.lock().iter().find_map(|cell| {
            let occupant = cell.occupant.as_ref()?;
            (occupant.node == node).then(|| occupant.destination.slots.available_permits())
        })
    }

    /// Whether admission is closed.
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.gate.is_closed()
    }

    /// How many reservations have entered the gate and have not left it.
    #[cfg(test)]
    pub(crate) fn admitted_now(&self) -> u64 {
        self.gate.count()
    }

    fn take_slot<'a>(
        &'a self,
        table: &mut [Cell],
        node: NodeId,
        ticket: GateTicket<'a>,
    ) -> Result<Reservation<'a>, Refusal> {
        let (slot, destination) = self.find_or_admit(table, node)?;
        // Taken under the table lock, so "nothing in flight" is an observation
        // eviction can trust: no slot can be taken on a cell being evicted.
        let permit = Arc::clone(&destination.slots)
            .try_acquire_owned()
            .map_err(|_| Refusal::NoSlot)?;
        Ok(Reservation {
            slot,
            destination,
            permit,
            ticket,
        })
    }

    /// Finds `node`'s cell or gives it one, and stamps it as reserved against.
    ///
    /// At most one cell holds a given node. A second cell for the same node
    /// would hand it a second semaphore, and its slot bound would be twice what
    /// the operator configured. The search below runs before every admission,
    /// which is what keeps that true.
    ///
    /// The stamp is taken before the caller tries for a permit, so it means
    /// "last reserved against". A destination whose slots are all taken would
    /// otherwise never refresh it, and would be the first eviction candidate
    /// the moment it went idle — it would lose its cell under exactly the
    /// load that wants it kept.
    fn find_or_admit(
        &self,
        table: &mut [Cell],
        node: NodeId,
    ) -> Result<(usize, Arc<Destination>), Refusal> {
        let live = table.iter().enumerate().find_map(|(slot, cell)| {
            let occupant = cell.occupant.as_ref()?;
            (occupant.node == node).then(|| (slot, Arc::clone(&occupant.destination)))
        });
        let (slot, destination) = if let Some(found) = live {
            found
        } else {
            let slot = self.free_cell(table).ok_or(Refusal::NoDestination)?;
            let destination = Arc::new(Destination::new(self.config));
            table[slot].occupant = Some(Occupant {
                node,
                admitted_at: self.next_stamp(),
                destination: Arc::clone(&destination),
            });
            self.admitted.fetch_add(1, Relaxed);
            (slot, destination)
        };
        table[slot].last_used = self.next_stamp();
        Ok((slot, destination))
    }

    /// An empty cell, else the least recently used idle one, which is evicted.
    ///
    /// A destination with a send in flight is never a candidate: evicting it
    /// would drop the table's reference while a worker still holds its slots,
    /// so a later admission of the same node would hand out a second set.
    ///
    /// Whether a cell is idle is the one thing this walk cannot read from the
    /// table, because a permit goes back outside the lock. So a cell is reached
    /// for only while it improves on the best candidate so far, which leaves
    /// the walk over the stamps themselves.
    fn free_cell(&self, table: &mut [Cell]) -> Option<usize> {
        if let Some(slot) = table.iter().position(|cell| cell.occupant.is_none()) {
            return Some(slot);
        }
        let mut candidate: Option<(u64, usize)> = None;
        for (slot, cell) in table.iter().enumerate() {
            if candidate.is_none_or(|(best, _)| cell.last_used < best) && cell.is_idle(self.config)
            {
                candidate = Some((cell.last_used, slot));
            }
        }
        let (_, idle) = candidate?;
        table[idle].occupant = None;
        self.evicted.fetch_add(1, Relaxed);
        Some(idle)
    }

    fn next_stamp(&self) -> u64 {
        self.stamp.fetch_add(1, Relaxed) + 1
    }
}

impl Cell {
    /// A cell no destination has ever occupied.
    const fn empty() -> Self {
        Self {
            last_used: 0,
            occupant: None,
        }
    }

    /// Whether this cell holds `node`.
    fn holds(&self, node: NodeId) -> bool {
        self.occupant
            .as_ref()
            .is_some_and(|occupant| occupant.node == node)
    }

    /// Whether this cell's destination has nothing queued and nothing in
    /// flight. An empty cell is not idle: it is free, which the caller finds
    /// first.
    fn is_idle(&self, config: FleetConfiguration) -> bool {
        self.occupant.as_ref().is_some_and(|occupant| {
            occupant.destination.slots.available_permits() == config.slots_each
        })
    }
}

impl Destination {
    fn new(config: FleetConfiguration) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(config.slots_each)),
            rate: RateLimit::new(config.sends_per_second),
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

    /// The destination itself, for a sender that must keep it alive.
    pub(crate) const fn destination(&self) -> &Arc<Destination> {
        &self.destination
    }

    /// Hands the slot to `queue`, then leaves the admission gate.
    ///
    /// The gate covers the whole hand-over, not the reservation alone. A
    /// shutdown that has seen the count reach zero must find nobody still about
    /// to queue work, so the hand-over runs inside a closure the gate outlives.
    /// `queue` reports only whether it took the slot, so the slot cannot leave
    /// through the return value. The slot itself stays taken until the permit
    /// `queue` was given drops, which is what keeps the destination unevictable
    /// while its send is in flight.
    pub(crate) fn commit<E, F>(self, queue: F) -> Result<(), E>
    where
        F: FnOnce(OwnedSemaphorePermit) -> Result<(), E>,
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
