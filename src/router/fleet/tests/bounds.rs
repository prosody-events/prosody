//! The fleet's bounds: a table that never grows, slots that never multiply, a
//! destination with a send in flight that is never evicted, and a reservation
//! that leaves the admission gate only after its slot is elsewhere.

use super::{fleet, node};
use crate::router::fleet::Refusal;
use color_eyre::Result;
use futures::poll;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::future::Future;
use std::pin::{Pin, pin};
use std::task::{Context, Waker};
use tokio::runtime::Builder;

/// Cells in the fleets these suites build.
const CELLS: usize = 4;

/// Slots in each of those cells.
const SLOTS: usize = 2;

/// Distinct nodes a generated stream draws from. Larger than the table, so a
/// stream long enough forces eviction and refusal.
const POOL: u8 = 12;

/// Reservations a generated trace keeps at once, beyond the anchor. It equals
/// every slot the fleet has, so a trace can saturate the table and then release
/// it again.
const HELD: usize = CELLS * SLOTS;

/// The fleet is bounded whatever it is asked for, a destination with a send in
/// flight survives every admission that follows, and no node ever occupies two
/// cells.
///
/// One reservation is held on one node for the whole trace, so that node is
/// provably busy. The trace also keeps a bounded set of further reservations,
/// so several destinations are busy at once and the eviction scan has to
/// choose. Every step then re-reads the table directly: the length never
/// changes, the live cells never exceed the configured maximum, no destination
/// hands out more slots than it has, and the held node keeps the very cell and
/// generation it started with.
#[quickcheck]
fn prop_the_fleet_stays_bounded_and_keeps_a_busy_destination(steps: Vec<u8>) -> TestResult {
    let Ok(fleet) = fleet(CELLS, SLOTS) else {
        return TestResult::error("a fleet inside every ceiling must be buildable");
    };
    let held_node = node(0);
    let Ok(held) = fleet.reserve(held_node) else {
        return TestResult::error("the first reservation on an empty fleet must succeed");
    };
    let Some(origin) = fleet.live(held_node) else {
        return TestResult::error("a reserved node must be live");
    };
    let mut busy = Vec::new();

    for step in steps {
        // Release before reserving, so a saturated trace can recover and the
        // eviction path stays reachable after the first few steps.
        if step % 3 == 0 && !busy.is_empty() {
            drop(busy.remove(usize::from(step) % busy.len()));
        }
        let refused = fleet.refused();
        let outcome = fleet.reserve(node(step % POOL));
        let capacity_refusal = u64::from(outcome.is_err());
        match outcome {
            Ok(reservation) if busy.len() < HELD => busy.push(reservation),
            Ok(reservation) => drop(reservation),
            Err(_) => {}
        }

        assert_eq!(
            fleet.refused(),
            refused + capacity_refusal,
            "every refusal is counted, and nothing else is"
        );
        assert_eq!(fleet.capacity(), CELLS, "the table never grows");
        assert!(
            fleet.live_count() <= CELLS,
            "live destinations must stay inside the table"
        );
        assert_eq!(
            fleet.live(held_node),
            Some(origin),
            "a destination with a send in flight must never be evicted"
        );
        for index in 0..POOL {
            assert!(
                fleet.cells_holding(node(index)) <= 1,
                "node {index} occupies more than one cell, so it holds more slots than it has"
            );
            if let Some(available) = fleet.available(node(index)) {
                assert!(
                    available <= SLOTS,
                    "node {index} offers {available} slots, more than the {SLOTS} it has"
                );
            }
        }
    }

    assert_eq!(
        fleet.cells_holding(held_node),
        1,
        "a node with a send in flight must occupy exactly one cell"
    );
    drop(busy);
    drop(held);
    TestResult::passed()
}

/// A fleet whose every cell is busy refuses a new destination rather than
/// taking one of theirs, and says so in the count. A live destination with no
/// free slot is refused for that reason, not for want of a cell.
#[test]
fn a_full_fleet_of_busy_destinations_refuses_and_counts() -> Result<()> {
    let fleet = fleet(CELLS, SLOTS)?;
    let mut held = Vec::new();
    for index in 0..CELLS {
        for _ in 0..SLOTS {
            held.push(fleet.reserve(node(index as u8))?);
        }
    }

    let refused = fleet.refused();
    assert_eq!(
        fleet.reserve(node(POOL)).err(),
        Some(Refusal::NoDestination),
        "a new node must be refused while every cell is busy"
    );
    assert_eq!(
        fleet.reserve(node(0)).err(),
        Some(Refusal::NoSlot),
        "a live node with no free slot must be refused for its slots, not for a cell"
    );
    assert_eq!(
        fleet.refused(),
        refused + 2,
        "both refusals must be counted"
    );
    drop(held);
    Ok(())
}

/// An idle destination gives up its cell before a new node is refused, and the
/// least recently used one goes first.
#[test]
fn an_idle_destination_is_evicted_before_a_new_one_is_refused() -> Result<()> {
    let fleet = fleet(CELLS, SLOTS)?;
    for index in 0..CELLS {
        drop(fleet.reserve(node(index as u8))?);
    }
    // Touching the oldest cell again makes the second-oldest the least recently
    // used, so the assertion below pins the order rather than the position.
    drop(fleet.reserve(node(0))?);

    assert!(
        fleet.reserve(node(POOL)).is_ok(),
        "an idle cell must be given to a new node"
    );
    assert_eq!(
        fleet.evicted(),
        1,
        "exactly one destination must be evicted"
    );
    assert_eq!(
        fleet.admitted(),
        CELLS as u64 + 1,
        "every distinct node must be admitted exactly once"
    );
    assert_eq!(
        fleet.live(node(1)),
        None,
        "the least recently used destination must be the one evicted"
    );
    assert!(
        fleet.live(node(0)).is_some(),
        "a destination used since must survive"
    );
    Ok(())
}

/// Once admission closes, no reservation succeeds — neither for a node that was
/// live before the close nor for one that never was.
#[test]
fn a_closed_fleet_refuses_every_reservation() -> Result<()> {
    let runtime = Builder::new_current_thread().enable_time().build()?;
    let fleet = fleet(CELLS, SLOTS)?;
    runtime.block_on(async {
        drop(fleet.reserve(node(0)));
        fleet.close().await;

        assert!(fleet.is_closed(), "the gate must report itself closed");
        for index in [0, POOL] {
            assert_eq!(
                fleet.reserve(node(index)).err(),
                Some(Refusal::ShuttingDown),
                "node {index} must be refused once admission is closed"
            );
        }
    });
    Ok(())
}

/// A reservation leaves the admission gate only after its slot is somewhere
/// else: handed to a queue, or released.
///
/// This is what a shutdown depends on. A drain that has seen the gate empty
/// must find nobody still about to queue work, so the hand-over has to finish
/// inside the gate. The drain is polled from inside the hand-over itself, which
/// is the only place that ordering is observable.
#[test]
fn a_reservation_leaves_the_gate_only_after_its_slot_does() -> Result<()> {
    let runtime = Builder::new_current_thread().enable_time().build()?;
    let handed = fleet(CELLS, SLOTS)?;
    let released = fleet(CELLS, SLOTS)?;
    runtime.block_on(async {
        let reservation = handed.reserve(node(0))?;
        let mut drain = pin!(handed.close());
        assert!(
            poll!(drain.as_mut()).is_pending(),
            "the drain must wait while a reservation is held"
        );
        let slot = reservation.commit(|slot| {
            assert!(
                still_draining(drain.as_mut()),
                "the gate must still hold the reservation while its slot is handed on"
            );
            slot
        });
        assert!(
            poll!(drain.as_mut()).is_ready(),
            "the drain must finish once the slot has been handed on"
        );
        drop(slot);

        let reservation = released.reserve(node(0))?;
        let mut drain = pin!(released.close());
        assert!(
            poll!(drain.as_mut()).is_pending(),
            "the drain must wait while a reservation is held"
        );
        drop(reservation);
        assert!(
            poll!(drain.as_mut()).is_ready(),
            "the drain must finish once the reservation is released"
        );
        Ok(())
    })
}

/// Whether `drain` is still waiting, polled by hand with a no-op waker.
///
/// `poll!` needs an async context, and the closure a reservation hands its slot
/// through is synchronous. Re-polling after a wakeup is the caller's job here,
/// which every use above does.
fn still_draining(drain: Pin<&mut impl Future<Output = ()>>) -> bool {
    drain
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending()
}
