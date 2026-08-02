//! The fleet's bounds: a table that never grows, slots that never multiply,
//! and a destination with a send in flight that is never evicted.

use super::{fleet, node};
use crate::router::fleet::Refusal;
use color_eyre::Result;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use tokio::runtime::Builder;

/// Cells in the fleets these suites build.
const CELLS: usize = 4;

/// Slots in each of those cells.
const SLOTS: usize = 2;

/// Distinct nodes a generated stream draws from. Larger than the table, so a
/// stream long enough forces eviction and refusal.
const POOL: u8 = 12;

/// The fleet is bounded whatever it is asked for, and a destination with a send
/// in flight survives every admission that follows.
///
/// One reservation is held on one node for the whole trace, so that node is
/// provably busy. Every step then re-reads the table directly: the length never
/// changes, the live cells never exceed the configured maximum, no destination
/// hands out more slots than it has, and the held node keeps the very cell and
/// generation it started with.
#[quickcheck]
fn prop_the_fleet_stays_bounded_and_keeps_a_busy_destination(steps: Vec<u8>) -> TestResult {
    let fleet = fleet(CELLS, SLOTS);
    let held_node = node(0);
    let Ok(held) = fleet.reserve(held_node) else {
        return TestResult::error("the first reservation on an empty fleet must succeed");
    };
    let Some(origin) = fleet.live(held_node) else {
        return TestResult::error("a reserved node must be live");
    };

    for step in steps {
        let refused = fleet.refused();
        let outcome = fleet.reserve(node(step % POOL));
        let capacity_refusal = u64::from(outcome.is_err());
        drop(outcome);

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
            if let Some(available) = fleet.available(node(index)) {
                assert!(
                    available <= SLOTS,
                    "node {index} offers {available} slots, more than the {SLOTS} it has"
                );
            }
        }
    }

    drop(held);
    TestResult::passed()
}

/// A fleet whose every cell is busy refuses a new destination rather than
/// taking one of theirs, and says so in the count.
#[test]
fn a_full_fleet_of_busy_destinations_refuses_and_counts() -> Result<()> {
    let fleet = fleet(CELLS, SLOTS);
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
        fleet.refused(),
        refused + 1,
        "the refusal must be counted once"
    );
    drop(held);
    Ok(())
}

/// An idle destination gives up its cell before a new node is refused, and the
/// least recently used one goes first.
#[test]
fn an_idle_destination_is_evicted_before_a_new_one_is_refused() -> Result<()> {
    let fleet = fleet(CELLS, SLOTS);
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
    runtime.block_on(async {
        let fleet = fleet(CELLS, SLOTS);
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
