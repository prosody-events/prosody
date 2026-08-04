//! What an open set of destinations costs a sender: a bounded table, a queue
//! per cell that serves every occupant of it, and no response unaccounted for.

use super::{Harness, PUBLISHED_NODES, config, node, paused, port};
use crate::router::SendFailure;
use crate::router::loopback::Script;
use color_eyre::Result;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use tokio::task::yield_now;

/// The two destinations the example below addresses.
const NODE_A: u8 = 1;
const NODE_B: u8 = 2;

/// Cells and slots the fleets in this suite are built with. Small, so a
/// generated stream reaches eviction and refusal within a few responses.
const CELLS: usize = 2;
const SLOTS: usize = 2;

/// A generated stream of responses naming more nodes than the fleet holds keeps
/// the fleet inside its table, counts every refusal, accounts for every
/// response it accepted, and delivers each one to the node it was queued for.
///
/// The last of those is what only this layer can prove. One queue serves one
/// cell for the process's whole life, so successive occupants of that cell
/// share it; a response paced or addressed by whatever occupies the cell now,
/// rather than by the destination it was queued against, would reach the wrong
/// node.
///
/// The workers are given a turn between responses, which is what frees slots
/// and makes eviction reachable. How far they get is the executor's business:
/// every assertion here holds whatever progress they made.
#[quickcheck]
fn prop_a_stream_of_many_nodes_stays_bounded_and_is_accounted_for(targets: Vec<u8>) -> TestResult {
    let Ok(runtime) = paused() else {
        return TestResult::error("a paused runtime must be buildable");
    };
    runtime.block_on(async {
        let Ok(harness) = Harness::new(config(CELLS, SLOTS)) else {
            return TestResult::error("a fleet inside every ceiling must be buildable");
        };
        let fleet = harness.fleet();
        let mut refused = 0;
        let mut targeted = Vec::new();

        for target in targets {
            let index = target % PUBLISHED_NODES;
            match harness.send(index) {
                Ok(()) => targeted.push(port(index)),
                Err(_) => refused += 1,
            }
            assert!(
                fleet.live_count() <= CELLS,
                "{} destinations are live, more than the {CELLS} cells the table has",
                fleet.live_count()
            );
            // A refusal the fleet did not count is the queue turning a response
            // away after it held a slot, which cannot happen.
            assert_eq!(
                fleet.refused(),
                refused,
                "every refusal must be the fleet's own, and every one must be counted"
            );
            yield_now().await;
        }

        // The conservation rule — every accepted response ends as exactly one
        // of sent or dropped — is the drain's own assertion.
        let Ok(drained) = harness.drain().await else {
            return TestResult::error("every accepted response must be accounted for");
        };
        for delivery in &drained.deliveries {
            assert!(
                targeted.contains(&delivery.port),
                "a response reached port {}, which no response was queued for",
                delivery.port
            );
        }
        TestResult::passed()
    })
}

/// A node the fleet has no room for still gets its response: the idle occupant
/// gives its cell up, and the queue that cell owns serves the new destination
/// exactly as it served the one before.
///
/// The fleet holds one destination with one slot, so every response after the
/// first needs an eviction. A recorded delivery means its worker finished that
/// response and gave the slot back, so the next response finds the cell idle
/// without waiting on a clock.
#[test]
fn a_new_destination_takes_an_idle_cell_and_is_still_delivered() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut harness = Harness::new(config(1, 1))?;
        let fleet = harness.fleet();

        for index in [NODE_A, NODE_B, NODE_A] {
            harness.send(index)?;
            assert_eq!(
                harness.next_delivery().await?.port,
                port(index),
                "the response must reach the node it was queued for"
            );
        }

        let drained = harness.drain().await?;
        assert_eq!(drained.sent, 3, "every response must be delivered");
        assert_eq!(
            fleet.admitted(),
            3,
            "each response must admit the destination it named"
        );
        assert_eq!(
            fleet.evicted(),
            2,
            "each admission after the first must evict the idle occupant"
        );
        assert_eq!(
            fleet.live(node(NODE_A)).map(|(cell, _)| cell),
            Some(0),
            "the one cell must hold the destination named last"
        );
        Ok(())
    })
}

/// However many nodes a stream names, and however many of their endpoints fail,
/// nothing outside the bounded table remembers which endpoint answered.
///
/// Every node's direct endpoint fails and its advertised endpoint answers, so
/// every delivered response leaves a preference behind. The count of those
/// preferences can never exceed the count of live cells, because a preference
/// lives in a cell and nowhere else. A record kept beside the table would grow
/// with the number of distinct nodes instead.
#[quickcheck]
fn prop_no_endpoint_verdict_is_remembered_outside_the_fleet(targets: Vec<u8>) -> TestResult {
    let Ok(runtime) = paused() else {
        return TestResult::error("a paused runtime must be buildable");
    };
    runtime.block_on(async {
        let Ok(harness) = Harness::dual_homed(config(CELLS, SLOTS)) else {
            return TestResult::error("a fleet inside every ceiling must be buildable");
        };
        let fleet = harness.fleet();
        for index in 0..PUBLISHED_NODES {
            harness.script(
                index,
                Script::Fail {
                    failure: SendFailure::Unreachable,
                    times: usize::MAX,
                },
            );
        }

        // A capacity refusal is the fleet doing its job. Either way the bounds
        // below are the subject, so a refusal is counted rather than failed on.
        let mut refused = 0_usize;
        for target in targets {
            if harness.send(target % PUBLISHED_NODES).is_err() {
                refused += 1;
            }
            yield_now().await;
        }
        assert!(
            u64::try_from(refused).is_ok_and(|refused| refused <= fleet.refused()),
            "{refused} responses were refused, more than the fleet counted"
        );
        if harness.drain().await.is_err() {
            return TestResult::error("every accepted response must be accounted for");
        }

        assert!(
            fleet.live_count() <= fleet.capacity(),
            "{} destinations are live, more than the {} cells the table has",
            fleet.live_count(),
            fleet.capacity()
        );
        assert!(
            fleet.remembered() <= fleet.live_count(),
            "{} endpoint verdicts are remembered, more than the {} live destinations that can \
             hold one",
            fleet.remembered(),
            fleet.live_count()
        );
        TestResult::passed()
    })
}
