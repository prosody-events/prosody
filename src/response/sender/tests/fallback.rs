//! What a responder does when the first endpoint it tries does not answer.

use super::{Harness, attempts_on, paused};
use crate::router::SendFailure;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{Script, advertised_port, node, port};
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tonic::Code;

/// The node the responses in this suite are addressed to.
const TARGET: u8 = 1;

/// Two other nodes, enough to take every cell of the table below.
const CROWD: [u8; 2] = [2, 3];

/// Cells and slots the fleet here holds.
const CELLS: usize = 2;
const SLOTS: usize = 2;

/// Attempts one response may make against one endpoint.
///
/// One, deliberately. A budget shared between the two endpoints would make the
/// fallback unreachable at exactly this setting, so this is the value that
/// proves the budget is per endpoint.
const ATTEMPTS: u32 = 1;

/// Sends per second in the pacing case, and the interval that follows from it.
/// One per second, so no rounding can hide a turn that was never claimed.
const PACED: u32 = 1;
const PERIOD: Duration = Duration::from_secs(1);

/// A direct endpoint that does not answer is retried on the advertised endpoint
/// inside the same response, and the next response to that node starts where
/// the last one succeeded.
///
/// The two endpoints are distinct ports, so the counts below say which endpoint
/// each attempt reached. Three attempts in total is what separates a remembered
/// preference from a route decided again: without the memory the second
/// response would try the dead endpoint once more, and there would be four.
#[test]
fn a_failed_direct_endpoint_falls_back_and_is_then_remembered() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::dual_homed(settings())?;
        let fleet = harness.fleet();
        harness.script(
            TARGET,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        );

        harness.send(TARGET)?;
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        assert_eq!(
            attempts_on(&drained.deliveries, port(TARGET)),
            1,
            "the dead endpoint must be tried once, by the first response alone"
        );
        assert_eq!(
            attempts_on(&drained.deliveries, advertised_port(TARGET)),
            2,
            "the answering endpoint must serve the first response's fallback and the whole of the \
             second"
        );
        assert_eq!(drained.sent, 2, "both responses must be delivered");
        assert_eq!(
            fleet.remembered(),
            1,
            "the destination that answered must remember which endpoint did"
        );
        assert!(
            fleet.live(node(TARGET)).is_some(),
            "the preference must live in the destination's own cell"
        );
        Ok(())
    })
}

/// A remembered endpoint dies with the cell that holds it.
///
/// The first response falls back and the destination remembers the endpoint
/// that answered; the second reaches that endpoint alone, which is the memory
/// working. Two other nodes then take every cell, so this destination is
/// evicted. The next response therefore walks the route from the start and
/// reaches the dead direct endpoint again. A verdict held anywhere the table
/// cannot evict — a map keyed by node id, say — would survive that eviction,
/// and the last response would never touch the dead endpoint.
#[test]
fn an_evicted_destination_forgets_which_endpoint_answered() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut harness = Harness::dual_homed(settings())?;
        let fleet = harness.fleet();
        harness.script(
            TARGET,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        );

        harness.send(TARGET)?;
        assert_eq!(
            harness.next_delivery().await?.port,
            port(TARGET),
            "the first response must try the direct endpoint first"
        );
        assert_eq!(
            harness.next_delivery().await?.port,
            advertised_port(TARGET),
            "the first response must fall back to the endpoint that answers"
        );
        harness.send(TARGET)?;
        assert_eq!(
            harness.next_delivery().await?.port,
            advertised_port(TARGET),
            "the second response must start at the remembered endpoint"
        );

        // A recorded delivery means that response's worker finished it and gave
        // the slot back, so the next admission finds an idle cell to take
        // without waiting on a clock.
        for index in CROWD {
            harness.send(index)?;
            assert_eq!(
                harness.next_delivery().await?.port,
                port(index),
                "each crowding response must reach the node it was queued for"
            );
        }
        assert!(
            fleet.live(node(TARGET)).is_none(),
            "the crowd must have evicted the destination that remembered an endpoint"
        );

        harness.send(TARGET)?;
        let drained = harness.drain().await?;
        assert_eq!(
            attempts_on(&drained.deliveries, port(TARGET)),
            1,
            "the last response must try the dead direct endpoint again, because the verdict went \
             with the cell"
        );
        assert_eq!(
            attempts_on(&drained.deliveries, advertised_port(TARGET)),
            1,
            "the last response must fall back again"
        );
        assert_eq!(drained.sent, 5, "every response must be delivered");
        Ok(())
    })
}

/// Every attempt one response makes claims the destination's pacing, the
/// fallback included.
///
/// The direct endpoint never answers and the entry point does, so one response
/// makes exactly two attempts under an allowance of one attempt per endpoint.
/// The destination is paced at one send per second, so the second attempt must
/// go a whole second after the first. An attempt that claimed no turn would go
/// out at the same instant as the one before it, and this destination would be
/// asked for twice what its rate limit allows.
#[test]
fn every_attempt_of_one_response_claims_the_destination_pacing() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::dual_homed(FleetConfiguration {
            sends_per_second: PACED,
            ..settings()
        })?;
        harness.script(
            TARGET,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        );
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        let [first, second] = drained.deliveries.as_slice() else {
            bail!(
                "one response over two endpoints must make two attempts, not {}",
                drained.deliveries.len()
            );
        };
        assert_eq!(
            (first.port, second.port),
            (port(TARGET), advertised_port(TARGET)),
            "the response must try the direct endpoint and then the entry point"
        );
        assert_eq!(
            second.at.duration_since(first.at),
            PERIOD,
            "the fallback attempt must wait its own turn on the destination's rate limit"
        );
        Ok(())
    })
}

/// A destination that refused the response still names the endpoint that
/// refused it.
///
/// The direct endpoint answers a status of its own, which is the node speaking
/// rather than the endpoint failing. So the walk stops there — the entry point
/// is never tried — and the endpoint that spoke is what the destination
/// remembers, though nothing was delivered. A responder that only remembered a
/// delivery would forget it here and try the whole route again next time.
#[test]
fn an_endpoint_that_refused_the_response_is_still_the_one_that_answered() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::dual_homed(settings())?;
        let fleet = harness.fleet();
        harness.script(
            TARGET,
            Script::Fail {
                failure: SendFailure::Status(Code::Internal),
                times: usize::MAX,
            },
        );
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        assert_eq!(
            attempts_on(&drained.deliveries, port(TARGET)),
            1,
            "the endpoint that answered must be tried once and not again"
        );
        assert_eq!(
            attempts_on(&drained.deliveries, advertised_port(TARGET)),
            0,
            "a node that answered for itself must not have its other endpoint tried"
        );
        assert_eq!(
            drained.sent, 0,
            "the refused response must not count as sent"
        );
        assert_eq!(
            fleet.remembered(),
            1,
            "the destination must remember the endpoint that answered, refusal and all"
        );
        Ok(())
    })
}

/// A destination forgets an endpoint that stops answering.
///
/// The first response falls back and is delivered, so the destination remembers
/// the entry point. That entry point then goes silent too, and the next
/// response walks the whole route without an answer. A verdict kept past that
/// would send every later response to a dead address first, for as long as the
/// cell lived.
///
/// The runtime is single threaded, so the worker is not running while the
/// assertions are. A recorded attempt therefore means the response that made it
/// has gone as far as it can, and what it left in the cell is settled.
#[test]
fn a_destination_forgets_an_endpoint_that_stopped_answering() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut harness = Harness::dual_homed(settings())?;
        let fleet = harness.fleet();
        harness.script(
            TARGET,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        );

        harness.send(TARGET)?;
        assert_eq!(
            harness.next_delivery().await?.port,
            port(TARGET),
            "the first response must try the direct endpoint first"
        );
        assert_eq!(
            harness.next_delivery().await?.port,
            advertised_port(TARGET),
            "the first response must fall back to the endpoint that answers"
        );
        assert_eq!(
            fleet.remembered(),
            1,
            "the delivered response must leave the endpoint that took it remembered"
        );

        harness.script_advertised(
            TARGET,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        );
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        assert_eq!(drained.sent, 1, "only the first response must be delivered");
        assert!(
            fleet.live(node(TARGET)).is_some(),
            "the destination must still hold its cell"
        );
        assert_eq!(
            fleet.remembered(),
            0,
            "an endpoint that answered nothing must not stay remembered"
        );
        Ok(())
    })
}

/// An endpoint that answers nothing at all still leaves the other one tried.
///
/// The direct endpoint never answers and never refuses, which is what a
/// firewall that drops packets looks like — and it is the failure the fallback
/// exists for, because an address that belongs to something unrelated here is
/// exactly what a misapplied label reaches. It is given a slice of the response
/// deadline rather than all of it, so the entry point is still reachable inside
/// the same response. Handing the first endpoint the whole deadline would end
/// the response there with the working endpoint untried.
#[test]
fn a_direct_endpoint_that_answers_nothing_still_leaves_the_entry_point_tried() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::dual_homed(settings())?;
        // Nothing ever releases the barrier: only the slice of the deadline
        // this endpoint was given can end the attempt.
        harness.script(TARGET, Script::Hold(Arc::new(Semaphore::new(0))));
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        assert_eq!(
            attempts_on(&drained.deliveries, port(TARGET)),
            1,
            "the silent endpoint must be tried once"
        );
        assert_eq!(
            attempts_on(&drained.deliveries, advertised_port(TARGET)),
            1,
            "the response must reach the entry point while its own deadline still has room"
        );
        assert_eq!(drained.sent, 1, "the response must be delivered");
        Ok(())
    })
}

/// A refusal from a process that is not the target does not end the walk.
///
/// A relay that already sent the frame on answers `FAILED_PRECONDITION`, and a
/// relay with no capacity answers `RESOURCE_EXHAUSTED`. Both reach a responder
/// as the status of the endpoint it dialed, so reading either as the target's
/// own word would leave the entry point — the one address that works — untried
/// for every response to that node.
#[test]
fn a_status_only_a_relay_answers_does_not_end_the_walk() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        for refusal in [Code::FailedPrecondition, Code::ResourceExhausted] {
            let harness = Harness::dual_homed(settings())?;
            harness.script(
                TARGET,
                Script::Fail {
                    failure: SendFailure::Status(refusal),
                    times: usize::MAX,
                },
            );
            harness.send(TARGET)?;

            let drained = harness.drain().await?;
            assert_eq!(
                attempts_on(&drained.deliveries, advertised_port(TARGET)),
                1,
                "a response refused {refusal:?} must still reach the entry point"
            );
            assert_eq!(drained.sent, 1, "the entry point must deliver it");
        }
        Ok(())
    })
}

/// The fleet every case here runs against.
fn settings() -> FleetConfiguration {
    FleetConfiguration {
        max_destinations: CELLS,
        slots_each: SLOTS,
        max_send_attempts: ATTEMPTS,
        ..FleetConfiguration::default()
    }
}
