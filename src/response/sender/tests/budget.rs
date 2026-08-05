//! How one response's deadline is divided between the endpoints of its route.

use super::{Harness, attempts_on, paused};
use crate::router::SendFailure;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{Delivery, Script, advertised_port, port};
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::Instant;

/// The node the responses in this suite are addressed to.
const TARGET: u8 = 1;

/// Cells and slots the fleet here holds. Deep enough to queue a backlog against
/// one destination.
const CELLS: usize = 2;
const SLOTS: usize = 4;

/// One in this many of what is left is what an endpoint keeps for the fallback
/// behind it. Written here as data, so the split is not read back from the
/// constant that decides it.
const RESERVED: u32 = 4;

/// Sends per second in the paced case, and the interval that follows from it.
/// One per second, so no rounding can hide a turn that was never claimed.
const PACED: u32 = 1;
const PERIOD: Duration = Duration::from_secs(1);

/// How far apart a share this suite computes and the instant a sleep woke at
/// may be. Tokio rounds a deadline up to the whole millisecond, and every share
/// these cases tell apart is seconds wide, so a millisecond hides nothing.
const GRANULARITY: Duration = Duration::from_millis(1);

/// Responses the paced case queues at once. Enough that turns are claimed
/// ahead of the last one, so its own turn is what that case is about.
const BACKLOG: u32 = 4;

/// The deadline every response of the paced backlog carries.
///
/// It is what makes the last response of the backlog the subject. Two turns go
/// before that response, so it has 1.2 s left when its worker reaches it. Its
/// own turn is 1 s away, so it still has time to send. The first response
/// proved its endpoint, so the share that endpoint gets is 0.9 s — less than
/// the wait. A wait charged to the share would therefore end that response
/// before it dialed.
const PACED_DEADLINE: Duration = Duration::from_millis(3_200);

/// Attempts one response may make against one endpoint. One, so a case that
/// counts attempts counts endpoints. The silent case below asks for more, and
/// its subject is what a spent share does to them.
const ATTEMPTS: u32 = 1;
const RETRIED: u32 = 3;

/// An endpoint that answers nothing gives the fallback everything it kept.
///
/// The direct endpoint never answers and never refuses, which is what a
/// firewall that drops packets looks like — and it is the failure the fallback
/// exists for, because an address that belongs to something unrelated here is
/// exactly what a misapplied label reaches. It has never answered, so it gets
/// one share of what is left and the entry point gets the rest. Handing the
/// first endpoint the whole deadline would end the response there with the
/// working endpoint untried.
///
/// It is tried once rather than once per attempt: the share is gone when the
/// first attempt ends, and a retry into a spent share would claim one of the
/// destination's turns and send nothing.
#[test]
fn an_endpoint_that_answers_nothing_gives_the_fallback_what_it_kept() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::dual_homed(FleetConfiguration {
            max_send_attempts: RETRIED,
            ..settings()
        })?;
        // Nothing ever releases the barrier: only the share of the deadline
        // this endpoint was given can end the attempt.
        harness.script(TARGET, Script::Hold(Arc::new(Semaphore::new(0))));
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        let [silent, fallback] = drained.deliveries.as_slice() else {
            bail!(
                "one response over two endpoints must make two attempts under an allowance of \
                 {RETRIED}, not {}",
                drained.deliveries.len()
            );
        };
        assert_eq!(
            (silent.port, fallback.port),
            (port(TARGET), advertised_port(TARGET)),
            "the response must try the silent endpoint and then the entry point"
        );
        let kept = left(silent).saturating_sub(left(silent) / RESERVED);
        assert!(
            is_about(left(fallback), kept),
            "the entry point got {:?} of the deadline, not the {kept:?} the silent endpoint did \
             not keep",
            left(fallback)
        );
        assert_eq!(drained.sent, 1, "the response must be delivered");
        Ok(())
    })
}

/// The last endpoint of a route spends everything the response has left.
///
/// The direct endpoint answers at once that it is the wrong endpoint, so the
/// walk reaches the entry point with nearly the whole deadline. Nothing answers
/// there either, and there is no candidate behind it to keep time for, so it
/// must hold the response until the deadline itself. An endpoint given a share
/// of what is left would give up with time the response could still have spent
/// and nowhere left to spend it.
#[test]
fn the_last_endpoint_of_a_route_spends_what_is_left() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::dual_homed(settings())?;
        harness.script(TARGET, dead(usize::MAX));
        // Nothing ever releases the barrier. The share of the last endpoint is
        // everything that is left, so the job's own deadline is what ends this
        // response.
        harness.script_advertised(TARGET, Script::Hold(Arc::new(Semaphore::new(0))));
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        // The drain returns once the one worker has exited, and this response
        // is the only work the runtime holds, so nothing moves the clock
        // between the last attempt ending and this instant.
        let ended = Instant::now();
        let [wrong, last] = drained.deliveries.as_slice() else {
            bail!(
                "one response over two endpoints must make two attempts, not {}",
                drained.deliveries.len()
            );
        };
        assert_eq!(
            (wrong.port, last.port),
            (port(TARGET), advertised_port(TARGET)),
            "the response must try the direct endpoint and then the entry point"
        );
        assert_eq!(
            drained.dropped, 1,
            "no endpoint answered, so the response must be dropped"
        );
        let spent = ended.duration_since(last.at);
        assert!(
            is_about(spent, left(last)),
            "the last endpoint gave the response up after {spent:?} of the {:?} it had left",
            left(last)
        );
        Ok(())
    })
}

/// The endpoint a destination already answered on keeps the larger share.
///
/// The first response demotes the direct endpoint, so the second starts at the
/// entry point. That endpoint answered once, so it is worth waiting for: it
/// gets everything but the one share kept for the endpoint behind it. Dividing
/// by position instead would give the endpoint that works the small share and
/// hand the rest to the one already known to be dead; giving it the whole
/// deadline would leave the fallback with nothing at all.
#[test]
fn a_remembered_endpoint_keeps_the_larger_share() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut harness = Harness::dual_homed(settings())?;
        harness.script(TARGET, dead(usize::MAX));
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

        // The next response is queued only once the first has finished, so the
        // endpoint it starts at is the one the first response left remembered.
        harness.script(TARGET, dead(0));
        harness.script_advertised(TARGET, Script::Hold(Arc::new(Semaphore::new(0))));
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        // The first response's attempts were read above, so what the drain
        // collects is the second response alone.
        let [remembered, behind] = drained.deliveries.as_slice() else {
            bail!(
                "the second response over two endpoints must make two attempts, not {}",
                drained.deliveries.len()
            );
        };
        assert_eq!(
            (remembered.port, behind.port),
            (advertised_port(TARGET), port(TARGET)),
            "the second response must start at the remembered endpoint and fall back to the other"
        );
        let kept = left(remembered) / RESERVED;
        assert!(
            is_about(left(behind), kept),
            "the endpoint behind the remembered one got {:?} of the deadline, not the {kept:?} \
             the remembered one keeps for it",
            left(behind)
        );
        assert_eq!(drained.sent, 2, "both responses must be delivered");
        Ok(())
    })
}

/// A destination with a queue of its own still reaches the endpoint its route
/// prefers.
///
/// The pacing wait is this process's own backlog rather than anything the
/// endpoint did, so it is not spent against the endpoint's share. Every
/// response here reaches the direct endpoint, which answers at once. A wait
/// charged to the share would end the last response's first attempt before it
/// dialed: that response would report an endpoint that answered nothing, and
/// claim a second turn for the entry point from a destination whose queue is
/// already the problem.
///
/// The turns are the second half of the claim. One claimed turn per delivered
/// response is what the rate limit promises; a turn claimed with no send makes
/// this destination receive less than the operator allowed.
#[test]
fn a_paced_backlog_still_dials_the_endpoint_the_route_prefers() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::dual_homed(FleetConfiguration {
            sends_per_second: PACED,
            send_deadline: PACED_DEADLINE,
            ..settings()
        })?;
        for _ in 0..BACKLOG {
            harness.send(TARGET)?;
        }

        let drained = harness.drain().await?;
        assert_eq!(
            attempts_on(&drained.deliveries, port(TARGET)),
            BACKLOG as usize,
            "every response must reach the endpoint the route prefers"
        );
        assert_eq!(
            attempts_on(&drained.deliveries, advertised_port(TARGET)),
            0,
            "a queue of this process's own must not send anything to the entry point"
        );
        let (Some(first), Some(last)) = (drained.deliveries.first(), drained.deliveries.last())
        else {
            bail!("a queued response must reach the transport");
        };
        assert_eq!(
            last.at.duration_since(first.at),
            PERIOD * (BACKLOG - 1),
            "the backlog must claim one turn per response, and no turn without a send"
        );
        assert_eq!(
            drained.sent,
            u64::from(BACKLOG),
            "every response must be delivered"
        );
        Ok(())
    })
}

/// What one attempt had left of its response's deadline when it was made.
fn left(delivery: &Delivery) -> Duration {
    delivery.deadline.duration_since(delivery.at)
}

/// Whether one share is the other, inside [`GRANULARITY`].
fn is_about(share: Duration, expected: Duration) -> bool {
    share.abs_diff(expected) <= GRANULARITY
}

/// An endpoint that answers nothing for the next `times` attempts.
const fn dead(times: usize) -> Script {
    Script::Fail {
        failure: SendFailure::Unreachable,
        times,
    }
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
