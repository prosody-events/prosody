//! How one response's deadline is divided between the endpoints of its route.

use super::{Harness, paused};
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

/// One in this many of what is left is what an endpoint keeps for the fallback
/// behind it. Written here as data, so the split is not read back from the
/// constant that decides it.
const RESERVED: u32 = 4;

/// How far apart a share this suite computes and the instant a sleep woke at
/// may be. Tokio rounds a deadline up to the whole millisecond, and every share
/// these cases tell apart is seconds wide, so a millisecond hides nothing.
const GRANULARITY: Duration = Duration::from_millis(1);

/// Attempts one response may make against one endpoint. One, so a case that
/// counts attempts counts endpoints. The silent case below asks for more, and
/// its subject is what a spent share does to them.
const RETRIED: u32 = 1;

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
        let harness = Harness::dual_homed(settings())?;
        // Only the share of the deadline this endpoint was given can end the
        // attempt.
        harness.script(TARGET, held());
        harness.send(TARGET).await?;

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
/// Nothing answers there, and there is no candidate behind it to keep time for,
/// so it must hold the response until the deadline itself. A partial share
/// would give the response up with time it could still spend, and nowhere left
/// to spend it.
///
/// Every shape of route ends at such an endpoint, and each is checked here. A
/// route of one candidate has it first, whether or not this destination
/// answered there before. A route of two reaches it once the first fails.
#[test]
fn the_last_endpoint_of_a_route_spends_what_is_left() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let alone = Harness::new(settings())?;
        alone.script(TARGET, held());
        spends_what_is_left(alone, &[port(TARGET)]).await?;

        // The same route, once its one endpoint is the one the destination
        // remembers. What it answered before decides nothing here: there is
        // still no candidate behind it to keep time for.
        let mut remembered = Harness::new(settings())?;
        remembered.send(TARGET).await?;
        assert_eq!(
            remembered.next_delivery().await?.port,
            port(TARGET),
            "the first response must reach the one endpoint the route offers"
        );
        remembered.script(TARGET, held());
        spends_what_is_left(remembered, &[port(TARGET)]).await?;

        let walked = Harness::dual_homed(settings())?;
        walked.script(TARGET, dead(usize::MAX));
        walked.script_advertised(TARGET, held());
        spends_what_is_left(walked, &[port(TARGET), advertised_port(TARGET)]).await
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
        harness.send(TARGET).await?;
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

        // The next response starts only after the first has finished, so the
        // endpoint it starts at is the one the first response left remembered.
        harness.script(TARGET, dead(0));
        harness.script_advertised(TARGET, held());
        harness.send(TARGET).await?;

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

/// Sends one response, and proves the last endpoint of its route spent
/// everything that response had left.
///
/// `route` names every endpoint the walk must reach, in the order it must reach
/// them. The caller scripts the last of them to answer nothing, so the job's
/// own deadline is what ends the response.
async fn spends_what_is_left(harness: Harness, route: &[u16]) -> Result<()> {
    harness.send(TARGET).await?;
    let drained = harness.drain().await?;
    // The send completes after its final attempt. Thus, no other operation
    // moves the clock before this read.
    let ended = Instant::now();
    let Some(last) = drained.deliveries.last() else {
        bail!("the response must reach the endpoints its route offers");
    };
    let walked: Vec<u16> = drained
        .deliveries
        .iter()
        .map(|delivery| delivery.port)
        .collect();
    assert_eq!(
        walked, route,
        "the response must try the endpoints of its route in order"
    );
    assert_eq!(
        drained.dropped, 1,
        "nothing delivered the response, so it must be dropped"
    );
    let spent = ended.duration_since(last.at);
    assert!(
        is_about(spent, left(last)),
        "the sender gave the last endpoint up after {spent:?} of the {:?} the response had left",
        left(last)
    );
    Ok(())
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

/// An endpoint that answers nothing at all: nothing ever releases the barrier,
/// so only a deadline can end an attempt made against it.
fn held() -> Script {
    Script::Hold(Arc::new(Semaphore::new(0)))
}

/// The fleet every case here runs against.
fn settings() -> FleetConfiguration {
    FleetConfiguration::default()
}
