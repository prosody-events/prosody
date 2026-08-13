//! What one destination's trouble costs the others: nothing.

use super::{Harness, PAYLOAD, attempts, paused};
use crate::peer::response::frame::FrameHeader;
use crate::peer::response::frame::encode::Staged;
use crate::peer::response::frame::tests::CountingCodec;
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::sender::route::PreparedResponse;
use crate::peer::response::sender::{DropReason, ResponseRoute, RouteOutcome, stage};
use crate::peer::router::SendFailure;
use crate::peer::router::loopback::{HANG_GUARD, Script, TestRouter, direct_uri, peer};
use color_eyre::Result;
use opentelemetry::Context;
use std::array;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::timeout;

/// The two destinations these suites address.
const PEER_A: u8 = 1;
const PEER_B: u8 = 2;

/// Concurrent requests sent to the held destination.
const HELD_REQUESTS: usize = 2;

/// A destination whose transport never answers does not delay another peer.
///
/// The held destination's first attempt is awaited before the healthy one is
/// sent, so the barrier is provably up when the healthy delivery is asserted.
#[test]
fn a_held_destination_never_delays_a_healthy_one() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut harness = Harness::new()?;
        let barrier = Arc::new(Semaphore::new(0));
        harness.script(PEER_A, Script::Hold(Arc::clone(&barrier)))?;

        let held = array::from_fn::<_, HELD_REQUESTS, _>(|_| harness.start_send(PEER_A));
        let peer_a = direct_uri(PEER_A)?;
        let peer_b = direct_uri(PEER_B)?;
        let mut held_attempts = usize::from(harness.next_delivery().await?.uri == peer_a);

        let healthy = harness.start_send(PEER_B);
        let mut healthy_attempted = false;
        for _ in 0..HELD_REQUESTS {
            let delivery = harness.next_delivery().await?;
            held_attempts += usize::from(delivery.uri == peer_a);
            healthy_attempted |= delivery.uri == peer_b;
        }
        assert!(
            healthy_attempted,
            "a healthy destination must deliver while another one is held"
        );

        barrier.add_permits(1);
        for send in held {
            send.await??;
        }
        healthy.await??;
        let drained = harness.drain().await?;
        assert_eq!(
            held_attempts + attempts(&drained.deliveries, PEER_A)?,
            HELD_REQUESTS,
            "each held response must make one attempt"
        );
        assert_eq!(
            drained.sent,
            HELD_REQUESTS as u64 + 1,
            "every response must be delivered, not merely attempted"
        );
        Ok(())
    })
}

/// The request deadline cancels a directory lookup that does not answer.
#[test]
fn the_deadline_bounds_route_resolution() -> Result<()> {
    paused()?.block_on(async {
        let (mut route, _deliveries) = TestRouter::new()?;
        route.hold_lookup();
        let harness = Harness::new()?;
        let held = frame(&harness)?;
        let deadline = short_deadline()?;
        assert_deadline(route.deliver(held, deadline, &Context::new())).await?;
        Ok(())
    })
}

/// The request deadline cancels a held transport and classifies expiration.
#[test]
fn the_deadline_bounds_transport_readiness() -> Result<()> {
    paused()?.block_on(async {
        let harness = Harness::new()?;
        harness.script(PEER_A, Script::Hold(Arc::new(Semaphore::new(0))))?;
        let route = harness.router.clone();
        let held = frame(&harness)?;
        let deadline = short_deadline()?;
        assert_deadline(route.deliver(held, deadline, &Context::new())).await?;
        harness.script(
            PEER_A,
            Script::Fail {
                failure: SendFailure::Expired,
                times: 1,
            },
        )?;
        let outcome = route
            .deliver(frame(&harness)?, short_deadline()?, &Context::new())
            .await;
        assert!(matches!(outcome, Err(DropReason::DeadlineExceeded)));
        Ok(())
    })
}

fn frame(harness: &Harness) -> Result<Staged> {
    let payload = PAYLOAD.to_vec();
    let PreparedResponse::Ready(frame) = stage::<CountingCodec, Infallible>(
        FrameHeader {
            target: peer(PEER_A),
            ..harness.header.clone()
        },
        Ok(&payload),
    ) else {
        return Err(color_eyre::eyre::eyre!("the test payload must encode"));
    };
    Ok(frame)
}

fn short_deadline() -> Result<RequestDeadline> {
    RequestDeadline::after(Duration::from_secs(1))
        .ok_or_else(|| color_eyre::eyre::eyre!("the test deadline must be representable"))
}

async fn assert_deadline(
    delivery: impl Future<Output = Result<RouteOutcome, DropReason>>,
) -> Result<()> {
    let outcome = timeout(HANG_GUARD, delivery).await?;
    assert!(matches!(outcome, Err(DropReason::DeadlineExceeded)));
    Ok(())
}
