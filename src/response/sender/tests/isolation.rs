//! What one destination's trouble costs the others: nothing.

use super::{Harness, attempts, config, paused};
use crate::router::loopback::{Script, direct_uri};
use color_eyre::Result;
use std::array;
use std::sync::Arc;
use tokio::sync::Semaphore;

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
        let mut harness = Harness::new(config())?;
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
