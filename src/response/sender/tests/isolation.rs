//! What one destination's trouble costs the others: nothing.

use super::{Harness, attempts, config, paused, port};
use crate::router::loopback::Script;
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::Instant;

/// The two destinations these suites address.
const NODE_A: u8 = 1;
const NODE_B: u8 = 2;

/// Cells and slots the isolation fleets are built with.
const CELLS: usize = 4;
const SLOTS: usize = 2;

/// Sends per second the pacing suite configures, and the period that implies.
const PACED_PER_SECOND: u32 = 2;
const PERIOD: Duration = Duration::from_millis(500);

/// Responses each destination is given in the pacing suite.
const PACED_RESPONSES: usize = 4;

/// A destination whose transport never answers holds only its own slots.
///
/// The held destination's first attempt is awaited before the healthy one is
/// queued, so the barrier is provably up when the healthy delivery is asserted.
#[test]
fn a_held_destination_never_delays_a_healthy_one() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut harness = Harness::new(config(CELLS, SLOTS))?;
        let barrier = Arc::new(Semaphore::new(0));
        harness.script(NODE_A, Script::Hold(Arc::clone(&barrier)));

        for _ in 0..SLOTS {
            harness.send(NODE_A)?;
        }
        assert_eq!(
            harness.next_delivery().await?.port,
            port(NODE_A),
            "the held destination must reach its transport first"
        );

        harness.send(NODE_B)?;
        assert_eq!(
            harness.next_delivery().await?.port,
            port(NODE_B),
            "a healthy destination must deliver while another one is held"
        );

        barrier.add_permits(1);
        let drained = harness.drain().await?;
        assert_eq!(
            attempts(&drained.deliveries, NODE_A),
            SLOTS - 1,
            "the held destination's remaining response must arrive once released"
        );
        Ok(())
    })
}

/// A destination's rate limit paces that destination and no other.
///
/// Both destinations are configured at the same rate, so a limiter shared
/// between them would interleave their sends onto one schedule and neither
/// would keep its own. Paused time makes the instants exact.
#[test]
fn a_rate_limit_bounds_only_its_own_destination() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut settings = config(CELLS, PACED_RESPONSES);
        settings.sends_per_second = PACED_PER_SECOND;
        let harness = Harness::new(settings)?;
        let start = Instant::now();

        for _ in 0..PACED_RESPONSES {
            harness.send(NODE_A)?;
            harness.send(NODE_B)?;
        }

        let drained = harness.drain().await?;
        let expected: Vec<Duration> = (0..PACED_RESPONSES)
            .map(|step| PERIOD * step as u32)
            .collect();
        for index in [NODE_A, NODE_B] {
            let paced: Vec<Duration> = drained
                .deliveries
                .iter()
                .filter(|delivery| delivery.port == port(index))
                .map(|delivery| delivery.at.duration_since(start))
                .collect();
            if paced != expected {
                bail!("node {index} was paced at {paced:?}, not at {expected:?}");
            }
        }
        Ok(())
    })
}
