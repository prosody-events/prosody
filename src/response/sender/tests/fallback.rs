//! What a responder does when the first endpoint it tries does not answer.

use super::{Harness, attempts_on, paused};
use crate::router::SendFailure;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{Script, advertised_port, node, port};
use color_eyre::Result;

/// The node both responses in this suite are addressed to.
const TARGET: u8 = 1;

/// Cells and slots the fleet here holds.
const CELLS: usize = 2;
const SLOTS: usize = 2;

/// Attempts one response may make against one endpoint.
///
/// One, deliberately. A budget shared between the two endpoints would make the
/// fallback unreachable at exactly this setting, so this is the value that
/// proves the budget is per endpoint.
const ATTEMPTS: u32 = 1;

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
        let harness = Harness::dual_homed(FleetConfiguration {
            max_destinations: CELLS,
            slots_each: SLOTS,
            max_send_attempts: ATTEMPTS,
            ..FleetConfiguration::default()
        })?;
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
