//! The limits an operator may set, and the ones the registry refuses.

use crate::requester::config::{MAX_IN_FLIGHT, MIN_TIMEOUT, RequesterConfiguration};
use crate::requester::registry::PendingRegistry;
use crate::response::frame::FrameCap;
use crate::response::headers::MAX_AWAITED;
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::time::Duration;
use validator::Validate;

/// Longer than any timeout or grace an operator may set.
const OVER_CEILING: Duration = Duration::from_mins(11);

/// Shorter than the shortest grace between two sweep passes.
const UNDER_GRACE_FLOOR: Duration = Duration::from_millis(1);

/// The defaults build a working registry, and every degenerate limit is
/// refused before one exists.
#[tokio::test]
async fn a_degenerate_limit_never_builds_a_registry() -> Result<()> {
    let working = RequesterConfiguration::builder().build()?;
    working.validate()?;
    PendingRegistry::new(&working)?;

    let refused = [
        RequesterConfiguration {
            max_in_flight: 0,
            ..working.clone()
        },
        RequesterConfiguration {
            max_in_flight: MAX_IN_FLIGHT + 1,
            ..working.clone()
        },
        RequesterConfiguration {
            max_awaited: 0,
            ..working.clone()
        },
        RequesterConfiguration {
            // Above the wire's own ceiling, so a record naming that many
            // subsystems could not be parsed by the responder reading it.
            max_awaited: MAX_AWAITED + 1,
            ..working.clone()
        },
        RequesterConfiguration {
            max_timeout: Duration::ZERO,
            ..working.clone()
        },
        RequesterConfiguration {
            // Under the shortest timeout a request may ask for, so the accepted
            // range is empty and every call would fail at runtime.
            max_timeout: MIN_TIMEOUT / 2,
            ..working.clone()
        },
        RequesterConfiguration {
            max_timeout: OVER_CEILING,
            ..working.clone()
        },
        // A zero grace lets the sweep race a waiter still entitled to finish.
        RequesterConfiguration {
            sweep_grace: Duration::ZERO,
            ..working.clone()
        },
        // A grace under the floor makes the sweep scan the map continuously.
        RequesterConfiguration {
            sweep_grace: UNDER_GRACE_FLOOR,
            ..working.clone()
        },
        RequesterConfiguration {
            sweep_grace: OVER_CEILING,
            ..working.clone()
        },
        RequesterConfiguration {
            max_response_bytes: FrameCap::MIN_BYTES - 1,
            ..working.clone()
        },
        RequesterConfiguration {
            max_response_bytes: FrameCap::MAX_BYTES + 1,
            ..working.clone()
        },
        // Every limit is plausible alone. Their product is what the registry
        // would have to hold, and it is far over the process budget.
        RequesterConfiguration {
            max_in_flight: MAX_IN_FLIGHT,
            max_awaited: MAX_AWAITED,
            max_response_bytes: FrameCap::MAX_BYTES,
            ..working
        },
    ];
    for config in refused {
        if PendingRegistry::new(&config).is_ok() {
            bail!("a registry was built from {config:?}");
        }
    }
    Ok(())
}
