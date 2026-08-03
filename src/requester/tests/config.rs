//! The limits an operator may set, and the ones the registry refuses.

use crate::requester::config::RequesterConfiguration;
use crate::requester::registry::PendingRegistry;
use crate::response::headers::MAX_AWAITED;
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::time::Duration;
use validator::Validate;

/// Longer than any timeout or grace an operator may set.
const OVER_CEILING: Duration = Duration::from_mins(11);

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
            max_timeout: OVER_CEILING,
            ..working.clone()
        },
        // A zero grace lets the sweep race a waiter still entitled to finish.
        RequesterConfiguration {
            sweep_grace: Duration::ZERO,
            ..working.clone()
        },
        RequesterConfiguration {
            sweep_grace: OVER_CEILING,
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
