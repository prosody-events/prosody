//! Unit tests for consumer-build configuration cross-checks.

use super::validate_recovery_ttl_margin;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, eyre};
use std::time::Duration;

/// The deduplication-TTL floor is `max(48 × recovery_delay, 1h)`
/// (invariant 10). These cases pin both arms of the `max` and the boundary.
mod recovery_ttl_margin {
    use super::*;

    /// Below the 1-hour absolute floor with a small recovery delay (where
    /// `48 × delay` < 1h) is rejected, and the required floor is the hour.
    #[test]
    fn below_the_one_hour_floor_is_rejected() -> Result<()> {
        // 48 × 30s = 1440s < 3600s, so the 1h floor dominates.
        let delay = CompactDuration::new(30);
        let err = validate_recovery_ttl_margin(Duration::from_secs(3_599), delay)
            .err()
            .ok_or_else(|| eyre!("a sub-hour dedup TTL must be rejected"))?;
        assert_eq!(err.required, 3_600);
        Ok(())
    }

    /// Exactly the 1-hour floor passes (the bound is `≥`).
    #[test]
    fn at_the_one_hour_floor_is_allowed() {
        let delay = CompactDuration::new(30);
        assert!(validate_recovery_ttl_margin(Duration::from_hours(1), delay).is_ok());
    }

    /// With a large recovery delay the `48 ×` arm dominates the 1-hour floor:
    /// `48 × 90s = 4320s > 3600s`. A TTL between the two is still rejected.
    #[test]
    fn below_the_multiplier_arm_is_rejected() -> Result<()> {
        let delay = CompactDuration::new(90);
        let err = validate_recovery_ttl_margin(Duration::from_secs(4_319), delay)
            .err()
            .ok_or_else(|| eyre!("a TTL below 48 × recovery_delay must be rejected"))?;
        assert_eq!(err.required, 4_320);
        Ok(())
    }

    /// Exactly the multiplier arm passes (`48 × 90s = 4320s = 72 min`).
    #[test]
    fn at_the_multiplier_arm_is_allowed() {
        let delay = CompactDuration::new(90);
        assert!(validate_recovery_ttl_margin(Duration::from_mins(72), delay).is_ok());
    }

    /// The default dedup TTL (7 days) clears the default recovery delay (30s)
    /// comfortably — the common case must not fail the build.
    #[test]
    fn the_defaults_clear_the_margin() {
        let delay = CompactDuration::new(30);
        assert!(validate_recovery_ttl_margin(Duration::from_hours(7 * 24), delay).is_ok());
    }
}
