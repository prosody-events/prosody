//! Deferral decision abstraction for defer middleware.
//!
//! Provides [`DeferralDecider`] trait for controlling when transient failures
//! should be deferred for retry. Production code uses
//! [`failure_tracker::FailureTracker`](super::failure_tracker::FailureTracker)
//! which tracks failure rates. Tests use [`AlwaysDefer`], [`NeverDefer`], or
//! [`TraceBasedDecider`] for deterministic control.

use portable_atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Controls whether transient failures should be deferred for retry.
///
/// # Implementors
///
/// - [`FailureTracker`](super::failure_tracker::FailureTracker): Production
///   implementation - defers based on failure rate
/// - [`AlwaysDefer`]: Test double - always enables deferral
/// - [`NeverDefer`]: Test double - always disables deferral
/// - [`TraceBasedDecider`]: Test double - returns value set by test harness
///
/// # Thread Safety
///
/// All implementations must be `Clone + Send + Sync + 'static`.
///
/// # Example
///
/// ```ignore
/// // Production usage - unchanged API
/// let middleware = DeferMiddleware::new(config, consumer_config, ...)?;
///
/// // Test usage - inject test double
/// let decider = TraceBasedDecider::new();
/// let middleware = DeferMiddleware::with_decider(config, ..., decider)?;
/// ```
pub trait DeferralDecider: Clone + Send + Sync + 'static {
    /// Returns `true` if messages should be deferred on transient failure.
    ///
    /// # Production Behavior ([`FailureTracker`](super::failure_tracker::FailureTracker))
    ///
    /// Returns `failure_rate < threshold` where:
    /// - `failure_rate` = failures / (failures + successes) in sliding window
    /// - `threshold` = configured threshold (e.g., 0.9)
    ///
    /// # Test Behavior ([`TraceBasedDecider`])
    ///
    /// Returns value set by [`TraceBasedDecider::set_next`] before each message
    /// event.
    fn should_defer(&self) -> bool;
}

// ============================================================================
// Test Doubles
// ============================================================================

/// Always enables deferral.
///
/// Use for testing paths where all transient failures should be deferred.
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysDefer;

impl DeferralDecider for AlwaysDefer {
    fn should_defer(&self) -> bool {
        true
    }
}

/// Never enables deferral.
///
/// Use for testing paths where transient failures should propagate.
#[derive(Clone, Copy, Debug, Default)]
pub struct NeverDefer;

impl DeferralDecider for NeverDefer {
    fn should_defer(&self) -> bool {
        false
    }
}

/// Trace-controlled deferral decisions.
///
/// Test harness sets `next_decision` before each `MessageEvent`,
/// based on the trace's `Transient { defer: bool }` field.
///
/// # Example
///
/// ```ignore
/// let decider = TraceBasedDecider::new();
///
/// // Before processing a message that should be deferred
/// decider.set_next(true);
/// harness.process_message(event);
///
/// // Before processing a message that should NOT be deferred
/// decider.set_next(false);
/// harness.process_message(event);
/// ```
#[derive(Clone, Debug, Default)]
pub struct TraceBasedDecider {
    /// Atomic bool set by test, read by middleware.
    next_decision: Arc<AtomicBool>,
}

impl TraceBasedDecider {
    /// Creates a new decider (defaults to `true`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_decision: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Sets the return value for the next `should_defer()` call.
    pub fn set_next(&self, value: bool) {
        self.next_decision.store(value, Ordering::Relaxed);
    }
}

impl DeferralDecider for TraceBasedDecider {
    fn should_defer(&self) -> bool {
        self.next_decision.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_defer_returns_true() {
        let decider = AlwaysDefer;
        assert!(decider.should_defer());
        assert!(decider.should_defer()); // multiple calls
    }

    #[test]
    fn never_defer_returns_false() {
        let decider = NeverDefer;
        assert!(!decider.should_defer());
        assert!(!decider.should_defer()); // multiple calls
    }

    #[test]
    fn trace_based_decider_defaults_to_true() {
        let decider = TraceBasedDecider::new();
        assert!(decider.should_defer());
    }

    #[test]
    fn trace_based_decider_returns_configured_value() {
        let decider = TraceBasedDecider::new();

        decider.set_next(false);
        assert!(!decider.should_defer());

        decider.set_next(true);
        assert!(decider.should_defer());

        decider.set_next(false);
        assert!(!decider.should_defer());
    }

    #[test]
    fn trace_based_decider_is_clone_safe() {
        let decider1 = TraceBasedDecider::new();
        let decider2 = decider1.clone();

        decider1.set_next(false);
        assert!(!decider2.should_defer()); // Clones share state
    }
}
