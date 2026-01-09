//! Defer middleware for transient failure recovery.
//!
//! When a message or timer fails transiently (network error, timeout), it must
//! be retried—but blocking the key during backoff would stall all subsequent
//! items. The defer system solves this by:
//!
//! 1. **Persisting** the failed item to a per-key FIFO queue
//! 2. **Unblocking** the key so later items can process
//! 3. **Rescheduling** a retry after exponential backoff
//! 4. **Restoring** the key's blocked state when retry fires
//!
//! This maintains per-key ordering (critical for Kafka semantics) while
//! allowing cross-key parallelism during retry delays.
//!
//! # Modules
//!
//! - `segment`: Partition identity (`UUIDv5` of `topic/partition:group`)
//! - `message`: Kafka offset queues with retry counts
//! - `timer`: Timer queues with span context and retry counts
//!
//! # Store Backends
//!
//! - **Memory**: Volatile, for testing
//! - **Cassandra**: Persistent, for production (with write-through cache)
//!
//! # Composability
//!
//! Message and timer defer middlewares compose independently via `.layer()`.

use crate::timers::duration::CompactDuration;
use rand::Rng;
use std::cmp::min;

pub mod config;
pub mod decider;
pub mod error;
pub mod message;
pub mod segment;
pub mod timer;

pub use config::{DeferConfigError, DeferConfiguration, DeferConfigurationBuilder};
pub use decider::{AlwaysDefer, DeferralDecider, FailureTracker, NeverDefer, TraceBasedDecider};
pub use error::{CassandraDeferStoreError, DeferInitError};
pub use message::MessageDeferMiddleware;
pub use timer::{TimerDeferMiddleware, TimerDeferProvider};

// ============================================================================
// Utility Functions
// ============================================================================

/// Jittered exponential backoff: `random(1, min(base * 2^retry, max))`.
///
/// Returns `CompactDuration::MIN` (0) for `retry_count == 0` to allow
/// immediate retry on first deferral.
///
/// # Algorithm
///
/// 1. Calculate delay: `base * 2^(retry_count - 1)`
/// 2. Cap at `max_delay`, with minimum of 1 second
/// 3. Apply full jitter: `random(1..=capped_seconds)`
///
/// Full jitter prevents thundering herd when many keys retry simultaneously.
#[must_use]
pub fn calculate_backoff(config: &config::DeferConfiguration, retry_count: u32) -> CompactDuration {
    // No delay for the initial attempt
    if retry_count == 0 {
        return CompactDuration::MIN;
    }

    let base_seconds = u32::try_from(config.base.as_secs()).unwrap_or(u32::MAX);
    let max_delay_seconds = u32::try_from(config.max_delay.as_secs()).unwrap_or(u32::MAX);

    // Calculate exponential backoff: base * 2^(retry_count - 1)
    // Subtract 1 so first retry (count=1) uses base delay
    let multiplier = 2_u32.saturating_pow(retry_count - 1);
    let delay_seconds = base_seconds.saturating_mul(multiplier);

    // Cap at max_delay, with minimum of 1 second.
    // Minimum 1 second ensures a meaningful delay when jitter would
    // otherwise produce 0.
    let capped_seconds = min(delay_seconds, max_delay_seconds).max(1);

    // Apply full jitter: random(1..=capped_seconds)
    let jittered_seconds = rand::rng().random_range(1..=capped_seconds);

    CompactDuration::new(jittered_seconds)
}

/// State of a key in the defer system.
///
/// Tracks whether a key has deferred messages and the current retry count
/// for backoff calculations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeferState {
    /// Key has no deferred messages.
    NotDeferred,
    /// Key has deferred messages with the given retry count.
    Deferred {
        /// Current retry count for backoff calculation.
        retry_count: u32,
    },
}
