//! Timer defer middleware for handling transient timer failures.
//!
//! Defers transiently-failed application timers to a background retry queue,
//! maintaining throughput during downstream outages while preserving timer
//! ordering guarantees.
//!
//! # Architecture
//!
//! Timer defer follows the same pattern as message defer:
//! - Failed timers are stored in `deferred_timers` table
//! - `DeferredTimer` triggers retry without blocking the key
//! - New timers queue behind failed ones (preserving order)
//! - Keys are unblocked between retry attempts
//!
//! # Key Differences from Message Defer
//!
//! - Stores `Trigger` with span context (no Kafka reload needed)
//! - First deferral schedules at `original_time` (preserves timer semantics)
//! - Uses `TimerDeferContext` wrapper for unified timer operations

pub mod context;
pub mod handler;
pub mod middleware;
pub mod store;

#[cfg(test)]
mod tests;

pub use context::{TimerDeferContext, TimerDeferContextError};
pub use handler::TimerDeferHandler;
pub use middleware::{TimerDeferMiddleware, TimerDeferProvider};
pub use store::TimerDeferStore;
