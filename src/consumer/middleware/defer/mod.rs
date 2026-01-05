//! Defer middleware for handling transient failures.
//!
//! This module provides deferral handling for both messages and timers,
//! allowing keys to be unblocked between retry attempts while maintaining
//! ordering guarantees.

pub mod config;
pub mod decider;
pub mod error;
pub mod failure_tracker;
pub mod message;
pub mod segment;
pub mod timer;

pub use config::{DeferConfigError, DeferConfiguration, DeferConfigurationBuilder};
pub use decider::{AlwaysDefer, DeferralDecider, NeverDefer, TraceBasedDecider};
pub use error::DeferInitError;
pub use message::{MessageDeferMiddleware, MessageLoader};

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
