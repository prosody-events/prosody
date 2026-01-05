pub mod config;
pub mod decider;
pub mod error;
pub mod failure_tracker;
pub mod handler;
pub mod loader;
pub mod segment;
pub mod store;
pub mod timer;

pub use config::{DeferConfigError, DeferConfiguration, DeferConfigurationBuilder};
pub use decider::{AlwaysDefer, DeferralDecider, NeverDefer, TraceBasedDecider};
pub use error::DeferInitError;
pub use handler::MessageDeferMiddleware;
pub use loader::MessageLoader;

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
