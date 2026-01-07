//! Message defer middleware for handling transient message failures.
//!
//! Defers transiently-failed messages to timer-based retry instead of blocking
//! the partition, maintaining throughput during downstream outages.
//!
//! - Failed messages are stored in `deferred_offsets` table
//! - `DeferredMessage` timers trigger retry without blocking the key
//! - New messages queue behind failed ones (preserving order)
//! - Keys are unblocked between retry attempts
//!
//! # Modules
//!
//! - [`handler`]: Message defer handler and middleware implementation
//! - [`loader`]: Message loaders for reloading messages from Kafka
//! - [`store`]: Storage trait and implementations for deferred message offsets

pub mod handler;
pub mod loader;
pub mod store;

pub use handler::{
    DeferStoreProviders, MessageDeferMiddleware, MessageStoreProvider, TimerStoreProvider,
};
