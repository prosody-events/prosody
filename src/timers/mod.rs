//! Timer system for scheduling and managing time-based events.
//!
//! This module provides a comprehensive timer system that integrates with
//! Prosody's event processing framework. It allows applications to:
//! - Schedule timer events by key and execution time.
//! - Persist timers so they survive application restarts.
//! - Coordinate timer processing across distributed instances.
//! - Partition timers into time-based slabs for efficient storage and queries.
//!
//! Core components:
//! - **`Trigger`**: Metadata for a scheduled event.
//! - **`TimerManager`**: Coordinates scheduling, loading, and delivery.
//! - **`TriggerStore`**: Persistent storage abstraction.
//! - **`Scheduler`**: In-memory queue for precise timing.
//! - **`Slab`**: Fixed-duration partitions for time-range queries.
//!
//! Integration example (in an event handler):
//! ```rust,no_run
//! use prosody::consumer::event_context::EventContext;
//! use prosody::consumer::message::UncommittedMessage;
//! use prosody::consumer::{EventHandler, Keyed, Uncommitted};
//! use prosody::timers::store::TriggerStore;
//! use prosody::timers::{Trigger, UncommittedTimer};
//!
//! struct MyHandler;
//!
//! impl EventHandler for MyHandler {
//!     async fn on_message<C>(&self, _context: C, _message: UncommittedMessage)
//!     where
//!         C: EventContext,
//!     {
//!         // Handle message processing
//!     }
//!
//!     async fn on_timer<C, T>(&self, context: C, timer: T)
//!     where
//!         C: EventContext,
//!         T: UncommittedTimer,
//!     {
//!         // Process the timer event
//!         println!("Timer fired for key: {:?}", timer.key());
//!
//!         // Commit the timer to mark it as processed
//!         timer.commit().await;
//!     }
//!
//!     async fn shutdown(self) {
//!         // Handle shutdown
//!     }
//! }
//! ```
//!
//! The system uses compact, 32-bit representations for time (`CompactDateTime`)
//! and durations (`CompactDuration`). Timers are grouped into slabs of fixed
//! size for efficient range operations.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use educe::Educe;
use tracing::Span;

/// A scheduled timer event with a key, execution time, and tracing span.
///
/// A `Trigger` carries the data needed to identify and execute a timer:
/// - `key`: Logical entity identifier (e.g., user ID, order ID).
/// - `time`: When the timer should fire, stored as `CompactDateTime`.
/// - `span`: Tracing context for observability.
///
/// ## Ordering and Equality
///
/// `Trigger` values are ordered first by `key`, then by `time`. The `span`
/// is ignored in hashing and equality to ensure consistent behavior
/// across contexts.
///
/// # Example
///
/// ```rust
/// use prosody::Key;
/// use prosody::timers::{Trigger, datetime::CompactDateTime};
/// use tracing::Span;
///
/// let trigger = Trigger {
///     key: Key::from("user-123"),
///     time: CompactDateTime::from(1_234_567_890_u32),
///     span: Span::current(),
/// };
/// ```
#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    /// The logical entity this timer is associated with.
    pub key: Key,

    /// The scheduled execution time of the timer.
    pub time: CompactDateTime,

    /// Tracing span for distributed observability.
    ///
    /// Excluded from hash and equality to avoid context-specific differences.
    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    pub span: Span,
}

mod active;
pub mod datetime;
pub mod duration;
pub mod error;
mod loader;
mod manager;
mod queue;
mod scheduler;
mod slab;
mod slab_lock;
pub mod store;
mod uncommitted;

/// The maximum number of concurrent slab load tasks.
///
/// Used internally by the slab loader to bound parallelism.
const LOAD_CONCURRENCY: usize = 16;

/// The maximum number of concurrent delete operations during timer cleanup.
///
/// Controls parallelism when removing timers, for example in `unschedule_all`.
pub const DELETE_CONCURRENCY: usize = 16;

// Re-export primary APIs:
pub use manager::TimerManager;
pub use uncommitted::{ConcreteUncommittedTimer, UncommittedTimer};
