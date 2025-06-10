//! Timer system for scheduling and managing time-based events.
//!
//! This module provides a comprehensive timer system that integrates with
//! Prosody's event processing framework. It allows applications to:
//! - Schedule timer events by key and execution time.
//! - Persist timers so they survive application restarts.
//! - Coordinate timer processing across distributed instances.
//! - Partition timers into time-based slabs for efficient storage and queries.
//!
//! # Architecture
//!
//! The timer system is built around several core components that work together
//! to provide reliable, persistent timer functionality:
//!
//! - **`Trigger`**: Metadata for a scheduled event, containing the key,
//!   execution time, and tracing context.
//! - **`TimerManager`**: Coordinates scheduling, loading, and delivery of
//!   timers between persistent storage and in-memory scheduling.
//! - **`TriggerStore`**: Persistent storage abstraction that maintains timer
//!   data across restarts.
//! - **`Scheduler`**: In-memory queue for precise timing and efficient timer
//!   dispatch.
//! - **`Slab`**: Fixed-duration partitions for time-range queries and efficient
//!   storage organization.
//!
//! # Usage
//!
//! The timer system integrates seamlessly with Prosody's event handling
//! framework. Applications implement the `EventHandler` trait to process both
//! regular messages and timer events:
//!
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
//! # Performance Characteristics
//!
//! The system uses compact, 32-bit representations for time (`CompactDateTime`)
//! and durations (`CompactDuration`) to minimize memory usage. Timers are
//! grouped into slabs of fixed size for efficient range operations and to
//! enable concurrent processing with bounded parallelism.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use educe::Educe;
use tracing::Span;

/// A scheduled timer event with a key, execution time, and tracing span.
///
/// A `Trigger` contains all the metadata needed to identify and execute a timer
/// when it fires. Each trigger is uniquely identified by its key and execution
/// time, and includes tracing context for distributed observability.
///
/// # Ordering and Equality
///
/// `Trigger` values are ordered first by `key`, then by `time`. The `span`
/// field is excluded from hashing and equality comparisons to ensure consistent
/// behavior across different tracing contexts while maintaining the ability to
/// correlate timer execution with distributed traces.
///
/// # Examples
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
    ///
    /// This key identifies the entity (such as a user ID, order ID, or session
    /// ID) that the timer relates to. Multiple timers can share the same
    /// key, allowing for key-based operations like querying all timers for
    /// a specific entity or canceling all timers associated with an entity.
    pub key: Key,

    /// The scheduled execution time of the timer.
    ///
    /// This timestamp represents when the timer should fire and be delivered
    /// to the application for processing. The time is stored as a
    /// `CompactDateTime` for memory efficiency while maintaining sufficient
    /// precision for most timer use cases.
    pub time: CompactDateTime,

    /// Tracing span for distributed observability.
    ///
    /// This span provides context for distributed tracing, allowing timer
    /// execution to be correlated with the original operation that
    /// scheduled the timer. The span is excluded from hash and equality
    /// comparisons to avoid context-specific differences affecting timer
    /// identity.
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
/// This constant controls the parallelism of background slab loading operations
/// to prevent overwhelming the system while ensuring efficient loading of
/// timer data from persistent storage.
const LOAD_CONCURRENCY: usize = 16;

/// The maximum number of concurrent delete operations during timer cleanup.
///
/// This constant controls the parallelism when removing multiple timers,
/// such as during `unschedule_all` operations, to balance performance
/// with resource usage.
pub const DELETE_CONCURRENCY: usize = 16;

// Re-export primary APIs for convenient access to timer functionality
pub use manager::TimerManager;
pub use uncommitted::{PendingTimer, UncommittedTimer};
