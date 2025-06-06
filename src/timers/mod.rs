//! Timer system for scheduling and managing time-based events.
//!
//! This module provides a comprehensive timer system that integrates with
//! Prosody's event processing capabilities. It allows applications to schedule
//! timer events that fire at specific times and are processed alongside regular
//! Kafka messages.
//!
//! ## Overview
//!
//! The timer system consists of several key components:
//!
//! - **Triggers**: Represent scheduled timer events with a key and execution
//!   time
//! - **Timer Manager**: Coordinates timer scheduling, storage, and execution
//! - **Trigger Store**: Provides persistent storage for timer data
//! - **Scheduler**: Manages in-memory timer queues and delivery
//! - **Slabs**: Time-based partitioning for efficient timer storage and
//!   retrieval
//!
//! ## Key Features
//!
//! - **Persistent Storage**: Timers are stored persistently and survive
//!   application restarts
//! - **Distributed Operation**: Multiple consumer instances can coordinate
//!   timer processing
//! - **Key-based Organization**: Timers are organized by keys, maintaining
//!   ordering guarantees
//! - **Efficient Querying**: Time-based slab organization enables efficient
//!   range queries
//! - **Integration**: Seamless integration with existing event processing
//!   pipelines
//!
//! ## Usage
//!
//! Timers are typically used through the consumer's event handler interface:
//!
//! ```rust,no_run
//! use prosody::consumer::message::{EventContext, UncommittedMessage};
//! use prosody::consumer::{EventHandler, Keyed, Uncommitted};
//! use prosody::timers::store::TriggerStore;
//! use prosody::timers::{Trigger, UncommittedTimer};
//!
//! struct MyHandler;
//!
//! impl EventHandler for MyHandler {
//!     async fn on_timer<T>(&self, context: EventContext<T>, timer: UncommittedTimer<T>)
//!     where
//!         T: TriggerStore,
//!     {
//!         // Process the timer event
//!         println!("Timer fired for key: {:?}", timer.key());
//!
//!         // Commit the timer to mark it as processed
//!         timer.commit().await;
//!     }
//!
//!     async fn on_message<T>(&self, _context: EventContext<T>, _message: UncommittedMessage)
//!     where
//!         T: TriggerStore,
//!     {
//!         // Handle message processing
//!     }
//!
//!     async fn shutdown(self) {
//!         // Handle shutdown
//!     }
//! }
//! ```
//!
//! ## Architecture
//!
//! The timer system uses a multi-layered architecture:
//!
//! 1. **Storage Layer**: Persistent storage via [`TriggerStore`]
//!    implementations
//! 2. **Management Layer**: [`TimerManager`] coordinates timer lifecycle
//! 3. **Scheduling Layer**: In-memory scheduling and delivery to consumers
//! 4. **Integration Layer**: Seamless integration with event processing
//!
//! ## Time Representation
//!
//! The system uses compact time representations for efficiency:
//!
//! - [`CompactDateTime`]: 32-bit epoch seconds for timer execution times
//! - [`CompactDuration`]: 32-bit seconds for duration calculations
//! - **Slab-based Partitioning**: Timers are organized into time-based slabs
//!   for efficient access

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use educe::Educe;
use tracing::Span;

/// Represents a scheduled timer event with an associated key and execution
/// time.
///
/// A [`Trigger`] encapsulates the information needed to identify and execute
/// a timer event. It consists of a key for logical grouping and ordering,
/// an execution time, and a tracing span for observability.
///
/// ## Key Properties
///
/// - **Key**: Associates the timer with a logical entity (e.g., user ID, order
///   ID)
/// - **Time**: Specifies when the timer should execute using
///   [`CompactDateTime`]
/// - **Span**: Provides distributed tracing context for the timer event
///
/// ## Ordering and Equality
///
/// Triggers are ordered first by key, then by time. The tracing span is
/// excluded from equality and hash calculations to ensure consistent behavior
/// across different execution contexts.
///
/// ## Example
///
/// ```rust,no_run
/// use prosody::Key;
/// use prosody::timers::{Trigger, datetime::CompactDateTime};
/// use tracing::Span;
///
/// let trigger = Trigger {
///     key: Key::from("user-123"),
///     time: CompactDateTime::from(1234567890_u32),
///     span: Span::current(),
/// };
/// ```
#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    /// The key that identifies the logical entity this timer is associated
    /// with.
    ///
    /// Used for maintaining ordering guarantees and logical grouping of timers.
    pub key: Key,

    /// The time when this timer should execute.
    ///
    /// Represented as a [`CompactDateTime`] for efficient storage and
    /// comparison.
    pub time: CompactDateTime,

    /// Tracing span for observability and debugging.
    ///
    /// Excluded from equality and hash calculations to ensure consistent
    /// behavior across different execution contexts.
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

// Re-export main public APIs
pub use manager::TimerManager;
pub use uncommitted::UncommittedTimer;

const LOAD_CONCURRENCY: usize = 16;

/// Maximum number of concurrent delete operations for timer cleanup.
///
/// This constant controls the parallelism level when performing bulk
/// cleanup operations such as removing all timers for a specific key.
pub const DELETE_CONCURRENCY: usize = 16;
