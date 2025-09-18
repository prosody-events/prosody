//! Timer scheduling and management system for time-based events.
//!
//! This module implements a distributed timer system that schedules events for
//! future execution with persistence and fault tolerance. The system partitions
//! timers into time-based slabs for efficient storage and retrieval.
//!
//! # Core Components
//!
//! - [`Trigger`] - Timer event metadata with key, execution time, and tracing
//!   context
//! - [`TimerManager`] - Primary interface for scheduling and managing timers
//! - [`store::TriggerStore`] - Persistent storage abstraction for timer data
//! - `TriggerScheduler` - In-memory delay queue for precise timer execution
//! - `Slab` - Time-based partition containing related timer events
//!
//! # Example Usage
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
//!         // Process regular messages
//!     }
//!
//!     async fn on_timer<C, T>(&self, context: C, timer: T)
//!     where
//!         C: EventContext,
//!         T: UncommittedTimer,
//!     {
//!         println!("Timer fired for key: {:?}", timer.key());
//!         timer.commit().await;
//!     }
//!
//!     async fn shutdown(self) {
//!         // Cleanup resources
//!     }
//! }
//! ```

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use arc_swap::{ArcSwap, Guard};
use educe::Educe;
use std::sync::Arc;
use tracing::Span;

/// Scheduled timer event containing execution metadata.
///
/// Contains the key, execution time, and tracing context for a timer that will
/// fire at a specific moment. The `span` field is excluded from equality and
/// ordering comparisons to ensure consistent behavior across different tracing
/// contexts.
#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    /// Entity key identifying what this timer belongs to.
    pub key: Key,

    /// When this timer should execute.
    pub time: CompactDateTime,

    /// Tracing span for distributed observability context.
    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    span: Arc<ArcSwap<Span>>,
}

impl Trigger {
    /// Creates a new timer trigger for scheduled execution.
    ///
    /// The span is wrapped in an [`ArcSwap`] for thread-safe access during
    /// concurrent timer operations while preserving distributed tracing
    /// context.
    ///
    /// # Arguments
    ///
    /// * `key` – Entity key identifying what this timer belongs to
    /// * `time` – When this timer should execute
    /// * `span` – Tracing span for distributed observability context
    ///
    /// # Returns
    ///
    /// A new [`Trigger`] instance ready for scheduling.
    #[must_use]
    pub fn new(key: Key, time: CompactDateTime, span: Span) -> Self {
        Self {
            key,
            time,
            span: ArcSwap::from_pointee(span).into(),
        }
    }

    /// Returns the tracing span associated with this trigger.
    ///
    /// The span is wrapped in an atomic guard to enable interior mutability,
    /// allowing the span to be replaced (e.g., with `Span::none()`) to force
    /// deterministic span flushing when timer processing completes.
    ///
    /// # Returns
    ///
    /// A guard containing the current span, which can be used for tracing
    /// operations or span linking.
    #[must_use]
    pub fn span(&self) -> Guard<Arc<Span>> {
        self.span.load()
    }
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
pub mod uncommitted;

/// Maximum concurrent slab loading operations.
const LOAD_CONCURRENCY: usize = 16;

/// Maximum concurrent timer deletion operations.
pub const DELETE_CONCURRENCY: usize = 16;

// Re-export primary APIs for convenient access to timer functionality
pub use manager::TimerManager;
pub use uncommitted::{PendingTimer, UncommittedTimer};
