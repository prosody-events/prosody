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
//! use prosody::consumer::{DemandType, EventHandler, Keyed, Uncommitted};
//! use prosody::timers::store::TriggerStore;
//! use prosody::timers::{Trigger, UncommittedTimer};
//!
//! struct MyHandler;
//!
//! impl EventHandler for MyHandler {
//!     type Payload = serde_json::Value;
//!
//!     async fn on_message<C>(
//!         &self,
//!         _context: C,
//!         _message: UncommittedMessage<serde_json::Value>,
//!         _demand_type: DemandType,
//!     ) where
//!         C: EventContext,
//!     {
//!         // Process regular messages
//!     }
//!
//!     async fn on_timer<C, T>(&self, context: C, timer: T, _demand_type: DemandType)
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
pub use crate::timers::datetime::CompactDateTime;
use crate::timers::error::ParseError;
use arc_swap::ArcSwap;
use educe::Educe;
use serde::Serialize;
use std::sync::Arc;
use strum::EnumCount;
use tokio::sync::Semaphore;
use tracing::Span;

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

/// Classifies a timer by its origin, used to route and account for execution
/// separately across concurrent timer pools.
///
/// Application code should use [`TimerType::Application`] when scheduling
/// timers via [`crate::consumer::event_context::EventContext::schedule`] or
/// [`crate::consumer::event_context::EventContext::clear_and_schedule`].
/// The `DeferredMessage` and `DeferredTimer` variants are reserved for internal
/// middleware use.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    strum::EnumCount,
    strum::VariantArray,
)]
#[serde(rename_all = "camelCase")]
#[repr(i8)]
pub enum TimerType {
    /// Application-scheduled timer. Use this when scheduling timers from a
    /// handler via [`crate::consumer::event_context::EventContext`].
    #[default]
    Application = 0,
    /// Internal: timer scheduled by defer middleware to retry a failed message.
    DeferredMessage = 1,
    /// Internal: timer scheduled by defer middleware to retry a failed timer.
    DeferredTimer = 2,
}

impl From<TimerType> for i8 {
    fn from(timer_type: TimerType) -> Self {
        timer_type as i8
    }
}

impl TryFrom<i8> for TimerType {
    type Error = ParseError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Application),
            1 => Ok(Self::DeferredMessage),
            2 => Ok(Self::DeferredTimer),
            _ => Err(ParseError::UnknownTimerType(value)),
        }
    }
}

/// One semaphore per [`TimerType`] variant, indexed by `timer_type as usize`.
pub type TimerSemaphores = [Arc<Semaphore>; TimerType::COUNT];

/// Scheduled timer event containing execution metadata.
///
/// Contains the key, execution time, timer type, and tracing context for a
/// timer that will fire at a specific moment. The `span` field is excluded from
/// equality and ordering comparisons to ensure consistent behavior across
/// different tracing contexts.
#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    /// Entity key identifying what this timer belongs to.
    pub key: Key,

    /// When this timer should execute.
    pub time: CompactDateTime,

    /// Timer type classification.
    pub timer_type: TimerType,

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
    /// * `timer_type` – Timer type classification
    /// * `span` – Tracing span for distributed observability context
    ///
    /// # Returns
    ///
    /// A new [`Trigger`] instance ready for scheduling.
    #[must_use]
    pub fn new(key: Key, time: CompactDateTime, timer_type: TimerType, span: Span) -> Self {
        Self {
            key,
            time,
            timer_type,
            span: ArcSwap::from_pointee(span).into(),
        }
    }

    /// Create a test trigger with minimal dependencies.
    ///
    /// Creates a `Trigger` suitable for unit testing without requiring
    /// complex span setup. Uses the current span context.
    ///
    /// # Arguments
    ///
    /// * `key` - Entity key
    /// * `time` - Execution time
    /// * `timer_type` - Timer classification
    #[cfg(test)]
    #[must_use]
    pub fn for_testing(key: Key, time: CompactDateTime, timer_type: TimerType) -> Self {
        Self::new(key, time, timer_type, Span::current())
    }

    /// Returns the tracing span associated with this trigger.
    ///
    /// Returns the current span for tracing operations or span linking.
    #[must_use]
    pub fn span(&self) -> Span {
        let span = self.span.load();
        span.as_ref().clone()
    }

    /// Replaces the tracing span on this trigger.
    ///
    /// Used to refresh context when a duplicate trigger is inserted, ensuring
    /// the most recent caller's trace context is preserved.
    pub fn set_span(&self, span: Span) {
        self.span.store(Arc::new(span));
    }
}

/// Maximum concurrent slab loading operations.
const LOAD_CONCURRENCY: usize = 16;

/// Maximum concurrent timer deletion operations.
pub const DELETE_CONCURRENCY: usize = 16;

// Re-export primary APIs for convenient access to timer functionality
pub use manager::{TimerManager, TimerManagerConfig};
pub use uncommitted::{FiringTimer, PendingTimer, UncommittedTimer};
