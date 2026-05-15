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
//! use prosody::timers::UncommittedTimer;
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
use rand::RngExt;
use serde::Serialize;
use std::sync::Arc;
use strum::EnumCount;
use tokio::sync::Semaphore;
use tracing::Span;

mod active;
pub mod datetime;
pub mod duration;
pub mod error;
mod manager;
mod queue;
mod scheduler;
mod segment;
mod slab;
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

/// Application request to schedule a timer.
///
/// This is the public scheduling shape: callers provide the logical timer
/// identity and tracing context, while the timer system owns the commit-oracle
/// tag. Persisted and delivered timers use [`Trigger`] internally.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct TimerRequest {
    /// Entity key identifying what this timer belongs to.
    pub key: Key,

    /// When this timer should execute.
    pub time: CompactDateTime,

    /// Timer type classification.
    pub timer_type: TimerType,

    /// Tracing span for distributed observability context.
    #[educe(Debug(ignore))]
    pub span: Span,
}

impl TimerRequest {
    /// Creates a tagless timer scheduling request.
    #[must_use]
    pub fn new(key: Key, time: CompactDateTime, timer_type: TimerType, span: Span) -> Self {
        Self {
            key,
            time,
            timer_type,
            span,
        }
    }

    /// Converts this request into a tagged internal trigger with a fresh tag.
    #[must_use]
    pub(crate) fn into_trigger(self) -> Trigger {
        Trigger::new(self.key, self.time, self.timer_type, self.span)
    }

    /// Converts this request into a tagged internal trigger with `tag`.
    #[must_use]
    pub(crate) fn into_trigger_with_tag(self, tag: i32) -> Trigger {
        Trigger::with_tag(self.key, self.time, self.timer_type, tag, self.span)
    }
}

/// Scheduled timer event containing execution metadata.
///
/// Contains the key, execution time, timer type, and tracing context for a
/// timer that will fire at a specific moment. The `span` and `tag` fields are
/// excluded from equality and ordering comparisons.
///
/// `tag` is excluded from `Hash/Eq/Ord` to preserve the schema's primary-key
/// invariant `(key, time, timer_type)`: two `Trigger`s for the same logical
/// timer compare equal regardless of their tag, so `queue_keys`' occupied-entry
/// upsert continues to work correctly when the tag is rotated after a
/// `complete()`-from-`FiringRescheduled`.
#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    /// Entity key identifying what this timer belongs to.
    pub key: Key,

    /// When this timer should execute.
    pub time: CompactDateTime,

    /// Timer type classification.
    pub timer_type: TimerType,

    /// Random 32-bit identity rotated by `complete()` from `FiringRescheduled`.
    /// Excluded from `Hash/Eq/Ord` — see struct doc.
    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    pub tag: i32,

    /// Tracing span for distributed observability context.
    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    span: Arc<ArcSwap<Span>>,
}

/// Logical identity of a timer.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct TriggerId {
    key: Key,
    time: CompactDateTime,
    timer_type: TimerType,
}

impl Trigger {
    /// Creates a new timer trigger for scheduled execution.
    ///
    /// Generates a fresh random `tag` via `rand::rng().random::<i32>()` so
    /// every newly constructed trigger has a unique identity.
    ///
    /// # Arguments
    ///
    /// * `key` – Entity key identifying what this timer belongs to
    /// * `time` – When this timer should execute
    /// * `timer_type` – Timer type classification
    /// * `span` – Tracing span for distributed observability context
    #[must_use]
    pub fn new(key: Key, time: CompactDateTime, timer_type: TimerType, span: Span) -> Self {
        Self::with_tag(key, time, timer_type, rand::rng().random::<i32>(), span)
    }

    /// Restore-from-store constructor. Preserves the loaded `tag`.
    ///
    /// Use this when deserializing a trigger from persistent storage so the
    /// stored tag value is kept intact rather than replaced with a fresh
    /// random.
    #[must_use]
    pub fn with_tag(
        key: Key,
        time: CompactDateTime,
        timer_type: TimerType,
        tag: i32,
        span: Span,
    ) -> Self {
        Self {
            key,
            time,
            timer_type,
            tag,
            span: ArcSwap::from_pointee(span).into(),
        }
    }

    /// Create a test trigger with minimal dependencies.
    ///
    /// Uses `tag = 0` for reproducibility in tests.
    #[cfg(test)]
    #[must_use]
    pub fn for_testing(key: Key, time: CompactDateTime, timer_type: TimerType) -> Self {
        Self::with_tag(key, time, timer_type, 0, Span::current())
    }

    /// Returns the tracing span associated with this trigger.
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

    /// Returns the immutable logical identity for this trigger.
    #[must_use]
    pub(crate) fn id(&self) -> TriggerId {
        TriggerId {
            key: self.key.clone(),
            time: self.time,
            timer_type: self.timer_type,
        }
    }
}

/// Maximum concurrent timer deletion operations.
pub const DELETE_CONCURRENCY: usize = 16;

// Re-export primary APIs for convenient access to timer functionality
pub use active::TimerSnapshot;
pub use manager::{TimerManager, TimerManagerConfig};
pub use uncommitted::{FiringTimer, PendingTimer, UncommittedTimer};
