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
//!     async fn on_message<C>(
//!         &self,
//!         _context: C,
//!         _message: UncommittedMessage,
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
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::ParseError;
use arc_swap::ArcSwap;
use educe::Educe;
use opentelemetry::Context as OtelContext;
use serde::Serialize;
use std::sync::Arc;
use strum::EnumCount;
use tokio::sync::Semaphore;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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

/// Timer type identifier for distinguishing concurrent timer types.
///
/// Timers can have different types that determine their purpose and routing.
/// This is an internal classification not exposed to applications.
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
    /// User-scheduled application timers (default).
    #[default]
    Application = 0,
    /// Defer middleware message retry timers.
    DeferredMessage = 1,
    /// Defer middleware timer retry timers.
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

/// Controls how a language-client execution span relates to the `OTel` context
/// that caused the timer to be scheduled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TriggerLinking {
    /// The execution span is a direct child of the schedule span.
    #[default]
    SetParent,
    /// The execution span starts a new trace linked to the schedule span.
    AddLink,
}

/// Scheduled timer event containing execution metadata.
///
/// Contains the key, execution time, timer type, and tracing context for a
/// timer that will fire at a specific moment. The `span` and `OTel` context
/// fields are excluded from equality and ordering comparisons to ensure
/// consistent behavior across different tracing contexts.
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

    /// How execution spans should relate to the schedule context.
    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    linking: TriggerLinking,

    /// The `OTel` context of the span that scheduled this timer. Used by
    /// language-client handlers to build the carrier with the correct parent
    /// or link target. `None` for in-memory triggers where `span()` already
    /// IS the schedule span.
    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    link_context: Option<OtelContext>,
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
            linking: TriggerLinking::default(),
            link_context: None,
        }
    }

    /// Sets the span linking mode for this trigger (builder pattern).
    #[must_use]
    pub fn with_linking(mut self, linking: TriggerLinking) -> Self {
        self.linking = linking;
        self
    }

    /// Sets the schedule `OTel` context used to build the carrier (builder
    /// pattern).
    #[must_use]
    pub fn with_link_context(mut self, context: OtelContext) -> Self {
        self.link_context = Some(context);
        self
    }

    /// Returns the span linking mode.
    #[must_use]
    pub fn linking(&self) -> TriggerLinking {
        self.linking
    }

    /// Returns the schedule `OTel` context, falling back to `span().context()`
    /// for in-memory triggers where the trigger span IS the schedule span.
    #[must_use]
    pub fn link_context_or_span_context(&self) -> OtelContext {
        self.link_context
            .clone()
            .unwrap_or_else(|| self.span().context())
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
