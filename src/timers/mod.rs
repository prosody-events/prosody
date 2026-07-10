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
//! use prosody::timers::UncommittedTimer;
//! use prosody::timers::store::TriggerStore;
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
use opentelemetry::Context;
use rand::RngExt;
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
mod manager;
mod queue;
mod scheduler;
mod segment;
mod slab;
pub mod store;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
pub mod uncommitted;

/// Classifies a timer by its origin, used to route and account for execution
/// separately across concurrent timer pools.
///
/// Application code should use [`TimerType::Application`] when scheduling
/// timers via [`crate::consumer::event_context::EventContext::schedule`] or
/// [`crate::consumer::event_context::EventContext::clear_and_schedule`].
/// The `DeferredMessage`, `DeferredTimer`, and `StateRecovery` variants are
/// reserved for internal middleware use.
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
    /// Internal: keyed-state recovery sweep scheduled by the keyed-state
    /// middleware after stage. Routes back into the middleware on fire and is
    /// never dispatched to user handlers.
    StateRecovery = 3,
}

impl TimerType {
    /// `true` only for [`TimerType::Application`] — timers scheduled by user
    /// code, as opposed to the framework-internal types.
    ///
    /// This is the span-level axis: spans for application timers export at
    /// INFO, spans for internal timer types at DEBUG, so a trace filtered at
    /// INFO contains only spans the user's own code caused (see the
    /// crate-internal `timer_span!` macro). The match is exhaustive so a new
    /// variant must pick a side.
    #[must_use]
    pub fn is_application(self) -> bool {
        match self {
            Self::Application => true,
            Self::DeferredMessage | Self::DeferredTimer | Self::StateRecovery => false,
        }
    }
}

/// Creates a timer-operation span at the level owed to its [`TimerType`]:
/// `info_span!` for application timers, `debug_span!` for framework-internal
/// ones (the [`TimerType::is_application`] invariant). A macro because a
/// tracing callsite's level is static — selecting it at runtime requires
/// branching between two invocations.
macro_rules! timer_span {
    ($timer_type:expr, $name:literal $(, $($fields:tt)*)?) => {
        if $crate::timers::TimerType::is_application($timer_type) {
            ::tracing::info_span!($name $(, $($fields)*)?)
        } else {
            ::tracing::debug_span!($name $(, $($fields)*)?)
        }
    };
}
pub(crate) use timer_span;

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
            3 => Ok(Self::StateRecovery),
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

/// Tracing state of a [`Trigger`], from scheduling to dispatch.
///
/// A trigger starts `Scheduled`, holding only the scheduling-time
/// [`Context`] — captured at construction for in-process triggers, or
/// deserialized from storage for restored ones. Spans are never persisted or
/// carried pre-fire: a span must finish to flush, while a context is plain
/// data, so memory- and Cassandra-backed triggers hold exactly the same
/// thing. At fire time [`FiringTimer::set_dispatch_span`] creates the live
/// dispatch span from that context and moves the trace to `Dispatched`.
///
/// [`FiringTimer::set_dispatch_span`]: uncommitted::FiringTimer::set_dispatch_span
#[derive(Debug)]
pub(crate) enum TriggerTrace {
    /// Pre-dispatch: the scheduling-time trace context.
    Scheduled(Context),
    /// Post-dispatch: the live dispatch span.
    Dispatched(Span),
}

/// Scheduled timer event containing execution metadata.
///
/// Contains the key, execution time, timer type, and tracing context for a
/// timer that will fire at a specific moment. The `trace` and `tag` fields
/// are excluded from equality and ordering comparisons.
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

    /// Tracing state for distributed observability; phase invariant documented
    /// on [`TriggerTrace`]. A duplicate insert may replace one `Scheduled`
    /// trace with a newer one ([`Trigger::adopt_trace_from`]) — never the
    /// reverse.
    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    trace: Arc<ArcSwap<TriggerTrace>>,
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
    #[must_use]
    pub fn new(key: Key, time: CompactDateTime, timer_type: TimerType, span: Span) -> Self {
        Self::with_tag(key, time, timer_type, rand::rng().random::<i32>(), span)
    }

    /// Creates a trigger with a caller-provided `tag`, capturing `span`'s
    /// context as the scheduling context.
    #[must_use]
    pub fn with_tag(
        key: Key,
        time: CompactDateTime,
        timer_type: TimerType,
        tag: i32,
        span: Span,
    ) -> Self {
        // The span is consumed by value at the scheduling boundary; only its
        // context survives into the trace (see `TriggerTrace`).
        let context = span.context();
        drop(span);
        Self::restored(key, time, timer_type, tag, context)
    }

    /// Restore-from-store constructor: preserves the stored `tag` and carries
    /// the persisted scheduling `context` directly.
    #[must_use]
    pub fn restored(
        key: Key,
        time: CompactDateTime,
        timer_type: TimerType,
        tag: i32,
        context: Context,
    ) -> Self {
        Self {
            key,
            time,
            timer_type,
            tag,
            trace: ArcSwap::from_pointee(TriggerTrace::Scheduled(context)).into(),
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

    /// Returns the live dispatch span, or `Span::none()` before dispatch.
    ///
    /// Pre-dispatch a trigger holds only a scheduling [`Context`] — there is
    /// no live span to return until
    /// [`FiringTimer::set_dispatch_span`](uncommitted::FiringTimer::set_dispatch_span)
    /// installs one at fire time. Use [`Trigger::context`] for the trace
    /// context, which exists in both phases.
    #[must_use]
    pub fn span(&self) -> Span {
        match &**self.trace.load() {
            TriggerTrace::Scheduled(_) => Span::none(),
            TriggerTrace::Dispatched(span) => span.clone(),
        }
    }

    /// Returns the trace context: the scheduling context before dispatch, or
    /// the dispatch span's context after.
    #[must_use]
    pub fn context(&self) -> Context {
        match &**self.trace.load() {
            TriggerTrace::Scheduled(context) => context.clone(),
            TriggerTrace::Dispatched(span) => span.context(),
        }
    }

    /// Installs the live dispatch span, moving the trace to `Dispatched`.
    pub fn set_span(&self, span: Span) {
        self.trace.store(Arc::new(TriggerTrace::Dispatched(span)));
    }

    /// Adopts `other`'s trace, so a duplicate insert re-parents this trigger
    /// onto the newest caller's trace.
    pub(crate) fn adopt_trace_from(&self, other: &Self) {
        self.trace.store(other.trace.load_full());
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
