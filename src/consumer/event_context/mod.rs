//! Execution context for Kafka message and timer event handling.
//!
//! This module defines abstractions for delivering shutdown signals and
//! managing timer scheduling within message handlers. It provides:
//! - `EventContext`: Trait for handler contexts to schedule, unschedule, clear,
//!   and list timers, bind keyed-state descriptors, and detect shutdown.
//! - `TerminationSignals`: Internal trait for distinguishing shutdown from
//!   message-level cancellation (used by retry middleware).
//! - `PartitionEventContext<T, S>`: Concrete `EventContext` implementation
//!   backed by a `TimerManager<T>` and a per-event keyed-state session `S`.
//! - `DynEventContext`: Object-safe wrapper around any `EventContext`.

use crate::Key;
use crate::codec::ErasedStateCodec;
use crate::consumer::kafka_state::{message_deque_state, message_map_state, message_state};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::RepinProof;
use crate::consumer::partition::ShutdownPhase;
use crate::error::ClassifyError;
use crate::loader::MessageLoader;
use crate::state::collection::StateSession;
use crate::state::descriptor::{
    Registered, StateDescriptor, deque_state, map_state, set_state, value_state,
};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state::session::EventSession;
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::TimerManagerError;
use crate::timers::store::TriggerStore;
use crate::timers::{TimerManager, TimerRequest, TimerType, timer_span};
use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use dyn_clone::DynClone;
use educe::Educe;
use futures::FutureExt;
use serde::de::StdError;
use std::error::Error;
use std::future::{Future, ready};
use std::ops::AsyncFnOnce;
use std::sync::Arc;
use tokio::select;
use tokio::sync::watch;
use tracing::{Instrument, Span, error, field::Empty, field::display};

mod erased;

pub use crate::state::erased::{
    BoxDequeState, BoxMapState, BoxSetState, BoxStateCursor, BoxValueState, DynDequeState,
    DynMapState, DynSetState, DynValueState, ErasedCategory, ErasedStateError, StateCursor,
};
use crate::state::erased::{ErasedDeque, ErasedMap, ErasedSet, ErasedValue};

/// Marker trait for errors that can be returned from event context operations.
///
/// This trait is automatically implemented for any type that satisfies the
/// bounds.
pub trait EventContextError: StdError + ClassifyError + Send + Sync + 'static {}

impl<T> EventContextError for T where T: StdError + ClassifyError + Send + Sync + 'static {}

/// Provides cancellation notifications and timer operations to message
/// handlers.
///
/// Handlers receive an implementation of `EventContext` that allows them to:
/// - Await a cancellation signal (includes partition shutdown).
/// - Schedule a new timer for the current message key.
/// - Unschedule one or all existing timers for the key.
/// - Clear any scheduled timers and reschedule a fresh one.
/// - Inspect all scheduled timer execution times for the key.
/// - Check synchronously if cancellation has been requested.
pub trait EventContext: TerminationSignals + Clone + Send + Sync + 'static {
    /// The message payload type events on this context carry.
    ///
    /// Leaf contexts pin it to the consumer's codec payload; wrapper
    /// contexts forward their inner context's payload. Handler traits
    /// bound their contexts with `C: EventContext<Payload = Self::Payload>`,
    /// which is what lets payload-typed capabilities (e.g. the keyed-state
    /// Kafka-message handles) stay fully typed inside generic handlers.
    type Payload: Send + Sync + 'static;

    /// Error type returned by timer-related operations.
    type Error: EventContextError;

    /// Returns `true` if this message processing has been cancelled.
    ///
    /// Cancellation includes both message-level cancellation and partition
    /// shutdown.
    fn should_cancel(&self) -> bool;

    /// Returns a future that resolves when message processing is cancelled.
    ///
    /// Cancellation includes both message-level cancellation and partition
    /// shutdown.
    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static;

    /// Trigger cancellation for this context.
    ///
    /// Signals that the current operation should be cancelled. Handlers should
    /// check `should_cancel()` or await `on_cancel()` and clean up promptly.
    ///
    /// This is used by middleware (e.g., timeout) to signal cancellation while
    /// continuing to wait for the handler to finish cleanup. Calling multiple
    /// times is idempotent.
    fn cancel(&self);

    /// Resets the message-level cancellation flag.
    ///
    /// Called by the canceller after the inner operation completes, so
    /// subsequent retry attempts start with a clean state. This is the
    /// counterpart to [`cancel`](Self::cancel).
    fn uncancel(&self);

    /// Schedule a new timer at the given execution time for this key.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if scheduling in the persistent store
    /// or in-memory scheduler fails.
    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule all existing timers for this key, then schedule exactly one.
    ///
    /// All prior timers for this key are removed in parallel before a new
    /// timer at `time` is added.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if any unschedule or the final schedule
    /// operation fails.
    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule a single timer for this key at the specified time.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if the unschedule operation fails.
    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule *all* timers for this key of the specified type.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if any unschedule operation fails.
    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// List all scheduled execution times for timers on this key of the
    /// specified type.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if retrieving times from the persistent
    /// store fails.
    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static;

    /// The per-event keyed-state session descriptor binds operate over.
    ///
    /// Leaf contexts carry the session the partition loop minted for the
    /// event; wrapper contexts forward their inner context's session type
    /// (`type State = C::State`). State itself is Kafka-agnostic, so the
    /// payload tie lives here. The session's loader yields `Self::Payload`,
    /// which keeps Kafka-message handles fully typed inside generic handlers.
    /// [`EventSession`] names no payload; the `Loader` associated type its
    /// [`StateSession`] supertrait carries is fixed to the message loader
    /// here.
    type State: EventSession<Loader: MessageLoader<Payload = Self::Payload>>;

    /// Binds a registered keyed-state collection, returning its typed handle.
    ///
    /// Takes a [`Registered<DESC>`] capability handle, not a raw descriptor, so
    /// a handler can bind only collections it registered — binding an
    /// unregistered one is a compile error, not a runtime one. (The bind-time
    /// registration check the session's engine performs is the backstop for
    /// names that slip past the type system, e.g. through the erased FFI seam.)
    ///
    /// Works in message and timer handlers alike. The returned handle owns a
    /// cheap `Arc`-backed clone of the session, so it is `Send + Sync +
    /// 'static`; repeated binds of one collection share the per-event
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] when keyed state is not
    /// wired, [`StateAccessError::Unregistered`] for a collection never
    /// registered with the consumer, or
    /// [`StateAccessError::IdentityMismatch`] when the registered identity
    /// differs from the descriptor's.
    fn state<DESC>(
        &self,
        registered: Registered<DESC>,
    ) -> Result<DESC::Handle<Self::State>, StateAccessError>
    where
        DESC: StateDescriptor;

    /// Rebuilds this context re-pinned to the session's CURRENT attempt epoch —
    /// the crate-internal re-pin primitive wrapper contexts forward (like
    /// [`state`](Self::state)). Gated by [`RepinProof`], so only the
    /// `next_attempt` verb and the settle final-hook stamp (the two mint sites)
    /// produce a live attempt-N+1 (or stamped-final) view; a leaked stale clone
    /// can never re-pin itself.
    ///
    /// The leaf rebuilds into a **fresh** inner cell, so a leaked clone of the
    /// prior context keeps its stale pin (and stays fenced). An invalidated
    /// context (its inner already stored `None`) stays invalidated — re-pin
    /// must never resurrect it. Wrapper contexts forward to their inner,
    /// recursively, so the fence reaches the whole stack.
    #[must_use]
    fn redispatch(&self, proof: RepinProof) -> Self;

    /// Return a boxed, type-erased event context for the FFI seam.
    ///
    /// The payload must map to a codec ([`ErasedStateCodec`]) — the erased
    /// state ops recover it from the payload — which every FFI payload does.
    fn boxed(self) -> BoxEventContext<Self::Payload>
    where
        Self::Payload: ErasedStateCodec,
    {
        Box::new(self)
    }
}

/// Distinguishes shutdown signals from message-level cancellation.
///
/// This trait is used internally by the retry middleware to determine whether
/// to abort immediately (shutdown) or treat cancellation as a transient error
/// and continue retrying (message cancellation).
///
/// - **Shutdown**: Partition revoked or consumer stopping. Processing must stop
///   immediately to release the partition.
/// - **Message cancellation**: Requested by middleware (e.g., timeout). Should
///   be treated as a transient error; retry logic should continue.
///
/// # Note
///
/// This trait is a supertrait of [`EventContext`] and must be public, but it is
/// considered an implementation detail. External users should not rely on these
/// methods directly.
pub trait TerminationSignals {
    /// Returns `true` if shutdown has been requested.
    ///
    /// Shutdown means the partition is being revoked or the consumer is
    /// stopping. Processing must abort immediately.
    fn is_shutdown(&self) -> bool;

    /// Returns `true` if message-level cancellation has been requested.
    ///
    /// Message cancellation should be treated as a transient error by retry
    /// logic, not as a signal to abort.
    fn is_message_cancelled(&self) -> bool;

    /// Returns a future that resolves when shutdown is requested.
    ///
    /// Use this in `select!` to abort retry sleep on shutdown while ignoring
    /// message-level cancellation.
    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static;

    /// Returns a future that resolves when message-level cancellation is
    /// requested.
    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static;
}

mod partition;
pub use partition::PartitionEventContext;

pub use erased::{BoxEventContext, BoxEventContextError, DynEventContext};

/// The keyed-state capability error, raised by the [`EventContext`] state
/// surface and by descriptor binds. Defined in [`crate::state`] (its
/// `IdentityMismatch` embeds state's `StructuralIdentity`) and re-exported here
/// so the capability's error keeps its `EventContext`-local path.
pub use crate::state::StateAccessError;

#[cfg(test)]
mod tests;
