//! Timer defer context wrapper for unified timer operations.
//!
//! Provides [`TimerDeferContext`] which wraps an inner [`EventContext`] and
//! unifies active timers (not yet fired) with deferred timers (fired but
//! failed).
//!
//! # Design
//!
//! The context wrapper intercepts `Application` timer operations to provide
//! consistent behavior regardless of deferral state:
//!
//! - **Not deferred**: Operations delegate directly to inner context
//! - **Deferred**: Operations interact with both defer store and inner context
//!
//! Non-`Application` timer types (`DeferredMessage`, `DeferredTimer`) pass
//! through unchanged - these are middleware-internal.

use crate::Key;
use crate::consumer::Keyed;
use crate::consumer::event_context::{EventContext, TerminationSignals};
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use std::future::Future;
use thiserror::Error;
use tokio::task::coop::cooperative;

/// Context wrapper that unifies active and deferred timer operations.
///
/// Implements [`EventContext`] by intercepting `Application` timer methods
/// to provide a unified view regardless of whether the key is deferred.
///
/// # Timer Operations
///
/// For `Application` timers:
/// - `schedule()`: Delegates if not deferred; appends to store if deferred
/// - `unschedule()`: Removes from both defer store and inner context
/// - `clear_scheduled()`: Clears both stores, cancels `DeferredTimer`, deletes
///   key
/// - `scheduled()`: Merges streams from both stores, sorted and deduplicated
/// - `clear_and_schedule()`: Deletes key, clears `DeferredTimer`, delegates to
///   inner
///
/// For non-`Application` timers: Pass through to inner context unchanged.
#[derive(Clone)]
pub struct TimerDeferContext<C, S> {
    inner: C,
    store: S,
    key: Key,
}

impl<C, S> TimerDeferContext<C, S>
where
    C: EventContext,
    S: TimerDeferStore,
{
    /// Creates a new timer defer context wrapping the inner context.
    #[must_use]
    pub fn new(inner: C, store: S, key: Key) -> Self {
        Self { inner, store, key }
    }

    /// Schedules a retry timer at the specified time.
    ///
    /// Clears any existing `DeferredTimer` and schedules a new one.
    ///
    /// # Errors
    ///
    /// Returns an error if the context operation fails.
    pub async fn schedule_retry_timer(
        &self,
        fire_time: CompactDateTime,
    ) -> Result<(), TimerDeferContextError<C::Error, S::Error>> {
        self.inner
            .clear_and_schedule(fire_time, TimerType::DeferredTimer)
            .await
            .map_err(TimerDeferContextError::Context)
    }

    /// Clears the retry timer for this key.
    ///
    /// # Errors
    ///
    /// Returns an error if the context operation fails.
    pub async fn clear_retry_timer(
        &self,
    ) -> Result<(), TimerDeferContextError<C::Error, S::Error>> {
        self.inner
            .clear_scheduled(TimerType::DeferredTimer)
            .await
            .map_err(TimerDeferContextError::Context)
    }

    /// Returns whether the key is currently deferred.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn is_deferred(&self) -> Result<bool, TimerDeferContextError<C::Error, S::Error>> {
        self.store
            .is_deferred(&self.key)
            .await
            .map(|opt| opt.is_some())
            .map_err(TimerDeferContextError::Store)
    }
}

impl<C, S> Keyed for TimerDeferContext<C, S>
where
    C: EventContext,
    S: TimerDeferStore,
{
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.key
    }
}

impl<C, S> TerminationSignals for TimerDeferContext<C, S>
where
    C: EventContext + Clone + Send + Sync,
    S: TimerDeferStore + Clone + Send + Sync,
{
    fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    fn is_message_cancelled(&self) -> bool {
        self.inner.is_message_cancelled()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_shutdown()
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_message_cancelled()
    }
}

impl<C, S> EventContext for TimerDeferContext<C, S>
where
    C: EventContext + Clone + Send + Sync,
    S: TimerDeferStore + Clone + Send + Sync,
{
    type Error = TimerDeferContextError<C::Error, S::Error>;

    fn should_cancel(&self) -> bool {
        self.inner.should_cancel()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_cancel()
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    fn uncancel(&self) {
        self.inner.uncancel();
    }

    fn invalidate(self) {
        self.inner.invalidate();
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = self.inner.clone();
        let store = self.store.clone();
        let key = self.key().clone();

        async move {
            // Non-Application timers pass through
            if timer_type != TimerType::Application {
                return inner
                    .schedule(time, timer_type)
                    .await
                    .map_err(TimerDeferContextError::Context);
            }

            // Check if key is deferred
            let is_deferred = store
                .is_deferred(&key)
                .await
                .map_err(TimerDeferContextError::Store)?
                .is_some();

            if is_deferred {
                // Append to defer store
                let trigger =
                    Trigger::new(key, time, TimerType::Application, tracing::Span::current());
                store
                    .append_deferred_timer(&trigger)
                    .await
                    .map_err(TimerDeferContextError::Store)
            } else {
                // Delegate to inner context
                inner
                    .schedule(time, timer_type)
                    .await
                    .map_err(TimerDeferContextError::Context)
            }
        }
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = self.inner.clone();
        let store = self.store.clone();
        let key = self.key().clone();

        async move {
            // Non-Application timers pass through
            if timer_type != TimerType::Application {
                return inner
                    .unschedule(time, timer_type)
                    .await
                    .map_err(TimerDeferContextError::Context);
            }

            // Remove from defer store (idempotent)
            store
                .remove_deferred_timer(&key, time)
                .await
                .map_err(TimerDeferContextError::Store)?;

            // Remove from inner context (idempotent)
            inner
                .unschedule(time, timer_type)
                .await
                .map_err(TimerDeferContextError::Context)
        }
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = self.inner.clone();
        let store = self.store.clone();
        let key = self.key().clone();

        async move {
            // Non-Application timers pass through
            if timer_type != TimerType::Application {
                return inner
                    .clear_scheduled(timer_type)
                    .await
                    .map_err(TimerDeferContextError::Context);
            }

            // Delete key from defer store (clears all deferred timers + retry count)
            store
                .delete_key(&key)
                .await
                .map_err(TimerDeferContextError::Store)?;

            // Clear DeferredTimer (internal retry timer)
            inner
                .clear_scheduled(TimerType::DeferredTimer)
                .await
                .map_err(TimerDeferContextError::Context)?;

            // Clear from inner context
            inner
                .clear_scheduled(timer_type)
                .await
                .map_err(TimerDeferContextError::Context)
        }
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = self.inner.clone();
        let store = self.store.clone();
        let key = self.key().clone();

        async move {
            // Non-Application timers pass through
            if timer_type != TimerType::Application {
                return inner
                    .clear_and_schedule(time, timer_type)
                    .await
                    .map_err(TimerDeferContextError::Context);
            }

            // Delete key from defer store
            store
                .delete_key(&key)
                .await
                .map_err(TimerDeferContextError::Store)?;

            // Clear DeferredTimer
            inner
                .clear_scheduled(TimerType::DeferredTimer)
                .await
                .map_err(TimerDeferContextError::Context)?;

            // Delegate clear_and_schedule to inner (new timer goes to active store)
            inner
                .clear_and_schedule(time, timer_type)
                .await
                .map_err(TimerDeferContextError::Context)
        }
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        scheduled_impl(
            self.inner.clone(),
            self.store.clone(),
            self.key().clone(),
            timer_type,
        )
    }
}

/// Implementation of `scheduled()` that handles both Application and
/// non-Application types.
fn scheduled_impl<C, S>(
    inner: C,
    store: S,
    key: Key,
    timer_type: TimerType,
) -> impl Stream<Item = Result<CompactDateTime, TimerDeferContextError<C::Error, S::Error>>>
+ Send
+ 'static
where
    C: EventContext + Clone + Send + Sync,
    S: TimerDeferStore + Clone + Send + Sync,
{
    try_stream! {
        let active_stream = inner.scheduled(timer_type);
        pin_mut!(active_stream);

        // Non-Application timers: just pass through from inner context
        if timer_type != TimerType::Application {
            while let Some(time) = cooperative(active_stream.try_next())
                .await
                .map_err(TimerDeferContextError::Context)?
            {
                yield time;
            }
            return;
        }

        // Application timers: merge active and deferred streams with deduplication
        let deferred_stream = store.deferred_times(&key);
        pin_mut!(deferred_stream);

        // Get first item from each stream
        let mut active_next = cooperative(active_stream.try_next())
            .await
            .map_err(TimerDeferContextError::Context)?;
        let mut deferred_next = cooperative(deferred_stream.try_next())
            .await
            .map_err(TimerDeferContextError::Store)?;

        let mut last_yielded: Option<CompactDateTime> = None;

        // Merge sorted streams with deduplication
        loop {
            let next = match (active_next, deferred_next) {
                (Some(a), Some(d)) => {
                    if a <= d {
                        active_next = cooperative(active_stream.try_next())
                            .await
                            .map_err(TimerDeferContextError::Context)?;
                        if a == d {
                            // Same time in both - advance both, yield once
                            deferred_next = cooperative(deferred_stream.try_next())
                                .await
                                .map_err(TimerDeferContextError::Store)?;
                        }
                        Some(a)
                    } else {
                        deferred_next = cooperative(deferred_stream.try_next())
                            .await
                            .map_err(TimerDeferContextError::Store)?;
                        Some(d)
                    }
                }
                (Some(a), None) => {
                    active_next = cooperative(active_stream.try_next())
                        .await
                        .map_err(TimerDeferContextError::Context)?;
                    Some(a)
                }
                (None, Some(d)) => {
                    deferred_next = cooperative(deferred_stream.try_next())
                        .await
                        .map_err(TimerDeferContextError::Store)?;
                    Some(d)
                }
                (None, None) => None,
            };

            match next {
                Some(time) if last_yielded != Some(time) => {
                    last_yielded = Some(time);
                    yield time;
                }
                Some(_) => {} // Duplicate, skip
                None => break,
            }
        }
    }
}

/// Errors from timer defer context operations.
#[derive(Debug, Error)]
pub enum TimerDeferContextError<CE, SE> {
    /// Error from inner context operations.
    #[error("context error: {0}")]
    Context(#[source] CE),

    /// Error from timer defer store operations.
    #[error("store error: {0}")]
    Store(#[source] SE),
}

impl<CE, SE> ClassifyError for TimerDeferContextError<CE, SE>
where
    CE: ClassifyError,
    SE: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Context(e) => e.classify_error(),
            Self::Store(e) => e.classify_error(),
        }
    }
}

/// Stored timer entry for queue management.
///
/// Contains all data needed to reconstruct a `Trigger` without accessing
/// Kafka. Unlike message defer which stores only offsets, timer defer stores
/// the complete timer data including span context.
#[derive(Clone, Debug)]
pub struct StoredTimerEntry {
    /// The timer's original fire time.
    pub original_time: CompactDateTime,
    /// The reconstructed trigger (with restored span).
    pub trigger: Trigger,
    /// Current retry count.
    pub retry_count: u32,
}
