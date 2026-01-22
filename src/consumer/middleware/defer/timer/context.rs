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
use std::cmp::Ordering;
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
}

/// Checks if a key is deferred (has pending timers in the defer store).
async fn is_deferred<C, S>(
    store: &S,
    key: &Key,
) -> Result<bool, TimerDeferContextError<C, S::Error>>
where
    S: TimerDeferStore,
{
    store
        .is_deferred(key)
        .await
        .map(|opt| opt.is_some())
        .map_err(TimerDeferContextError::Store)
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

    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        // Non-Application timers pass through
        if timer_type != TimerType::Application {
            return self
                .inner
                .schedule(time, timer_type)
                .await
                .map_err(TimerDeferContextError::Context);
        }

        if is_deferred(&self.store, &self.key).await? {
            // Append to defer store
            let trigger = Trigger::new(
                self.key.clone(),
                time,
                TimerType::Application,
                tracing::Span::current(),
            );
            self.store
                .append_deferred_timer(&trigger)
                .await
                .map_err(TimerDeferContextError::Store)
        } else {
            // Delegate to inner context
            self.inner
                .schedule(time, timer_type)
                .await
                .map_err(TimerDeferContextError::Context)
        }
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        // Non-Application timers pass through
        if timer_type != TimerType::Application {
            return self
                .inner
                .clear_and_schedule(time, timer_type)
                .await
                .map_err(TimerDeferContextError::Context);
        }

        if is_deferred(&self.store, &self.key).await? {
            // Clear defer store, DeferredTimer, and schedule new timer concurrently
            let (store_result, deferred_result, schedule_result) = tokio::join!(
                self.store.delete_key(&self.key),
                self.inner.clear_scheduled(TimerType::DeferredTimer),
                self.inner.clear_and_schedule(time, TimerType::Application),
            );
            store_result.map_err(TimerDeferContextError::Store)?;
            deferred_result.map_err(TimerDeferContextError::Context)?;
            schedule_result.map_err(TimerDeferContextError::Context)
        } else {
            // Not deferred - only need to clear and schedule in inner context
            self.inner
                .clear_and_schedule(time, TimerType::Application)
                .await
                .map_err(TimerDeferContextError::Context)
        }
    }

    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        // Non-Application timers pass through
        if timer_type != TimerType::Application {
            return self
                .inner
                .unschedule(time, timer_type)
                .await
                .map_err(TimerDeferContextError::Context);
        }

        if is_deferred(&self.store, &self.key).await? {
            // Timer could be in either store - remove from both concurrently
            let (store_result, context_result) = tokio::join!(
                self.store.remove_deferred_timer(&self.key, time),
                self.inner.unschedule(time, TimerType::Application),
            );
            store_result.map_err(TimerDeferContextError::Store)?;
            context_result.map_err(TimerDeferContextError::Context)?;
            Ok(())
        } else {
            // Not deferred - timer can only be in inner context
            self.inner
                .unschedule(time, TimerType::Application)
                .await
                .map_err(TimerDeferContextError::Context)
        }
    }

    async fn clear_scheduled(&self, timer_type: TimerType) -> Result<(), Self::Error> {
        // Non-Application timers pass through
        if timer_type != TimerType::Application {
            return self
                .inner
                .clear_scheduled(timer_type)
                .await
                .map_err(TimerDeferContextError::Context);
        }

        if is_deferred(&self.store, &self.key).await? {
            // Clear all three sources concurrently
            let (store_result, deferred_result, application_result) = tokio::join!(
                self.store.delete_key(&self.key),
                self.inner.clear_scheduled(TimerType::DeferredTimer),
                self.inner.clear_scheduled(TimerType::Application),
            );
            store_result.map_err(TimerDeferContextError::Store)?;
            deferred_result.map_err(TimerDeferContextError::Context)?;
            application_result.map_err(TimerDeferContextError::Context)?;
            Ok(())
        } else {
            // Not deferred - only clear Application timers from inner context
            self.inner
                .clear_scheduled(TimerType::Application)
                .await
                .map_err(TimerDeferContextError::Context)
        }
    }

    fn invalidate(self) {
        self.inner.invalidate();
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        merge_scheduled_streams(
            self.inner.clone(),
            self.store.clone(),
            self.key().clone(),
            timer_type,
        )
    }
}

/// Implementation of `scheduled()` that handles both Application and
/// non-Application types.
fn merge_scheduled_streams<C, S>(
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
            while let Some(time) = advance_active(&mut active_stream).await? {
                yield time;
            }
            return;
        }

        // Application timers: merge active and deferred streams
        let deferred_stream = store.deferred_times(&key);
        pin_mut!(deferred_stream);

        let mut active_next = advance_active(&mut active_stream).await?;
        let mut deferred_next = advance_deferred(&mut deferred_stream).await?;

        // Merge while both streams have items
        while let (Some(a), Some(d)) = (active_next, deferred_next) {
            match a.cmp(&d) {
                Ordering::Less => {
                    yield a;
                    active_next = advance_active(&mut active_stream).await?;
                }
                Ordering::Greater => {
                    yield d;
                    deferred_next = advance_deferred(&mut deferred_stream).await?;
                }
                Ordering::Equal => {
                    yield a;
                    active_next = advance_active(&mut active_stream).await?;
                    deferred_next = advance_deferred(&mut deferred_stream).await?;
                }
            }
        }

        // Drain remaining from active
        while let Some(a) = active_next {
            yield a;
            active_next = advance_active(&mut active_stream).await?;
        }

        // Drain remaining from deferred
        while let Some(d) = deferred_next {
            yield d;
            deferred_next = advance_deferred(&mut deferred_stream).await?;
        }
    }
}

/// Advances a stream, wrapping errors in `TimerDeferContextError::Context`.
async fn advance_active<CE, SE>(
    stream: &mut (impl Stream<Item = Result<CompactDateTime, CE>> + Unpin),
) -> Result<Option<CompactDateTime>, TimerDeferContextError<CE, SE>> {
    cooperative(stream.try_next())
        .await
        .map_err(TimerDeferContextError::Context)
}

/// Advances a stream, wrapping errors in `TimerDeferContextError::Store`.
async fn advance_deferred<CE, SE>(
    stream: &mut (impl Stream<Item = Result<CompactDateTime, SE>> + Unpin),
) -> Result<Option<CompactDateTime>, TimerDeferContextError<CE, SE>> {
    cooperative(stream.try_next())
        .await
        .map_err(TimerDeferContextError::Store)
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
