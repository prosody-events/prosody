//! Timer defer storage backend.
//!
//! Manages per-key FIFO queues of deferred timers with shared retry counters
//! and OpenTelemetry span context preservation.

pub mod cached;
pub mod cassandra;
pub mod memory;
pub mod provider;

#[cfg(test)]
pub mod tests;

use crate::Key;
use crate::error::ClassifyError;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use opentelemetry::Context;
use std::error::Error;
use std::future::Future;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub use cached::CachedTimerDeferStore;
pub use cassandra::{CassandraTimerDeferStore, CassandraTimerDeferStoreProvider};
pub use memory::{MemoryTimerDeferStore, MemoryTimerDeferStoreProvider};
pub use provider::TimerDeferStoreProvider;

/// Outcome of completing a successful timer retry.
#[derive(Debug, Clone)]
pub enum TimerRetryCompletionResult {
    /// Queue has more timers; retry count reset to 0.
    MoreTimers {
        /// Next timer's fire time (for scheduling).
        next_time: CompactDateTime,
        /// Parent trace context for span reconstruction.
        context: Context,
    },

    /// Queue empty; key deleted from storage.
    Completed,
}

/// Storage backend for deferred timer entries.
///
/// Each key maintains a FIFO queue of timers (indexed by `original_time`) with
/// a shared retry counter. Segment context is established at construction.
///
/// # Key Differences from `MessageDeferStore`
///
/// - Stores `Trigger` with serialized span context (W3C trace format)
/// - Reconstructs spans on read via
///   [`SpanLink::apply`](crate::consumer::SpanLink::apply)
///
/// # Invariants
///
/// - Use [`defer_first_timer`](Self::defer_first_timer) for first failure,
///   [`defer_additional_timer`](Self::defer_additional_timer) for subsequent
/// - Timers retry in time order; retry count applies to queue head
pub trait TimerDeferStore: Clone + Send + Sync + 'static {
    /// Error type. Must implement [`ClassifyError`] for retry decisions.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    /// Defers a timer on a fresh key (initializes retry count to 0).
    ///
    /// # Precondition
    ///
    /// Key must not already have deferred timers.
    fn defer_first_timer(
        &self,
        trigger: &Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Appends timer to an existing deferred key (preserves retry count).
    ///
    /// # Precondition
    ///
    /// Key must already have deferred timers.
    fn defer_additional_timer(
        &self,
        trigger: &Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { self.append_deferred_timer(trigger).await }
    }

    /// Removes timer after successful processing; resets retry count or
    /// deletes key.
    fn complete_retry_success(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<TimerRetryCompletionResult, Self::Error>> + Send {
        async move {
            self.remove_deferred_timer(key, time).await?;

            if let Some((trigger, _)) = self.get_next_deferred_timer(key).await? {
                self.set_retry_count(key, 0).await?;
                let context = trigger.span().context();
                Ok(TimerRetryCompletionResult::MoreTimers {
                    next_time: trigger.time,
                    context,
                })
            } else {
                self.delete_key(key).await?;
                Ok(TimerRetryCompletionResult::Completed)
            }
        }
    }

    /// Increments retry count after failed attempt; returns new count.
    fn increment_retry_count(
        &self,
        key: &Key,
        current_retry_count: u32,
    ) -> impl Future<Output = Result<u32, Self::Error>> + Send {
        async move {
            let new_count = current_retry_count.saturating_add(1);
            self.set_retry_count(key, new_count).await?;
            Ok(new_count)
        }
    }

    /// Returns oldest timer (with reconstructed span) and retry count.
    fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Option<(Trigger, u32)>, Self::Error>> + Send;

    /// Returns retry count if deferred, `None` otherwise. Cheaper than
    /// [`get_next_deferred_timer`](Self::get_next_deferred_timer).
    fn is_deferred(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Option<u32>, Self::Error>> + Send {
        async move {
            Ok(self
                .get_next_deferred_timer(key)
                .await?
                .map(|(_, count)| count))
        }
    }

    /// Returns all deferred times for a key (ascending order).
    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static;

    /// Low-level: appends timer without touching retry count.
    fn append_deferred_timer(
        &self,
        trigger: &Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Low-level: removes timer without cleanup.
    fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Internal: sets retry count directly.
    fn set_retry_count(
        &self,
        key: &Key,
        retry_count: u32,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Internal: deletes all data for a key.
    fn delete_key(&self, key: &Key) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Cached timer state: time, trace context, and retry count.
///
/// Caches `Context` rather than `Trigger` because spans get replaced with
/// `Span::none()` after processing. Fresh spans are created at read time via
/// [`SpanLink::apply`](crate::consumer::SpanLink::apply).
#[derive(Clone, Debug)]
pub struct CachedTimerEntry {
    /// Earliest timer's fire time.
    pub time: CompactDateTime,
    /// Parent trace context for span reconstruction.
    pub context: Context,
    /// Retry count for backoff calculation.
    pub retry_count: u32,
}
