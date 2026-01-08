//! Storage trait and implementations for timer defer middleware.
//!
//! Provides [`TimerDeferStore`] trait for managing deferred timers with
//! FIFO ordering per key and retry count tracking.

pub mod cached;
pub mod cassandra;
pub mod memory;

#[cfg(test)]
pub mod tests;

use crate::Key;
use crate::consumer::middleware::ClassifyError;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use futures::Stream;
use opentelemetry::Context;
use std::error::Error;
use std::future::Future;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub use cached::CachedTimerDeferStore;
pub use memory::MemoryTimerDeferStore;

/// Result of [`TimerDeferStore::complete_retry_success`].
#[derive(Debug, Clone)]
pub enum TimerRetryCompletionResult {
    /// More timers remain; `retry_count` has been reset to 0.
    MoreTimers {
        /// The next timer's original fire time (for scheduling).
        next_time: CompactDateTime,
        /// The next timer's parent trace context for span reconstruction.
        context: Context,
    },

    /// No more timers; key has been deleted from storage.
    Completed,
}

/// Storage backend for deferred timer entries.
///
/// Manages a FIFO queue of timers per key with a shared `retry_count`. The
/// segment context (`topic/partition/consumer_group`) is established at
/// store construction (internally for Cassandra stores).
///
/// # Invariants
///
/// - **First vs. additional**: Use
///   [`defer_first_timer`](Self::defer_first_timer) for the first failure
///   (initializes `retry_count`),
///   [`defer_additional_timer`](Self::defer_additional_timer) for subsequent
///   timers. Mixing these corrupts state.
/// - **FIFO ordering**: Timers retry in `original_time` order; `retry_count`
///   applies to the head.
/// - **Cleanup**: [`complete_retry_success`](Self::complete_retry_success)
///   handles queue progression and key deletion when empty.
///
/// # Key Differences from `MessageDeferStore`
///
/// - Methods accept `Trigger` for trace context (span serialized to storage)
/// - `get_next_deferred_timer` returns `Option<(Trigger, u32)>` with restored
///   span
/// - Timers are indexed by `original_time` (not offset)
pub trait TimerDeferStore: Clone + Send + Sync + 'static {
    /// Error type. Must implement [`ClassifyError`] for retry decisions.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    // ─────────────────────────────────────────────────────────────────────────
    // Compound Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Defers a timer for the first time on this key.
    ///
    /// Stores the timer entry with its span context and initializes
    /// `retry_count = 0`.
    ///
    /// # Precondition
    ///
    /// Key must not already have deferred timers. See trait-level invariants.
    fn defer_first_timer(
        &self,
        trigger: &Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Defers an additional timer for an already-deferred key.
    ///
    /// Appends the timer without modifying `retry_count`. Queued timers
    /// retry in FIFO order after the head succeeds.
    ///
    /// # Precondition
    ///
    /// Key must already have deferred timers. See trait-level invariants.
    fn defer_additional_timer(
        &self,
        trigger: &Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { self.append_deferred_timer(trigger).await }
    }

    /// Completes a successful retry and advances or clears the queue.
    ///
    /// Removes the timer at `time`, then either resets `retry_count = 0` for
    /// the next timer or deletes the key entirely if the queue is empty.
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

    /// Increments retry count after a failed retry attempt.
    ///
    /// Returns the new count for backoff calculation.
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

    // ─────────────────────────────────────────────────────────────────────────
    // Queries
    // ─────────────────────────────────────────────────────────────────────────

    /// Returns the oldest deferred timer and current retry count, or `None`.
    ///
    /// The returned `Trigger` contains a freshly-constructed span linked to
    /// the stored parent context via `span.set_parent(context)`.
    fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Option<(Trigger, u32)>, Self::Error>> + Send;

    /// Returns `retry_count` if key is deferred, `None` otherwise.
    ///
    /// Prefer over [`get_next_deferred_timer`](Self::get_next_deferred_timer)
    /// when you don't need the timer data.
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

    /// Returns a stream of all deferred timer times for a key, sorted
    /// ascending.
    ///
    /// Used by `TimerDeferContext::scheduled()` to merge deferred times
    /// with active timer times for a unified view. Times are streamed in
    /// ascending order (Cassandra clustering order or `BTreeMap` key order).
    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static;

    // ─────────────────────────────────────────────────────────────────────────
    // Primitives (prefer compound operations above)
    // ─────────────────────────────────────────────────────────────────────────

    /// Appends a timer without modifying `retry_count`.
    ///
    /// Prefer [`defer_first_timer`](Self::defer_first_timer) or
    /// [`defer_additional_timer`](Self::defer_additional_timer).
    fn append_deferred_timer(
        &self,
        trigger: &Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes a timer without modifying `retry_count` or deleting the key.
    ///
    /// Prefer [`complete_retry_success`](Self::complete_retry_success).
    fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // ─────────────────────────────────────────────────────────────────────────
    // Internal (used by default implementations)
    // ─────────────────────────────────────────────────────────────────────────

    /// Sets `retry_count` directly. Used by default implementations.
    fn set_retry_count(
        &self,
        key: &Key,
        retry_count: u32,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes all data for a key. Used by default implementations.
    fn delete_key(&self, key: &Key) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Cached entry for timer defer state.
///
/// Stores the minimum timer's original time, parent trace context for span
/// reconstruction, and current retry count.
///
/// # Why Cache `Context` Instead of `Trigger`
///
/// The timer system uses `Arc<ArcSwap<Span>>` - when processing completes,
/// the span is replaced with `Span::none()`. Caching `Trigger` directly would
/// cause subsequent cache reads to get dead spans.
///
/// Instead, we cache the extracted `Context` (containing only upstream trace
/// identifiers) and create fresh spans at each access site using
/// `span.set_parent(context)`.
#[derive(Clone, Debug)]
pub struct CachedTimerEntry {
    /// Earliest timer's original fire time.
    pub time: CompactDateTime,
    /// Extracted parent trace context for span reconstruction.
    pub context: Context,
    /// Current retry count for backoff calculation.
    pub retry_count: u32,
}
