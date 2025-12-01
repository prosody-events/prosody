//! Error types for timer management operations.
//!
//! Defines [`TimerManagerError`] enum covering all error conditions in
//! [`super::manager::TimerManager`]: storage failures, scheduling failures,
//! invalid datetime values, and timer lifecycle errors.

use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTimeError;
use crate::timers::scheduler::TimerSchedulerError;
use chrono::OutOfRangeError;
use std::error::Error;
use std::fmt::Debug;
use thiserror::Error;

/// Errors returned by [`super::manager::TimerManager`] methods.
///
/// The type parameter `T` is the error type from the underlying storage
/// implementation. Covers storage errors, scheduling errors, datetime
/// conversion/range errors, and timer lifecycle errors.
#[derive(Debug, Error)]
pub enum TimerManagerError<T>
where
    T: ClassifyError + Error + Debug,
{
    /// An error occurred in the persistent store layer.
    #[error("Timer store error: {0:#}")]
    Store(T),

    /// Failed to schedule or unschedule a timer in the in-memory scheduler.
    #[error("Failed to schedule timer: {0:#}")]
    Scheduler(#[from] TimerSchedulerError),

    /// A datetime conversion or arithmetic operation failed.
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    /// The provided time was not in the future.
    #[error("Time must be in the future: {0:#}")]
    PastTime(#[from] OutOfRangeError),

    /// The partition has been shut down and cannot process operations.
    #[error("Partition has been shutdown")]
    Shutdown,

    /// Message processing has been cancelled.
    #[error("Message processing has been cancelled")]
    Cancelled,

    /// The context is no longer valid because the event has already been
    /// processed.
    #[error("The context is no longer valid because the event has already been processed")]
    InvalidContext,

    /// Attempted to schedule a timer at the same time and type as the
    /// currently processing timer.
    ///
    /// This would cause a collision in the active triggers registry, where
    /// committing the current timer would deactivate the newly scheduled one.
    #[error("Cannot schedule timer at the same time and type as the current timer being processed")]
    ConflictsWithCurrentTimer,
}

/// Errors encountered when parsing timer-related data.
#[derive(Debug, Error)]
pub enum ParseError {
    /// Unknown timer type value encountered.
    #[error("Unknown timer type: {0}")]
    UnknownTimerType(i8),
}

impl<T> ClassifyError for TimerManagerError<T>
where
    T: ClassifyError + Error + Debug,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Persistent store error. Delegate to nested error's classification.
            TimerManagerError::Store(e) => e.classify_error(),

            // Scheduler error. Delegate to nested error's classification.
            TimerManagerError::Scheduler(e) => e.classify_error(),

            // DateTime conversion/arithmetic failed. Delegate to nested error's classification.
            TimerManagerError::DateTime(e) => e.classify_error(),

            // Provided time was not in the future. Delegate to nested error's classification.
            TimerManagerError::PastTime(e) => e.classify_error(),

            // Partition has been shut down. System is shutting down - cannot process operations.
            // Terminal to crash and signal that partition is no longer operational.
            TimerManagerError::Shutdown => ErrorCategory::Terminal,

            // Message processing was cancelled due to timeout or graceful shutdown. Transient
            // allows retry after system recovers - cancellation was due to external constraint,
            // not bad data.
            TimerManagerError::Cancelled => ErrorCategory::Transient,

            // Context no longer valid because event already processed. Duplicate processing
            // attempt with stale context. Permanent to drop duplicate rather than retry endlessly.
            //
            // ConflictsWithCurrentTimer: Attempted to schedule at the same time/type as current
            // timer. This is a programming error - the caller should use a different time.
            // Permanent to surface the bug.
            TimerManagerError::InvalidContext | TimerManagerError::ConflictsWithCurrentTimer => {
                ErrorCategory::Permanent
            }
        }
    }
}

// ClassifyError for external chrono type
impl ClassifyError for OutOfRangeError {
    fn classify_error(&self) -> ErrorCategory {
        // Time is outside representable range (before 1970 or after 2106, or negative
        // duration). Data-dependent - specific message has invalid time value.
        // Permanent to drop this bad message rather than retry endlessly.
        ErrorCategory::Permanent
    }
}

impl ClassifyError for ParseError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // UnknownTimerType: Failed to parse timer type from database. Indicates
            // corrupted data (invalid i8 value) or version skew where database has
            // timer types our code doesn't recognize. Not recoverable by retry.
            Self::UnknownTimerType(_) => ErrorCategory::Permanent,
        }
    }
}
