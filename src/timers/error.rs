//! Error types for timer management operations.
//!
//! Defines [`TimerManagerError`] enum covering all error conditions in
//! [`super::manager::TimerManager`]: storage failures, scheduling failures,
//! invalid datetime values, and timer lifecycle errors.

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
    T: Error + Debug,
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

    /// No timer matching the specified key and time was found.
    #[error("Time not found")]
    NotFound,

    /// The timer is currently inactive in the scheduler.
    #[error("Timer is inactive")]
    Inactive,

    /// The scheduler has been shut down and cannot process operations.
    #[error("Timer has been shutdown")]
    Shutdown,

    /// The context is no longer valid because the event has already been
    /// processed.
    #[error("The context is no longer valid because the event has already been processed")]
    InvalidContext,
}
