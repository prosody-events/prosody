//! Error types for timer management operations.
//!
//! This module defines the [`TimerManagerError`] enum, which represents all
//! possible error conditions that can occur when interacting with the
//! [`super::manager::TimerManager`], including storage failures, scheduling
//! failures, invalid datetime values, and timer lifecycle errors.

use crate::timers::datetime::CompactDateTimeError;
use crate::timers::scheduler::TimerSchedulerError;
use chrono::OutOfRangeError;
use std::error::Error;
use std::fmt::Debug;
use thiserror::Error;

/// Errors returned by [`super::manager::TimerManager`] methods.
///
/// The type parameter `T` is the error type returned by the underlying
/// storage implementation. This enum covers:
/// - Storage errors (`Store`)
/// - Scheduling errors (`Scheduler`)
/// - Datetime conversion or range errors (`DateTime`, `PastTime`)
/// - Logical errors in timer lifecycle (`NotFound`, `Inactive`, `Shutdown`)
#[derive(Debug, Error)]
pub enum TimerManagerError<T>
where
    T: Error + Debug,
{
    /// An error occurred in the persistent store layer.
    ///
    /// Wraps the underlying error type `T` returned by the storage
    /// implementation.
    #[error("Timer store error: {0:#}")]
    Store(T),

    /// Failed to schedule or unschedule a timer in the in-memory scheduler.
    ///
    /// This may occur if the scheduler has been shut down or cannot accept
    /// new commands.
    #[error("Failed to schedule timer: {0:#}")]
    Scheduler(#[from] TimerSchedulerError),

    /// A datetime conversion or arithmetic operation failed.
    ///
    /// Wraps [`CompactDateTimeError`] when converting between chrono types,
    /// performing duration arithmetic, or comparing datetimes.
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    /// The provided time was not in the future.
    ///
    /// Wraps [`OutOfRangeError`] from the `chrono` crate when attempting to
    /// schedule a timer in the past.
    #[error("Time must be in the future: {0:#}")]
    PastTime(#[from] OutOfRangeError),

    /// No timer matching the specified key and time was found.
    #[error("Time not found")]
    NotFound,

    /// The timer is currently inactive in the scheduler.
    ///
    /// This can occur if the timer has not been loaded into the in-memory
    /// scheduler or has already been deactivated.
    #[error("Timer is inactive")]
    Inactive,

    /// The scheduler has been shut down and cannot process further operations.
    #[error("Timer has been shutdown")]
    Shutdown,
}
