use crate::timers::datetime::CompactDateTimeError;
use crate::timers::scheduler::TimerSchedulerError;
use chrono::OutOfRangeError;
use std::error::Error;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TimerManagerError<T>
where
    T: Error + Debug,
{
    #[error("Timer store error: {0:#}")]
    Store(T),

    #[error("Failed to schedule timer: {0:#}")]
    Scheduler(#[from] TimerSchedulerError),

    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    #[error("Time must be in the future: {0:#}")]
    PastTime(#[from] OutOfRangeError),

    #[error("Time not found")]
    NotFound,

    #[error("Timer is inactive")]
    Inactive,

    #[error("Timer has been shutdown")]
    Shutdown,
}
