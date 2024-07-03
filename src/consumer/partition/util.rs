//! This module provides a utility to wrap a future and a value, returning the
//! value once the future completes.
//!
//! The primary struct, `WithValue`, is designed to be used in asynchronous
//! contexts where the completion of a future should result in a predetermined
//! value being returned. This can simplify scenarios where the outcome of an
//! asynchronous operation is not the value produced by the future, but a fixed
//! value associated with the completion of the future.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;

/// The `WithValue` struct wraps a future and a value, returning the value once
/// the future completes.
#[pin_project]
pub struct WithValue<T, F> {
    /// The value to be returned once the future completes.
    value: T,

    /// The future to be awaited.
    #[pin]
    future: F,
}

impl<T, F> WithValue<T, F> {
    /// Creates a new `WithValue` instance.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to be returned once the future completes.
    /// * `future` - The future to be awaited.
    ///
    /// # Returns
    ///
    /// A new `WithValue` instance.
    pub fn new(value: T, future: F) -> Self {
        WithValue { value, future }
    }
}

impl<T, F> Future for WithValue<T, F>
where
    T: Clone,
    F: Future,
{
    type Output = T;

    /// Polls the future, returning the value once the future completes.
    ///
    /// # Arguments
    ///
    /// * `cx` - The task context to poll with.
    ///
    /// # Returns
    ///
    /// `Poll::Ready` with the value if the future completes, otherwise
    /// `Poll::Pending`.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Project the fields to handle pinning safely.
        let mut projection = self.project();

        // Poll the inner future. If it is ready, return the cloned value.
        projection
            .future
            .as_mut()
            .poll(cx)
            .map(|_| projection.value.clone())
    }
}
