//! How a keyed-state read runs its per-cell futures.
//!
//! A cell type selects its [`Fanout`] at compile time. A resolver that can
//! wait on a loader overlaps its futures in a bounded window. A cell that
//! finishes on its first poll runs its futures in order, with no task per
//! cell.

use futures::stream::{Stream, StreamExt};
use std::convert::identity;
use std::future::Future;

/// Runs a stream of per-cell futures and yields their outputs in input order.
pub trait Fanout {
    /// Drives `futures` in input order. `window` bounds the futures in flight.
    fn drive<St>(
        futures: St,
        window: usize,
    ) -> impl Stream<Item = <St::Item as Future>::Output> + Send
    where
        St: Stream + Send,
        St::Item: Future + Send,
        <St::Item as Future>::Output: Send;
}

/// Polls one future at a time. The stream holds the future inline, so the
/// drive allocates nothing. Use it only for futures that never wait on I/O.
pub enum Sequential {}

/// Overlaps up to `window` futures through
/// [`buffered`](StreamExt::buffered). Each future in flight costs one task
/// allocation.
pub enum Concurrent {}

impl Fanout for Sequential {
    fn drive<St>(
        futures: St,
        _window: usize,
    ) -> impl Stream<Item = <St::Item as Future>::Output> + Send
    where
        St: Stream + Send,
        St::Item: Future + Send,
        <St::Item as Future>::Output: Send,
    {
        futures.then(identity)
    }
}

impl Fanout for Concurrent {
    fn drive<St>(
        futures: St,
        window: usize,
    ) -> impl Stream<Item = <St::Item as Future>::Output> + Send
    where
        St: Stream + Send,
        St::Item: Future + Send,
        <St::Item as Future>::Output: Send,
    {
        futures.buffered(window)
    }
}
