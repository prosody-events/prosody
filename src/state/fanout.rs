//! How a keyed-state read runs its per-cell futures. Each cell type selects
//! its [`Fanout`] at compile time.

use futures::stream::{Stream, StreamExt};
use std::convert::identity;
use std::future::Future;

/// Runs a stream of per-cell futures and yields their outputs in input order.
pub trait Fanout {
    /// Drives `futures` in input order. `window` bounds the futures in flight.
    fn drive<F: Future<Output: Send> + Send>(
        futures: impl Stream<Item = F> + Send,
        window: usize,
    ) -> impl Stream<Item = F::Output> + Send;
}

/// Polls one future at a time. The stream holds the future inline, so the
/// drive allocates nothing. Use it only for futures that never wait on I/O.
pub enum Sequential {}

/// Overlaps up to `window` futures through
/// [`buffered`](StreamExt::buffered). Each future in flight costs one task
/// allocation.
pub enum Concurrent {}

impl Fanout for Sequential {
    fn drive<F: Future<Output: Send> + Send>(
        futures: impl Stream<Item = F> + Send,
        _window: usize,
    ) -> impl Stream<Item = F::Output> + Send {
        futures.then(identity)
    }
}

impl Fanout for Concurrent {
    fn drive<F: Future<Output: Send> + Send>(
        futures: impl Stream<Item = F> + Send,
        window: usize,
    ) -> impl Stream<Item = F::Output> + Send {
        futures.buffered(window)
    }
}
