//! Demand-driven cursors and their lifecycle.

use super::ErasedStateError;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use std::num::NonZeroUsize;
use tokio::sync::Mutex;

/// A demand-driven scan across the FFI boundary.
///
/// Owns the erased typed stream and is polled only from inside a foreign
/// `next()` — no spawn, no channel, no fencing state (the typed stream
/// self-terminates at attempt boundaries). Concurrent `next()` callers
/// serialize on the mutex; the guard is held across the poll, which is sound
/// because the typed stream never holds the session gate across a yield.
pub struct StateCursor<Item> {
    inner: Mutex<CursorInner<Item>>,
}

/// The cursor's states. Exhaustion and close/failure are distinct so a
/// fully-drained cursor keeps answering `Ok(None)` (fused) while a closed or
/// failed one errors on the next `next()`.
enum CursorInner<Item> {
    /// Live: the boxed typed stream, not yet exhausted or closed.
    Open(BoxStream<'static, Result<Item, ErasedStateError>>),

    /// The stream returned `None`; further `next()` calls fuse to `Ok(None)`.
    Exhausted,

    /// A ready-chunk pull encountered an error after one or more ready items.
    /// Those items are returned first; the next pull surfaces this original
    /// error and transitions to `Closed`.
    DeferredError(Option<ErasedStateError>),

    /// Explicit `close()` or a first error; further `next()` calls error.
    Closed,
}

impl<Item> StateCursor<Item> {
    /// Wraps an owned erased stream as a fresh, open cursor.
    pub(crate) fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Item, ErasedStateError>> + Send + 'static,
    {
        Self {
            inner: Mutex::new(CursorInner::Open(stream.boxed())),
        }
    }

    /// Polls one item. Normal exhaustion fuses to `Ok(None)`; the first error
    /// closes the cursor and returns it; a `next()` after `close()` or an error
    /// returns a `Transient` terminated error. Concurrent callers serialize on
    /// the mutex.
    ///
    /// # Errors
    ///
    /// Returns the stream's [`ErasedStateError`], or a terminated-family error
    /// once the cursor is closed.
    pub async fn next(&self) -> Result<Option<Item>, ErasedStateError> {
        let mut guard = self.inner.lock().await;
        let stream = match &mut *guard {
            CursorInner::Exhausted => return Ok(None),
            CursorInner::Closed => {
                return Err(ErasedStateError::terminated(
                    "state cursor used after it was closed",
                ));
            }
            CursorInner::DeferredError(error) => {
                let Some(error) = error.take() else {
                    *guard = CursorInner::Closed;
                    return Err(ErasedStateError::terminated(
                        "state cursor used after it was closed",
                    ));
                };
                *guard = CursorInner::Closed;
                return Err(error);
            }
            CursorInner::Open(stream) => stream,
        };
        match stream.next().await {
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(error)) => {
                *guard = CursorInner::Closed;
                Err(error)
            }
            None => {
                *guard = CursorInner::Exhausted;
                Ok(None)
            }
        }
    }

    /// Awaits one item, then drains up to `max_items - 1` additional items
    /// only while they are immediately ready.
    ///
    /// This is the shared FFI batching primitive: clients transport the
    /// returned vector across their language boundary and flatten it into the
    /// language's item-oriented async iterator. It never waits to fill the
    /// vector, so first-item latency and demand-driven backpressure are
    /// preserved. Whole chunk pulls serialize on the cursor mutex.
    ///
    /// If an error is immediately ready after one or more items, the items are
    /// returned first and the original error is deferred until the next pull,
    /// preserving item-before-error stream order. Exhaustion remains fused.
    ///
    /// # Errors
    ///
    /// Returns the stream's [`ErasedStateError`], including a deferred error
    /// from the preceding chunk, or a terminated-family error once closed.
    pub async fn next_ready_chunk(
        &self,
        max_items: NonZeroUsize,
    ) -> Result<Option<Vec<Item>>, ErasedStateError> {
        enum Tail {
            Pending,
            Exhausted,
            Failed(ErasedStateError),
        }

        let mut guard = self.inner.lock().await;
        let stream = match &mut *guard {
            CursorInner::Exhausted => return Ok(None),
            CursorInner::Closed => {
                return Err(ErasedStateError::terminated(
                    "state cursor used after it was closed",
                ));
            }
            CursorInner::DeferredError(error) => {
                let Some(error) = error.take() else {
                    *guard = CursorInner::Closed;
                    return Err(ErasedStateError::terminated(
                        "state cursor used after it was closed",
                    ));
                };
                *guard = CursorInner::Closed;
                return Err(error);
            }
            CursorInner::Open(stream) => stream,
        };

        let Some(first) = stream.next().await else {
            *guard = CursorInner::Exhausted;
            return Ok(None);
        };
        let first = match first {
            Ok(item) => item,
            Err(error) => {
                *guard = CursorInner::Closed;
                return Err(error);
            }
        };

        // Clients currently use 256. Bound eager allocation even if another
        // caller supplies a much larger cap; the vector can still grow to that
        // cap when the stream actually has that many ready items.
        let mut items = Vec::with_capacity(max_items.get().min(256));
        items.push(first);
        let mut tail = Tail::Pending;
        while items.len() < max_items.get() {
            match stream.next().now_or_never() {
                None => break,
                Some(None) => {
                    tail = Tail::Exhausted;
                    break;
                }
                Some(Some(Ok(item))) => items.push(item),
                Some(Some(Err(error))) => {
                    tail = Tail::Failed(error);
                    break;
                }
            }
        }
        match tail {
            Tail::Pending => {}
            Tail::Exhausted => *guard = CursorInner::Exhausted,
            Tail::Failed(error) => *guard = CursorInner::DeferredError(Some(error)),
        }
        Ok(Some(items))
    }

    /// Closes the cursor, dropping the stream (RAII releases any resources it
    /// holds). Idempotent; a subsequent `next()` returns a terminated error.
    pub async fn close(&self) {
        *self.inner.lock().await = CursorInner::Closed;
    }
}

#[cfg(test)]
mod tests {
    //! Pure state-machine pins for [`StateCursor`]. Seam-level cursor pins
    //! (laziness against a counting store, parity) live in
    //! `consumer::event_context::tests`; these drive synthetic streams to prove
    //! the three-state transitions, so they need no session substrate.

    use super::{CursorInner, ErasedStateError, StateCursor};
    use crate::state::erased::ErasedCategory;
    use color_eyre::eyre::{Result, eyre};
    use futures::stream::{self, StreamExt};
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    /// Builds a cursor over an explicit item sequence.
    fn cursor(items: Vec<Result<i32, ErasedStateError>>) -> StateCursor<i32> {
        StateCursor::new(stream::iter(items))
    }

    fn boom() -> ErasedStateError {
        ErasedStateError {
            category: ErasedCategory::Transient,
            message: "boom".to_owned(),
        }
    }

    /// Normal exhaustion fuses: after the stream returns `None`, every further
    /// `next()` keeps answering `Ok(None)` rather than erroring — `Exhausted`
    /// is distinct from `Closed`. Falsify: point exhaustion at `Closed`
    /// (`*guard = CursorInner::Closed` on the `None` arm) and the second
    /// post-exhaustion `next()` errors.
    #[tokio::test]
    async fn exhaustion_is_fused() -> Result<()> {
        let cursor = cursor(vec![Ok(1_i32), Ok(2_i32)]);
        assert_eq!(cursor.next().await?, Some(1_i32));
        assert_eq!(cursor.next().await?, Some(2_i32));
        assert_eq!(cursor.next().await?, None);
        assert_eq!(cursor.next().await?, None, "exhaustion must stay fused");
        assert!(matches!(*cursor.inner.lock().await, CursorInner::Exhausted));
        Ok(())
    }

    /// The first error closes the cursor: it surfaces once, then `next()`
    /// returns a `Transient` terminated error — never `Ok(None)` and never the
    /// items after it. Falsify: set `Exhausted` on the error arm and the
    /// post-error `next()` returns `Ok(None)`.
    #[tokio::test]
    async fn first_error_closes() -> Result<()> {
        let cursor = cursor(vec![Ok(1_i32), Err(boom()), Ok(3_i32)]);
        assert_eq!(cursor.next().await?, Some(1_i32));
        let Err(error) = cursor.next().await else {
            return Err(eyre!("the errored item must surface"));
        };
        assert_eq!(error.message(), "boom");
        let Err(terminated) = cursor.next().await else {
            return Err(eyre!(
                "a closed cursor must error, not yield Ok(None) or item 3"
            ));
        };
        assert_eq!(terminated.category(), ErasedCategory::Transient);
        Ok(())
    }

    /// Ready chunks stop at the caller's cap, preserve every item exactly
    /// once, and retain fused exhaustion. Falsify: remove the length guard
    /// from `next_ready_chunk` and the first assertion receives all items.
    #[tokio::test]
    async fn ready_chunks_respect_the_cap_and_fuse() -> Result<()> {
        let cursor = cursor((0_i32..600_i32).map(Ok).collect());
        let cap = NonZeroUsize::new(256).ok_or_else(|| eyre!("256 is nonzero"))?;
        let first = cursor
            .next_ready_chunk(cap)
            .await?
            .ok_or_else(|| eyre!("first chunk missing"))?;
        let second = cursor
            .next_ready_chunk(cap)
            .await?
            .ok_or_else(|| eyre!("second chunk missing"))?;
        let third = cursor
            .next_ready_chunk(cap)
            .await?
            .ok_or_else(|| eyre!("third chunk missing"))?;
        assert_eq!((first.len(), second.len(), third.len()), (256, 256, 88));
        let all: Vec<i32> = first.into_iter().chain(second).chain(third).collect();
        assert_eq!(all, (0_i32..600_i32).collect::<Vec<_>>());
        assert_eq!(cursor.next_ready_chunk(cap).await?, None);
        assert_eq!(cursor.next_ready_chunk(cap).await?, None);
        Ok(())
    }

    /// A ready error behind successful items is not allowed to erase those
    /// items: the chunk arrives first, then the original categorized error,
    /// then the ordinary closed-cursor error. Falsify: return the tail error
    /// immediately and the first call loses `[1, 2]`.
    #[tokio::test]
    async fn ready_chunk_defers_an_error_behind_items() -> Result<()> {
        let cursor = cursor(vec![Ok(1_i32), Ok(2_i32), Err(boom()), Ok(4_i32)]);
        let cap = NonZeroUsize::new(256).ok_or_else(|| eyre!("256 is nonzero"))?;
        assert_eq!(
            cursor.next_ready_chunk(cap).await?,
            Some(vec![1_i32, 2_i32])
        );
        let Err(error) = cursor.next().await else {
            return Err(eyre!("the original deferred error must surface"));
        };
        assert_eq!(error.message(), "boom");
        assert!(cursor.next_ready_chunk(cap).await.is_err());
        Ok(())
    }

    /// A ready chunk never waits for the cap after its first item. Falsify:
    /// replace the non-blocking tail polls with `.await` and this times out on
    /// the permanently-pending second item.
    #[tokio::test]
    async fn ready_chunk_does_not_wait_to_fill() -> Result<()> {
        let source = stream::once(async { Ok(7_i32) }).chain(stream::pending());
        let cursor = StateCursor::new(source);
        let cap = NonZeroUsize::new(256).ok_or_else(|| eyre!("256 is nonzero"))?;
        let chunk = timeout(Duration::from_millis(100), cursor.next_ready_chunk(cap))
            .await
            .map_err(|_| eyre!("ready chunk waited for a second item"))??;
        assert_eq!(chunk, Some(vec![7_i32]));
        Ok(())
    }

    /// Cancelling a blocked first-item pull leaves the owned stream in the
    /// open cursor, so a later pull can continue it. Falsify: move the stream
    /// out of `CursorInner` while awaiting and cancellation drops it.
    #[tokio::test]
    async fn cancelled_ready_chunk_pull_preserves_the_stream() -> Result<()> {
        let (release, blocked) = oneshot::channel();
        let source = stream::once(async move {
            blocked
                .await
                .map_err(|error| ErasedStateError::terminated(&error.to_string()))
        });
        let cursor = StateCursor::new(source);
        let cap = NonZeroUsize::new(256).ok_or_else(|| eyre!("256 is nonzero"))?;
        assert!(
            timeout(Duration::from_millis(10), cursor.next_ready_chunk(cap))
                .await
                .is_err(),
            "the first pull must block and be cancelled"
        );
        release
            .send(9_i32)
            .map_err(|_| eyre!("the cancelled pull dropped the stream"))?;
        assert_eq!(cursor.next_ready_chunk(cap).await?, Some(vec![9_i32]));
        Ok(())
    }

    /// `next()` after `close()` errors `Transient`. Falsify: make `close()` set
    /// `Exhausted` and the follow-up `next()` returns `Ok(None)`.
    #[tokio::test]
    async fn next_after_close_errors() -> Result<()> {
        let cursor = cursor(vec![Ok(1_i32), Ok(2_i32)]);
        cursor.close().await;
        let Err(error) = cursor.next().await else {
            return Err(eyre!("a closed cursor must error"));
        };
        assert_eq!(error.category(), ErasedCategory::Transient);
        Ok(())
    }

    /// `close()` is idempotent and closes an open cursor mid-scan.
    #[tokio::test]
    async fn close_is_idempotent() -> Result<()> {
        let cursor = cursor(vec![Ok(1_i32), Ok(2_i32), Ok(3_i32)]);
        assert_eq!(cursor.next().await?, Some(1_i32));
        cursor.close().await;
        cursor.close().await;
        assert!(cursor.next().await.is_err());
        Ok(())
    }

    /// Two tasks draining one cursor serialize on the mutex: their combined
    /// results are exactly the seeded set, each item once — no duplication, no
    /// loss. Falsify: a `next()` that did not advance the stream (returning a
    /// cached clone) would duplicate items and break the exact-once union.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_next_serialize_without_loss() -> Result<()> {
        const N: i32 = 200;
        let cursor = Arc::new(cursor((0_i32..N).map(Ok).collect()));
        let drain = |cursor: Arc<StateCursor<i32>>| async move {
            let mut seen = Vec::new();
            while let Some(item) = cursor.next().await? {
                seen.push(item);
            }
            Ok::<_, ErasedStateError>(seen)
        };
        let a = tokio::spawn(drain(cursor.clone()));
        let b = tokio::spawn(drain(cursor.clone()));
        let mut union: Vec<i32> = a.await??;
        union.extend(b.await??);
        let unique: BTreeSet<i32> = union.iter().copied().collect();
        assert_eq!(union.len(), N as usize, "no item was yielded twice or lost");
        assert_eq!(
            unique,
            (0_i32..N).collect::<BTreeSet<_>>(),
            "the union is exactly the seeded set"
        );
        Ok(())
    }
}
