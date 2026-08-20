//! Object-safe, type-erased keyed-state handles for the FFI seam.
//!
//! The four language clients (`prosody-{js,py,rb,cs}`) cannot name the typed
//! [`ValueHandle`]/[`MapHandle`]/[`DequeHandle`] — those carry a codec, a key
//! codec, and a resolver as type parameters. This module erases them behind
//! three object-safe traits — [`DynValueState`], [`DynMapState`],
//! [`DynDequeState`] — that a binding wraps as an opaque handle. The six vend
//! methods on
//! [`DynEventContext`](crate::consumer::event_context::DynEventContext) mint
//! them; a handler binds once and operates many, mirroring the typed API.
//!
//! # What erasure fixes
//!
//! - **Codec.** The value families monomorphize over the codec recovered from
//!   the payload ([`ErasedStateCodec`]); the message families over the
//!   session's loader. The finite `3 kinds × {value, message}` matrix lives
//!   here once.
//! - **Map keys are always `String`** ([`Utf8KeyCodec`]). The typed Rust API
//!   keeps `i64`/`u64`/custom key codecs; the erased seam does not expose them.
//! - **Errors never carry `Terminal`.** [`ErasedStateError`] is a two-way
//!   `{Permanent, Transient}` category plus a message; a lower-layer `Terminal`
//!   folds to `Transient` at the [`ErasedStateError`] boundary constructor, the
//!   single construction point.
//!
//! # Fencing is inherited, not re-implemented
//!
//! The attempt-epoch fence lives in the typed cell interface: a handle or
//! stream used past its handler attempt errors when its op takes effect. The
//! erased wrappers add no fencing state — the [`StateCursor`] simply drives the
//! typed stream, which self-terminates at attempt boundaries. See the typed
//! handles and the session gate for the fence itself.

use crate::codec::{Codec, ErasedStateCodec};
use crate::consumer::kafka_state::MessageCell;
use crate::consumer::message::ConsumerMessage;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MessageLoader;
use crate::state::cell_key::Direction;
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{
    CellCodecError, CellStateError, CellType, ContextOf, DequeHandle, DequeStateError, FromSession,
    MapHandle, MapStateError, ResolvedOf, ValueHandle,
};
use crate::state::order_codec::{UnitKey, Utf8KeyCodec};

use async_stream::try_stream;
use async_trait::async_trait;
use futures::FutureExt;
use futures::stream::{BoxStream, StreamExt};
use std::fmt::Display;
use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Bound;
use thiserror::Error;
use tokio::sync::Mutex;

/// Two-way error category for the FFI state seam.
///
/// `Terminal` is deliberately absent: the keyed-state layer never surfaces it
/// (owner posture — a lower-layer `Terminal` redelivers as `Transient`), so it
/// is structurally unrepresentable here even if a caller bypasses the boundary
/// fold on [`ErasedStateError`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ErasedCategory {
    /// Business-logic failure — do not retry (unregistered name, codec error,
    /// null-write rejection).
    Permanent,

    /// Transient failure — retry may succeed (store/loader hiccup, a
    /// terminated attempt, a folded lower-layer `Terminal`).
    Transient,
}

/// Erased map scan constraints.
#[derive(Clone, Debug)]
pub struct MapScanConfig {
    /// The scan direction.
    pub dir: Direction,
    /// The maximum number of present items.
    pub limit: Option<NonZeroUsize>,
    /// The direction-relative start edge.
    pub start: Bound<String>,
    /// The direction-relative end edge.
    pub end: Bound<String>,
}

impl Default for MapScanConfig {
    fn default() -> Self {
        Self {
            dir: Direction::Forward,
            limit: None,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

/// Erased deque scan constraints.
#[derive(Clone, Debug)]
pub struct DequeScanConfig {
    /// The scan direction.
    pub dir: Direction,
    /// The maximum number of present items.
    pub limit: Option<NonZeroUsize>,
    /// The inclusive or exclusive front-relative range start.
    pub start: Bound<u64>,
    /// The inclusive or exclusive front-relative range end.
    pub end: Bound<u64>,
}

impl Default for DequeScanConfig {
    fn default() -> Self {
        Self {
            dir: Direction::Forward,
            limit: None,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

impl From<ErasedCategory> for ErrorCategory {
    fn from(category: ErasedCategory) -> Self {
        match category {
            ErasedCategory::Permanent => ErrorCategory::Permanent,
            ErasedCategory::Transient => ErrorCategory::Transient,
        }
    }
}

/// The error every erased state op and vend method returns.
///
/// Carries its classification as data ([`ErasedCategory`]) so the four
/// bindings can branch on it directly; [`ClassifyError`] also reaches it
/// through the box, so the binding handler-error bridges reclassify with zero
/// changes. Fields are private so no caller can mint an inconsistently
/// classified error — the two `pub(crate)` constructors are the only mints.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct ErasedStateError {
    category: ErasedCategory,
    message: String,
}

impl ErasedStateError {
    /// The sole fold point from a typed error to the erased seam: any
    /// classification maps through, with `Terminal` folded to `Transient` (the
    /// state layer never surfaces `Terminal`).
    pub(crate) fn from_classified<E>(error: &E) -> Self
    where
        E: ClassifyError + Display,
    {
        let category = match error.classify_error() {
            ErrorCategory::Permanent => ErasedCategory::Permanent,
            ErrorCategory::Transient | ErrorCategory::Terminal => ErasedCategory::Transient,
        };
        Self {
            category,
            message: error.to_string(),
        }
    }

    /// A synthetic terminated-family error — a [`StateCursor`] used after
    /// `close()` or a failure. `Transient`, mirroring
    /// [`StateAccessError::Terminated`](crate::state::StateAccessError).
    fn terminated(message: &str) -> Self {
        Self {
            category: ErasedCategory::Transient,
            message: message.to_owned(),
        }
    }

    /// The `Permanent` rejection of a JSON-null value write, naming
    /// `clear`/`remove` as the way to express deletion.
    fn null_write() -> Self {
        Self {
            category: ErasedCategory::Permanent,
            message: "JSON null is not a storable value; use clear (value/deque) or remove (map) \
                      to delete an entry"
                .to_owned(),
        }
    }

    /// This error's category, as data for the bindings.
    #[must_use]
    pub fn category(&self) -> ErasedCategory {
        self.category
    }

    /// The rendered error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl ClassifyError for ErasedStateError {
    fn classify_error(&self) -> ErrorCategory {
        self.category.into()
    }
}

/// Erased single-value collection — the object-safe face of
/// [`ValueHandle`].
#[async_trait]
pub trait DynValueState<Item: Send + 'static>: Send + Sync {
    /// Reads the current value (`None` when absent/cleared).
    async fn get(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Buffers a write of `item`. Rejects the JSON-null sentinel (`Permanent`).
    async fn set(&self, item: Item) -> Result<(), ErasedStateError>;

    /// Buffers a clear of the value.
    async fn clear(&self) -> Result<(), ErasedStateError>;

    /// Durably commits buffered ops mid-handler (at-least-once).
    async fn commit(&self) -> Result<(), ErasedStateError>;

    /// Discards buffered uncommitted ops. Infallible no-op on a terminated
    /// session.
    async fn rollback(&self);
}

/// Erased ordered map — the object-safe face of
/// [`MapHandle`], keys always `String`.
#[async_trait]
pub trait DynMapState<Item: Send + 'static>: Send + Sync {
    /// Reads `key`'s value (`None` when absent).
    async fn get(&self, key: String) -> Result<Option<Item>, ErasedStateError>;

    /// Whether a stored cell exists for `key`, without decoding its value or
    /// running the resolver (a presence read through the dirty overlay).
    async fn contains_key(&self, key: String) -> Result<bool, ErasedStateError>;

    /// Whether the map holds no live entries.
    async fn is_empty(&self) -> Result<bool, ErasedStateError>;

    /// Reads each key in input order as one aligned batch. Absent keys yield
    /// `None`, and duplicate keys retain their positions.
    async fn get_many(&self, keys: Vec<String>) -> Result<Vec<Option<Item>>, ErasedStateError>;

    /// Tests each key for presence in input order as one aligned batch.
    async fn contains_many(&self, keys: Vec<String>) -> Result<Vec<bool>, ErasedStateError>;

    /// Inserts or overwrites `key`. Rejects the JSON-null sentinel
    /// (`Permanent`).
    async fn set(&self, key: String, item: Item) -> Result<(), ErasedStateError>;

    /// Removes `key`.
    async fn remove(&self, key: String) -> Result<(), ErasedStateError>;

    /// Removes every entry.
    async fn clear(&self) -> Result<(), ErasedStateError>;

    /// A demand-driven cursor over the live entries in key order.
    fn scan(&self, config: MapScanConfig) -> BoxStateCursor<(String, Item)>;

    /// A demand-driven cursor over the live entry **keys** in key order,
    /// without decoding or resolving any value (zero Kafka fetches for a
    /// message-backed map). A key is present even when its value is not.
    fn keys(&self, config: MapScanConfig) -> BoxStateCursor<String>;

    /// Durably commits buffered ops mid-handler (at-least-once).
    async fn commit(&self) -> Result<(), ErasedStateError>;

    /// Discards buffered uncommitted ops.
    async fn rollback(&self);
}

/// Erased deque — the object-safe face of
/// [`DequeHandle`].
#[async_trait]
pub trait DynDequeState<Item: Send + 'static>: Send + Sync {
    /// The number of live elements.
    async fn len(&self) -> Result<usize, ErasedStateError>;

    /// Whether the deque holds no live elements.
    async fn is_empty(&self) -> Result<bool, ErasedStateError>;

    /// Reads the element at front-relative position `index` (`None` past the
    /// end).
    async fn get(&self, index: usize) -> Result<Option<Item>, ErasedStateError>;

    /// Appends at the back. Rejects the JSON-null sentinel (`Permanent`).
    async fn push_back(&self, item: Item) -> Result<(), ErasedStateError>;

    /// Prepends at the front. Rejects the JSON-null sentinel (`Permanent`).
    async fn push_front(&self, item: Item) -> Result<(), ErasedStateError>;

    /// Removes and returns the front element (`None` when empty).
    async fn pop_front(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Removes and returns the back element (`None` when empty).
    async fn pop_back(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Reads the front element without a length read (`None` when empty).
    async fn peek_front(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Reads the back element without a length read (`None` when empty).
    async fn peek_back(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Removes every element.
    async fn clear(&self) -> Result<(), ErasedStateError>;

    /// A demand-driven cursor over the live elements in index order.
    fn scan(&self, config: DequeScanConfig) -> BoxStateCursor<Item>;

    /// Durably commits buffered ops mid-handler (at-least-once).
    async fn commit(&self) -> Result<(), ErasedStateError>;

    /// Discards buffered uncommitted ops.
    async fn rollback(&self);
}

/// Boxed erased single-value handle a vend method returns.
pub type BoxValueState<Item> = Box<dyn DynValueState<Item>>;

/// Boxed erased map handle a vend method returns.
pub type BoxMapState<Item> = Box<dyn DynMapState<Item>>;

/// Boxed erased deque handle a vend method returns.
pub type BoxDequeState<Item> = Box<dyn DynDequeState<Item>>;

/// Boxed [`StateCursor`] a `scan` returns.
pub type BoxStateCursor<Item> = Box<StateCursor<Item>>;

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
    /// Wraps an erased typed stream as a fresh, open cursor.
    pub(crate) fn new(stream: BoxStream<'static, Result<Item, ErasedStateError>>) -> Self {
        Self {
            inner: Mutex::new(CursorInner::Open(stream)),
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

/// Lowers an owned erased item into a typed handle write, bridging the two FFI
/// write shapes — owned JSON payload vs borrowed message ref — with no clone.
///
/// Exactly two impls: the owned write for any [`Codec`]-backed value cell, and
/// the borrowed write for the Kafka [`MessageCell`]. Private and narrow: only
/// the four write shapes the seam needs (value set, map set, two deque pushes),
/// plus the value-family null guard.
trait ErasedWrite: CellType + Sized {
    /// Rejects the JSON-null "absent" sentinel on the value families; a no-op
    /// for message cells (a `ConsumerMessage` is never the null sentinel).
    fn reject_null(item: &ResolvedOf<Self>) -> Result<(), ErasedStateError>;

    fn value_set<'a, S>(
        handle: &'a ValueHandle<S, Self>,
        item: ResolvedOf<Self>,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>;

    fn map_set<'a, S>(
        handle: &'a MapHandle<S, Utf8KeyCodec, Self>,
        key: String,
        item: ResolvedOf<Self>,
    ) -> impl Future<Output = Result<(), MapStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>;

    fn deque_push_back<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: ResolvedOf<Self>,
    ) -> impl Future<Output = Result<(), DequeStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>;

    fn deque_push_front<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: ResolvedOf<Self>,
    ) -> impl Future<Output = Result<(), DequeStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>;
}

/// Owned write: a plain codec cell (JSON value, C# passthrough) — the write
/// value moves straight into the typed handle, no clone.
impl<C> ErasedWrite for C
where
    C: Codec,
    C::Payload: ErasedStateCodec,
{
    fn reject_null(item: &C::Payload) -> Result<(), ErasedStateError> {
        if item.is_absent_sentinel() {
            return Err(ErasedStateError::null_write());
        }
        Ok(())
    }

    fn value_set<'a, S>(
        handle: &'a ValueHandle<S, Self>,
        item: C::Payload,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.set(item)
    }

    fn map_set<'a, S>(
        handle: &'a MapHandle<S, Utf8KeyCodec, Self>,
        key: String,
        item: C::Payload,
    ) -> impl Future<Output = Result<(), MapStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.set(key, item)
    }

    fn deque_push_back<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: C::Payload,
    ) -> impl Future<Output = Result<(), DequeStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.push_back(item)
    }

    fn deque_push_front<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: C::Payload,
    ) -> impl Future<Output = Result<(), DequeStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.push_front(item)
    }
}

/// Borrowed write: the Kafka message cell — the typed handle takes the message
/// by reference, so the future owns the item and lends it. No overlap with the
/// owned impl: [`MessageCell`] is a `WithResolver`, which does not impl
/// [`Codec`].
impl<L: MessageLoader + 'static> ErasedWrite for MessageCell<L> {
    fn reject_null(_item: &ConsumerMessage<L::Payload>) -> Result<(), ErasedStateError> {
        // A message ref is never the null sentinel.
        Ok(())
    }

    async fn value_set<'a, S>(
        handle: &'a ValueHandle<S, Self>,
        item: ConsumerMessage<L::Payload>,
    ) -> Result<(), CellStateError<CellCodecError<Self>>>
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.set(&item).await
    }

    async fn map_set<'a, S>(
        handle: &'a MapHandle<S, Utf8KeyCodec, Self>,
        key: String,
        item: ConsumerMessage<L::Payload>,
    ) -> Result<(), MapStateError<CellCodecError<Self>>>
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.set(key, &item).await
    }

    async fn deque_push_back<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: ConsumerMessage<L::Payload>,
    ) -> Result<(), DequeStateError<CellCodecError<Self>>>
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.push_back(&item).await
    }

    async fn deque_push_front<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: ConsumerMessage<L::Payload>,
    ) -> Result<(), DequeStateError<CellCodecError<Self>>>
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.push_front(&item).await
    }
}

/// Erased value wrapper over a typed [`ValueHandle`].
pub(super) struct ErasedValue<S, T> {
    handle: ValueHandle<S, T>,
}

impl<S, T> ErasedValue<S, T> {
    pub(super) fn new(handle: ValueHandle<S, T>) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl<S, T> DynValueState<ResolvedOf<T>> for ErasedValue<S, T>
where
    S: WritableStateSession,
    T: CellType<Key = UnitKey> + ErasedWrite,
    ResolvedOf<T>: Send + 'static,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    async fn get(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .get()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn set(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::value_set(&self.handle, item)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.handle
            .clear()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn commit(&self) -> Result<(), ErasedStateError> {
        self.handle
            .commit()
            .await
            .map(drop)
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn rollback(&self) {
        self.handle.rollback().await;
    }
}

/// Erased map wrapper over a typed [`MapHandle`] monomorphized on
/// [`Utf8KeyCodec`].
pub(super) struct ErasedMap<S, T> {
    handle: MapHandle<S, Utf8KeyCodec, T>,
}

impl<S, T> ErasedMap<S, T> {
    pub(super) fn new(handle: MapHandle<S, Utf8KeyCodec, T>) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl<S, T> DynMapState<ResolvedOf<T>> for ErasedMap<S, T>
where
    S: WritableStateSession,
    T: CellType<Key = UnitKey> + ErasedWrite + 'static,
    ResolvedOf<T>: Send + 'static,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    async fn get(&self, key: String) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .get(&key)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn contains_key(&self, key: String) -> Result<bool, ErasedStateError> {
        self.handle
            .contains_key(&key)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn is_empty(&self) -> Result<bool, ErasedStateError> {
        self.handle
            .is_empty()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn get_many(
        &self,
        keys: Vec<String>,
    ) -> Result<Vec<Option<ResolvedOf<T>>>, ErasedStateError> {
        self.handle
            .get_many(&keys)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn contains_many(&self, keys: Vec<String>) -> Result<Vec<bool>, ErasedStateError> {
        self.handle
            .contains_many(&keys)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn set(&self, key: String, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::map_set(&self.handle, key, item)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn remove(&self, key: String) -> Result<(), ErasedStateError> {
        self.handle
            .remove(&key)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.handle
            .clear()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    fn scan(&self, config: MapScanConfig) -> BoxStateCursor<(String, ResolvedOf<T>)> {
        let handle = self.handle.clone();
        let stream = try_stream! {
            let mut query = handle.query(config.dir);
            query = match &config.start {
                Bound::Included(start) => query.from(start),
                Bound::Excluded(start) => query.after(start),
                Bound::Unbounded => query,
            };
            query = match &config.end {
                Bound::Included(end) => query.to(end),
                Bound::Excluded(end) => query.before(end),
                Bound::Unbounded => query,
            };
            if let Some(limit) = config.limit {
                query = query.limit(limit);
            }
            let inner = query.entries();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                let (key, value) = item.map_err(|e| ErasedStateError::from_classified(&e))?;
                yield (key, value);
            }
        };
        Box::new(StateCursor::new(Box::pin(stream)))
    }

    fn keys(&self, config: MapScanConfig) -> BoxStateCursor<String> {
        let handle = self.handle.clone();
        let stream = try_stream! {
            let mut query = handle.query(config.dir);
            query = match &config.start {
                Bound::Included(start) => query.from(start),
                Bound::Excluded(start) => query.after(start),
                Bound::Unbounded => query,
            };
            query = match &config.end {
                Bound::Included(end) => query.to(end),
                Bound::Excluded(end) => query.before(end),
                Bound::Unbounded => query,
            };
            if let Some(limit) = config.limit {
                query = query.limit(limit);
            }
            let inner = query.keys();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                let key = item.map_err(|e| ErasedStateError::from_classified(&e))?;
                yield key;
            }
        };
        Box::new(StateCursor::new(Box::pin(stream)))
    }

    async fn commit(&self) -> Result<(), ErasedStateError> {
        self.handle
            .commit()
            .await
            .map(drop)
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn rollback(&self) {
        self.handle.rollback().await;
    }
}

/// Erased deque wrapper over a typed [`DequeHandle`].
pub(super) struct ErasedDeque<S, T> {
    handle: DequeHandle<S, T>,
}

impl<S, T> ErasedDeque<S, T> {
    pub(super) fn new(handle: DequeHandle<S, T>) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl<S, T> DynDequeState<ResolvedOf<T>> for ErasedDeque<S, T>
where
    S: WritableStateSession,
    T: CellType<Key = UnitKey> + ErasedWrite + 'static,
    ResolvedOf<T>: Send + 'static,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    async fn len(&self) -> Result<usize, ErasedStateError> {
        self.handle
            .len()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn is_empty(&self) -> Result<bool, ErasedStateError> {
        self.handle
            .is_empty()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn get(&self, index: usize) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .get(index)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn push_back(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::deque_push_back(&self.handle, item)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn push_front(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::deque_push_front(&self.handle, item)
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn pop_front(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .pop_front()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn pop_back(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .pop_back()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn peek_front(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .peek_front()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn peek_back(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .peek_back()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.handle
            .clear()
            .await
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    fn scan(&self, config: DequeScanConfig) -> BoxStateCursor<ResolvedOf<T>> {
        let handle = self.handle.clone();
        let stream = try_stream! {
            let start = bound_usize(config.start);
            let end = bound_usize(config.end);
            let mut query = handle.query(config.dir).range((start, end));
            if let Some(limit) = config.limit {
                query = query.limit(limit);
            }
            let inner = query.values();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                let value = item.map_err(|e| ErasedStateError::from_classified(&e))?;
                yield value;
            }
        };
        Box::new(StateCursor::new(Box::pin(stream)))
    }

    async fn commit(&self) -> Result<(), ErasedStateError> {
        self.handle
            .commit()
            .await
            .map(drop)
            .map_err(|e| ErasedStateError::from_classified(&e))
    }

    async fn rollback(&self) {
        self.handle.rollback().await;
    }
}

fn bound_usize(bound: Bound<u64>) -> Bound<usize> {
    bound.map(|value| usize::try_from(value).unwrap_or(usize::MAX))
}

#[cfg(test)]
mod tests {
    //! Pure state-machine pins for [`StateCursor`]. Seam-level cursor pins
    //! (laziness against a counting store, parity) live in the parent module's
    //! `tests.rs`; these drive synthetic streams to prove the three-state
    //! transitions, so they need no session substrate.

    use super::{CursorInner, ErasedCategory, ErasedStateError, StateCursor};
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
        StateCursor::new(stream::iter(items).boxed())
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
        let cursor = StateCursor::new(source.boxed());
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
        let cursor = StateCursor::new(source.boxed());
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
