//! Deque operations bound to one state session.

use super::{
    Bound, CellCodecError, CellType, Collection, CollectionRead, CollectionWrite, ContextOf,
    DEQUE_POINT_ITERATION_MAX, DequeKind, DequeQuery, DequeStateError, Direction, Educe, Empty,
    FromSession, I64KeyCodec, Keyed, MetaDecodeError, NonZeroUsize, OrderedKeyCodec, Plan,
    ResolvedOf, Span, StateSession, StoreOutcome, Stream, UnitKey, Window, WritableStateSession,
    WriteOf, bounds, collection_methods, evictions, instrument, write_bounds,
};
use crate::state::cell::Values;
use crate::state::{DequeRead, ReadQuery, ReadSource};
use async_stream::try_stream;
use futures::StreamExt;
use tracing::{Instrument, info_span};

/// Typed, owned handle over a codec-backed deque.
///
/// The handle owns the bound collection. That collection's session clone is
/// `Clone + Send + Sync + 'static`, which FFI requires. Each method opens
/// exactly one scoped operation. [`values`](Self::values) runs a short planning
/// operation, then drives the plan it returns. `Clone` is cheap.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct DequeHandle<S, T> {
    pub(super) cells: Collection<S, DequeKind<T>>,
}

#[collection_methods(field = cells, session = S)]
impl<S, T> DequeHandle<S, T>
where
    S: StateSession,
    T: CellType<Key = UnitKey>,
{
    /// The number of live elements (`tail − head`, O(1) from the bounds cell).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt or the count exceeds `usize`, or an access error from the
    /// session.
    #[instrument(name = "deque.len", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn len(&self) -> Result<usize, DequeStateError<CellCodecError<T>>> {
        Ok(bounds(op).await?.len()?)
    }

    /// Whether the deque holds no live elements (`head == tail`).
    ///
    /// # Errors
    ///
    /// Returns a `Permanent` [`DequeStateError`] when the bounds cell is
    /// corrupt, or an access error from the session.
    #[instrument(name = "deque.is_empty", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn is_empty(&self) -> Result<bool, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        Ok(window.head == window.tail)
    }

    /// Reads and resolves the element at front-relative position `index`
    /// (`VecDeque::get` semantics): position `0` is the front, a single cell
    /// read at `head + index`, `None` when `index >= len`.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the cell does not decode, a
    /// `Permanent` meta error when the bounds cell is corrupt, or an access
    /// error from the session.
    #[instrument(
        name = "deque.get",
        skip_all,
        fields(collection = self.cells.name().as_str(), deque.index = Empty),
        err
    )]
    #[read(op)]
    pub async fn get(
        &self,
        index: usize,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        // Recorded as i64: the OTel layer exports signed ints as typed Int
        // attributes but stringifies unsigned values; a beyond-i64 index is
        // out of window anyway and stays unrecorded.
        if let Ok(index) = i64::try_from(index) {
            Span::current().record("deque.index", index);
        }
        let window = bounds(op).await?;
        if index >= window.len()? {
            return Ok(None);
        }
        let absolute = window.absolute(index)?;
        Ok(op.get(DequeKind::<T>::ENTRIES, &absolute).await?)
    }

    /// Reads and resolves the front element (position `0`) — exactly
    /// [`get(0)`](Self::get), reading the front slot `head` directly instead of
    /// deriving it from a position, so `None` when the deque is empty.
    ///
    /// # Endpoint-slot semantics
    ///
    /// A peek is an endpoint-*slot* read and never searches inward. Under a TTL
    /// the window can hold holes (see the module's window invariant): an
    /// expired endpoint slot yields `None` **even when [`len`](Self::len)
    /// `> 0` and live interior elements exist**, matching what a `get` at
    /// that position returns. [`peek_back`](Self::peek_back) is the
    /// symmetric back-endpoint read and shares this contract. Parity with
    /// `get` is total: an over-wide window whose span exceeds `usize` errors
    /// [`IndexOverflow`](MetaDecodeError::IndexOverflow) here exactly as it
    /// does through `get`'s length check — the span validation is pure
    /// arithmetic on the bounds cell already in hand, adding no read.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the entry does not decode, a
    /// `Permanent` meta error when the bounds cell is corrupt, or an access
    /// error from the session.
    #[instrument(name = "deque.peek_front", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn peek_front(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        if window.len()? == 0 {
            return Ok(None);
        }
        Ok(op.get(DequeKind::<T>::ENTRIES, &window.head).await?)
    }

    /// Reads and resolves the back element (position `len - 1`) — exactly
    /// [`get(len - 1)`](Self::get) reading the back slot `tail − 1` directly,
    /// and without the empty-deque negative-index error that a manual
    /// `len`-then-`get` incurs: `None` when the deque is empty. Shares
    /// [`peek_front`](Self::peek_front)'s endpoint-slot / TTL-hole contract and
    /// its total parity with `get`.
    ///
    /// # Errors
    ///
    /// See [`peek_front`](Self::peek_front).
    #[instrument(name = "deque.peek_back", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn peek_back(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        if window.len()? == 0 {
            return Ok(None);
        }
        let last = window
            .tail
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        Ok(op.get(DequeKind::<T>::ENTRIES, &last).await?)
    }

    /// Clamps the query to the stored window and captures its read plan.
    /// At most [`DEQUE_POINT_ITERATION_MAX`] positions use point reads.
    /// Wider selections use a bounded scan. Empty selections read no entries.
    #[read(op)]
    async fn stream_plan(
        &self,
        dir: Direction,
        start: &Bound<usize>,
        end: &Bound<usize>,
    ) -> Result<Plan<S, Keyed<I64KeyCodec, T>, [u8; 8]>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        let window_len = window.len()?;
        let start = match start {
            Bound::Included(position) => *position,
            Bound::Excluded(position) => position.saturating_add(1),
            Bound::Unbounded => 0,
        }
        .min(window_len);
        let end = match end {
            Bound::Included(position) => position.saturating_add(1),
            Bound::Excluded(position) => *position,
            Bound::Unbounded => window_len,
        }
        .min(window_len);
        if start >= end {
            return Ok(op.coordinates(DequeKind::<T>::ENTRIES, Vec::new()));
        }
        let len = end - start;
        let first = window.absolute(start)?;
        let last = window.absolute(end - 1)?;
        // The scan limit cannot exceed the selected position count.
        if let Some(limit) = NonZeroUsize::new(len).filter(|n| n.get() > DEQUE_POINT_ITERATION_MAX)
        {
            let (start, end) = match dir {
                Direction::Forward => (first, last),
                Direction::Backward => (last, first),
            };
            return Ok(op.range_within(DequeKind::<T>::ENTRIES, &start, dir, &end, limit));
        }
        // Both endpoints are valid, so interior index arithmetic cannot overflow.
        // Allocate at most 128 coordinates once per stream, before item reads.
        let mut coordinates = Vec::with_capacity(len);
        coordinates.extend((0..len).map(|offset| I64KeyCodec::encode(&(first + offset as i64))));
        if dir == Direction::Backward {
            coordinates.reverse();
        }
        Ok(op.coordinates(DequeKind::<T>::ENTRIES, coordinates))
    }

    /// Builds a query over live values in front-to-back order.
    ///
    /// The initial bounds read fixes positions, not values. Each fetch reads
    /// current values and skips absent positions. If a pop and push reuse a
    /// position before its fetch, the stream yields its new value.
    ///
    /// Small windows use point reads; wider windows use range scans.
    /// Both sources preserve order and skip absent cells. Earlier chunks can
    /// emit before a later fetch fails. A failed point chunk emits no values.
    ///
    /// Planning and point fetches hold session admission. Resolution and yields
    /// hold no admission; range scans run without admission after planning.
    /// The handler can mutate this deque between items. Every completion checks
    /// the attempt fence, including errors and exhaustion.
    pub fn values(
        &self,
    ) -> DequeRead<
        impl ReadSource<
            Query = DequeQuery,
            Output: Stream<Item = Result<ResolvedOf<T>, DequeStateError<CellCodecError<T>>>> + Send,
        > + '_,
    >
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        ReadQuery::new(DequeQuery::new(), move |query: DequeQuery| {
            let (start, end) = query.bounds();
            let span = info_span!(
                "deque.stream",
                collection = self.cells.name().as_str(),
                direction = ?query.dir,
            );
            try_stream! {
                let plan = self
                    .stream_plan(query.dir, &start, &end)
                    .instrument(span.clone())
                    .await?;
                let inner = plan.with_limit(query.limit).projected::<Values>();
                futures::pin_mut!(inner);
                while let Some(item) = inner.next().instrument(span.clone()).await {
                    let (_, value) = item?;
                    yield value;
                }
            }
        })
    }

    /// Appends `value` at the back, extending the window to `tail + 1`.
    ///
    /// # Bounded capacity
    ///
    /// On a deque registered with a `capacity`, a push first evicts from the
    /// **front** toward the cap (see the module's capacity invariant).
    ///
    /// The evictions and the append stage as one transaction. A
    /// `ReadCommitted` rollback restores the evicted front slots.
    /// `ReadUncommitted` applies them eagerly.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, a
    /// `Permanent` meta error on index-space exhaustion, or an access error.
    #[instrument(name = "deque.push_back", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn push_back(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        let next_tail = window
            .tail
            .checked_add(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let evict = i64::from(evictions(window, op.capacity()));
        let new_head = window
            .head
            .checked_add(evict)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        // Append first, the sole encode, then evict the front. `evict ≤ span`
        // (see `evictions`), so `new_head ≤ tail`. The cleared half-open range
        // therefore never holds the slot this push appended.
        op.set(DequeKind::<T>::ENTRIES.at(&window.tail), value)?;
        for index in window.head..new_head {
            op.clear(DequeKind::<T>::ENTRIES.at(&index));
        }
        write_bounds(op, Window::new(new_head, next_tail)?)
    }

    /// Prepends `value` at the front, extending the window to `head − 1`.
    ///
    /// This is the mirror of [`Self::push_back`]. On a bounded deque it evicts
    /// from the **back** toward the cap.
    ///
    /// # Errors
    ///
    /// See [`Self::push_back`].
    #[instrument(name = "deque.push_front", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn push_front(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        let prev_head = window
            .head
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let evict = i64::from(evictions(window, op.capacity()));
        let new_tail = window
            .tail
            .checked_sub(evict)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        op.set(DequeKind::<T>::ENTRIES.at(&prev_head), value)?;
        for index in new_tail..window.tail {
            op.clear(DequeKind::<T>::ENTRIES.at(&index));
        }
        write_bounds(op, Window::new(prev_head, new_tail)?)
    }

    /// Removes and returns the front element, and moves `head` past it. Returns
    /// `None` when the deque is empty. The element resolves *before* the clear
    /// and the head move, so a resolve failure stages nothing at all.
    ///
    /// A pop is an endpoint-slot mutation. Under a TTL an expired front slot
    /// yields `None`, and the pop still consumes that slot: it clears the slot
    /// and moves `head` on. A `while let Some(v) = pop_front()` drain therefore
    /// stops at the first hole. See [`peek_front`](Self::peek_front) for the
    /// endpoint-slot contract.
    ///
    /// A cancelled or failed pop is atomic for a structural reason, not a
    /// checked one. One journal holds the whole invocation's mutations, and
    /// that journal replays only on a successful return.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the entry does not decode, a
    /// `Permanent` meta error on corruption, or an access error.
    #[instrument(name = "deque.pop_front", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn pop_front(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        if window.head >= window.tail {
            return Ok(None);
        }
        // `head < tail` bounds `head` strictly below `i64::MAX`, so the move
        // cannot overflow. The check keeps the arithmetic total.
        let next_head = window
            .head
            .checked_add(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let value = op.take(DequeKind::<T>::ENTRIES, &window.head).await?;
        write_bounds(op, Window::new(next_head, window.tail)?)?;
        Ok(value)
    }

    /// Removes and returns the back element, and moves `tail` back past it.
    /// Returns `None` when the deque is empty. This mirrors
    /// [`Self::pop_front`]: the element resolves before the mutation.
    ///
    /// # Errors
    ///
    /// See [`Self::pop_front`].
    #[instrument(name = "deque.pop_back", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn pop_back(
        &self,
    ) -> Result<Option<ResolvedOf<T>>, DequeStateError<CellCodecError<T>>> {
        let window = bounds(op).await?;
        if window.head >= window.tail {
            return Ok(None);
        }
        let last = window
            .tail
            .checked_sub(1)
            .ok_or(MetaDecodeError::IndexOverflow)?;
        let value = op.take(DequeKind::<T>::ENTRIES, &last).await?;
        write_bounds(op, Window::new(window.head, last)?)?;
        Ok(value)
    }

    /// Removes every element and the window bounds, and **resets the index
    /// space** (see the module's window invariant). Within the event the deque
    /// reads empty from this program point, and the next push writes index 0.
    /// After a commit, exactly the repopulated elements survive. After an
    /// abort, the deque is untouched.
    ///
    /// The cost is O(handler writes). One whole-layout reset covers both
    /// declared sections, so no cell takes a per-cell path.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "deque.clear", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn clear(&self) -> Result<(), DequeStateError<CellCodecError<T>>> {
        op.clear_collection();
        Ok(())
    }

    /// Durably commits this deque's buffered ops mid-handler — entries and
    /// the window bounds together. At-least-once; the mid-handler durability
    /// section of the [`collection`](crate::state::collection) module states
    /// the contract, including the over-budget batch split.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "deque.commit", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn commit(&self) -> Result<StoreOutcome, DequeStateError<CellCodecError<T>>>
    where
        S: WritableStateSession,
    {
        Ok(self.cells.commit().await?)
    }

    /// Discards this deque's buffered uncommitted ops — entries and the window
    /// bounds together — reverting reads to the last [`commit`](Self::commit),
    /// or the pre-event committed state if none. Infallible; the mid-handler
    /// durability section of the [`collection`](crate::state::collection)
    /// module states the contract.
    #[instrument(name = "deque.rollback", skip_all, fields(collection = self.cells.name().as_str()))]
    pub async fn rollback(&self) -> StoreOutcome
    where
        S: WritableStateSession,
    {
        self.cells.rollback().await
    }
}
