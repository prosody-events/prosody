//! Deque queries over front-relative positions.

use super::{DequeHandle, DequeStateError};
use crate::state::cell::Values;
use crate::state::cell_key::Direction;
use crate::state::collection::StateSession;
use crate::state::descriptor::{CellCodecError, CellType, ContextOf, FromSession, ResolvedOf};
use crate::state::order_codec::UnitKey;
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};
use tracing::{Instrument, info_span};

/// A directional deque stream query over front-relative positions.
///
/// Build one with [`DequeHandle::query`]. Finish it with
/// [`values`](Self::values).
#[must_use]
pub struct DequeQuery<'a, S, T> {
    pub(super) handle: &'a DequeHandle<S, T>,
    pub(super) dir: Direction,
    pub(super) start: Bound<usize>,
    pub(super) end: Bound<usize>,
    pub(super) limit: Option<NonZeroUsize>,
}

impl<'a, S, T> DequeQuery<'a, S, T>
where
    S: StateSession,
    T: CellType<Key = UnitKey>,
{
    /// Sets the front-relative position range.
    pub fn range<R: RangeBounds<usize>>(mut self, range: R) -> Self {
        self.start = range.start_bound().cloned();
        self.end = range.end_bound().cloned();
        self
    }

    /// Sets the maximum number of present values.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Streams the query's live values.
    pub fn values(
        self,
    ) -> impl Stream<Item = Result<ResolvedOf<T>, DequeStateError<CellCodecError<T>>>> + 'a
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let span = info_span!(
            "deque.stream",
            collection = self.handle.cells.name().as_str(),
            direction = ?self.dir,
        );
        try_stream! {
            let plan = self.handle
                .stream_plan(self.dir, &self.start, &self.end)
                .instrument(span.clone())
                .await?;
            let inner = plan.with_limit(self.limit).projected::<Values>();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().instrument(span.clone()).await {
                let (_, value) = item?;
                yield value;
            }
        }
    }
}
