//! Standalone deque queries and their owned streams.

use super::StateReader;
use crate::Key;
use crate::codec::Codec;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{CellType, ContextOf, DequeDescriptor, FromSession, ResolvedOf};
use crate::state::order_codec::UnitKey;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::session::ReadSession;
use crate::state_reader::{MemoryReaderBackend, ReaderBackend};
use futures::{Stream, StreamExt};
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};
use tokio::task::coop::cooperative;

/// A directional deque stream query for a standalone reader.
#[must_use]
pub struct DequeReaderQuery<'a, T, C: Codec, B = MemoryReaderBackend<C>> {
    pub(super) reader: &'a StateReader<DequeDescriptor<T>, C, B>,
    pub(super) key: Key,
    pub(super) dir: Direction,
    pub(super) start: Bound<usize>,
    pub(super) end: Bound<usize>,
    pub(super) limit: Option<NonZeroUsize>,
}

impl<T, C, B> DequeReaderQuery<'_, T, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
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

    /// Streams committed values in the query direction.
    ///
    /// # Errors
    ///
    /// Returns an error when session acquisition or handle binding fails.
    pub async fn values(
        self,
    ) -> Result<
        impl Stream<Item = Result<ResolvedOf<T>, StateReaderError>> + 'static,
        StateReaderError,
    >
    where
        T: 'static,
        ResolvedOf<T>: 'static,
        for<'s> ContextOf<'s, T>: FromSession<'s, ReadSession<C, B>>,
    {
        let handle = self.reader.bound(self.key).await?;
        Ok(async_stream::try_stream! {
            let query = handle.query(self.dir).range((self.start, self.end));
            let query = match self.limit {
                Some(limit) => query.limit(limit),
                None => query,
            };
            let inner = query.values();
            futures::pin_mut!(inner);
            while let Some(item) = cooperative(inner.next()).await {
                yield item.map_err(|error| StateReaderError::store(&error))?;
            }
        })
    }
}
