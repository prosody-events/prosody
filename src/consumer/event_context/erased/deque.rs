//! The erased deque adapter.

use super::write::ErasedWrite;
use super::{
    BoxStateCursor, DequeScanConfig, DynDequeState, ErasedStateError, bound_usize, cursor,
};
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{CellType, ContextOf, DequeHandle, FromSession, ResolvedOf};
use crate::state::order_codec::UnitKey;
use async_stream::try_stream;
use async_trait::async_trait;

/// Erased deque wrapper over a typed [`DequeHandle`].
pub(in crate::consumer::event_context) struct ErasedDeque<S, T> {
    handle: DequeHandle<S, T>,
}

impl<S, T> ErasedDeque<S, T> {
    pub(in crate::consumer::event_context) fn new(handle: DequeHandle<S, T>) -> Self {
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
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn is_empty(&self) -> Result<bool, ErasedStateError> {
        self.handle
            .is_empty()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn get(&self, index: usize) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .get(index)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn push_back(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::deque_push_back(&self.handle, item)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn push_front(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::deque_push_front(&self.handle, item)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn pop_front(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .pop_front()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn pop_back(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .pop_back()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn peek_front(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .peek_front()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn peek_back(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.handle
            .peek_back()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.handle
            .clear()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    fn scan(&self, config: DequeScanConfig) -> BoxStateCursor<ResolvedOf<T>> {
        let handle = self.handle.clone();
        Box::new(cursor(try_stream! {
            let start = bound_usize(config.start);
            let end = bound_usize(config.end);
            let mut query = handle.query(config.dir).range((start, end));
            if let Some(limit) = config.limit {
                query = query.limit(limit);
            }
            for await item in query.values() {
                yield item.map_err(|error| ErasedStateError::from_classified(&error))?;
            }
        }))
    }

    async fn commit(&self) -> Result<(), ErasedStateError> {
        self.handle
            .commit()
            .await
            .map(drop)
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn rollback(&self) {
        self.handle.rollback().await;
    }
}
