//! The erased deque adapter.

use super::write::ErasedWrite;
use super::{DynDequeState, Erased, ErasedDequeRead, ErasedStateError, StateCursor, read};
use crate::state::StoreOutcome;
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{CellType, ContextOf, DequeHandle, FromSession, ResolvedOf};
use crate::state::order_codec::UnitKey;
use async_stream::try_stream;
use async_trait::async_trait;

#[async_trait]
impl<S, T> DynDequeState<ResolvedOf<T>> for Erased<DequeHandle<S, T>>
where
    S: WritableStateSession,
    T: CellType<Key = UnitKey> + ErasedWrite + 'static,
    ResolvedOf<T>: Send + 'static,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    async fn len(&self) -> Result<usize, ErasedStateError> {
        self.0
            .len()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn is_empty(&self) -> Result<bool, ErasedStateError> {
        self.0
            .is_empty()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn get(&self, index: usize) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.0
            .get(index)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn push_back(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::deque_push_back(&self.0, item)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn push_front(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::deque_push_front(&self.0, item)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn pop_front(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.0
            .pop_front()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn pop_back(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.0
            .pop_back()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn peek_front(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.0
            .peek_front()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn peek_back(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.0
            .peek_back()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.0
            .clear()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    fn values(&self) -> ErasedDequeRead<ResolvedOf<T>> {
        let handle = self.0.clone();
        read(move |query| {
            let handle = handle.clone();
            StateCursor::new(try_stream! {
                for await item in handle.values().with_query(query).stream() {
                    yield item.map_err(|error| ErasedStateError::from_classified(&error))?;
                }
            })
        })
    }

    async fn commit(&self) -> Result<StoreOutcome, ErasedStateError> {
        self.0
            .commit()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn rollback(&self) -> StoreOutcome {
        self.0.rollback().await
    }
}
