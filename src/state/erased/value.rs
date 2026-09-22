//! The erased value adapter.

use super::write::ErasedWrite;
use super::{DynValueState, Erased, ErasedStateError};
use crate::state::StoreOutcome;
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{CellType, ContextOf, FromSession, ResolvedOf, ValueHandle};
use crate::state::order_codec::UnitKey;
use async_trait::async_trait;

#[async_trait]
impl<S, T> DynValueState<ResolvedOf<T>> for Erased<ValueHandle<S, T>>
where
    S: WritableStateSession,
    T: CellType<Key = UnitKey> + ErasedWrite,
    ResolvedOf<T>: Send + 'static,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    async fn get(&self) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.0
            .get()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn set(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::value_set(&self.0, item)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.0
            .clear()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
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
