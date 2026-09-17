//! The erased value adapter.

use super::write::ErasedWrite;
use super::{DynValueState, ErasedStateError};
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{CellType, ContextOf, FromSession, ResolvedOf, ValueHandle};
use crate::state::order_codec::UnitKey;
use async_trait::async_trait;

// Erased value wrapper over a typed [`ValueHandle`].
pub(in crate::consumer::event_context) struct ErasedValue<S, T> {
    handle: ValueHandle<S, T>,
}

impl<S, T> ErasedValue<S, T> {
    pub(in crate::consumer::event_context) fn new(handle: ValueHandle<S, T>) -> Self {
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
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn set(&self, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::value_set(&self.handle, item)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.handle
            .clear()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
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
