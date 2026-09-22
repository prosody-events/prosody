//! The erased map adapter.

use super::write::ErasedWrite;
use super::{DynMapState, Erased, ErasedKeyRead, ErasedStateError, cursor, read};
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{CellType, ContextOf, FromSession, MapHandle, ResolvedOf};
use crate::state::order_codec::UnitKey;
use crate::state::order_codec::Utf8KeyCodec;
use crate::state::{ErasedKeyQuery, StoreOutcome};
use async_stream::try_stream;
use async_trait::async_trait;

#[async_trait]
impl<S, T> DynMapState<ResolvedOf<T>> for Erased<MapHandle<S, Utf8KeyCodec, T>>
where
    S: WritableStateSession,
    T: CellType<Key = UnitKey> + ErasedWrite + 'static,
    ResolvedOf<T>: Send + 'static,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    async fn get(&self, key: String) -> Result<Option<ResolvedOf<T>>, ErasedStateError> {
        self.0
            .get(&key)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn contains_key(&self, key: String) -> Result<bool, ErasedStateError> {
        self.0
            .contains_key(&key)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn is_empty(&self) -> Result<bool, ErasedStateError> {
        self.0
            .is_empty()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn get_many(
        &self,
        keys: Vec<String>,
    ) -> Result<Vec<Option<ResolvedOf<T>>>, ErasedStateError> {
        self.0
            .get_many(&keys)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn contains_many(&self, keys: Vec<String>) -> Result<Vec<bool>, ErasedStateError> {
        self.0
            .contains_many(&keys)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn set(&self, key: String, item: ResolvedOf<T>) -> Result<(), ErasedStateError> {
        T::reject_null(&item)?;
        T::map_set(&self.0, key, item)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn remove(&self, key: String) -> Result<(), ErasedStateError> {
        self.0
            .remove(&key)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.0
            .clear()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    fn entries(&self) -> ErasedKeyRead<(String, ResolvedOf<T>)> {
        let handle = self.0.clone();
        read(move |query: ErasedKeyQuery| {
            let handle = handle.clone();
            cursor(try_stream! {
                for await item in handle.entries().with_query(query.borrowed()).stream() {
                    yield item.map_err(|error| ErasedStateError::from_classified(&error))?;
                }
            })
        })
    }

    fn keys(&self) -> ErasedKeyRead<String> {
        let handle = self.0.clone();
        read(move |query: ErasedKeyQuery| {
            let handle = handle.clone();
            cursor(try_stream! {
                for await item in handle.keys().with_query(query.borrowed()).stream() {
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
