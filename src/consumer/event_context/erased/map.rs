//! The erased map adapter.

use super::write::ErasedWrite;
use super::{BoxStateCursor, DynMapState, ErasedStateError, KeyScanConfig, StateCursor, key_query};
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{CellType, ContextOf, FromSession, MapHandle, MapQuery, ResolvedOf};
use crate::state::order_codec::UnitKey;
use crate::state::order_codec::Utf8KeyCodec;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;

/// Erased map wrapper over a typed [`MapHandle`] monomorphized on
/// [`Utf8KeyCodec`].
pub(in crate::consumer::event_context) struct ErasedMap<S, T> {
    handle: MapHandle<S, Utf8KeyCodec, T>,
}

impl<S, T> ErasedMap<S, T> {
    pub(in crate::consumer::event_context) fn new(handle: MapHandle<S, Utf8KeyCodec, T>) -> Self {
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

    fn scan(&self, config: KeyScanConfig) -> BoxStateCursor<(String, ResolvedOf<T>)> {
        let handle = self.handle.clone();
        let stream = try_stream! {
            let inner = MapQuery::new(handle.cells(), key_query(config)).entries();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                let (key, value) = item.map_err(|e| ErasedStateError::from_classified(&e))?;
                yield (key, value);
            }
        };
        Box::new(StateCursor::new(Box::pin(stream)))
    }

    fn keys(&self, config: KeyScanConfig) -> BoxStateCursor<String> {
        let handle = self.handle.clone();
        let stream = try_stream! {
            let inner = MapQuery::new(handle.cells(), key_query(config)).keys();
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
