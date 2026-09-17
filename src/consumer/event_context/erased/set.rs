//! The erased set adapter.

use super::{BoxStateCursor, DynSetState, ErasedStateError, KeyScanConfig, StateCursor, key_query};
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{SetHandle, SetQuery};
use crate::state::order_codec::Utf8KeyCodec;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;

/// Erased set wrapper over a typed UTF-8 set.
pub(in crate::consumer::event_context) struct ErasedSet<S> {
    handle: SetHandle<S, Utf8KeyCodec>,
}

impl<S> ErasedSet<S> {
    pub(in crate::consumer::event_context) fn new(handle: SetHandle<S, Utf8KeyCodec>) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl<S> DynSetState for ErasedSet<S>
where
    S: WritableStateSession,
{
    async fn contains(&self, key: String) -> Result<bool, ErasedStateError> {
        self.handle
            .contains(&key)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn contains_many(&self, keys: Vec<String>) -> Result<Vec<bool>, ErasedStateError> {
        self.handle
            .contains_many(&keys)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn is_empty(&self) -> Result<bool, ErasedStateError> {
        self.handle
            .is_empty()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn insert(&self, key: String) -> Result<(), ErasedStateError> {
        self.handle
            .insert(key)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn remove(&self, key: String) -> Result<(), ErasedStateError> {
        self.handle
            .remove(&key)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn clear(&self) -> Result<(), ErasedStateError> {
        self.handle
            .clear()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    fn keys(&self, config: KeyScanConfig) -> BoxStateCursor<String> {
        let handle = self.handle.clone();
        let stream = try_stream! {
            let inner = SetQuery::new(handle.cells(), key_query(config)).keys();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item.map_err(|error| ErasedStateError::from_classified(&error))?;
            }
        };
        Box::new(StateCursor::new(Box::pin(stream)))
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
