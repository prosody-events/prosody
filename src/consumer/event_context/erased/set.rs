//! The erased set adapter.

use super::query::encoding_buffer;
use super::{BoxStateCursor, DynSetState, ErasedKeyQuery, ErasedStateError, cursor};
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::SetHandle;
use crate::state::order_codec::Utf8KeyCodec;
use async_stream::try_stream;
use async_trait::async_trait;

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
            .insert(&key)
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

    fn read_keys(&self, query: ErasedKeyQuery) -> BoxStateCursor<String> {
        let handle = self.handle.clone();
        Box::new(cursor(try_stream! {
            for await item in handle.keys(encoding_buffer(&query)).with_query(query.borrowed()).stream() {
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
