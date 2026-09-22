//! The erased set adapter.

use super::{DynSetState, ErasedKeyRead, ErasedStateError, cursor, read};
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::SetHandle;
use crate::state::order_codec::Utf8KeyCodec;
use crate::state::{ErasedKeyQuery, StoreOutcome};
use async_stream::try_stream;
use async_trait::async_trait;

/// Erased set wrapper over a typed UTF-8 set.
pub(crate) struct ErasedSet<S> {
    handle: SetHandle<S, Utf8KeyCodec>,
}

impl<S> ErasedSet<S> {
    pub(crate) fn new(handle: SetHandle<S, Utf8KeyCodec>) -> Self {
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

    fn keys(&self) -> ErasedKeyRead<String> {
        let handle = self.handle.clone();
        read(move |query: ErasedKeyQuery| {
            let handle = handle.clone();
            Box::new(cursor(try_stream! {
                for await item in handle.keys().with_query(query.borrowed()).stream() {
                    yield item.map_err(|error| ErasedStateError::from_classified(&error))?;
                }
            }))
        })
    }

    async fn commit(&self) -> Result<StoreOutcome, ErasedStateError> {
        self.handle
            .commit()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn rollback(&self) -> StoreOutcome {
        self.handle.rollback().await
    }
}
