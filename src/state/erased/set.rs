//! The erased set adapter.

use super::{DynSetState, Erased, ErasedKeyRead, ErasedStateError, cursor, read};
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::SetHandle;
use crate::state::order_codec::Utf8KeyCodec;
use crate::state::{ErasedKeyQuery, StoreOutcome};
use async_stream::try_stream;
use async_trait::async_trait;

#[async_trait]
impl<S> DynSetState for Erased<SetHandle<S, Utf8KeyCodec>>
where
    S: WritableStateSession,
{
    async fn contains(&self, key: String) -> Result<bool, ErasedStateError> {
        self.0
            .contains(&key)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn contains_many(&self, keys: Vec<String>) -> Result<Vec<bool>, ErasedStateError> {
        self.0
            .contains_many(&keys)
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn is_empty(&self) -> Result<bool, ErasedStateError> {
        self.0
            .is_empty()
            .await
            .map_err(|error| ErasedStateError::from_classified(&error))
    }

    async fn insert(&self, key: String) -> Result<(), ErasedStateError> {
        self.0
            .insert(&key)
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
