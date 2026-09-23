//! Read-only state adapters for foreign-language clients.

use crate::Key;
use crate::codec::Codec;
use crate::state::ErasedKeyQuery;
use crate::state::descriptor::{DequeDescriptor, MapDescriptor, SetDescriptor, ValueDescriptor};
use crate::state::erased::{
    Erased, ErasedDequeRead, ErasedKeyRead, ErasedStateError, StateCursor, read,
};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state_reader::{ReaderBackend, StateReader, StateReaderError};
use async_trait::async_trait;
use std::sync::Arc;

pub use crate::state::ReadCachePolicy as ErasedReadCache;

/// Read-only access to a published value collection.
#[async_trait]
pub trait ErasedValueReader<Item: Send + 'static>: Send + Sync {
    /// Reads the committed value for `key`.
    async fn get(&self, key: String) -> Result<Option<Item>, ErasedStateError>;
}

/// Shared value-reader representation stored by native FFI wrappers.
pub type SharedValueReader<Item> = Arc<dyn ErasedValueReader<Item>>;

/// Read-only access to a published string-keyed map collection.
#[async_trait]
pub trait ErasedMapReader<Item: Send + 'static>: Send + Sync {
    /// Reads one committed map entry.
    async fn get(&self, key: String, map_key: String) -> Result<Option<Item>, ErasedStateError>;

    /// Reports whether one committed map entry exists without decoding it.
    async fn contains_key(&self, key: String, map_key: String) -> Result<bool, ErasedStateError>;

    /// Reads committed map entries aligned with `map_keys`.
    async fn get_many(
        &self,
        key: String,
        map_keys: Vec<String>,
    ) -> Result<Vec<Option<Item>>, ErasedStateError>;

    /// Tests committed presence aligned with `map_keys`.
    async fn contains_many(
        &self,
        key: String,
        map_keys: Vec<String>,
    ) -> Result<Vec<bool>, ErasedStateError>;

    /// Reports whether the committed map is empty.
    async fn is_empty(&self, key: String) -> Result<bool, ErasedStateError>;

    /// A fluent query over committed entries in key order.
    fn entries(&self, key: String) -> ErasedKeyRead<(String, Item)>;

    /// A fluent query over committed keys, without decoding values.
    fn keys(&self, key: String) -> ErasedKeyRead<String>;
}

/// Shared map-reader representation stored by native FFI wrappers.
pub type SharedMapReader<Item> = Arc<dyn ErasedMapReader<Item>>;

/// Read-only access to a published string-keyed set collection.
#[async_trait]
pub trait ErasedSetReader: Send + Sync {
    /// Reports whether the committed set contains `member`.
    async fn contains(&self, key: String, member: String) -> Result<bool, ErasedStateError>;

    /// Tests committed membership aligned with `members`.
    async fn contains_many(
        &self,
        key: String,
        members: Vec<String>,
    ) -> Result<Vec<bool>, ErasedStateError>;

    /// Reports whether the committed set has no members.
    async fn is_empty(&self, key: String) -> Result<bool, ErasedStateError>;

    /// A fluent query over committed members in key order.
    fn keys(&self, key: String) -> ErasedKeyRead<String>;
}

/// Shared set-reader representation stored by native FFI wrappers.
pub type SharedSetReader = Arc<dyn ErasedSetReader>;

/// Read-only access to a published deque collection.
#[async_trait]
pub trait ErasedDequeReader<Item: Send + 'static>: Send + Sync {
    /// Reads one front-relative committed element.
    async fn get(&self, key: String, index: usize) -> Result<Option<Item>, ErasedStateError>;

    /// Returns the committed deque length.
    async fn len(&self, key: String) -> Result<usize, ErasedStateError>;

    /// Reports whether the committed deque is empty.
    async fn is_empty(&self, key: String) -> Result<bool, ErasedStateError>;

    /// Reads the committed front endpoint.
    async fn peek_front(&self, key: String) -> Result<Option<Item>, ErasedStateError>;

    /// Reads the committed back endpoint.
    async fn peek_back(&self, key: String) -> Result<Option<Item>, ErasedStateError>;

    /// A fluent query over committed elements in index order.
    fn values(&self, key: String) -> ErasedDequeRead<Item>;
}

/// Shared deque-reader representation stored by native FFI wrappers.
pub type SharedDequeReader<Item> = Arc<dyn ErasedDequeReader<Item>>;

#[async_trait]
impl<C, W, B> ErasedValueReader<C::Payload> for Erased<StateReader<ValueDescriptor<C>, W, B>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + Send + Sync + 'static,
    W: Codec,
    W::Payload: Clone,
    B: ReaderBackend<W>,
{
    async fn get(&self, key: String) -> Result<Option<C::Payload>, ErasedStateError> {
        self.0.get(Key::from(key)).await.map_err(Into::into)
    }
}

#[async_trait]
impl<C, W, B> ErasedMapReader<C::Payload>
    for Erased<StateReader<MapDescriptor<Utf8KeyCodec, C>, W, B>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + Send + Sync + 'static,
    W: Codec,
    W::Payload: Clone,
    B: ReaderBackend<W>,
{
    async fn get(
        &self,
        key: String,
        map_key: String,
    ) -> Result<Option<C::Payload>, ErasedStateError> {
        self.0
            .get(Key::from(key), &map_key)
            .await
            .map_err(Into::into)
    }

    async fn contains_key(&self, key: String, map_key: String) -> Result<bool, ErasedStateError> {
        self.0
            .contains_key(Key::from(key), &map_key)
            .await
            .map_err(Into::into)
    }

    async fn get_many(
        &self,
        key: String,
        map_keys: Vec<String>,
    ) -> Result<Vec<Option<C::Payload>>, ErasedStateError> {
        self.0
            .get_many(Key::from(key), &map_keys)
            .await
            .map_err(Into::into)
    }

    async fn contains_many(
        &self,
        key: String,
        map_keys: Vec<String>,
    ) -> Result<Vec<bool>, ErasedStateError> {
        self.0
            .contains_many(Key::from(key), &map_keys)
            .await
            .map_err(Into::into)
    }

    async fn is_empty(&self, key: String) -> Result<bool, ErasedStateError> {
        self.0.is_empty(Key::from(key)).await.map_err(Into::into)
    }

    fn entries(&self, key: String) -> ErasedKeyRead<(String, C::Payload)> {
        let reader = self.0.clone();
        let key = Key::from(key);
        read(move |query: ErasedKeyQuery| {
            let reader = reader.clone();
            let key = key.clone();
            StateCursor::new(async_stream::try_stream! {
                let stream = reader.entries(key).with_query(query.borrowed()).stream();
                for await item in stream { yield item?; }
            })
        })
    }

    fn keys(&self, key: String) -> ErasedKeyRead<String> {
        let reader = self.0.clone();
        let key = Key::from(key);
        read(move |query: ErasedKeyQuery| {
            let reader = reader.clone();
            let key = key.clone();
            StateCursor::new(async_stream::try_stream! {
                let stream = reader.keys(key).with_query(query.borrowed()).stream();
                for await item in stream { yield item?; }
            })
        })
    }
}

#[async_trait]
impl<W, B> ErasedSetReader for Erased<StateReader<SetDescriptor<Utf8KeyCodec>, W, B>>
where
    W: Codec,
    W::Payload: Clone,
    B: ReaderBackend<W>,
{
    async fn contains(&self, key: String, member: String) -> Result<bool, ErasedStateError> {
        self.0
            .contains(Key::from(key), &member)
            .await
            .map_err(Into::into)
    }

    async fn contains_many(
        &self,
        key: String,
        members: Vec<String>,
    ) -> Result<Vec<bool>, ErasedStateError> {
        self.0
            .contains_many(Key::from(key), &members)
            .await
            .map_err(Into::into)
    }

    async fn is_empty(&self, key: String) -> Result<bool, ErasedStateError> {
        self.0.is_empty(Key::from(key)).await.map_err(Into::into)
    }

    fn keys(&self, key: String) -> ErasedKeyRead<String> {
        let reader = self.0.clone();
        let key = Key::from(key);
        read(move |query: ErasedKeyQuery| {
            let reader = reader.clone();
            let key = key.clone();
            StateCursor::new(async_stream::try_stream! {
                let stream = reader.keys(key).with_query(query.borrowed()).stream();
                for await item in stream { yield item?; }
            })
        })
    }
}

#[async_trait]
impl<C, W, B> ErasedDequeReader<C::Payload> for Erased<StateReader<DequeDescriptor<C>, W, B>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + Send + Sync + 'static,
    W: Codec,
    W::Payload: Clone,
    B: ReaderBackend<W>,
{
    async fn get(&self, key: String, index: usize) -> Result<Option<C::Payload>, ErasedStateError> {
        self.0.get(Key::from(key), index).await.map_err(Into::into)
    }

    async fn len(&self, key: String) -> Result<usize, ErasedStateError> {
        self.0.len(Key::from(key)).await.map_err(Into::into)
    }

    async fn is_empty(&self, key: String) -> Result<bool, ErasedStateError> {
        self.0.is_empty(Key::from(key)).await.map_err(Into::into)
    }

    async fn peek_front(&self, key: String) -> Result<Option<C::Payload>, ErasedStateError> {
        self.0.peek_front(Key::from(key)).await.map_err(Into::into)
    }

    async fn peek_back(&self, key: String) -> Result<Option<C::Payload>, ErasedStateError> {
        self.0.peek_back(Key::from(key)).await.map_err(Into::into)
    }

    fn values(&self, key: String) -> ErasedDequeRead<C::Payload> {
        let reader = self.0.clone();
        let key = Key::from(key);
        read(move |query| {
            let reader = reader.clone();
            let key = key.clone();
            StateCursor::new(async_stream::try_stream! {
                let stream = reader.values(key).with_query(query).stream();
                for await item in stream { yield item?; }
            })
        })
    }
}

impl From<StateReaderError> for ErasedStateError {
    fn from(error: StateReaderError) -> Self {
        Self::from_classified(&error)
    }
}

#[cfg(test)]
mod tests;
