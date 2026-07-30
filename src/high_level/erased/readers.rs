//! Read-only keyed-state types materialized across FFI boundaries.

use crate::EventIdentity;
use crate::Key;
use crate::codec::Codec;
use crate::high_level::{ClientBackend, HighLevelClient, HighLevelClientError};
use crate::state::ReadCachePolicy;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{
    DequeDescriptor, MapDescriptor, StateDescriptor, ValueDescriptor, deque_state, map_state,
    value_state,
};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state_reader::{ConsumerReaderBackend, ReaderBackend, StateReader, StateReaderError};
use crate::subsystem::{SubsystemName, SubsystemNameError};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;

/// Cache policy accepted by foreign-language published-state readers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ErasedReadCache {
    /// Use the client's configured default.
    #[default]
    Inherit,
    /// Read durable storage for every operation.
    Disabled,
    /// Cache committed reads for this duration.
    Ttl(Duration),
}

impl From<ErasedReadCache> for ReadCachePolicy {
    fn from(cache: ErasedReadCache) -> Self {
        match cache {
            ErasedReadCache::Inherit => Self::Inherit,
            ErasedReadCache::Disabled => Self::Disabled,
            ErasedReadCache::Ttl(ttl) => Self::Ttl(ttl),
        }
    }
}

/// Ordering for a foreign-language state scan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ErasedDirection {
    /// Ascending map keys or front-to-back deque elements.
    #[default]
    Forward,
    /// Descending map keys or back-to-front deque elements.
    Backward,
}

impl From<ErasedDirection> for Direction {
    fn from(direction: ErasedDirection) -> Self {
        match direction {
            ErasedDirection::Forward => Self::Forward,
            ErasedDirection::Backward => Self::Backward,
        }
    }
}

/// One asynchronous read-only state stream.
#[async_trait]
pub trait ErasedStateStream<T>: Send + Sync {
    /// Returns the next item, or `None` after the stream ends.
    async fn next(&self) -> Option<Result<T, StateReaderError>>;
}

/// Shared stream representation stored by native FFI wrappers.
pub type SharedStateStream<T> = Arc<dyn ErasedStateStream<T>>;

/// Read-only access to a published value collection.
#[async_trait]
pub trait ErasedValueReader<C: Codec>: Send + Sync {
    /// Reads the committed value for `key`.
    async fn get(&self, key: String) -> Result<Option<C::Payload>, StateReaderError>;
}

/// Shared value-reader representation stored by native FFI wrappers.
pub type SharedValueReader<C> = Arc<dyn ErasedValueReader<C>>;

/// Read-only access to a published string-keyed map collection.
#[async_trait]
pub trait ErasedMapReader<C: Codec>: Send + Sync {
    /// Reads one committed map entry.
    async fn get(
        &self,
        key: String,
        map_key: String,
    ) -> Result<Option<C::Payload>, StateReaderError>;

    /// Reads committed map entries aligned with `map_keys`.
    async fn get_many(
        &self,
        key: String,
        map_keys: Vec<String>,
    ) -> Result<Vec<Option<C::Payload>>, StateReaderError>;

    /// Streams committed entries in key order.
    async fn stream(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<SharedStateStream<(String, C::Payload)>, StateReaderError>;
}

/// Shared map-reader representation stored by native FFI wrappers.
pub type SharedMapReader<C> = Arc<dyn ErasedMapReader<C>>;

/// Read-only access to a published deque collection.
#[async_trait]
pub trait ErasedDequeReader<C: Codec>: Send + Sync {
    /// Reads one front-relative committed element.
    async fn get(&self, key: String, index: usize) -> Result<Option<C::Payload>, StateReaderError>;

    /// Returns the committed deque length.
    async fn len(&self, key: String) -> Result<usize, StateReaderError>;

    /// Streams committed elements in index order.
    async fn stream(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<SharedStateStream<C::Payload>, StateReaderError>;
}

/// Shared deque-reader representation stored by native FFI wrappers.
pub type SharedDequeReader<C> = Arc<dyn ErasedDequeReader<C>>;

/// Failure to construct a foreign-language published-state reader.
#[derive(Debug, Error)]
pub enum ErasedReaderBuildError<E> {
    /// The subsystem name is empty.
    #[error(transparent)]
    InvalidSubsystem(#[from] SubsystemNameError),
    /// The high-level client could not compose the reader.
    #[error(transparent)]
    Client(#[from] HighLevelClientError<E>),
}

pub(super) async fn value<T, C, B>(
    client: &HighLevelClient<T, C, B>,
    subsystem: String,
    name: String,
    cache: ErasedReadCache,
) -> Result<SharedValueReader<C>, ErasedReaderBuildError<C::Error>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<C>,
    B::Reader: ConsumerReaderBackend<C>,
{
    let descriptor = value_state::<C>(&name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor).await?;
    Ok(Arc::new(ValueReader(reader)))
}

pub(super) async fn map<T, C, B>(
    client: &HighLevelClient<T, C, B>,
    subsystem: String,
    name: String,
    cache: ErasedReadCache,
) -> Result<SharedMapReader<C>, ErasedReaderBuildError<C::Error>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<C>,
    B::Reader: ConsumerReaderBackend<C>,
{
    let descriptor = map_state::<Utf8KeyCodec, C>(&name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor).await?;
    Ok(Arc::new(MapReader(reader)))
}

pub(super) async fn deque<T, C, B>(
    client: &HighLevelClient<T, C, B>,
    subsystem: String,
    name: String,
    cache: ErasedReadCache,
) -> Result<SharedDequeReader<C>, ErasedReaderBuildError<C::Error>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<C>,
    B::Reader: ConsumerReaderBackend<C>,
{
    let descriptor = deque_state::<C>(&name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor).await?;
    Ok(Arc::new(DequeReader(reader)))
}

fn subsystem_name<E>(name: String) -> Result<SubsystemName, ErasedReaderBuildError<E>> {
    Ok(SubsystemName::try_new(name)?)
}

struct ValueReader<C: Codec, B: ReaderBackend<C>>(StateReader<ValueDescriptor<C>, C, B>);

#[async_trait]
impl<C, B> ErasedValueReader<C> for ValueReader<C, B>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + Send + Sync + 'static,
    B: ReaderBackend<C>,
{
    async fn get(&self, key: String) -> Result<Option<C::Payload>, StateReaderError> {
        self.0.get(Key::from(key)).await
    }
}

struct MapReader<C: Codec, B: ReaderBackend<C>>(StateReader<MapDescriptor<Utf8KeyCodec, C>, C, B>);

#[async_trait]
impl<C, B> ErasedMapReader<C> for MapReader<C, B>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + Send + Sync + 'static,
    B: ReaderBackend<C>,
{
    async fn get(
        &self,
        key: String,
        map_key: String,
    ) -> Result<Option<C::Payload>, StateReaderError> {
        self.0.get(Key::from(key), &map_key).await
    }

    async fn get_many(
        &self,
        key: String,
        map_keys: Vec<String>,
    ) -> Result<Vec<Option<C::Payload>>, StateReaderError> {
        self.0.get_many(Key::from(key), &map_keys).await
    }

    async fn stream(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<SharedStateStream<(String, C::Payload)>, StateReaderError> {
        let stream = self.0.stream(Key::from(key), direction.into()).await?;
        Ok(StateStream::new(stream))
    }
}

struct DequeReader<C: Codec, B: ReaderBackend<C>>(StateReader<DequeDescriptor<C>, C, B>);

#[async_trait]
impl<C, B> ErasedDequeReader<C> for DequeReader<C, B>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + Send + Sync + 'static,
    B: ReaderBackend<C>,
{
    async fn get(&self, key: String, index: usize) -> Result<Option<C::Payload>, StateReaderError> {
        self.0.get(Key::from(key), index).await
    }

    async fn len(&self, key: String) -> Result<usize, StateReaderError> {
        self.0.len(Key::from(key)).await
    }

    async fn stream(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<SharedStateStream<C::Payload>, StateReaderError> {
        let stream = self.0.stream(Key::from(key), direction.into()).await?;
        Ok(StateStream::new(stream))
    }
}

type BoxStateStream<T> = Pin<Box<dyn Stream<Item = Result<T, StateReaderError>> + Send + 'static>>;

struct StateStream<T>(Mutex<BoxStateStream<T>>);

impl<T> StateStream<T> {
    fn new(stream: impl Stream<Item = Result<T, StateReaderError>> + Send + 'static) -> Arc<Self> {
        Arc::new(Self(Mutex::new(Box::pin(stream))))
    }
}

#[async_trait]
impl<T: Send + 'static> ErasedStateStream<T> for StateStream<T> {
    async fn next(&self) -> Option<Result<T, StateReaderError>> {
        self.0.lock().await.next().await
    }
}
