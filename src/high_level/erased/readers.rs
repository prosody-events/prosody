//! Read-only keyed-state types materialized across FFI boundaries.

use crate::EventIdentity;
use crate::Key;
use crate::codec::Codec;
use crate::consumer::event_context::{BoxStateCursor, ErasedStateError, StateCursor};
use crate::error::{ClassifyError, ErrorCategory};
use crate::high_level::{ClientBackend, HighLevelClient, HighLevelClientError};
use crate::state::ReadCachePolicy;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{
    DequeDescriptor, MapDescriptor, StateDescriptor, ValueDescriptor, deque_state, map_state,
    value_state,
};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state::registry::MAX_KEYSET_LIMIT;
use crate::state_reader::{ConsumerReaderBackend, ReaderBackend, StateReader, StateReaderError};
use crate::subsystem::{SubsystemName, SubsystemNameError};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

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

/// Read-only access to a published value collection.
#[async_trait]
pub trait ErasedValueReader<C: Codec>: Send + Sync {
    /// Reads the committed value for `key`.
    async fn get(&self, key: String) -> Result<Option<C::Payload>, ErasedStateError>;
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
    ) -> Result<Option<C::Payload>, ErasedStateError>;

    /// Reports whether one committed map entry exists without decoding it.
    async fn contains_key(&self, key: String, map_key: String) -> Result<bool, ErasedStateError>;

    /// Reads committed map entries aligned with `map_keys`.
    async fn get_many(
        &self,
        key: String,
        map_keys: Vec<String>,
    ) -> Result<Vec<Option<C::Payload>>, ErasedStateError>;

    /// Streams committed entries in key order.
    async fn stream(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<BoxStateCursor<(String, C::Payload)>, ErasedStateError>;

    /// Streams committed keys without decoding values.
    async fn keys(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<BoxStateCursor<String>, ErasedStateError>;
}

/// Shared map-reader representation stored by native FFI wrappers.
pub type SharedMapReader<C> = Arc<dyn ErasedMapReader<C>>;

/// Read-only access to a published deque collection.
#[async_trait]
pub trait ErasedDequeReader<C: Codec>: Send + Sync {
    /// Reads one front-relative committed element.
    async fn get(&self, key: String, index: usize) -> Result<Option<C::Payload>, ErasedStateError>;

    /// Returns the committed deque length.
    async fn len(&self, key: String) -> Result<usize, ErasedStateError>;

    /// Reports whether the committed deque is empty.
    async fn is_empty(&self, key: String) -> Result<bool, ErasedStateError>;

    /// Reads the committed front endpoint.
    async fn peek_front(&self, key: String) -> Result<Option<C::Payload>, ErasedStateError>;

    /// Reads the committed back endpoint.
    async fn peek_back(&self, key: String) -> Result<Option<C::Payload>, ErasedStateError>;

    /// Streams committed elements in index order.
    async fn stream(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<BoxStateCursor<C::Payload>, ErasedStateError>;
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

pub(super) fn value<T, C, B>(
    client: &HighLevelClient<T, C, B>,
    subsystem: String,
    name: &str,
    cache: ErasedReadCache,
) -> Result<SharedValueReader<C>, ErasedReaderBuildError<C::Error>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<C>,
    B::Reader: ConsumerReaderBackend<C>,
{
    let descriptor = value_state::<C>(name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor)?;
    Ok(Arc::new(ValueReader(reader)))
}

pub(super) fn map<T, C, B>(
    client: &HighLevelClient<T, C, B>,
    subsystem: String,
    name: &str,
    cache: ErasedReadCache,
) -> Result<SharedMapReader<C>, ErasedReaderBuildError<C::Error>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<C>,
    B::Reader: ConsumerReaderBackend<C>,
{
    let descriptor = map_state::<Utf8KeyCodec, C>(name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor)?;
    Ok(Arc::new(MapReader(reader)))
}

pub(super) fn deque<T, C, B>(
    client: &HighLevelClient<T, C, B>,
    subsystem: String,
    name: &str,
    cache: ErasedReadCache,
) -> Result<SharedDequeReader<C>, ErasedReaderBuildError<C::Error>>
where
    C: Codec + Send + Sync,
    C::Payload: Clone + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<C>,
    B::Reader: ConsumerReaderBackend<C>,
{
    let descriptor = deque_state::<C>(name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor)?;
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
    async fn get(&self, key: String) -> Result<Option<C::Payload>, ErasedStateError> {
        self.0.get(Key::from(key)).await.map_err(Into::into)
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
        validate_get_many_len(map_keys.len())?;
        self.0
            .get_many(Key::from(key), &map_keys)
            .await
            .map_err(Into::into)
    }

    async fn stream(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<BoxStateCursor<(String, C::Payload)>, ErasedStateError> {
        let stream = self
            .0
            .stream(Key::from(key), direction.into())
            .await
            .map_err(ErasedStateError::from)?;
        Ok(Box::new(state_cursor(stream)))
    }

    async fn keys(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<BoxStateCursor<String>, ErasedStateError> {
        let stream = self
            .0
            .keys(Key::from(key), direction.into())
            .await
            .map_err(ErasedStateError::from)?;
        Ok(Box::new(state_cursor(stream)))
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

    async fn stream(
        &self,
        key: String,
        direction: ErasedDirection,
    ) -> Result<BoxStateCursor<C::Payload>, ErasedStateError> {
        let stream = self
            .0
            .stream(Key::from(key), direction.into())
            .await
            .map_err(ErasedStateError::from)?;
        Ok(Box::new(state_cursor(stream)))
    }
}

fn state_cursor<T>(
    stream: impl futures::Stream<Item = Result<T, StateReaderError>> + Send + 'static,
) -> StateCursor<T> {
    let stream = stream
        .map_err(|error| ErasedStateError::from_classified(&error))
        .boxed();
    StateCursor::new(stream)
}

fn validate_get_many_len(found: usize) -> Result<(), ErasedStateError> {
    if found > MAX_KEYSET_LIMIT {
        return Err(ErasedStateError::from_classified(&ErasedReadLimitError {
            found,
            max: MAX_KEYSET_LIMIT,
        }));
    }
    Ok(())
}

impl From<StateReaderError> for ErasedStateError {
    fn from(error: StateReaderError) -> Self {
        Self::from_classified(&error)
    }
}

#[derive(Debug, Error)]
#[error("get_many accepts at most {max} keys; got {found}")]
struct ErasedReadLimitError {
    found: usize,
    max: usize,
}

impl ClassifyError for ErasedReadLimitError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::Result;
    use color_eyre::eyre::bail;

    /// The erased boundary accepts the typed API's maximum batch and rejects
    /// only larger batches. This prevents an FFI caller from allocating an
    /// uncapped transfer buffer before the shared typed batching begins.
    #[test]
    fn get_many_limit_matches_typed_keyset_limit() -> Result<()> {
        assert!(validate_get_many_len(MAX_KEYSET_LIMIT - 1).is_ok());
        assert!(validate_get_many_len(MAX_KEYSET_LIMIT).is_ok());
        let Err(error) = validate_get_many_len(MAX_KEYSET_LIMIT + 1) else {
            bail!("one key above the limit must be rejected");
        };
        assert_eq!(error.classify_error(), ErrorCategory::Permanent);
        assert_eq!(
            error.to_string(),
            format!(
                "get_many accepts at most {MAX_KEYSET_LIMIT} keys; got {}",
                MAX_KEYSET_LIMIT + 1
            )
        );
        Ok(())
    }
}
