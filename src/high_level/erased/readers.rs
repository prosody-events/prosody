//! Constructs erased readers from the high-level client.

use crate::EventIdentity;
use crate::codec::ErasedStateCodec;
use crate::high_level::codecs::StateCodec;
use crate::high_level::{
    ClientBackend, ClientHandler, HighLevelClient, HighLevelClientError, MessageCodec,
    MessageCodecError,
};
use crate::state::descriptor::{StateDescriptor, deque_state, map_state, set_state, value_state};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state_reader::ConsumerReaderBackend;
use crate::subsystem::{SubsystemName, SubsystemNameError};
use std::sync::Arc;
use thiserror::Error;

use crate::state_reader::erased::{DequeReader, MapReader, SetReader, ValueReader};
pub use crate::state_reader::erased::{
    ErasedDequeReader, ErasedMapReader, ErasedReadCache, ErasedSetReader, ErasedValueReader,
    SharedDequeReader, SharedMapReader, SharedSetReader, SharedValueReader,
};

pub(in crate::high_level) async fn value<T, B>(
    client: &HighLevelClient<T, B>,
    subsystem: String,
    name: &str,
    cache: ErasedReadCache,
) -> Result<SharedValueReader<T::Payload>, ErasedReaderBuildError<MessageCodecError<T>>>
where
    T: ClientHandler,
    T::Payload: Clone + ErasedStateCodec + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<MessageCodec<T>>,
    B::Reader: ConsumerReaderBackend<MessageCodec<T>>,
{
    let descriptor = value_state::<StateCodec<T>>(name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor).await?;
    Ok(Arc::new(ValueReader(reader)))
}

pub(in crate::high_level) async fn map<T, B>(
    client: &HighLevelClient<T, B>,
    subsystem: String,
    name: &str,
    cache: ErasedReadCache,
) -> Result<SharedMapReader<T::Payload>, ErasedReaderBuildError<MessageCodecError<T>>>
where
    T: ClientHandler,
    T::Payload: Clone + ErasedStateCodec + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<MessageCodec<T>>,
    B::Reader: ConsumerReaderBackend<MessageCodec<T>>,
{
    let descriptor = map_state::<Utf8KeyCodec, StateCodec<T>>(name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor).await?;
    Ok(Arc::new(MapReader(reader)))
}

pub(in crate::high_level) async fn set<T, B>(
    client: &HighLevelClient<T, B>,
    subsystem: String,
    name: &str,
    cache: ErasedReadCache,
) -> Result<SharedSetReader, ErasedReaderBuildError<MessageCodecError<T>>>
where
    T: ClientHandler,
    T::Payload: Clone + ErasedStateCodec + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<MessageCodec<T>>,
    B::Reader: ConsumerReaderBackend<MessageCodec<T>>,
{
    let descriptor = set_state::<Utf8KeyCodec>(name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor).await?;
    Ok(Arc::new(SetReader(reader)))
}

pub(in crate::high_level) async fn deque<T, B>(
    client: &HighLevelClient<T, B>,
    subsystem: String,
    name: &str,
    cache: ErasedReadCache,
) -> Result<SharedDequeReader<T::Payload>, ErasedReaderBuildError<MessageCodecError<T>>>
where
    T: ClientHandler,
    T::Payload: Clone + ErasedStateCodec + EventIdentity + Send + Sync + 'static,
    B: ClientBackend<MessageCodec<T>>,
    B::Reader: ConsumerReaderBackend<MessageCodec<T>>,
{
    let descriptor = deque_state::<StateCodec<T>>(name).read_cache(cache);
    let reader = client.state(subsystem_name(subsystem)?, descriptor).await?;
    Ok(Arc::new(DequeReader(reader)))
}

fn subsystem_name<E>(name: String) -> Result<SubsystemName, ErasedReaderBuildError<E>> {
    Ok(SubsystemName::try_new(name)?)
}

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
