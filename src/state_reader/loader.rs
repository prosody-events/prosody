//! Backend carriage for the reader's message loader.

use crate::codec::Codec;
use crate::consumer::message::ConsumerMessage;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::{
    KafkaLoader, KafkaLoaderError, MemoryLoader, MemoryLoaderError, MessageLoader,
};
use crate::{Offset, Partition, Topic};
use thiserror::Error;

/// The reader's message loader, carried as a closed enum so a Kafka-ref
/// descriptor resolves through the same [`MessageLoader`] surface with no
/// `dyn`. `Memory` backs the mock suites; `Kafka` the production reader.
///
/// Both arms' [`MessageLoader`] impls require `C::Payload: Clone` (the cache
/// hands out clones of a loaded body), so this enum carries the same bound on
/// its own `Clone` and `MessageLoader` impls rather than deriving them.
pub enum ReaderLoader<C: Codec> {
    /// Loads message bodies from Kafka (production).
    Kafka(KafkaLoader<C>),
    /// Loads message bodies from an in-memory map (mock/tests).
    Memory(MemoryLoader<C::Payload>),
}

impl<C: Codec> Clone for ReaderLoader<C>
where
    C::Payload: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Kafka(loader) => Self::Kafka(loader.clone()),
            Self::Memory(loader) => Self::Memory(loader.clone()),
        }
    }
}

impl<C: Codec> MessageLoader for ReaderLoader<C>
where
    C::Payload: Clone,
{
    type Error = ReaderLoaderError;
    type Payload = C::Payload;

    async fn load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<ConsumerMessage<Self::Payload>, Self::Error> {
        match self {
            Self::Kafka(loader) => loader
                .load_message(topic, partition, offset)
                .await
                .map_err(ReaderLoaderError::Kafka),
            Self::Memory(loader) => loader
                .load_message(topic, partition, offset)
                .await
                .map_err(ReaderLoaderError::Memory),
        }
    }
}

/// Error from a [`ReaderLoader`], delegating classification to the active arm.
#[derive(Debug, Error)]
pub enum ReaderLoaderError {
    /// The Kafka loader failed.
    #[error(transparent)]
    Kafka(KafkaLoaderError),
    /// The in-memory loader failed.
    #[error(transparent)]
    Memory(MemoryLoaderError),
}

impl ClassifyError for ReaderLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Kafka(error) => error.classify_error(),
            Self::Memory(error) => error.classify_error(),
        }
    }
}
