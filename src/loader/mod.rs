//! Message loaders for exact Kafka coordinates.
//!
//! This module provides abstractions for loading messages by their exact offset
//! coordinates. Deferral and keyed state share the same loader.
//!
//! # Implementations
//!
//! - **[`KafkaLoader`]**: Production loader that loads messages from Kafka
//!   using a dedicated consumer with manual partition assignment
//! - **[`MemoryLoader`]**: Test loader that loads messages from an in-memory
//!   map, enabling property-based tests without Kafka infrastructure
//!
//! # Architecture
//!
//! The [`MessageLoader`] trait provides a common interface for different
//! loading implementations. Each loader returns a [`ConsumerMessage`] with
//! appropriate backpressure (via semaphore permits) and tracing context.
//!
//! Error types implement [`ClassifyError`] so each caller can apply its retry
//! policy.

use crate::consumer::message::ConsumerMessage;
use crate::error::ClassifyError;
use crate::{Offset, Partition, Topic};
use std::error::Error as StdError;
use std::future::Future;

mod kafka;
mod memory;

pub use kafka::{
    KafkaLoader, KafkaLoaderConfigError, KafkaLoaderConfiguration, KafkaLoaderError,
    LoaderConfiguration,
};
pub use memory::{MemoryLoader, MemoryLoaderError};

#[derive(Clone, Copy)]
enum PermitMode {
    Wait,
    Available,
}

/// Loads messages by their exact offset coordinates.
///
/// This trait abstracts message loading to enable different implementations:
/// - [`KafkaLoader`] for production (loads from Kafka)
/// - [`MemoryLoader`] for testing (loads from in-memory map)
///
/// Deferral and keyed state share this interface.
pub trait MessageLoader: Send + Sync + Clone {
    /// The payload type of messages returned by this loader.
    type Payload: Send + Sync + 'static;

    /// Error type for load operations.
    ///
    /// Classifies permanent data loss separately from transient failures.
    type Error: StdError + ClassifyError + Send + Sync + 'static;

    /// Loads a specific message from storage by its exact coordinates.
    ///
    /// Returns a [`ConsumerMessage`] ready for processing, with appropriate
    /// permit and span context.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be loaded (deleted, network
    /// failure, etc.)
    fn load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerMessage<Self::Payload>, Self::Error>> + Send;

    /// Loads a message without waiting for loader capacity.
    ///
    /// Returns a transient error when every permit is held.
    fn try_load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerMessage<Self::Payload>, Self::Error>> + Send;
}
