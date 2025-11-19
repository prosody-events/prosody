//! Message loaders for the defer middleware.
//!
//! This module provides abstractions for loading messages by their exact offset
//! coordinates. The defer middleware uses loaders to reload failed messages
//! when retry timers fire.
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
//! Error types must implement [`ClassifyError`] so the defer middleware can
//! distinguish permanent failures (message deleted, decode error) from
//! transient failures (network issues, timeouts) and apply appropriate retry
//! logic.

use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::ClassifyError;
use crate::{Offset, Partition, Topic};
use std::error::Error as StdError;
use std::future::Future;

mod kafka;
mod memory;

pub use kafka::{KafkaLoader, KafkaLoaderError, LoaderConfiguration};
pub use memory::MemoryLoader;

/// Loads messages by their exact offset coordinates for retry.
///
/// This trait abstracts message loading to enable different implementations:
/// - [`KafkaLoader`] for production (loads from Kafka)
/// - [`MemoryLoader`] for testing (loads from in-memory map)
///
/// The loader is used by defer middleware to reload failed messages when their
/// retry timer fires.
pub trait MessageLoader: Send + Sync + Clone {
    /// Error type for load operations.
    ///
    /// Must implement [`ClassifyError`] so the defer middleware can determine
    /// if load failures are permanent (message deleted) or transient (network).
    type Error: StdError + ClassifyError + Send + Sync + 'static;

    /// Loads a specific message from storage by its exact coordinates.
    ///
    /// Returns a [`ConsumerMessage`] ready for processing, with appropriate
    /// permit and span context.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic containing the message
    /// * `partition` - The partition containing the message
    /// * `offset` - The exact offset of the message
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
    ) -> impl Future<Output = Result<ConsumerMessage, Self::Error>> + Send;
}
