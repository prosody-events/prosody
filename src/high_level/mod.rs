//! High-level client: one handle owning both a producer and a consumer.
//!
//! [`HighLevelClient`] is built from [`ConsumerBuilders`] and a [`Mode`], then
//! driven through [`subscribe`](HighLevelClient::subscribe) /
//! [`unsubscribe`](HighLevelClient::unsubscribe). The shared infrastructure it
//! hands to consumers and readers alike lives in `deps`; topic reconciliation
//! in `topics`; the consumer's state machine in [`state`].

use crate::consumer::{ProsodyConsumer, Responding, ResponsePolicy};
pub use crate::high_level::config::ConsumerBuilders;
use crate::high_level::config::ModeConfiguration;
pub use crate::high_level::error::HighLevelClientError;
pub use crate::high_level::mode::Mode;
use crate::high_level::state::{ConsumerState, ConsumerStateView};
use crate::peer::Router;
use crate::peer::requester::{ProsodyRequester, RequestError, SubsystemOutcomes};
use crate::producer::{ProducerConfiguration, ProsodyProducer};
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state_reader::{StateReader, StateReaderClient};
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::{Codec, Topic};
use educe::Educe;
use opentelemetry::propagation::TextMapCompositePropagator;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

mod backend;
mod client_impl;
mod codecs;
pub mod config;
mod construction;
mod deps;
pub mod erased;
mod error;
pub mod mode;
pub mod state;
mod topics;

pub use backend::{CassandraClientBackend, ClientBackend, MemoryClientBackend};
pub use codecs::{ClientHandler, CodecSet, Codecs, JsonBinaryCodecs, JsonCodecs};
#[doc(hidden)]
pub use deps::ReaderConfiguration;

/// High-level client using Cassandra storage.
pub type CassandraHighLevelClient<T> = HighLevelClient<T, CassandraClientBackend<MessageCodec<T>>>;

/// High-level client using in-memory storage.
pub type MemoryHighLevelClient<T> = HighLevelClient<T, MemoryClientBackend<MessageCodec<T>>>;

use codecs::{MessageCodec, ResponseCodec};
type MessageCodecError<T> = <MessageCodec<T> as Codec>::Error;
type ClientStateReader<T, B, D> =
    StateReader<D, MessageCodec<T>, <B as ClientBackend<MessageCodec<T>>>::Reader>;

#[cfg(test)]
mod tests;

/// A combined client that manages both producer and consumer operations.
#[derive(Educe)]
#[educe(Debug)]
pub struct HighLevelClient<T, B>
where
    T: ClientHandler,
    T::Payload: crate::EventIdentity,
    B: ClientBackend<MessageCodec<T>>,
{
    producer: ProsodyProducer<MessageCodec<T>>,
    producer_config: ProducerConfiguration,
    consumer: Mutex<ConsumerState<T, MessageCodec<T>>>,
    #[educe(Debug(ignore))]
    reader: OnceCell<StateReaderClient<MessageCodec<T>, B::Reader>>,
    #[educe(Debug(ignore))]
    reader_config: Option<ReaderConfiguration>,
    backend: B,
    #[educe(Debug(ignore))]
    requester: ProsodyRequester<MessageCodec<T>, ResponseCodec<T>>,
    #[educe(Debug(ignore))]
    subsystem: Option<SubsystemName>,
    #[educe(Debug(ignore))]
    router: B::Router,
    propagator: Arc<TextMapCompositePropagator>,
    telemetry: Telemetry,
}

// Concrete impls keep consumer construction internals out of ClientBackend's
// public bounds.
macro_rules! impl_subscribe {
    ($backend:ident) => {
        impl<T> HighLevelClient<T, $backend<MessageCodec<T>>>
        where
            T: ClientHandler + Clone,
            T::Payload: crate::EventIdentity + crate::EventType + Clone,
            T::Output: Sync + 'static,
            T::Error: Sync + 'static,
        {
            /// Subscribes the consumer with the provided handler.
            ///
            /// A configured subsystem answers peer requests. Without one, the
            /// consumer processes events without answers.
            ///
            /// # Errors
            ///
            /// Returns an error when the consumer is unconfigured, already
            /// subscribed, or cannot be initialized.
            pub fn subscribe(
                &self,
                handler: T,
            ) -> impl Future<Output = Result<(), HighLevelClientError<MessageCodecError<T>>>> + Send + '_
            {
                self.subscribe_inner(handler)
            }
        }
    };
}

impl_subscribe!(MemoryClientBackend);
impl_subscribe!(CassandraClientBackend);
