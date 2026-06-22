//! Kafka-message keyed state: a value collection whose cells hold a durable
//! pointer to a Kafka message body, resolved back to the full message through
//! the consumer's [`MessageLoader`].
//!
//! This is the one place the otherwise Kafka-agnostic keyed-state machinery
//! meets Kafka. It composes the generic [`ValueDescriptor<C, R>`] over two
//! strategies that live here, not in `src/state`:
//!
//! * [`MessageRefCodec`] — the cell typing: `bytes ↔ MessageRef`, `MsgPack`.
//! * [`MessageResolver`] — the resolution strategy: `MessageRef → message`,
//!   loading through the session's loader.
//!
//! Handlers declare a collection via [`message_state`] and bind it
//! like any other descriptor; the handle's `get` returns the full
//! [`ConsumerMessage`], and `set` takes the message in hand.

use crate::codec::Codec;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::message::ConsumerMessage;
use crate::loader::MessageLoader;
use crate::state::descriptor::{CellResolver, ResolverId, ValueDescriptor, ValueStateError};
use crate::state::session::StateSession;
use crate::{Offset, Partition, Topic};
use rmp_serde::decode::Error as MsgPackDecodeError;
use rmp_serde::encode::{Error as MsgPackEncodeError, write_named};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Durable pointer to a Kafka message body.
///
/// Persisted as the `MsgPack`-encoded cell of a Kafka-message collection and
/// resolved back to the full message via the consumer's [`MessageLoader`].
/// Derived from the [`ConsumerMessage`] in hand at write time — the only
/// production source of a ref.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRef {
    /// Kafka topic.
    #[serde(with = "topic_serde")]
    pub topic: Topic,

    /// Kafka partition.
    pub partition: Partition,

    /// Kafka offset within the partition.
    pub offset: Offset,
}

impl<P> From<&ConsumerMessage<P>> for MessageRef {
    fn from(message: &ConsumerMessage<P>) -> Self {
        Self {
            topic: message.topic(),
            partition: message.partition(),
            offset: message.offset(),
        }
    }
}

/// `MsgPack` [`Codec`] for [`MessageRef`] cells.
///
/// Codec id `"message-ref"` is frozen into the durable structural
/// identity; never change it once cells exist.
#[derive(Default)]
pub struct MessageRefCodec;

impl Codec for MessageRefCodec {
    type Error = MessageRefCodecError;
    type Payload = MessageRef;

    const CODEC_ID: &'static str = "message-ref";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        rmp_serde::from_slice(buf).map_err(MessageRefCodecError::Decode)
    }

    fn serialize(&mut self, payload: Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        write_named(buf, &payload).map_err(MessageRefCodecError::Encode)
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        // The codec is a zero-sized type with no reusable buffers, so there
        // is nothing to cache: hand `f` a fresh instance.
        f(&mut Self)
    }
}

/// Resolution strategy that loads the full message a [`MessageRef`] points
/// at, and lowers a message in hand back to its ref.
///
/// A zero-sized strategy: it reads the loader from the session it is handed at
/// [`CellResolver::resolve`] time, so the descriptor carries no resolver state.
pub struct MessageResolver;

impl ResolverId for MessageResolver {
    /// Frozen into the durable structural identity; never change it once cells
    /// exist. Shares the spelling of [`MessageRefCodec`]'s codec id by
    /// coincidence — the two tokens are independent identity columns.
    const RESOLVER_ID: Option<&'static str> = Some("message-ref");
}

impl<S> CellResolver<S> for MessageResolver
where
    S: StateSession<Loader: MessageLoader>,
{
    type Resolved = ConsumerMessage<<S::Loader as MessageLoader>::Payload>;
    type Stored = MessageRef;
    type Write<'a> = &'a ConsumerMessage<<S::Loader as MessageLoader>::Payload>;

    async fn resolve(session: &S, stored: MessageRef) -> Result<Self::Resolved, StateAccessError> {
        session
            .loader()
            .load_message(stored.topic, stored.partition, stored.offset)
            .await
            .map_err(|error| StateAccessError::load(&error))
    }

    fn stored_from(write: Self::Write<'_>) -> MessageRef {
        MessageRef::from(write)
    }
}

/// Descriptor for a collection whose cells reference Kafka message bodies.
///
/// A [`ValueDescriptor`] over [`MessageRefCodec`] + [`MessageResolver`];
/// declare via [`message_state`].
pub type MessageDescriptor = ValueDescriptor<MessageRefCodec, MessageResolver>;

/// Error returned by Kafka-message handle operations.
pub type MessageStateError = ValueStateError<MessageRefCodecError>;

/// Declares a Kafka-message collection named `name`.
///
/// `name` may be any runtime string and is interned (see
/// [`value_state`](crate::state::descriptor::value_state)); it is not
/// validated here — an empty name fails loudly at registration, the
/// fallible boundary.
#[must_use]
pub fn message_state(name: &str) -> MessageDescriptor {
    ValueDescriptor::new(name)
}

mod topic_serde {
    use crate::Topic;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(topic: &Topic, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(topic.as_ref())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Topic, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = <String as Deserialize<'de>>::deserialize(deserializer)?;
        Ok(Topic::from(value.as_str()))
    }
}

/// Error from the [`MessageRefCodec`] cell encode/decode.
///
/// Both variants classify Permanent through [`ValueStateError`]'s codec arm: a
/// cell that does not round-trip will not start to on retry.
#[derive(Debug, Error)]
pub enum MessageRefCodecError {
    /// The cell bytes did not decode as a [`MessageRef`].
    #[error("kafka message reference cell is corrupt")]
    Decode(#[source] MsgPackDecodeError),

    /// The reference failed to encode.
    #[error("kafka message reference failed to encode")]
    Encode(#[source] MsgPackEncodeError),
}

#[cfg(test)]
mod tests;
