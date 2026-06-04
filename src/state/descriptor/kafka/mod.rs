//! Kafka-message descriptor: cells hold a durable message reference that
//! resolves to the full consumer message through the consumer's message
//! loader.

use super::{
    CellKind, DescriptorIdentity, SchemaLabel, StateDescriptor, StructuralIdentity, ensure_live,
};
use crate::consumer::event_context::StateAccessError;
use crate::consumer::message::ConsumerMessage;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::CollectionKindId;
use crate::state::StateName;
use crate::state::StoreOutcome;
use crate::state::session::StateSession;
use crate::{Offset, Partition, Topic};
use bytes::Bytes;
use rmp_serde::decode::Error as MsgPackDecodeError;
use rmp_serde::encode::Error as MsgPackEncodeError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Durable pointer to a Kafka message body.
///
/// Persisted as the `MsgPack`-encoded cell of a Kafka-message collection;
/// resolved back to the full message via the consumer's
/// [`MessageLoader`](crate::consumer::middleware::defer::message::MessageLoader).
/// Derived from the [`ConsumerMessage`] in hand at
/// [`KafkaMessageHandle::set`] — the only production source of a ref.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaMessageRef {
    /// Kafka topic.
    #[serde(with = "topic_serde")]
    pub topic: Topic,

    /// Kafka partition.
    pub partition: Partition,

    /// Kafka offset within the partition.
    pub offset: Offset,
}

impl<P> From<&ConsumerMessage<P>> for KafkaMessageRef {
    fn from(message: &ConsumerMessage<P>) -> Self {
        Self {
            topic: message.topic(),
            partition: message.partition(),
            offset: message.offset(),
        }
    }
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

/// Descriptor for a collection whose cells reference Kafka message bodies.
///
/// Codec-free: message typing flows from the binding session's
/// [`StateSession::Payload`] — the consumer's own codec decodes the loaded
/// message, so the handle returns `ConsumerMessage<S::Payload>` with no
/// codec parameter. Declare as a `const` via [`kafka_message_state`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaMessageDescriptor {
    name: &'static str,
    schema_label: Option<&'static str>,
}

/// Declares a Kafka-message collection named `name`.
///
/// `name` is not validated here (const contexts cannot fail); an empty
/// name fails loudly at registration, the fallible boundary.
#[must_use]
pub const fn kafka_message_state(name: &'static str) -> KafkaMessageDescriptor {
    KafkaMessageDescriptor {
        name,
        schema_label: None,
    }
}

impl KafkaMessageDescriptor {
    /// Attaches an opt-in schema version label to the frozen identity.
    #[must_use]
    pub const fn with_schema_label(mut self, label: &'static str) -> Self {
        self.schema_label = Some(label);
        self
    }
}

impl DescriptorIdentity for KafkaMessageDescriptor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            cell_kind: CellKind::KafkaMessageRef,
            codec_id: None,
            schema_label: self.schema_label.map(SchemaLabel::from),
        }
    }
}

impl StateDescriptor for KafkaMessageDescriptor {
    type Handle<S: StateSession> = KafkaMessageHandle<S>;

    fn bind<S: StateSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        let name = session.verify_state_registration(self.name, &self.structural_identity())?;
        Ok(KafkaMessageHandle {
            session: session.clone(),
            name,
        })
    }
}

/// Typed, owned handle over a Kafka-message collection.
///
/// The cell is the `MsgPack`-encoded [`KafkaMessageRef`]; [`Self::get`]
/// resolves it to the full message through the session's loader, decoded
/// by the consumer's own codec. Owns a clone of the binding session; every
/// operation guards on session termination.
#[derive(Clone)]
pub struct KafkaMessageHandle<S> {
    session: S,
    name: StateName,
}

impl<S> KafkaMessageHandle<S>
where
    S: StateSession,
{
    /// Reads the current cell and loads the referenced message.
    ///
    /// Returns `Ok(None)` for an absent cell. A present cell whose
    /// referenced offset has been deleted or compacted away surfaces the
    /// loader's error (Permanent for a vanished body), never `None`.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session (including the type-erased
    /// loader failure) or a corrupt-ref error.
    pub async fn get(&self) -> Result<Option<ConsumerMessage<S::Payload>>, KafkaStateError> {
        ensure_live(&self.session)?;
        let Some(cell) = self.session.state_cell(&self.name).await? else {
            return Ok(None);
        };
        let message_ref = decode_ref(&cell)?;
        Ok(Some(self.session.load_message(message_ref).await?))
    }

    /// Buffers a set of the `MsgPack`-encoded reference to `message`.
    ///
    /// The reference is derived from the message in hand; the payload type
    /// equality between the handler's message and the session is enforced
    /// by the `C: EventContext<Payload = Self::Payload>` handler bound
    /// (contexts pin their session's payload), so this is a compile-time
    /// match.
    ///
    /// # Errors
    ///
    /// Returns an encode error or an access error from the session.
    pub async fn set(&self, message: &ConsumerMessage<S::Payload>) -> Result<(), KafkaStateError> {
        ensure_live(&self.session)?;
        let message_ref = KafkaMessageRef::from(message);
        let cell = rmp_serde::to_vec_named(&message_ref)
            .map(Bytes::from)
            .map_err(KafkaStateError::EncodeRef)?;
        Ok(self.session.set_state_cell(&self.name, cell).await?)
    }

    /// Buffers a clear operation.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub async fn clear(&self) -> Result<(), KafkaStateError> {
        ensure_live(&self.session)?;
        Ok(self.session.clear_state_cell(&self.name).await?)
    }

    /// Drains buffered ops directly to authoritative state and returns the
    /// transaction to `Clean`.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub async fn flush(&self) -> Result<StoreOutcome, KafkaStateError> {
        ensure_live(&self.session)?;
        Ok(self.session.flush_state_cell(&self.name).await?)
    }
}

/// Decodes a `MsgPack` cell back into a [`KafkaMessageRef`].
fn decode_ref(cell: &[u8]) -> Result<KafkaMessageRef, KafkaStateError> {
    rmp_serde::from_slice(cell).map_err(KafkaStateError::CorruptRef)
}

/// Error returned by [`KafkaMessageHandle`] operations.
#[derive(Debug, Error)]
pub enum KafkaStateError {
    /// The context refused or failed the state access (store and loader
    /// failures arrive here type-erased).
    #[error(transparent)]
    Access(#[from] StateAccessError),

    /// The cell bytes did not decode as a [`KafkaMessageRef`].
    #[error("kafka message reference cell is corrupt")]
    CorruptRef(#[source] MsgPackDecodeError),

    /// The reference failed to encode.
    #[error("kafka message reference failed to encode")]
    EncodeRef(#[source] MsgPackEncodeError),
}

impl ClassifyError for KafkaStateError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Access(e) => e.classify_error(),
            Self::CorruptRef(_) | Self::EncodeRef(_) => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
