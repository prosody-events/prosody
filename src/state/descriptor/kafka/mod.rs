//! Kafka-message descriptor: cells hold a durable message reference that
//! resolves to the full consumer message through the defer loader.

use super::{
    BindError, CellKind, DescriptorIdentity, DirtyErr, DurableErr, SchemaLabel, StateDescriptor,
    StructuralIdentity, require_registered,
};
use crate::codec::CodecId;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::message::MessageLoader;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::CollectionKindId;
use crate::state::StoreOutcome;
use crate::state::middleware::{
    ByteValueHandle, DirtyValueBundle, DurableValueBundle, KeyedStateContext,
};
use crate::state::value::TransactionValueStoreError;
use crate::{Offset, Partition, Topic};
use bytes::Bytes;
use rmp_serde::decode::Error as MsgPackDecodeError;
use rmp_serde::encode::Error as MsgPackEncodeError;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use thiserror::Error;

/// Durable pointer to a Kafka message body.
///
/// Persisted as the `MsgPack`-encoded cell of a Kafka-message collection;
/// resolved back to the full message via the defer [`MessageLoader`].
/// Inside a message handler, [`message_ref()`] on the context names the
/// message being processed — the only production source of a ref.
///
/// [`message_ref()`]: crate::state::middleware::KeyedStateContext::message_ref
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

/// Placeholder for the loader slot of consumers with no Kafka-message
/// collections.
///
/// Deliberately does **not** implement [`MessageLoader`], so a
/// [`KafkaMessageDescriptor`] cannot bind against a context carrying it —
/// `ctx.state(KAFKA_DESC)` fails to compile instead of failing at runtime.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLoader;

/// Descriptor for a collection whose cells reference Kafka message bodies.
///
/// Declare as a `const` via [`kafka_message_state`]. Binding requires the
/// context's loader to be a real [`MessageLoader`]; `get()` then returns
/// the full `ConsumerMessage` with the handler's payload type.
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
            codec_id: CodecId::None,
            schema_label: self.schema_label.map(SchemaLabel::from),
        }
    }
}

/// Binds a Kafka-message descriptor against any event scope, but only a
/// context whose loader is a real [`MessageLoader`] — with the default
/// [`NoLoader`] this impl does not exist, so `ctx.state(KAFKA_DESC)` is a
/// compile error rather than a runtime one. Reading needs no message
/// coordinates, so timer handlers may bind and `get()`; only `set` needs a
/// ref, and the only production source of one is the message-scoped
/// `message_ref()`.
impl<C, D, S, L, Scope> StateDescriptor<KeyedStateContext<C, D, S, L, Scope>>
    for KafkaMessageDescriptor
where
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
    L: MessageLoader,
{
    type Error = BindError;
    type Handle = KafkaValueHandle<L, D, S>;

    fn bind(self, ctx: &KeyedStateContext<C, D, S, L, Scope>) -> Result<Self::Handle, BindError> {
        let name = require_registered(ctx.registry(), &self)?;
        Ok(KafkaValueHandle::new(
            ctx.byte_handle(&name),
            ctx.loader().clone(),
        ))
    }
}

/// Typed handle over a Kafka-message collection.
///
/// Wraps the shared byte-transaction substrate: the cell is the
/// `MsgPack`-encoded [`KafkaMessageRef`]; `get()` resolves it to the full
/// message through the loader.
pub struct KafkaValueHandle<L, D, S> {
    inner: ByteValueHandle<D, S>,
    loader: L,
}

impl<L, D, S> Clone for KafkaValueHandle<L, D, S>
where
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            loader: self.loader.clone(),
        }
    }
}

impl<L, D, S> KafkaValueHandle<L, D, S>
where
    L: MessageLoader,
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    pub(crate) fn new(inner: ByteValueHandle<D, S>, loader: L) -> Self {
        Self { inner, loader }
    }

    /// Reads the current cell and loads the referenced message.
    ///
    /// Returns `Ok(None)` for an absent cell. A present cell whose
    /// referenced offset has been deleted or compacted away surfaces the
    /// loader's error (Permanent for a vanished body), never `None`.
    ///
    /// # Errors
    ///
    /// Returns a transaction error, a corrupt-ref error, or the loader's
    /// error when the message cannot be loaded.
    pub async fn get(
        &self,
    ) -> Result<
        Option<ConsumerMessage<L::Payload>>,
        KafkaValueError<DirtyErr<S>, DurableErr<D>, L::Error>,
    > {
        let Some(cell) = self.inner.get().await? else {
            return Ok(None);
        };
        let message_ref = decode_ref(&cell)?;
        let message = self
            .loader
            .load_message(message_ref.topic, message_ref.partition, message_ref.offset)
            .await
            .map_err(KafkaValueError::Loader)?;
        Ok(Some(message))
    }

    /// Buffers a set of the `MsgPack`-encoded `message_ref` cell.
    ///
    /// # Errors
    ///
    /// Returns an encode error or a transaction error from the underlying
    /// store.
    pub async fn set(
        &self,
        message_ref: KafkaMessageRef,
    ) -> Result<(), KafkaValueError<DirtyErr<S>, DurableErr<D>, L::Error>> {
        Ok(self.inner.set(encode_ref(&message_ref)?).await?)
    }

    /// Buffers a clear operation.
    ///
    /// # Errors
    ///
    /// Returns a transaction error from the underlying store.
    pub async fn clear(&self) -> Result<(), KafkaValueError<DirtyErr<S>, DurableErr<D>, L::Error>> {
        Ok(self.inner.clear().await?)
    }

    /// Drains buffered ops directly to authoritative state and returns the
    /// transaction to `Clean`.
    ///
    /// # Errors
    ///
    /// Returns a transaction error from the underlying store.
    pub async fn flush(
        &self,
    ) -> Result<StoreOutcome, KafkaValueError<DirtyErr<S>, DurableErr<D>, L::Error>> {
        Ok(self.inner.flush().await?)
    }
}

/// Encodes a [`KafkaMessageRef`] as its `MsgPack` cell bytes.
fn encode_ref<DirtyE, DurableE, LoaderE>(
    message_ref: &KafkaMessageRef,
) -> Result<Bytes, KafkaValueError<DirtyE, DurableE, LoaderE>>
where
    DirtyE: ClassifyError + Error + Send + Sync + 'static,
    DurableE: ClassifyError + Error + Send + Sync + 'static,
    LoaderE: ClassifyError + Error + Send + Sync + 'static,
{
    rmp_serde::to_vec_named(message_ref)
        .map(Bytes::from)
        .map_err(KafkaValueError::EncodeRef)
}

/// Decodes a `MsgPack` cell back into a [`KafkaMessageRef`].
fn decode_ref<DirtyE, DurableE, LoaderE>(
    cell: &[u8],
) -> Result<KafkaMessageRef, KafkaValueError<DirtyE, DurableE, LoaderE>>
where
    DirtyE: ClassifyError + Error + Send + Sync + 'static,
    DurableE: ClassifyError + Error + Send + Sync + 'static,
    LoaderE: ClassifyError + Error + Send + Sync + 'static,
{
    rmp_serde::from_slice(cell).map_err(KafkaValueError::CorruptRef)
}

/// Error returned by [`KafkaValueHandle`] operations.
#[derive(Debug, Error)]
pub enum KafkaValueError<DirtyE, DurableE, LoaderE>
where
    DirtyE: ClassifyError + Error + Send + Sync + 'static,
    DurableE: ClassifyError + Error + Send + Sync + 'static,
    LoaderE: ClassifyError + Error + Send + Sync + 'static,
{
    /// The underlying value transaction failed.
    #[error(transparent)]
    Tx(#[from] TransactionValueStoreError<DirtyE, DurableE>),

    /// The cell bytes did not decode as a [`KafkaMessageRef`].
    #[error("kafka message reference cell is corrupt")]
    CorruptRef(#[source] MsgPackDecodeError),

    /// The reference failed to encode.
    #[error("kafka message reference failed to encode")]
    EncodeRef(#[source] MsgPackEncodeError),

    /// The loader failed to resolve the referenced message.
    #[error("kafka message loader failed")]
    Loader(#[source] LoaderE),
}

impl<DirtyE, DurableE, LoaderE> ClassifyError for KafkaValueError<DirtyE, DurableE, LoaderE>
where
    DirtyE: ClassifyError + Error + Send + Sync + 'static,
    DurableE: ClassifyError + Error + Send + Sync + 'static,
    LoaderE: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Tx(e) => e.classify_error(),
            Self::CorruptRef(_) | Self::EncodeRef(_) => ErrorCategory::Permanent,
            Self::Loader(e) => e.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests;
