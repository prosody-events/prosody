//! Durable encoding for Kafka message references.

use super::MessageRef;
use crate::codec::Codec;
use bytes::Bytes;
use rmp_serde::decode::Error as MsgPackDecodeError;
use rmp_serde::encode::{Error as MsgPackEncodeError, write_named};
use thiserror::Error;

pub(super) mod topic_serde {
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

/// `MsgPack` [`Codec`] for [`MessageRef`] cells.
///
/// Codec id `"message-ref"` is frozen into the durable structural identity.
/// Never change it after cells exist. `MessagePack` borrows values and input
/// bytes. Ownership adds no faster path.
#[derive(Default)]
pub struct MessageRefCodec;

impl Codec for MessageRefCodec {
    type Error = MessageRefCodecError;
    type Payload = MessageRef;

    const FORMAT_ID: &'static str = "message-ref";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        rmp_serde::from_slice(buf).map_err(MessageRefCodecError::Decode)
    }

    fn deserialize_bytes(&mut self, buf: Bytes) -> Result<Self::Payload, Self::Error> {
        rmp_serde::from_slice(&buf).map_err(MessageRefCodecError::Decode)
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        write_named(buf, payload).map_err(MessageRefCodecError::Encode)
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        f(&mut Self)
    }
}

/// Error from the [`MessageRefCodec`] cell encode/decode.
///
/// Both variants classify Permanent through
/// [`CellStateError`](crate::state::descriptor::CellStateError). A corrupt
/// cell will not become valid on retry.
#[derive(Debug, Error)]
pub enum MessageRefCodecError {
    /// The cell bytes did not decode as a [`MessageRef`].
    #[error("kafka message reference cell is corrupt")]
    Decode(#[source] MsgPackDecodeError),

    /// The reference failed to encode.
    #[error("kafka message reference failed to encode")]
    Encode(#[source] MsgPackEncodeError),
}
