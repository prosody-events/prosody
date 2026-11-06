//! Binary codec that copies bytes verbatim and uses a caller-supplied
//! function to extract event metadata (id and type).

use super::owned_bytes;
use bytes::Bytes;
use serde::de::value::MapAccessDeserializer;
use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::borrow::Cow;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::{Formatter, Result as FmtResult};
use std::marker::PhantomData;

use crate::codec::Codec;
use crate::{EventIdentity, EventType};

/// Metadata extracted from a binary payload at decode time.
///
/// Both fields can borrow from the buffer passed to
/// [`BinaryExtractor::extract`]. A field owns its text when the extractor must
/// decode it, for example a JSON string that contains an escape.
#[derive(Default)]
pub struct BinaryMetadata<'a> {
    /// Stable identifier for the event, used by deduplication.
    pub event_id: Option<Cow<'a, str>>,
    /// Event-type tag, used by `allowed_events` filtering.
    pub event_type: Option<Cow<'a, str>>,
}

/// Stateful metadata extractor used by [`BinaryCodec`].
///
/// Extractors hold any reusable parsing state (for example
/// `simd_json::Buffers`) as fields and access them through `&mut self` —
/// mirroring [`Codec`]'s pattern. The codec stores one extractor instance
/// and reuses it across deserialize calls, so any internal buffers persist
/// for the codec's lifetime; callers who need a thread-local cached codec
/// inherit that reuse via [`Codec::with_cached_local`].
///
/// `extract` may mutate the input slice (for example, to allow an in-place
/// parser to rewrite the bytes). [`BinaryCodec::deserialize`] always runs
/// extraction *after* copying the input into [`BinaryPayload::bytes`], so
/// implementations are free to destroy the slice they receive.
///
/// Extraction failures are surfaced as [`Self::Error`] and propagated through
/// [`BinaryCodec::deserialize`] as [`BinaryCodecError::Extract`]. Use
/// [`std::convert::Infallible`] for extractors that cannot fail.
pub trait BinaryExtractor: Default + Send + Sync + 'static {
    /// Error returned when extraction fails.
    type Error: StdError + Send + Sync + 'static;

    /// Returns the [`BinaryMetadata`] extracted from `buf`, or an error if
    /// the buffer cannot be parsed.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the buffer cannot be parsed.
    fn extract<'a>(&mut self, buf: &'a mut [u8]) -> Result<BinaryMetadata<'a>, Self::Error>;

    /// Runs `f` with an owned cached extractor, returning the extractor and
    /// the closure's result so the cache can reclaim it.
    fn with_cached_local<R>(f: impl FnOnce(Self) -> (Self, R)) -> R;
}

/// Payload produced by [`BinaryCodec`]: the raw bytes plus optional
/// metadata extracted at decode time.
#[derive(Clone, Debug)]
pub struct BinaryPayload {
    /// The raw payload bytes, copied verbatim from the wire.
    pub bytes: Vec<u8>,
    event_id: Option<Box<str>>,
    event_type: Option<Box<str>>,
}

impl BinaryPayload {
    /// Constructs a [`BinaryPayload`] from owned bytes and optional metadata.
    #[must_use]
    pub fn new<I, T>(bytes: Vec<u8>, event_id: Option<I>, event_type: Option<T>) -> Self
    where
        I: Into<Box<str>>,
        T: Into<Box<str>>,
    {
        Self {
            bytes,
            event_id: event_id.map(Into::into),
            event_type: event_type.map(Into::into),
        }
    }
}

impl EventIdentity for BinaryPayload {
    fn event_id(&self) -> Option<&str> {
        self.event_id.as_deref()
    }
}

impl EventType for BinaryPayload {
    fn event_type(&self) -> Option<&str> {
        self.event_type.as_deref()
    }
}

/// Declares the durable byte format a [`BinaryCodec`] speaks.
///
/// `BinaryCodec` copies bytes verbatim, so the format of those bytes is the
/// application's contract, not the codec's — this marker forces the codec
/// definition to state it. The declared token is held to every
/// [`Codec::FORMAT_ID`] law: claim `"json"` only if every payload written
/// through the codec is a JSON document.
pub trait BinaryFormat: 'static {
    /// The [`Codec::FORMAT_ID`] the composed codec asserts.
    const FORMAT_ID: &'static str;
}

/// The JSON document format used by the binary JSON codecs.
///
/// It is format-equal with [`JsonCodec`](crate::codec::JsonCodec). This lets
/// clients with different JSON implementations share data.
pub struct JsonFormat;

impl BinaryFormat for JsonFormat {
    const FORMAT_ID: &'static str = "json";
}

/// Codec that performs a verbatim byte copy and delegates metadata extraction
/// to `E`. `F` declares the byte format the application commits to writing —
/// the codec itself cannot know it.
///
/// On `deserialize`, the input slice is first copied into
/// [`BinaryPayload::bytes`] and then [`BinaryExtractor::extract`] is invoked
/// on the (now scratch) input to pull out the event id and type. The codec
/// owns one extractor instance for its lifetime, so any state the extractor
/// keeps (parser buffers, lookup tables) is reused across calls. On
/// Owned serialization moves the byte vector into an empty output buffer, and
/// [`Codec::serialize_bytes`] returns the vector as `Bytes`. Borrowed
/// serialization copies bytes because the payload must retain them.
/// Both decode forms preserve wire bytes before a mutable extractor runs.
pub struct BinaryCodec<E: BinaryExtractor, F: BinaryFormat> {
    extractor: E,
    _format: PhantomData<fn() -> F>,
}

impl<E: BinaryExtractor, F: BinaryFormat> Default for BinaryCodec<E, F> {
    fn default() -> Self {
        Self {
            extractor: E::default(),
            _format: PhantomData,
        }
    }
}

impl<E: BinaryExtractor, F: BinaryFormat> Codec for BinaryCodec<E, F> {
    type Error = BinaryCodecError<E::Error>;
    type Payload = BinaryPayload;

    const FORMAT_ID: &'static str = F::FORMAT_ID;

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        let bytes = buf.to_vec();
        let metadata = self.extractor.extract(buf)?;
        Ok(BinaryPayload {
            bytes,
            event_id: metadata.event_id.map(Into::into),
            event_type: metadata.event_type.map(Into::into),
        })
    }

    fn serialize(
        &mut self,
        mut payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        if buf.is_empty() {
            *buf = payload.bytes;
        } else {
            buf.append(&mut payload.bytes);
        }
        Ok(())
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        // A borrow requires one copy because the payload must retain its bytes.
        buf.extend_from_slice(&payload.bytes);
        Ok(())
    }

    fn serialize_bytes(&mut self, payload: Self::Payload) -> Result<Bytes, Self::Error> {
        Ok(owned_bytes(payload.bytes))
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        // A generic `BinaryCodec<E, F>` can't host a `thread_local!` of
        // itself, because statics can't depend on a generic parameter.
        // Delegate the cache to `E`, which is concrete at the implementor site
        // and can back its `with_cached_local` with a real `thread_local!`
        // when it owns expensive state.
        E::with_cached_local(|extractor| {
            let mut codec = BinaryCodec {
                extractor,
                _format: PhantomData,
            };
            let result = f(&mut codec);
            (codec.extractor, result)
        })
    }
}

/// [`BinaryExtractor`] that extracts the top-level `"id"` and `"type"` string
/// fields of a JSON document.
///
/// It streams the document with `serde_json` into a two-field view. It does
/// not build a parse tree. All JSON values are valid messages. Values without
/// string metadata fields return no metadata.
///
/// The extractor checks only the syntax of the fields that it does not read.
/// A lone surrogate escape or a very large integer in such a field does not
/// cause an error. The codec keeps the bytes, so the handler receives them
/// unchanged. The parser allocates a small stack when it skips a value nested
/// more than one level deep.
#[derive(Default)]
pub struct JsonExtractor;

/// [`BinaryCodec`] preconfigured with [`JsonExtractor`] for metadata
/// extraction and the declared [`JsonFormat`]: the application commits to
/// writing JSON documents, and the codec is format-equal with
/// [`JsonCodec`](crate::codec::JsonCodec).
pub type JsonBinaryMessageCodec = BinaryCodec<JsonExtractor, JsonFormat>;

/// A [`BinaryExtractor`] that pulls no metadata and never parses.
///
/// Non-message values do not need an event id or type. They can contain any
/// JSON shape, so this extractor leaves them unchanged.
#[derive(Default)]
pub struct NoopExtractor;

impl BinaryExtractor for NoopExtractor {
    type Error = Infallible;

    fn extract<'a>(&mut self, _buf: &'a mut [u8]) -> Result<BinaryMetadata<'a>, Infallible> {
        Ok(BinaryMetadata::default())
    }

    fn with_cached_local<R>(f: impl FnOnce(Self) -> (Self, R)) -> R {
        // Zero-sized and stateless: no buffers to preserve across calls.
        let (_extractor, result) = f(Self);
        result
    }
}

/// Verbatim JSON codec for a caller that owns JSON validation.
///
/// It does not parse the bytes or extract message metadata. The caller must
/// supply JSON documents to keep the [`JsonFormat`] contract.
pub type JsonBinaryCodec = BinaryCodec<NoopExtractor, JsonFormat>;

#[derive(Default, Deserialize)]
struct JsonMetaView<'a> {
    #[serde(borrow, default, deserialize_with = "string_field")]
    id: Option<Cow<'a, str>>,
    #[serde(borrow, default, rename = "type", deserialize_with = "string_field")]
    event_type: Option<Cow<'a, str>>,
}

struct JsonDocument<'a>(JsonMetaView<'a>);

fn empty_json_document<'a>() -> JsonDocument<'a> {
    JsonDocument(JsonMetaView::default())
}

impl<'de> Deserialize<'de> for JsonDocument<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonDocumentVisitor)
    }
}

struct JsonDocumentVisitor;

impl<'de> Visitor<'de> for JsonDocumentVisitor {
    type Value = JsonDocument<'de>;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("a JSON value")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        JsonMetaView::deserialize(MapAccessDeserializer::new(map)).map(JsonDocument)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<IgnoredAny>()?.is_some() {}
        Ok(empty_json_document())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(empty_json_document())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(empty_json_document())
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(empty_json_document())
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(empty_json_document())
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(empty_json_document())
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(empty_json_document())
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(empty_json_document())
    }
}

fn string_field<'de, D>(deserializer: D) -> Result<Option<Cow<'de, str>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(StringFieldVisitor)
}

struct StringFieldVisitor;

impl<'de> Visitor<'de> for StringFieldVisitor {
    type Value = Option<Cow<'de, str>>;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("a JSON value")
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(Some(Cow::Borrowed(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(Some(Cow::Owned(value.to_owned())))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<IgnoredAny>()?.is_some() {}
        Ok(None)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(None)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(None)
    }
}

impl BinaryExtractor for JsonExtractor {
    type Error = JsonExtractError;

    fn extract<'a>(&mut self, buf: &'a mut [u8]) -> Result<BinaryMetadata<'a>, Self::Error> {
        let JsonDocument(view) = serde_json::from_slice::<JsonDocument<'a>>(buf)?;
        Ok(BinaryMetadata {
            event_id: view.id,
            event_type: view.event_type,
        })
    }

    fn with_cached_local<R>(f: impl FnOnce(Self) -> (Self, R)) -> R {
        // Zero-sized and stateless: no buffers to preserve across calls.
        let (_extractor, result) = f(Self);
        result
    }
}

/// Errors produced by [`JsonExtractor::extract`].
#[derive(Debug, thiserror::Error)]
#[error("serde_json error: {0}")]
pub struct JsonExtractError(#[from] serde_json::Error);

/// Errors produced by [`BinaryCodec`], parameterised by the extractor's
/// error type.
///
/// `BinaryCodec` itself never fails on its own; the only error path is
/// extraction. Use [`std::convert::Infallible`] as `E` for extractors that
/// cannot fail — the variant becomes uninhabited.
#[derive(Debug, thiserror::Error)]
pub enum BinaryCodecError<E: StdError + Send + Sync + 'static> {
    /// Metadata extraction failed.
    #[error("metadata extraction failed: {0}")]
    Extract(#[from] E),
}

#[cfg(test)]
mod tests;
