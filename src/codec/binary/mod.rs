//! Binary codec that copies bytes verbatim and uses a caller-supplied
//! function to extract event metadata (id and type).

use serde::de::value::MapAccessDeserializer;
use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_slice_with_buffers;
use std::cell::RefCell;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::{Formatter, Result as FmtResult};
use std::marker::PhantomData;

use crate::codec::Codec;
use crate::{EventIdentity, EventType};

/// Metadata extracted from a binary payload at decode time.
///
/// Both fields borrow from the buffer passed to
/// [`BinaryExtractor::extract`].
#[derive(Default)]
pub struct BinaryMetadata<'a> {
    /// Stable identifier for the event, used by deduplication.
    pub event_id: Option<&'a str>,
    /// Event-type tag, used by `allowed_events` filtering.
    pub event_type: Option<&'a str>,
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
/// Owned serialization moves the byte vector into an empty output buffer.
/// Borrowed serialization copies bytes because the payload must retain them.
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
/// Uses the same backend as [`crate::codec::JsonCodec`] — `simd_json` on
/// non-ARM targets, `serde_json` on ARM. It streams the document into a
/// two-field view that borrows from `buf`. It does not build a parse tree.
/// All JSON values are valid messages. Values without string metadata fields
/// return no metadata.
///
/// On non-ARM targets the extractor owns a `simd_json::Buffers` instance; the
/// parent [`BinaryCodec`] holds one [`JsonExtractor`] for its lifetime, so the
/// buffers are reused across calls.
///
/// Returns absent fields as `None`. The bytes in `buf` are left in an
/// unspecified state on non-ARM targets due to `simd_json`'s in-place rewrite;
/// this is safe because [`BinaryCodec`] always copies the input *before*
/// invoking the extractor.
#[derive(Default)]
pub struct JsonExtractor {
    #[cfg(not(target_arch = "arm"))]
    buffers: simd_json::Buffers,
}

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
    #[serde(borrow, default, deserialize_with = "borrowed_string")]
    id: Option<&'a str>,
    #[serde(borrow, default, rename = "type", deserialize_with = "borrowed_string")]
    event_type: Option<&'a str>,
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

    fn visit_borrowed_str<E>(self, _value: &'de str) -> Result<Self::Value, E> {
        Ok(JsonDocument(JsonMetaView::default()))
    }
}

fn borrowed_string<'de, D>(deserializer: D) -> Result<Option<&'de str>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(BorrowedStringVisitor)
}

struct BorrowedStringVisitor;

impl<'de> Visitor<'de> for BorrowedStringVisitor {
    type Value = Option<&'de str>;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("a JSON value")
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(Some(value))
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

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(None)
    }
}

impl BinaryExtractor for JsonExtractor {
    type Error = JsonExtractError;

    fn extract<'a>(&mut self, buf: &'a mut [u8]) -> Result<BinaryMetadata<'a>, Self::Error> {
        #[cfg(target_arch = "arm")]
        {
            let JsonDocument(view) = serde_json::from_slice::<JsonDocument<'a>>(buf)?;
            Ok(BinaryMetadata {
                event_id: view.id,
                event_type: view.event_type,
            })
        }
        #[cfg(not(target_arch = "arm"))]
        {
            let JsonDocument(view) =
                from_slice_with_buffers::<JsonDocument<'a>>(buf, &mut self.buffers)?;
            Ok(BinaryMetadata {
                event_id: view.id,
                event_type: view.event_type,
            })
        }
    }

    fn with_cached_local<R>(f: impl FnOnce(Self) -> (Self, R)) -> R {
        // `JsonExtractor` is concrete here, so a `thread_local!` of its own
        // type is well-formed. The `Option` slot lets us hand the extractor
        // out by `take` and put it back after `f` returns, preserving the
        // `simd_json::Buffers` allocation across calls. A panic inside `f`
        // leaves the slot empty; the next call constructs fresh.
        thread_local! {
            static CACHE: RefCell<Option<JsonExtractor>> = const { RefCell::new(None) };
        }
        CACHE.with_borrow_mut(|slot| {
            let extractor = slot.take().unwrap_or_default();
            let (extractor, result) = f(extractor);
            *slot = Some(extractor);
            result
        })
    }
}

/// Errors produced by [`JsonExtractor::extract`].
#[derive(Debug, thiserror::Error)]
pub enum JsonExtractError {
    /// Deserialization failed via `serde_json` (ARM only).
    #[cfg(target_arch = "arm")]
    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    /// Deserialization failed via `simd_json` (non-ARM only).
    #[cfg(not(target_arch = "arm"))]
    #[error("simd_json error: {0}")]
    Simd(#[from] simd_json::Error),
}

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
