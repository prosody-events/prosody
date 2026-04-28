//! Binary codec that copies bytes verbatim and uses a caller-supplied
//! function to extract event metadata (id and type).

use serde::Deserialize;
#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_slice_with_buffers;
use std::cell::RefCell;
use std::error::Error as StdError;

use crate::codec::Codec;
use crate::codec::serialize_to_json;
use crate::{EventIdentity, EventType, TimerReplayPayload};

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

impl TimerReplayPayload for BinaryPayload {
    /// Builds a synthetic timer-replay payload by JSON-encoding `{"key",
    /// "time"}` into [`BinaryPayload::bytes`]. Mirrors the
    /// [`JsonCodec`](crate::JsonCodec) shape so a consumer routing
    /// failure-topic timer replays through a binary pipeline produces bytes
    /// that downstream JSON-aware tooling can read.
    fn timer_replay(key: &str, time: &str) -> Self {
        #[derive(serde::Serialize)]
        struct Replay<'a> {
            key: &'a str,
            time: &'a str,
        }
        let mut bytes = Vec::new();
        let _ = serialize_to_json(&Replay { key, time }, &mut bytes);
        Self {
            bytes,
            event_id: None,
            event_type: None,
        }
    }
}

/// Codec that performs a verbatim byte copy and delegates metadata extraction
/// to `E`.
///
/// On `deserialize`, the input slice is first copied into
/// [`BinaryPayload::bytes`] and then [`BinaryExtractor::extract`] is invoked
/// on the (now scratch) input to pull out the event id and type. The codec
/// owns one extractor instance for its lifetime, so any state the extractor
/// keeps (parser buffers, lookup tables) is reused across calls. On
/// `serialize`, the stored bytes are appended to the output buffer.
#[derive(Default)]
pub struct BinaryCodec<E: BinaryExtractor> {
    extractor: E,
}

impl<E: BinaryExtractor> Codec for BinaryCodec<E> {
    type Error = BinaryCodecError<E::Error>;
    type Payload = BinaryPayload;

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        let bytes = buf.to_vec();
        let metadata = self.extractor.extract(buf)?;
        Ok(BinaryPayload {
            bytes,
            event_id: metadata.event_id.map(Into::into),
            event_type: metadata.event_type.map(Into::into),
        })
    }

    fn serialize(&mut self, payload: &Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        buf.extend_from_slice(&payload.bytes);
        Ok(())
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        // A generic `BinaryCodec<E>` can't host a `thread_local!` of itself,
        // because statics can't depend on a generic parameter. Delegate the
        // cache to `E`, which is concrete at the implementor site and can back
        // its `with_cached_local` with a real `thread_local!` when it owns
        // expensive state.
        E::with_cached_local(|extractor| {
            let mut codec = BinaryCodec { extractor };
            let result = f(&mut codec);
            (codec.extractor, result)
        })
    }
}

/// [`BinaryExtractor`] that extracts the top-level `"id"` and `"type"` string
/// fields of a JSON document.
///
/// Uses the same backend as [`crate::codec::JsonCodec`] — `simd_json` on
/// non-ARM targets, `serde_json` on ARM — but deserializes into a two-field
/// view that borrows the values directly from `buf`. No full parse tree is
/// materialized.
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
/// extraction.
pub type JsonBinaryCodec = BinaryCodec<JsonExtractor>;

#[derive(Deserialize)]
struct JsonMetaView<'a> {
    #[serde(borrow)]
    id: Option<&'a str>,
    #[serde(borrow, rename = "type")]
    event_type: Option<&'a str>,
}

impl BinaryExtractor for JsonExtractor {
    type Error = JsonExtractError;

    fn extract<'a>(&mut self, buf: &'a mut [u8]) -> Result<BinaryMetadata<'a>, Self::Error> {
        #[cfg(target_arch = "arm")]
        {
            let view = serde_json::from_slice::<JsonMetaView<'a>>(buf)?;
            Ok(BinaryMetadata {
                event_id: view.id,
                event_type: view.event_type,
            })
        }
        #[cfg(not(target_arch = "arm"))]
        {
            let view = from_slice_with_buffers::<JsonMetaView<'a>>(buf, &mut self.buffers)?;
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
mod tests {
    use std::convert::Infallible;
    use std::str;

    use super::*;

    /// Test extractor: reads the first 4 bytes as a big-endian length, then
    /// returns the next `len` bytes interpreted as UTF-8 as the event id. The
    /// `event_type` is left unset.
    #[derive(Default)]
    struct PrefixExtractor;

    impl BinaryExtractor for PrefixExtractor {
        type Error = Infallible;

        fn extract<'a>(&mut self, buf: &'a mut [u8]) -> Result<BinaryMetadata<'a>, Self::Error> {
            if buf.len() < 4 {
                return Ok(BinaryMetadata::default());
            }
            let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let Some(end) = 4_usize.checked_add(len) else {
                return Ok(BinaryMetadata::default());
            };
            if end > buf.len() {
                return Ok(BinaryMetadata::default());
            }
            Ok(BinaryMetadata {
                event_id: str::from_utf8(&buf[4..end]).ok(),
                event_type: None,
            })
        }

        fn with_cached_local<R>(f: impl FnOnce(Self) -> (Self, R)) -> R {
            f(Self).1
        }
    }

    fn frame(id: &[u8]) -> color_eyre::Result<Vec<u8>> {
        let len = u32::try_from(id.len())?;
        let mut out = len.to_be_bytes().to_vec();
        out.extend_from_slice(id);
        Ok(out)
    }

    #[test]
    fn deserialize_preserves_verbatim_bytes() -> color_eyre::Result<()> {
        let original = frame(b"abc")?;
        let mut wire = original.clone();

        let mut codec = BinaryCodec::<PrefixExtractor>::default();
        let payload = codec.deserialize(&mut wire)?;

        assert_eq!(payload.bytes, original, "Vec must hold the verbatim copy");
        assert_eq!(payload.event_id(), Some("abc"));
        Ok(())
    }

    #[test]
    fn serialize_round_trips_bytes() -> color_eyre::Result<()> {
        let payload = BinaryPayload::new(
            b"hello world".to_vec(),
            Some("hello".to_owned()),
            None::<String>,
        );
        let mut buf = Vec::new();
        let mut codec = BinaryCodec::<PrefixExtractor>::default();
        codec.serialize(&payload, &mut buf)?;
        assert_eq!(buf, b"hello world");
        Ok(())
    }

    #[test]
    fn missing_event_id_yields_none() -> color_eyre::Result<()> {
        #[derive(Default)]
        struct NoMetadataExtractor;
        impl BinaryExtractor for NoMetadataExtractor {
            type Error = Infallible;

            fn extract<'a>(
                &mut self,
                _buf: &'a mut [u8],
            ) -> Result<BinaryMetadata<'a>, Self::Error> {
                Ok(BinaryMetadata::default())
            }

            fn with_cached_local<R>(f: impl FnOnce(Self) -> (Self, R)) -> R {
                f(Self).1
            }
        }
        let mut wire = b"\x00\x00\x00\x00payload".to_vec();
        let mut codec = BinaryCodec::<NoMetadataExtractor>::default();
        let payload = codec.deserialize(&mut wire)?;
        assert!(payload.event_id().is_none());
        assert!(payload.event_type().is_none());
        Ok(())
    }

    fn json_id(input: &[u8]) -> Result<Option<String>, JsonExtractError> {
        let mut buf = input.to_vec();
        Ok(JsonExtractor::default()
            .extract(&mut buf)?
            .event_id
            .map(str::to_owned))
    }

    fn json_type(input: &[u8]) -> Result<Option<String>, JsonExtractError> {
        let mut buf = input.to_vec();
        Ok(JsonExtractor::default()
            .extract(&mut buf)?
            .event_type
            .map(str::to_owned))
    }

    #[test]
    fn json_id_first_field() -> Result<(), JsonExtractError> {
        assert_eq!(json_id(br#"{"id":"abc"}"#)?.as_deref(), Some("abc"));
        Ok(())
    }

    #[test]
    fn json_id_with_whitespace() -> Result<(), JsonExtractError> {
        let input = b"  {\n  \"id\" : \"abc-123\"  ,\n  \"x\": 1\n}";
        assert_eq!(json_id(input)?.as_deref(), Some("abc-123"));
        Ok(())
    }

    #[test]
    fn json_id_after_other_fields() -> Result<(), JsonExtractError> {
        let input = br#"{"name":"Alice","kind":"user","id":"42"}"#;
        assert_eq!(json_id(input)?.as_deref(), Some("42"));
        Ok(())
    }

    #[test]
    fn json_id_skips_string_value_containing_id_token() -> Result<(), JsonExtractError> {
        // Earlier value contains the literal characters `"id":` — must not
        // false-match.
        let input = br#"{"note":"\"id\": fake","id":"real"}"#;
        assert_eq!(json_id(input)?.as_deref(), Some("real"));
        Ok(())
    }

    #[test]
    fn json_id_ignores_nested_id() -> Result<(), JsonExtractError> {
        let input = br#"{"data":{"id":"nested"},"id":"top"}"#;
        assert_eq!(json_id(input)?.as_deref(), Some("top"));
        Ok(())
    }

    #[test]
    fn json_id_no_top_level_id_returns_none() -> Result<(), JsonExtractError> {
        let input = br#"{"data":{"id":"nested"}}"#;
        assert_eq!(json_id(input)?, None);
        Ok(())
    }

    #[test]
    fn json_id_skips_arrays_and_numbers() -> Result<(), JsonExtractError> {
        let input = br#"{"a":[1,2,{"id":"inner"}],"b":3.14,"c":true,"d":null,"id":"x"}"#;
        assert_eq!(json_id(input)?.as_deref(), Some("x"));
        Ok(())
    }

    #[test]
    fn json_id_value_with_escapes_is_decoded() -> Result<(), JsonExtractError> {
        // serde/simd_json decode escape sequences in place; we get the
        // unescaped string back.
        let input = br#"{"id":"a\"b"}"#;
        assert_eq!(json_id(input)?.as_deref(), Some("a\"b"));
        Ok(())
    }

    #[test]
    fn json_id_null_value_yields_none() -> Result<(), JsonExtractError> {
        // `null` deserializes to Option::None — a successful extraction with
        // no id present.
        assert_eq!(json_id(br#"{"id":null}"#)?, None);
        Ok(())
    }

    #[test]
    fn json_id_non_string_value_propagates_error() {
        // Non-null, non-string values for `id` are a parse error.
        assert!(json_id(br#"{"id":123}"#).is_err());
    }

    #[test]
    fn json_id_non_object_propagates_error() {
        // Inputs that aren't a JSON object cannot be parsed as the metadata
        // view — the error propagates.
        assert!(json_id(b"[1,2,3]").is_err());
        assert!(json_id(b"").is_err());
        assert!(json_id(b"   ").is_err());
    }

    #[test]
    fn json_id_via_binary_codec() -> color_eyre::Result<()> {
        let mut wire = br#"{"id":"evt-1","payload":{"x":1}}"#.to_vec();
        let original = wire.clone();
        let mut codec = BinaryCodec::<JsonExtractor>::default();
        let payload = codec.deserialize(&mut wire)?;
        assert_eq!(payload.bytes, original);
        assert_eq!(payload.event_id(), Some("evt-1"));
        Ok(())
    }

    #[test]
    fn json_type_extracts_field() -> Result<(), JsonExtractError> {
        assert_eq!(
            json_type(br#"{"type":"user.created"}"#)?.as_deref(),
            Some("user.created")
        );
        Ok(())
    }

    #[test]
    fn json_type_with_id_field_present() -> Result<(), JsonExtractError> {
        let input = br#"{"id":"evt-1","type":"order.placed"}"#;
        assert_eq!(json_id(input)?.as_deref(), Some("evt-1"));
        assert_eq!(json_type(input)?.as_deref(), Some("order.placed"));
        Ok(())
    }

    #[test]
    fn json_type_missing_returns_none() -> Result<(), JsonExtractError> {
        assert_eq!(json_type(br#"{"id":"x"}"#)?, None);
        Ok(())
    }

    #[test]
    fn json_type_via_binary_codec() -> color_eyre::Result<()> {
        let mut wire = br#"{"id":"evt-1","type":"user.created","data":{}}"#.to_vec();
        let original = wire.clone();
        let mut codec = BinaryCodec::<JsonExtractor>::default();
        let payload = codec.deserialize(&mut wire)?;
        assert_eq!(payload.bytes, original);
        assert_eq!(payload.event_id(), Some("evt-1"));
        assert_eq!(payload.event_type(), Some("user.created"));
        Ok(())
    }
}
