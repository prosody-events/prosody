//! Binary codec that copies bytes verbatim and uses a caller-supplied
//! function to extract the event ID.

use serde::Deserialize;
#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_slice_with_buffers;
#[cfg(not(target_arch = "arm"))]
use std::cell::RefCell;
use std::convert::Infallible;
use std::marker::PhantomData;

use crate::EventIdentity;
use crate::codec::Codec;

/// Provides the function used by [`BinaryCodec`] to extract an event ID from
/// a mutable byte slice.
///
/// The slice may be mutated during extraction (for example, to allow an
/// in-place parser to rewrite the bytes). [`BinaryCodec::deserialize`] always
/// runs extraction *after* copying the input into [`BinaryPayload::bytes`], so
/// implementations are free to destroy the slice they receive.
pub trait BinaryEventId: Send + Sync + 'static {
    /// Returns the event ID extracted from `buf`, if any.
    fn event_id(buf: &mut [u8]) -> Option<&str>;
}

/// Payload produced by [`BinaryCodec`]: the raw bytes plus an optional
/// event ID extracted at decode time.
#[derive(Clone, Debug)]
pub struct BinaryPayload {
    /// The raw payload bytes, copied verbatim from the wire.
    pub bytes: Vec<u8>,
    event_id: Option<Box<str>>,
}

impl BinaryPayload {
    /// Constructs a [`BinaryPayload`] from owned bytes and an optional event
    /// ID.
    #[must_use]
    pub fn new<S>(bytes: Vec<u8>, event_id: Option<S>) -> Self
    where
        S: Into<Box<str>>,
    {
        Self {
            bytes,
            event_id: event_id.map(Into::into),
        }
    }
}

impl EventIdentity for BinaryPayload {
    fn event_id(&self) -> Option<&str> {
        self.event_id.as_deref()
    }
}

/// Codec that performs a verbatim byte copy and delegates event-ID extraction
/// to `E`.
///
/// On `deserialize`, the input slice is first copied into
/// [`BinaryPayload::bytes`] and then `E::event_id` is invoked on the (now
/// scratch) input to extract the identifier. On `serialize`, the stored bytes
/// are appended to the output buffer.
pub struct BinaryCodec<E: BinaryEventId>(PhantomData<fn() -> E>);

impl<E: BinaryEventId> Default for BinaryCodec<E> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<E: BinaryEventId> Codec for BinaryCodec<E> {
    type Error = BinaryCodecError;
    type Payload = BinaryPayload;

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        let bytes = buf.to_vec();
        let event_id = E::event_id(buf).map(Into::into);
        Ok(BinaryPayload { bytes, event_id })
    }

    fn serialize(&mut self, payload: &Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        buf.extend_from_slice(&payload.bytes);
        Ok(())
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        let mut codec = Self::default();
        f(&mut codec)
    }
}

/// [`BinaryEventId`] that extracts the top-level `"id"` string field of a JSON
/// document.
///
/// Uses the same backend as [`crate::codec::JsonCodec`] — `simd_json` on
/// non-ARM targets, `serde_json` on ARM — but deserializes into a one-field
/// view that borrows the value directly from `buf`. No full parse tree is
/// materialized.
///
/// Returns `None` if the input is not a JSON object, the `id` field is absent,
/// or the value is not a string. The bytes in `buf` are left in an unspecified
/// state on non-ARM targets due to `simd_json`'s in-place rewrite; this is
/// safe because [`BinaryCodec`] always copies the input *before* invoking the
/// extractor.
pub struct JsonId;

/// [`BinaryCodec`] preconfigured with [`JsonId`] for event-ID extraction.
pub type JsonBinaryCodec = BinaryCodec<JsonId>;

#[derive(Deserialize)]
struct JsonIdView<'a> {
    #[serde(borrow)]
    id: Option<&'a str>,
}

impl BinaryEventId for JsonId {
    fn event_id(buf: &mut [u8]) -> Option<&str> {
        #[cfg(target_arch = "arm")]
        {
            serde_json::from_slice::<JsonIdView<'_>>(buf).ok()?.id
        }
        #[cfg(not(target_arch = "arm"))]
        {
            thread_local! {
                static BUFFERS: RefCell<simd_json::Buffers> =
                    RefCell::new(simd_json::Buffers::default());
            }
            BUFFERS.with_borrow_mut(move |buffers| {
                from_slice_with_buffers::<JsonIdView<'_>>(buf, buffers)
                    .ok()?
                    .id
            })
        }
    }
}

/// Errors produced by [`BinaryCodec`].
///
/// `BinaryCodec` itself is infallible; this enum exists so that an extraction
/// function may surface failures in the future without a breaking change.
#[derive(Debug, thiserror::Error)]
pub enum BinaryCodecError {
    /// Reserved for future use.
    #[error(transparent)]
    Never(#[from] Infallible),
}

#[cfg(test)]
mod tests {
    use std::str;

    use super::*;

    /// Test extractor: reads the first 4 bytes as a big-endian length, then
    /// returns the next `len` bytes interpreted as UTF-8. To exercise the
    /// "mutable slice" contract, it XORs every byte it reads with 0xAA so the
    /// caller can confirm the input slice is mutated *after* the verbatim copy
    /// has already been taken.
    struct PrefixId;

    impl BinaryEventId for PrefixId {
        fn event_id(buf: &mut [u8]) -> Option<&str> {
            if buf.len() < 4 {
                return None;
            }
            let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let end = 4_usize.checked_add(len)?;
            if end > buf.len() {
                return None;
            }
            for byte in &mut buf[4..end] {
                *byte ^= 0xAA;
            }
            str::from_utf8(&buf[4..end]).ok()
        }
    }

    fn frame(id: &[u8]) -> color_eyre::Result<Vec<u8>> {
        let len = u32::try_from(id.len())?;
        let mut out = len.to_be_bytes().to_vec();
        out.extend_from_slice(id);
        Ok(out)
    }

    #[test]
    fn deserialize_copies_bytes_then_mutates_input() -> color_eyre::Result<()> {
        let id_bytes = b"abc"; // XOR-encoded under 0xAA so extractor returns "abc"
        let scrambled: Vec<u8> = id_bytes.iter().map(|b| b ^ 0xAA).collect();
        let original = frame(&scrambled)?;
        let mut wire = original.clone();

        let mut codec = BinaryCodec::<PrefixId>::default();
        let payload = codec.deserialize(&mut wire)?;

        assert_eq!(payload.bytes, original, "Vec must hold the verbatim copy");
        assert_eq!(payload.event_id(), Some("abc"));
        assert_ne!(wire, original, "input slice should be mutated by extractor");
        Ok(())
    }

    #[test]
    fn serialize_round_trips_bytes() -> color_eyre::Result<()> {
        let payload = BinaryPayload::new(b"hello world".to_vec(), Some("hello".to_owned()));
        let mut buf = Vec::new();
        let mut codec = BinaryCodec::<PrefixId>::default();
        codec.serialize(&payload, &mut buf)?;
        assert_eq!(buf, b"hello world");
        Ok(())
    }

    #[test]
    fn missing_event_id_yields_none() -> color_eyre::Result<()> {
        struct NoId;
        impl BinaryEventId for NoId {
            fn event_id(_buf: &mut [u8]) -> Option<&str> {
                None
            }
        }
        let mut wire = b"\x00\x00\x00\x00payload".to_vec();
        let mut codec = BinaryCodec::<NoId>::default();
        let payload = codec.deserialize(&mut wire)?;
        assert!(payload.event_id().is_none());
        Ok(())
    }

    fn json_id(input: &[u8]) -> Option<String> {
        let mut buf = input.to_vec();
        JsonId::event_id(&mut buf).map(str::to_owned)
    }

    #[test]
    fn json_id_first_field() {
        assert_eq!(json_id(br#"{"id":"abc"}"#).as_deref(), Some("abc"));
    }

    #[test]
    fn json_id_with_whitespace() {
        let input = b"  {\n  \"id\" : \"abc-123\"  ,\n  \"x\": 1\n}";
        assert_eq!(json_id(input).as_deref(), Some("abc-123"));
    }

    #[test]
    fn json_id_after_other_fields() {
        let input = br#"{"name":"Alice","kind":"user","id":"42"}"#;
        assert_eq!(json_id(input).as_deref(), Some("42"));
    }

    #[test]
    fn json_id_skips_string_value_containing_id_token() {
        // Earlier value contains the literal characters `"id":` — must not
        // false-match.
        let input = br#"{"note":"\"id\": fake","id":"real"}"#;
        assert_eq!(json_id(input).as_deref(), Some("real"));
    }

    #[test]
    fn json_id_ignores_nested_id() {
        let input = br#"{"data":{"id":"nested"},"id":"top"}"#;
        assert_eq!(json_id(input).as_deref(), Some("top"));
    }

    #[test]
    fn json_id_no_top_level_id_returns_none() {
        let input = br#"{"data":{"id":"nested"}}"#;
        assert_eq!(json_id(input), None);
    }

    #[test]
    fn json_id_skips_arrays_and_numbers() {
        let input = br#"{"a":[1,2,{"id":"inner"}],"b":3.14,"c":true,"d":null,"id":"x"}"#;
        assert_eq!(json_id(input).as_deref(), Some("x"));
    }

    #[test]
    fn json_id_value_with_escapes_is_decoded() {
        // serde/simd_json decode escape sequences in place; we get the
        // unescaped string back.
        let input = br#"{"id":"a\"b"}"#;
        assert_eq!(json_id(input).as_deref(), Some("a\"b"));
    }

    #[test]
    fn json_id_non_string_value_returns_none() {
        assert_eq!(json_id(br#"{"id":123}"#), None);
        assert_eq!(json_id(br#"{"id":null}"#), None);
    }

    #[test]
    fn json_id_non_object_returns_none() {
        assert_eq!(json_id(b"[1,2,3]"), None);
        assert_eq!(json_id(b""), None);
        assert_eq!(json_id(b"   "), None);
    }

    #[test]
    fn json_id_via_binary_codec() -> color_eyre::Result<()> {
        let mut wire = br#"{"id":"evt-1","payload":{"x":1}}"#.to_vec();
        let original = wire.clone();
        let mut codec = BinaryCodec::<JsonId>::default();
        let payload = codec.deserialize(&mut wire)?;
        assert_eq!(payload.bytes, original);
        assert_eq!(payload.event_id(), Some("evt-1"));
        Ok(())
    }
}
