//! JSON codec backed by `serde_json` (ARM) or `simd_json` (non-ARM).

#[cfg(not(target_arch = "arm"))]
use serde::Deserialize;
#[cfg(not(target_arch = "arm"))]
use simd_json::{Buffers, Deserializer, Node, Tape, fill_tape};
use std::cell::RefCell;
#[cfg(not(target_arch = "arm"))]
use std::mem;

use crate::codec::Codec;
use crate::{EventIdentity, EventType};

/// JSON codec using `serde_json` (ARM) or `simd_json` (non-ARM).
///
/// On non-ARM targets `simd_json` parses **in place**, overwriting the input
/// buffer with tape data; the bytes passed to `deserialize` are unspecified
/// after the call returns.
/// Both serializers borrow the JSON tree because the writer does not consume
/// it. Both decoders mutate input for `simd_json` and reuse its parser state.
///
/// Both backends follow these decoding rules:
/// - An integer outside the `i64` and `u64` ranges decodes as a float.
/// - A lone UTF-16 surrogate escape, such as `"\ud800"`, is an error, because a
///   Rust string cannot hold it.
#[derive(Default)]
pub struct JsonCodec {
    #[cfg(not(target_arch = "arm"))]
    parser: SimdParser,
}

/// Reusable `simd_json` parser state.
///
/// It keeps the parse buffers and the tape across calls. After the first
/// calls grow them, a parse allocates nothing.
#[cfg(not(target_arch = "arm"))]
#[derive(Default)]
pub(crate) struct SimdParser {
    buffers: Buffers,
    tape: Vec<Node<'static>>,
}

impl Codec for JsonCodec {
    type Error = JsonCodecError;
    type Payload = serde_json::Value;

    const FORMAT_ID: &'static str = "json";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        #[cfg(target_arch = "arm")]
        {
            serde_json::from_slice(buf).map_err(JsonCodecError::Serde)
        }
        #[cfg(not(target_arch = "arm"))]
        {
            self.parser.parse(buf).map_err(JsonCodecError::Simd)
        }
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        // The serializer reads JSON by reference, so no payload copy is needed.
        write_json(payload, buf)
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        thread_local! {
            static CACHE: RefCell<JsonCodec> = RefCell::new(JsonCodec::default());
        }
        CACHE.with_borrow_mut(f)
    }
}

#[cfg(not(target_arch = "arm"))]
impl SimdParser {
    /// Parses `buf` in place and deserializes it as `T`.
    pub(crate) fn parse<'a, T: Deserialize<'a>>(
        &mut self,
        buf: &'a mut [u8],
    ) -> Result<T, simd_json::Error> {
        let mut tape = Tape(mem::take(&mut self.tape)).reset();
        let filled = fill_tape(buf, &mut self.buffers, &mut tape);
        let mut deserializer = Deserializer::from_tape(tape);
        let value = filled.and_then(|()| T::deserialize(&mut deserializer));
        self.tape = deserializer.into_tape().reset().0;
        value
    }
}

impl EventIdentity for serde_json::Value {
    /// Extracts the event ID from the JSON `"id"` field.
    fn event_id(&self) -> Option<&str> {
        self.get("id")?.as_str()
    }
}

impl EventType for serde_json::Value {
    /// Extracts the event type from the JSON `"type"` field.
    fn event_type(&self) -> Option<&str> {
        self.get("type")?.as_str()
    }
}

/// Serializes a value to JSON into the provided buffer.
///
/// Uses `simd_json` on non-ARM targets and `serde_json` on ARM. Returns
/// `true` on success and `false` if serialization fails.
pub fn serialize_to_json<T: serde::Serialize>(value: &T, buf: &mut Vec<u8>) -> bool {
    write_json(value, buf).is_ok()
}

/// Writes `value` as JSON into `buf` using the arch-appropriate backend
/// (`serde_json` on ARM, `simd_json` elsewhere) — the one place that
/// decision is made.
fn write_json<T: serde::Serialize>(value: &T, buf: &mut Vec<u8>) -> Result<(), JsonCodecError> {
    #[cfg(target_arch = "arm")]
    {
        serde_json::to_writer(buf, value).map_err(JsonCodecError::Serde)
    }
    #[cfg(not(target_arch = "arm"))]
    {
        simd_json::to_writer(buf, value).map_err(JsonCodecError::Simd)
    }
}

/// Errors produced by [`JsonCodec`].
#[derive(Debug, thiserror::Error)]
pub enum JsonCodecError {
    /// Serialization or deserialization failed via `serde_json`.
    #[error("serde_json error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Serialization or deserialization failed via `simd_json` (non-ARM only).
    #[cfg(not(target_arch = "arm"))]
    #[error("simd_json error: {0}")]
    Simd(simd_json::Error),
}
