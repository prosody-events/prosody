//! JSON codec backed by `serde_json` (ARM) or `simd_json` (non-ARM).

#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_slice_with_buffers;
use std::cell::RefCell;

use crate::codec::Codec;
use crate::{EventIdentity, EventTypeExtract, TimerReplayPayload};

/// JSON codec using `serde_json` (ARM) or `simd_json` (non-ARM).
///
/// On non-ARM targets `simd_json` parses **in place**, overwriting the input
/// buffer with tape data; the bytes passed to `deserialize` are unspecified
/// after the call returns.
#[derive(Default)]
pub struct JsonCodec {
    #[cfg(not(target_arch = "arm"))]
    buffers: simd_json::Buffers,
}

impl Codec for JsonCodec {
    type Error = JsonCodecError;
    type Payload = serde_json::Value;

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        #[cfg(target_arch = "arm")]
        {
            serde_json::from_slice(buf).map_err(JsonCodecError::Serde)
        }
        #[cfg(not(target_arch = "arm"))]
        {
            from_slice_with_buffers(buf, &mut self.buffers).map_err(JsonCodecError::Simd)
        }
    }

    fn serialize(&mut self, payload: &Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        serde_json::to_writer(buf, payload).map_err(JsonCodecError::Serde)?;
        Ok(())
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        thread_local! {
            static CACHE: RefCell<JsonCodec> = RefCell::new(JsonCodec::default());
        }
        CACHE.with_borrow_mut(f)
    }
}

impl EventIdentity for serde_json::Value {
    /// Extracts the event ID from the JSON `"id"` field.
    fn event_id(&self) -> Option<&str> {
        self.get("id")?.as_str()
    }
}

impl EventTypeExtract for serde_json::Value {
    /// Extracts the event type from the JSON `"type"` field.
    fn event_type(&self) -> Option<&str> {
        self.get("type")?.as_str()
    }
}

impl TimerReplayPayload for serde_json::Value {
    fn timer_replay(key: &str, time: &str) -> Self {
        serde_json::json!({ "key": key, "time": time })
    }
}

/// Serializes a value to JSON into the provided buffer.
///
/// Uses `simd_json` on non-ARM targets and `serde_json` on ARM. Returns
/// `true` on success and `false` if serialization fails.
pub fn serialize_to_json<T: serde::Serialize>(value: &T, buf: &mut Vec<u8>) -> bool {
    #[cfg(target_arch = "arm")]
    {
        serde_json::to_writer(buf, value).is_ok()
    }
    #[cfg(not(target_arch = "arm"))]
    {
        simd_json::to_writer(buf, value).is_ok()
    }
}

/// Errors produced by [`JsonCodec`].
#[derive(Debug, thiserror::Error)]
pub enum JsonCodecError {
    /// Serialization or deserialization failed via `serde_json`.
    #[error("serde_json error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Deserialization failed via `simd_json` (non-ARM only).
    #[cfg(not(target_arch = "arm"))]
    #[error("simd_json error: {0}")]
    Simd(simd_json::Error),
}
