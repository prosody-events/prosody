//! The deadline that a caller set on one peer call.

use std::time::Duration;
use tokio::time::Instant;
use tonic::metadata::MetadataMap;

/// The header that carries a gRPC caller's timeout.
const TIMEOUT_HEADER: &str = "grpc-timeout";

/// Most digits the gRPC protocol allows in a timeout value.
const MAX_DIGITS: usize = 8;

/// The absolute instant when a caller's `grpc-timeout` ends.
///
/// This crate parses the header itself because tonic parses it privately and
/// never puts the parsed value anywhere a service can read it.
pub(super) fn inbound_deadline(metadata: &MetadataMap) -> Option<Instant> {
    let Ok(value) = metadata.get(TIMEOUT_HEADER)?.to_str() else {
        return None;
    };
    let stated = parse_timeout(value)?;
    Instant::now().checked_add(stated)
}

/// One `grpc-timeout` value: up to eight digits, then one unit character.
///
/// A value outside that shape is invalid.
fn parse_timeout(value: &str) -> Option<Duration> {
    let (&unit, digits) = value.as_bytes().split_last()?;
    if digits.is_empty() || digits.len() > MAX_DIGITS || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let amount = digits.iter().try_fold(0_u32, |amount, digit| {
        amount.checked_mul(10)?.checked_add(u32::from(digit - b'0'))
    })?;
    let quantum = match unit {
        b'H' => Duration::from_hours(1),
        b'M' => Duration::from_mins(1),
        b'S' => Duration::from_secs(1),
        b'm' => Duration::from_millis(1),
        b'u' => Duration::from_micros(1),
        b'n' => Duration::from_nanos(1),
        _ => return None,
    };
    quantum.checked_mul(amount)
}
