//! What is left of the budget a caller granted one peer call.

use std::time::Duration;
use tokio::time::Instant;
use tonic::metadata::MetadataMap;

/// The header a gRPC caller states its own budget in.
const TIMEOUT_HEADER: &str = "grpc-timeout";

/// Most digits the gRPC protocol allows in a timeout value.
const MAX_DIGITS: usize = 8;

/// The absolute instant a caller's `grpc-timeout` runs out, held to `cap`.
///
/// `cap` is this process's own ceiling, and it applies twice: a caller that set
/// no timeout, or one this build cannot read, gets `cap`, and a caller that
/// asked for more than `cap` gets `cap` too. That is what bounds the work done
/// on a caller's behalf whatever the caller stated. The clamp is applied to the
/// duration before it is added to the present, so a caller that asks for years
/// cannot overflow the instant.
///
/// This crate parses the header itself because tonic parses it privately and
/// never puts the parsed value anywhere a service can read it.
pub(super) fn inbound_deadline(metadata: &MetadataMap, cap: Duration) -> Instant {
    let stated = metadata
        .get(TIMEOUT_HEADER)
        // A header this build cannot read states nothing this build can spend
        // against, so it reads as absent rather than as a failure.
        .and_then(|value| match value.to_str() {
            Ok(value) => parse_timeout(value, cap),
            Err(_) => None,
        });
    Instant::now() + stated.unwrap_or(cap)
}

/// One `grpc-timeout` value: up to eight digits, then one unit character.
///
/// A value outside that shape reads as absent, and a value over `cap` reads as
/// `cap`. Arithmetic that saturates is over `cap` by definition.
fn parse_timeout(value: &str, cap: Duration) -> Option<Duration> {
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
    Some(quantum.saturating_mul(amount).min(cap))
}
