//! The reserved Kafka headers a record uses to ask for a peer response.
//!
//! Five plain UTF-8 headers carry the request metadata, so a stuck request is
//! readable in any broker UI without a decoder: `response-version`,
//! `response-request-id`, `response-peer`, `response-deadline`, and one
//! `response-awaited` per subsystem the record awaits. The four singletons
//! must occur **exactly** once. A producer may repeat a Kafka header key, so
//! accepting the first or the last would be an unstated precedence — and
//! `response-awaited` repeats rather than comma-separating because a comma is a
//! legal character in a [`SubsystemName`].
//!
//! A record whose reserved headers are unusable yields no request and one
//! counted rejection, on a consumer configured to answer for a subsystem. A
//! consumer that answers for none never reads the headers, so it counts
//! nothing. It is never a failed event: asking for a response badly is not a
//! reason to stop processing the message. The count is per decode, not per
//! record — a poll, a deferred reload and a state read each decode the same
//! record — so read the counter as a rate, never as a population.
//!
//! [`ProsodyRequester`](crate::peer::requester::ProsodyRequester) writes these
//! headers through the same names this module reserves, so the writer and this
//! parser cannot drift apart.

use crate::peer::response::RequestId;
use crate::peer::response::frame::FrameHeader;
use crate::peer::router::PeerId;
use crate::subsystem::SubsystemName;
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use std::str;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::time::Instant;
use uuid::Uuid;
use uuid::fmt::Hyphenated;

#[cfg(test)]
mod tests;

// The five header names this protocol reserves. Two sites must agree on the
// set: the `match key` arms of `parse_result_request`, and
// RESERVED_REQUEST_HEADERS below, which `is_reserved` reads. A name the parser
// matches but the array omits is one a caller can inject, so add every new name
// to both.
pub(crate) const RESPONSE_VERSION_HEADER: &str = "response-version";
pub(crate) const RESPONSE_REQUEST_ID_HEADER: &str = "response-request-id";
pub(crate) const RESPONSE_PEER_HEADER: &str = "response-peer";
pub(crate) const RESPONSE_DEADLINE_HEADER: &str = "response-deadline";
pub(crate) const RESPONSE_AWAITED_HEADER: &str = "response-awaited";

/// Header names that
/// [`ProsodyRequester`](crate::peer::requester::ProsodyRequester) refuses in
/// caller-supplied headers.
pub(crate) const RESERVED_REQUEST_HEADERS: [&str; 5] = [
    RESPONSE_VERSION_HEADER,
    RESPONSE_REQUEST_ID_HEADER,
    RESPONSE_PEER_HEADER,
    RESPONSE_DEADLINE_HEADER,
    RESPONSE_AWAITED_HEADER,
];

/// The one request-metadata revision this responder understands, in the exact
/// text a producer must write.
///
/// This revision freezes what the Kafka headers mean. Kafka headers have no
/// schema evolution rules that can protect a responder from incompatible
/// semantics. One revision has one text form, as one id has one text form:
/// `01` and `+1` are refused. The requester writes this value, so the writer
/// and reader cannot differ.
pub(crate) const REQUEST_REVISION: &str = "2";

/// The only accepted length of an id header value: the hyphenated UUID that
/// [`id_text`] writes. Fixing the length rejects the simple, braced and URN
/// forms, so one id has one text form.
pub(crate) const ID_TEXT_LEN: usize = Hyphenated::LENGTH;

/// Decodes refused by their reserved headers, by fixed reason label.
static REJECTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.request_headers_rejected")
        .with_description("Message decodes whose reserved response headers were unusable")
        .with_unit("{decode}")
        .build()
});

/// A Kafka message's request for one handler result.
///
/// Two fixed-size ids and no strings: everything else the headers carry is
/// consumed while the Kafka record is still borrowed. [`peer`](Self::peer) is a
/// directory lookup key rather than an address, so a topic writer can name a
/// live prosody peer and nothing else.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResultRequest {
    id: RequestId,
    peer: PeerId,
    deadline: RequestDeadline,
}

/// One request deadline in wire time and local runtime time.
///
/// A requester creates both values together. The registry and Kafka header
/// therefore use one deadline calculation.
#[derive(Clone, Copy, Debug)]
pub struct RequestDeadline {
    unix_micros: u64,
    expires_at: Instant,
}

impl ResultRequest {
    /// Pairs a request with the peer that awaits its result.
    pub(crate) const fn new(id: RequestId, peer: PeerId, deadline: RequestDeadline) -> Self {
        Self { id, peer, deadline }
    }

    /// Converts this request into the header for its handler result.
    ///
    /// The sender resolves the peer through the directory. A Kafka header can
    /// never supply an address. A responder never sets the relay.
    pub(crate) fn delivery_header(self, subsystem: SubsystemName) -> FrameHeader {
        FrameHeader {
            target: self.peer,
            request: self.id,
            subsystem,
            relay: None,
        }
    }

    /// Returns the deadline that a remote response must put on gRPC.
    pub(crate) const fn deadline(self) -> RequestDeadline {
        self.deadline
    }
}

impl RequestDeadline {
    /// Creates a deadline from canonical Unix microseconds.
    pub(crate) fn from_unix_micros(unix_micros: u64) -> Self {
        let now = Instant::now();
        let wall_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_micros());
        let remaining = u128::from(unix_micros).saturating_sub(wall_micros);
        let remaining = u64::try_from(remaining).unwrap_or(u64::MAX);
        Self {
            unix_micros,
            expires_at: now
                .checked_add(Duration::from_micros(remaining))
                .unwrap_or(now),
        }
    }

    /// Creates a wire deadline from a requester timeout.
    pub(crate) fn after(timeout: Duration) -> Option<Self> {
        let now = Instant::now();
        let Ok(wall) = SystemTime::now().duration_since(UNIX_EPOCH) else {
            return None;
        };
        let micros = wall.as_micros().checked_add(timeout.as_micros())?;
        let Ok(unix_micros) = u64::try_from(micros) else {
            return None;
        };
        Some(Self {
            unix_micros,
            expires_at: now.checked_add(timeout)?,
        })
    }

    /// Writes the canonical decimal header value without allocation.
    pub(crate) fn text(self, buffer: &mut itoa::Buffer) -> &str {
        buffer.format(self.unix_micros)
    }

    pub(crate) const fn expires_at(self) -> Instant {
        self.expires_at
    }
}

impl PartialEq for RequestDeadline {
    fn eq(&self, other: &Self) -> bool {
        self.unix_micros == other.unix_micros
    }
}

impl Eq for RequestDeadline {}

/// Reads one record's reserved headers, for the subsystem this consumer answers
/// for.
///
/// * `Ok(None)` — the record reserves no header, or awaits only other
///   subsystems. Ordinary traffic; nothing is counted.
/// * `Ok(Some(request))` — every header is present, singular and in bounds, the
///   revision is supported, and `responder` is among the awaited subsystems.
/// * `Err(rejection)` — a reserved header is present but unusable. The caller
///   counts it and drops the request; the event still processes normally.
///
/// Validation precedes the awaited match, so a malformed request is counted by
/// every responder that reads it rather than only by the one it meant to reach.
///
/// **Which of the three answers a header set gets does not depend on the order
/// the producer wrote it in.** Only the rejection *reason* does, and only for a
/// record carrying more than one defect: the scan reports the first it meets.
/// Reporting all of them would mean scanning past a defect the parse has
/// already refused to trust.
///
/// The parse allocates nothing — a handful of stack scalars and two 16-byte ids
/// out — and is linear in the record's header count, which the broker's own
/// record-size limit bounds.
pub(crate) fn parse_result_request<'h, H>(
    headers: H,
    responder: &SubsystemName,
) -> Result<Option<ResultRequest>, HeaderRejection>
where
    H: IntoIterator<Item = (&'h str, Option<&'h [u8]>)>,
{
    let mut version = None;
    let mut id = None;
    let mut peer = None;
    let mut deadline = None;
    let mut awaited = 0_usize;
    let mut addressed = false;

    for (key, value) in headers {
        match key {
            RESPONSE_AWAITED_HEADER => {
                awaited += 1;
                // Every name is validated even after a match is found: stopping
                // early would make a later name's validity depend on where the
                // producer put the match.
                let name = awaited_name(value)?;
                addressed |= name == responder.as_str();
            }
            RESPONSE_VERSION_HEADER => {
                set_once(&mut version, || check_revision(value))?;
            }
            RESPONSE_REQUEST_ID_HEADER => {
                set_once(&mut id, || parse_id(value).map(RequestId::from_bytes))?;
            }
            RESPONSE_PEER_HEADER => {
                set_once(&mut peer, || parse_id(value).map(PeerId::from_bytes))?;
            }
            RESPONSE_DEADLINE_HEADER => {
                set_once(&mut deadline, || parse_deadline(value))?;
            }
            // Every other header belongs to the producer, not to this protocol.
            _ => {}
        }
    }

    // No reserved header at all, so this record asks for nothing.
    if version.is_none() && id.is_none() && peer.is_none() && deadline.is_none() && awaited == 0 {
        return Ok(None);
    }
    let (Some(id), Some(peer), Some(deadline)) = (id, peer, deadline) else {
        return Err(HeaderRejection::MissingSingleton);
    };
    if version.is_none() || awaited == 0 {
        return Err(HeaderRejection::MissingSingleton);
    }

    Ok(addressed.then_some(ResultRequest::new(id, peer, deadline)))
}

fn set_once<T>(
    slot: &mut Option<T>,
    value: impl FnOnce() -> Result<T, HeaderRejection>,
) -> Result<(), HeaderRejection> {
    if slot.is_some() {
        return Err(HeaderRejection::DuplicateSingleton);
    }
    *slot = Some(value()?);
    Ok(())
}

/// Renders one id in its 36-character header form without an allocation.
pub(crate) fn id_text(id: Uuid, buf: &mut [u8; ID_TEXT_LEN]) -> &str {
    id.hyphenated().encode_lower(buf)
}

/// Reports whether `name` belongs to the request protocol.
pub(crate) fn is_reserved(name: &str) -> bool {
    RESERVED_REQUEST_HEADERS.contains(&name)
}

/// Reads one awaited subsystem name.
///
/// [`SubsystemName::checked`] applies the rule, so a padded name addresses the
/// same subsystem and a name no responder could ever hold is refused rather
/// than compared. Nothing is copied: the name is compared where it lies in the
/// record.
fn awaited_name(value: Option<&[u8]>) -> Result<&str, HeaderRejection> {
    let bytes = value.ok_or(HeaderRejection::MalformedAwaited)?;
    let name = str::from_utf8(bytes).map_err(|_| HeaderRejection::MalformedAwaited)?;

    SubsystemName::checked(name).map_err(|_| HeaderRejection::MalformedAwaited)
}

/// Reads one id header into its 16 bytes.
fn parse_id(value: Option<&[u8]>) -> Result<[u8; 16], HeaderRejection> {
    let text = value.ok_or(HeaderRejection::MalformedId)?;
    if text.len() != ID_TEXT_LEN {
        return Err(HeaderRejection::MalformedId);
    }

    Uuid::try_parse_ascii(text)
        .map(Uuid::into_bytes)
        .map_err(|_| HeaderRejection::MalformedId)
}

/// Reads one canonical decimal Unix deadline in microseconds.
fn parse_deadline(value: Option<&[u8]>) -> Result<RequestDeadline, HeaderRejection> {
    let text = value.ok_or(HeaderRejection::MalformedDeadline)?;
    if text.is_empty()
        || text.len() > 20
        || (text.len() > 1 && text[0] == b'0')
        || !text.iter().all(u8::is_ascii_digit)
    {
        return Err(HeaderRejection::MalformedDeadline);
    }
    str::from_utf8(text)
        .map_err(|_| HeaderRejection::MalformedDeadline)?
        .parse()
        .map(RequestDeadline::from_unix_micros)
        .map_err(|_| HeaderRejection::MalformedDeadline)
}

/// Accepts the one revision this responder reads the other headers under, and
/// discards it: keeping it would only let a later reader re-decide a question
/// settled here.
fn check_revision(value: Option<&[u8]>) -> Result<(), HeaderRejection> {
    if value == Some(REQUEST_REVISION.as_bytes()) {
        return Ok(());
    }
    Err(HeaderRejection::UnsupportedVersion)
}

/// Why a record's reserved headers yield no result request.
///
/// Never propagated as an event failure — the caller counts it and processes
/// the message — so it carries no error classification.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[cfg_attr(test, derive(strum::VariantArray))]
pub(crate) enum HeaderRejection {
    #[error("a singleton response header occurred more than once")]
    DuplicateSingleton,
    #[error("a required response header is missing")]
    MissingSingleton,
    #[error("the request metadata revision is not supported")]
    UnsupportedVersion,
    #[error("a response id is not a 36-character UUID")]
    MalformedId,
    #[error("a response deadline is not canonical Unix microseconds")]
    MalformedDeadline,
    #[error("an awaited subsystem name is empty, not UTF-8, or too long")]
    MalformedAwaited,
}

impl HeaderRejection {
    /// Counts one refused decode under this rejection's fixed label.
    pub(crate) fn record(self) {
        REJECTED.add(1, &[KeyValue::new("reason", self.label())]);
    }

    /// The metric label for this rejection.
    ///
    /// A fixed string per variant: nothing a topic writer supplies ever
    /// becomes a label.
    const fn label(self) -> &'static str {
        match self {
            Self::DuplicateSingleton => "duplicate",
            Self::MissingSingleton => "missing",
            Self::UnsupportedVersion => "unsupported_version",
            Self::MalformedId => "malformed_id",
            Self::MalformedDeadline => "malformed_deadline",
            Self::MalformedAwaited => "malformed_awaited",
        }
    }
}
