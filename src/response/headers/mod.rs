//! The reserved Kafka headers a record uses to ask for a peer response.
//!
//! Four plain UTF-8 headers carry the request metadata, so a stuck request is
//! readable in any broker UI without a decoder: `response-version`,
//! `response-request-id`, `response-node`, and one `response-awaited` per
//! subsystem the record awaits. The three singletons must occur **exactly**
//! once — a producer may repeat a Kafka header key, so accepting the first or
//! the last would be an unstated precedence — and `response-awaited` repeats
//! rather than comma-separating because a comma is a legal character in a
//! [`SubsystemName`].
//!
//! A record whose reserved headers are unusable yields no tag and one counted
//! rejection, on a consumer configured to answer for a subsystem. A consumer
//! that answers for none never reads the headers, so it counts nothing. It is
//! never a failed event: asking for a response badly is not a reason to stop
//! processing the message. The count is per decode, not per
//! record — a poll, a deferred reload and a state read each decode the same
//! record — so read the counter as a rate, never as a population.
//!
//! [`ProsodyRequester`](crate::requester::ProsodyRequester) writes these
//! headers through the same names this module reserves, so the writer and this
//! parser cannot drift apart.

use crate::response::frame::FrameHeader;
use crate::response::{RequestId, ResponseStatus};
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use std::str;
use std::sync::LazyLock;
use thiserror::Error;
use uuid::Uuid;
use uuid::fmt::Hyphenated;

#[cfg(test)]
mod tests;

// The four header names this protocol reserves. Two sites must agree on the
// set: the `match key` arms of `parse_request_tag`, and
// RESERVED_REQUEST_HEADERS below, which `is_reserved` reads. A name the parser
// matches but the array omits is one a caller can inject, so add every new name
// to both.
pub(crate) const RESPONSE_VERSION_HEADER: &str = "response-version";
pub(crate) const RESPONSE_REQUEST_ID_HEADER: &str = "response-request-id";
pub(crate) const RESPONSE_NODE_HEADER: &str = "response-node";
pub(crate) const RESPONSE_AWAITED_HEADER: &str = "response-awaited";

/// Header names that [`ProsodyRequester`](crate::requester::ProsodyRequester)
/// refuses in caller-supplied headers.
pub(crate) const RESERVED_REQUEST_HEADERS: [&str; 4] = [
    RESPONSE_VERSION_HEADER,
    RESPONSE_REQUEST_ID_HEADER,
    RESPONSE_NODE_HEADER,
    RESPONSE_AWAITED_HEADER,
];

/// The one request-metadata revision this responder understands, in the exact
/// text a producer must write.
///
/// Distinct from
/// [`RESPONSE_PROTOCOL_VERSION`](super::RESPONSE_PROTOCOL_VERSION),
/// which versions the response frame between two peers. This one freezes what
/// the headers *mean*, so a later revision that redefines a header cannot be
/// read under the old rules and answered confidently. One revision has one text
/// form, as one id has one text form: `01` and `+1` are refused. The requester
/// writes this value, so the writer and reader cannot differ.
pub(crate) const REQUEST_REVISION: &str = "1";

/// The only accepted length of an id header value: the hyphenated UUID that
/// [`id_text`] writes. Fixing the length rejects the simple, braced and URN
/// forms, so one id has one text form.
pub(crate) const ID_TEXT_LEN: usize = Hyphenated::LENGTH;

/// Most `response-awaited` headers one record may carry.
///
/// The ceiling is the wire's, not a requester's: a record naming more
/// subsystems than this is refused rather than scanned, so the parse stays
/// bounded whatever a topic writer composes.
pub(crate) const MAX_AWAITED: usize = 32;

/// Decodes refused by their reserved headers, by fixed reason label.
static REJECTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.request_headers_rejected")
        .with_description("Message decodes whose reserved response headers were unusable")
        .with_unit("{decode}")
        .build()
});

/// Where one request's response must go.
///
/// Two fixed-size ids and no strings: everything else the headers carry is
/// consumed while the Kafka record is still borrowed. [`node`](Self::node) is a
/// directory lookup key rather than an address, so a topic writer can name a
/// live prosody node and nothing else.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestTag {
    id: RequestId,
    node: NodeId,
}

impl RequestTag {
    /// Pairs a request with the node awaiting its response.
    pub(crate) const fn new(id: RequestId, node: NodeId) -> Self {
        Self { id, node }
    }

    /// The frame header a response to this request must carry.
    ///
    /// The sender resolves the node through the directory. A Kafka header can
    /// never supply an address. A responder never sets the relay.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the respond layer is this method's production caller, and the respond \
                      suites exercise it"
        )
    )]
    pub(crate) fn header(self, subsystem: SubsystemName, status: ResponseStatus) -> FrameHeader {
        FrameHeader {
            target: self.node,
            request: self.id,
            subsystem,
            status,
            relay: None,
        }
    }
}

/// Reads one record's reserved headers, for the subsystem this consumer answers
/// for.
///
/// * `Ok(None)` — the record reserves no header, or awaits only other
///   subsystems. Ordinary traffic; nothing is counted.
/// * `Ok(Some(tag))` — every header is present, singular and in bounds, the
///   revision is supported, and `responder` is among the awaited subsystems.
/// * `Err(rejection)` — a reserved header is present but unusable. The caller
///   counts it and drops the tag; the event still processes normally.
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
/// record-size limit bounds. Only the awaited names carry a protocol cap, since
/// they alone are a list a producer chooses the length of.
pub(crate) fn parse_request_tag<'h, H>(
    headers: H,
    responder: &SubsystemName,
) -> Result<Option<RequestTag>, HeaderRejection>
where
    H: IntoIterator<Item = (&'h str, Option<&'h [u8]>)>,
{
    let mut version_seen = false;
    let mut id = None;
    let mut node = None;
    let mut awaited = 0_usize;
    let mut addressed = false;

    for (key, value) in headers {
        match key {
            RESPONSE_AWAITED_HEADER => {
                awaited += 1;
                if awaited > MAX_AWAITED {
                    return Err(HeaderRejection::TooManyAwaited);
                }
                // Every name is validated even after a match is found: stopping
                // early would make a later name's validity depend on where the
                // producer put the match.
                let name = awaited_name(value)?;
                addressed |= name == responder.as_str();
            }
            RESPONSE_VERSION_HEADER => {
                if version_seen {
                    return Err(HeaderRejection::DuplicateSingleton);
                }
                version_seen = true;
                check_revision(value)?;
            }
            RESPONSE_REQUEST_ID_HEADER => {
                if id.is_some() {
                    return Err(HeaderRejection::DuplicateSingleton);
                }
                id = Some(RequestId::from_bytes(parse_id(value)?));
            }
            RESPONSE_NODE_HEADER => {
                if node.is_some() {
                    return Err(HeaderRejection::DuplicateSingleton);
                }
                node = Some(NodeId::from_bytes(parse_id(value)?));
            }
            // Every other header belongs to the producer, not to this protocol.
            _ => {}
        }
    }

    // No reserved header at all, so this record asks for nothing.
    if !version_seen && id.is_none() && node.is_none() && awaited == 0 {
        return Ok(None);
    }
    let (Some(id), Some(node)) = (id, node) else {
        return Err(HeaderRejection::MissingSingleton);
    };
    if !version_seen || awaited == 0 {
        return Err(HeaderRejection::MissingSingleton);
    }

    Ok(addressed.then_some(RequestTag::new(id, node)))
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

/// Accepts the one revision this responder reads the other headers under, and
/// discards it: keeping it would only let a later reader re-decide a question
/// settled here.
fn check_revision(value: Option<&[u8]>) -> Result<(), HeaderRejection> {
    if value == Some(REQUEST_REVISION.as_bytes()) {
        return Ok(());
    }
    Err(HeaderRejection::UnsupportedVersion)
}

/// Why a record's reserved headers yield no tag.
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
    #[error("an awaited subsystem name is empty, not UTF-8, or too long")]
    MalformedAwaited,
    #[error("more awaited subsystems than a request may name")]
    TooManyAwaited,
}

impl HeaderRejection {
    /// Counts one refused decode under this rejection's fixed label.
    pub(crate) fn record(self) {
        REJECTED.add(1, &[KeyValue::new("reason", self.reason())]);
    }

    /// The metric label for this rejection.
    ///
    /// A fixed string per variant: nothing a topic writer supplies ever
    /// becomes a label.
    const fn reason(self) -> &'static str {
        match self {
            Self::DuplicateSingleton => "duplicate",
            Self::MissingSingleton => "missing",
            Self::UnsupportedVersion => "unsupported_version",
            Self::MalformedId => "malformed_id",
            Self::MalformedAwaited => "malformed_awaited",
            Self::TooManyAwaited => "over_bound",
        }
    }
}
