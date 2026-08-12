//! OpenTelemetry fields for the peer gRPC method.

use opentelemetry_semantic_conventions::attribute::{ERROR_TYPE, RPC_RESPONSE_STATUS_CODE};
use tonic::Code;
use tracing::Span;

/// The logical gRPC method name.
pub(super) const METHOD: &str = "prosody.peer.v1.PeerService/DeliverResult";

/// Records the standard gRPC result fields.
pub(super) fn record_status(span: &Span, code: Code) {
    let name = status_name(code);
    span.record(RPC_RESPONSE_STATUS_CODE, name);
    if code != Code::Ok {
        span.record(ERROR_TYPE, name);
    }
}

/// Returns the standard gRPC status name.
const fn status_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}
