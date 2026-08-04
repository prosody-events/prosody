use super::{RequestId, ResponseDisposition};
use crate::test_util::assert_distinct_labels;
use color_eyre::Result;
use strum::VariantArray;
use tonic::Code;
use uuid::{Uuid, Version};

/// The status each disposition is reported as, one row per documented pair —
/// including the one row that may claim `OK`, since success is not something a
/// rejection can spell.
#[test]
fn each_disposition_reports_its_documented_status() {
    let expected = [
        (ResponseDisposition::Accepted, Code::Ok),
        (ResponseDisposition::UnknownRequest, Code::NotFound),
        (ResponseDisposition::ClosedRequest, Code::NotFound),
        (ResponseDisposition::DuplicateSubsystem, Code::AlreadyExists),
        (
            ResponseDisposition::UnexpectedSubsystem,
            Code::FailedPrecondition,
        ),
        (
            ResponseDisposition::FormatMismatch,
            Code::FailedPrecondition,
        ),
        (
            ResponseDisposition::ResponseTooLarge,
            Code::ResourceExhausted,
        ),
        (ResponseDisposition::MalformedTarget, Code::InvalidArgument),
        (
            ResponseDisposition::AlreadyRelayed,
            Code::FailedPrecondition,
        ),
        (
            ResponseDisposition::NoRelayCapacity,
            Code::ResourceExhausted,
        ),
        (
            ResponseDisposition::RelayDeadlineExceeded,
            Code::DeadlineExceeded,
        ),
        (ResponseDisposition::Unreachable, Code::Unavailable),
    ];
    assert_eq!(
        expected.len(),
        ResponseDisposition::VARIANTS.len(),
        "a new disposition must be given a status here"
    );
    for (disposition, status) in expected {
        assert_eq!(
            disposition.status(),
            status,
            "{disposition:?} reports the wrong status"
        );
    }
}

/// Every disposition counts under its own label, so one answer can never be
/// read as another in a dashboard.
///
/// That an arriving frame reaches no label at all is the receive leg's own
/// claim, pinned by `every_answer_counts_once_under_a_fixed_label` in
/// `src/router/grpc/tests/metrics.rs`.
#[test]
fn each_disposition_has_a_distinct_label() -> Result<()> {
    assert_distinct_labels(
        ResponseDisposition::VARIANTS
            .iter()
            .map(|disposition| disposition.label()),
    )
}

/// Request ids are `UUIDv7`, so the id a trace carries places the request in
/// time and two ids minted in order sort in that order. Two mints already
/// differ.
#[test]
fn every_minted_request_id_is_a_uuid_v7() {
    let first = RequestId::new();
    let second = RequestId::new();
    assert_ne!(first, second, "two mints must not collide");
    for id in [first, second] {
        assert_eq!(
            Uuid::from(id).get_version(),
            Some(Version::SortRand),
            "{id} must be a UUIDv7"
        );
    }
    assert!(
        first.into_bytes() < second.into_bytes(),
        "{first} was minted before {second} and must sort before it"
    );
}
