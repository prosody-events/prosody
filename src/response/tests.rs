use super::ResponseDisposition;
use strum::VariantArray;
use tonic::Code;

/// Success is not something a rejection can spell. Totality is the exhaustive
/// `match` in [`ResponseDisposition::status`]; this pins which variant may
/// claim `OK`.
#[test]
fn only_an_accepted_response_maps_to_ok() {
    for disposition in ResponseDisposition::VARIANTS {
        assert_eq!(
            disposition.status() == Code::Ok,
            matches!(disposition, ResponseDisposition::Accepted),
            "{disposition:?} maps to {:?}",
            disposition.status()
        );
    }
}

/// The status each disposition is reported as, one row per documented pair.
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
