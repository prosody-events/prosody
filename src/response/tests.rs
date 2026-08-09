use super::{RequestId, ResponseDisposition, ResponseStatus, SUCCESS};
use crate::error::ErrorCategory;
use crate::test_util::assert_distinct_labels;
use color_eyre::Result;
use color_eyre::eyre::ensure;
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
        (
            ResponseDisposition::AlreadyRelayed,
            Code::FailedPrecondition,
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

/// Every disposition counts under its own label and answers the sender in its
/// own words. So one answer can never be read as another, in a dashboard or on
/// the wire.
///
/// That an arriving frame reaches no label at all is the receive leg's own
/// claim, pinned by `every_answer_counts_once_under_a_fixed_label` in
/// `src/router/grpc/tests/metrics.rs`.
#[test]
fn each_disposition_has_a_distinct_label_and_message() -> Result<()> {
    assert_distinct_labels(
        ResponseDisposition::VARIANTS
            .iter()
            .map(|disposition| disposition.label()),
    )?;
    let mut seen: Vec<&str> = Vec::new();
    for disposition in ResponseDisposition::VARIANTS {
        let message = disposition.message();
        ensure!(!message.is_empty(), "{disposition:?} answers with nothing");
        ensure!(
            !seen.contains(&message),
            "{message:?} answers more than one disposition"
        );
        seen.push(message);
    }
    Ok(())
}

/// A response status round trips through its wire discriminant, and no error
/// category claims the success discriminant.
///
/// The two conversions read one `SUCCESS` const, so this proves the whole
/// mapping. A fourth [`ErrorCategory`] given `4` would read a handler failure
/// back as a success, and the second half of this test refuses it.
#[test]
fn every_response_status_round_trips_and_none_collides_with_success() -> Result<()> {
    let statuses = [ResponseStatus::Success].into_iter().chain(
        ErrorCategory::VARIANTS
            .iter()
            .copied()
            .map(ResponseStatus::Error),
    );
    for status in statuses {
        let wire = i32::from(status);
        ensure!(
            ResponseStatus::try_from(wire)? == status,
            "{status:?} did not survive its wire form {wire}"
        );
    }
    for category in ErrorCategory::VARIANTS {
        ensure!(
            i32::from(*category) != SUCCESS,
            "{category:?} claims the success discriminant"
        );
    }
    Ok(())
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
