//! What a node answers for one delivery, and what that answer proves.

use super::{ALPHA, BETA, Harness, OVER_RESPONSE_BYTES, frame, header, payload, register};
use crate::codec::Codec;
use crate::requester::registry::PendingRegistry;
use crate::response::frame::tests::CountingCodec;
use crate::response::{RequestId, ResponseDisposition};
use crate::router::NodeId;
use crate::router::grpc::TRANSPORT;
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::sync::Arc;
use strum::VariantArray;
use tonic::Code;

/// A format token no codec in these suites speaks.
const OTHER_FORMAT: &str = "not-the-test-format";

/// A short payload, for the cases whose size is not the subject.
const SHORT: usize = 8;

/// Every case the wire can reach. The property draws from this list, so a case
/// deleted from it is a case the property stops covering — which
/// `every_disposition_has_a_reachable_wire_case` then reports.
const SCENARIOS: &[Scenario] = &[
    Scenario::Accepted,
    Scenario::UnknownRequest,
    Scenario::ClosedRequest,
    Scenario::DuplicateSubsystem,
    Scenario::UnexpectedSubsystem,
    Scenario::FormatMismatch,
    Scenario::ResponseTooLarge,
];

/// Dispositions no delivery can reach, and why.
///
/// A target that is not 16 bytes is a frame the reader refuses, so it never
/// reaches the service at all and no decoded frame can carry it.
const UNREACHABLE: &[ResponseDisposition] = &[ResponseDisposition::MalformedTarget];

/// Dispositions the relay suites cover, because each one needs a forward this
/// listener's relay never completes.
///
/// `a_frame_this_process_already_relayed_is_never_relayed_again` reaches
/// `AlreadyRelayed`, `a_flood_of_forwards_cannot_take_a_busy_cell` reaches
/// `NoRelayCapacity`, and
/// `a_forward_with_no_time_left_answers_deadline_exceeded`
/// reaches `RelayDeadlineExceeded`.
const RELAYED: &[ResponseDisposition] = &[
    ResponseDisposition::AlreadyRelayed,
    ResponseDisposition::NoRelayCapacity,
    ResponseDisposition::RelayDeadlineExceeded,
];

/// One registry outcome, together with the seeding and the deliveries that
/// reach it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::VariantArray)]
enum Scenario {
    Accepted,
    UnknownRequest,
    ClosedRequest,
    DuplicateSubsystem,
    UnexpectedSubsystem,
    FormatMismatch,
    ResponseTooLarge,
}

impl Arbitrary for Scenario {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(SCENARIOS).unwrap_or(&Self::Accepted)
    }
}

impl Scenario {
    /// Seeds one registry and returns the request the deliveries name.
    fn seed(self, registry: &Arc<PendingRegistry>) -> Result<RequestId> {
        match self {
            // Nothing is registered, so the id names no request anywhere.
            Self::UnknownRequest => Ok(RequestId::new()),
            Self::FormatMismatch => register(registry, &[ALPHA], OTHER_FORMAT),
            // Two positions, so a repeat delivery finds the request still open
            // and reports the duplicate rather than the closure.
            Self::DuplicateSubsystem => {
                register(registry, &[ALPHA, BETA], CountingCodec::FORMAT_ID)
            }
            _ => register(registry, &[ALPHA], CountingCodec::FORMAT_ID),
        }
    }

    /// The deliveries this case makes, in order. The last one is the case.
    fn deliveries(self) -> &'static [(&'static str, usize)] {
        match self {
            Self::ClosedRequest | Self::DuplicateSubsystem => &[(ALPHA, SHORT), (ALPHA, SHORT)],
            Self::UnexpectedSubsystem => &[(BETA, SHORT)],
            Self::ResponseTooLarge => &[(ALPHA, OVER_RESPONSE_BYTES)],
            _ => &[(ALPHA, SHORT)],
        }
    }

    /// What the last delivery must come to.
    const fn expected(self) -> ResponseDisposition {
        match self {
            Self::Accepted => ResponseDisposition::Accepted,
            Self::UnknownRequest => ResponseDisposition::UnknownRequest,
            Self::ClosedRequest => ResponseDisposition::ClosedRequest,
            Self::DuplicateSubsystem => ResponseDisposition::DuplicateSubsystem,
            Self::UnexpectedSubsystem => ResponseDisposition::UnexpectedSubsystem,
            Self::FormatMismatch => ResponseDisposition::FormatMismatch,
            Self::ResponseTooLarge => ResponseDisposition::ResponseTooLarge,
        }
    }
}

/// Parity: the status a real listener returns for one frame is the status the
/// registry's own disposition names for the same frame.
///
/// The scope is the transport and the mapping, not the registry: both sides
/// call the same registry code, so what this proves is that nothing between the
/// socket and the disposition changes the answer. The registry's own
/// correctness is the requester suites'.
///
/// It also proves `OK` means stored: every accepting answer is followed by the
/// same delivery again, which must no longer be accepted.
#[quickcheck]
fn the_wire_status_is_the_registry_disposition(scenario: Scenario) -> TestResult {
    init_test_logging();
    match TEST_RUNTIME.block_on(play(scenario)) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{scenario:?}: {error:#}")),
    }
}

/// Drives `scenario` over the wire and against the oracle, and compares.
async fn play(scenario: Scenario) -> Result<()> {
    let harness = Harness::shared().await?;
    let wire_request = scenario.seed(&harness.registry)?;
    let oracle_request = scenario.seed(&harness.oracle)?;
    let mut wire = Code::Ok;
    let mut oracle = Code::Ok;
    for (subsystem, bytes) in scenario.deliveries() {
        wire = harness
            .deliver(
                &header(harness.node, wire_request, subsystem)?,
                payload(*bytes),
            )
            .await?;
        oracle = harness
            .oracle
            .accept(frame(
                header(harness.node, oracle_request, subsystem)?,
                &payload(*bytes),
            ))
            .status();
    }
    ensure!(
        wire == oracle,
        "the wire answered {wire:?} where the registry's disposition is {oracle:?}"
    );
    ensure!(
        wire == scenario.expected().status(),
        "{scenario:?} must answer {:?}, not {wire:?}",
        scenario.expected().status()
    );
    if wire == Code::Ok {
        let repeat = harness
            .deliver(&header(harness.node, wire_request, ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            repeat != Code::Ok,
            "an accepted response must fill its position, so the same delivery again cannot be \
             accepted"
        );
    }
    Ok(())
}

/// An accepted response stores the bytes it carried, not merely a mark that a
/// position was filled.
#[test]
fn an_accepted_response_stores_the_payload_it_carried() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let request = register(&harness.registry, &[ALPHA], CountingCodec::FORMAT_ID)?;
        let sent = payload(SHORT);
        let answered = harness
            .deliver(&header(harness.node, request, ALPHA)?, sent.clone())
            .await?;
        ensure!(answered == Code::Ok, "a well-formed response is accepted");
        let stored = harness
            .registry
            .stored_payload(request, &SubsystemName::try_new(ALPHA)?);
        ensure!(
            stored.as_deref() == Some(sent.as_slice()),
            "the position must hold the bytes the frame carried, not {stored:?}"
        );
        Ok(())
    })
}

/// A frame for another node forwards and finds no published target.
///
/// The failed forward never reaches the registry. The request remains fillable.
#[test]
fn a_frame_for_another_node_is_never_accepted() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let request = register(&harness.registry, &[ALPHA], CountingCodec::FORMAT_ID)?;
        let misrouted = TRANSPORT.misrouted();
        let elsewhere = harness
            .deliver(&header(NodeId::new(), request, ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            elsewhere == ResponseDisposition::Unreachable.status(),
            "a frame for another node must answer UNAVAILABLE, not {elsewhere:?}"
        );
        ensure!(
            TRANSPORT.misrouted() == misrouted + 1,
            "a frame for another node must be counted as misrouted"
        );
        let here = harness
            .deliver(&header(harness.node, request, ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            here == Code::Ok,
            "the misrouted frame must have left the request untouched, but it answered {here:?}"
        );
        Ok(())
    })
}

/// Every disposition is reached by a case above, or named as one the relay
/// suites reach, or named as one nothing can reach. Without this, a case
/// dropped from the generator would stop being covered silently.
#[test]
fn every_disposition_has_a_reachable_wire_case() -> Result<()> {
    // The generator draws from `SCENARIOS`, and both `seed` and `deliveries`
    // have a catch-all arm, so a case left out of that list would be
    // unreachable and invisible.
    ensure!(
        SCENARIOS.len() == Scenario::VARIANTS.len(),
        "every scenario must be listed in SCENARIOS, but {} of {} are",
        SCENARIOS.len(),
        Scenario::VARIANTS.len()
    );
    for disposition in ResponseDisposition::VARIANTS {
        let covered = SCENARIOS
            .iter()
            .any(|scenario| scenario.expected() == *disposition)
            || *disposition == ResponseDisposition::Unreachable
            || RELAYED.contains(disposition)
            || UNREACHABLE.contains(disposition);
        ensure!(
            covered,
            "{disposition:?} is reached by no case here, and is named neither a relay outcome nor \
             an unreachable one"
        );
    }
    Ok(())
}
