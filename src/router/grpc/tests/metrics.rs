//! What the receive leg tells an operator, and what it never tells them.
//!
//! This is the leg the attacker-controlled values arrive on: a frame's claimed
//! subsystem and its peer id come off a Kafka header a topic writer controls.
//! So the attribute set of every point is compared **exactly**, rather than
//! searched for the label it should carry.
//!
//! [`GlobalMetrics`] is installed as the first statement: the instrument binds
//! to whatever meter provider is global when it is first touched, and nextest
//! gives this case its own process.

use super::{ALPHA, Harness, header, payload, register};
use crate::response::RequestId;
use crate::test_util::{GlobalMetrics, TEST_RUNTIME, label};
use color_eyre::Result;
use color_eyre::eyre::ensure;
use tonic::Code;

/// The counter of delivery attempts this process decided.
const DISPOSITIONS: &str = "prosody.response.dispositions";

/// A short payload; its size is not the subject.
const SHORT: usize = 8;

/// One accepted delivery and one refused delivery each count once, under their
/// own fixed label and nothing else.
///
/// The refusal is what makes the two points a classification rather than one
/// number, and comparing the whole attribute map is what catches a header value
/// that ever reaches a label.
#[test]
fn every_answer_counts_once_under_a_fixed_label() -> Result<()> {
    let metrics = GlobalMetrics::install();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let request = register(&harness.registry, &[ALPHA])?;
        let accepted = harness
            .deliver(&header(harness.peer, request.id(), ALPHA)?, payload(SHORT))
            .await?;
        ensure!(accepted == Code::Ok, "a well-formed response is accepted");

        // No registration anywhere names this id, so the same wire path ends in
        // a refusal the service decided.
        let refused = harness
            .deliver(
                &header(harness.peer, RequestId::new(), ALPHA)?,
                payload(SHORT),
            )
            .await?;
        ensure!(
            refused == Code::NotFound,
            "a response for no live request is refused, not answered {refused:?}"
        );

        let counted = metrics.points(DISPOSITIONS)?;
        ensure!(
            counted
                == vec![
                    (label("disposition", "accepted"), 1),
                    (label("disposition", "unknown_request"), 1),
                ],
            "each answer must count once under its own label alone: {counted:?}"
        );
        Ok(())
    })
}
