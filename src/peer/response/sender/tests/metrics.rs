//! What the send path tells an operator, and what it never tells them.
//!
//! The claim these cases pin is a security one: an unresolvable peer id arrives
//! in a Kafka header a topic writer controls, so putting it in a label would
//! let that writer choose the metrics pipeline's cardinality. The attribute set
//! is therefore compared **exactly**, not searched for the label it should
//! carry.
//!
//! Each case supplies local instruments to its response route.

use super::super::metrics::{DropReason, Stage};
use super::{Harness, PAYLOAD, attempts, deadline};
use crate::codec::Codec;
use crate::peer::response::sender::{deliver_response, stage};
use crate::peer::router::loopback::{UNPUBLISHED_PEER, paused, peer};
use crate::test_util::{GlobalMetrics, assert_distinct_labels, label};
use color_eyre::Result;
use color_eyre::eyre::ensure;
use opentelemetry::Context;
use std::convert::Infallible;
use std::io::Error;
use strum::VariantArray;

/// A peer every suite router publishes.
const PUBLISHED: u8 = 0;

#[derive(Default)]
struct FailingCodec;

impl Codec for FailingCodec {
    type Error = Error;
    type Payload = Vec<u8>;

    const FORMAT_ID: &'static str = "test-failure";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Vec<u8>, Error> {
        Ok(buf.to_vec())
    }

    fn serialize_ref(&mut self, _payload: &Vec<u8>, _buf: &mut Vec<u8>) -> Result<(), Error> {
        Err(Error::other("injected encode failure"))
    }
}

/// A codec failure stops before framing and names the encode failure.
#[test]
fn a_codec_failure_records_the_encode_drop() -> Result<()> {
    let metrics = GlobalMetrics::install();
    paused()?.block_on(async {
        let harness = Harness::with_metrics(metrics.metrics())?;
        let payload = PAYLOAD.to_vec();
        let prepared = stage::<FailingCodec, Infallible, _>(
            &harness.router,
            harness.header.clone(),
            Ok(&payload),
        );
        deliver_response(&harness.router, prepared, Context::current(), deadline()).await;
        Ok::<_, color_eyre::Report>(())
    })?;
    ensure!(
        metrics.points("prosody.response.dropped")? == vec![(label("reason", "encode_failed"), 1)],
        "the drop must name the codec failure"
    );
    ensure!(
        metrics.points("prosody.response.stages")? == vec![(label("stage", "attempted"), 1)],
        "the response must stop before framing"
    );
    Ok(())
}

/// One response to a peer no registration names is dropped under a fixed
/// reason, and the peer's id appears nowhere in the metrics it moved.
///
/// The published response beside it is what makes the stage counters a
/// progression rather than one number: it reaches `delivered`, and the
/// unresolvable one stops after `attempted`.
#[test]
fn a_drop_names_its_reason_and_never_the_peer() -> Result<()> {
    let metrics = GlobalMetrics::install();
    let drained = paused()?.block_on(async {
        let harness = Harness::with_metrics(metrics.metrics())?;
        harness.send(PUBLISHED).await?;
        harness.send(UNPUBLISHED_PEER).await?;
        harness.drain().await
    })?;
    ensure!(
        (drained.sent, drained.dropped) == (1, 1),
        "one response must reach the listener and one must be dropped, not {} and {}",
        drained.sent,
        drained.dropped
    );
    ensure!(
        attempts(&drained.deliveries, UNPUBLISHED_PEER)? == 0,
        "an unpublished peer must reach no address"
    );
    ensure!(
        metrics.points("prosody.response.dropped")?
            == vec![(label("reason", "unresolvable_requester"), 1)],
        "the drop must be counted under its reason alone: {:?}",
        metrics.points("prosody.response.dropped")?
    );
    ensure!(
        metrics.points("prosody.response.stages")?
            == vec![
                (label("stage", "attempted"), 2),
                (label("stage", "delivered"), 1),
                (label("stage", "framed"), 2),
            ],
        "the stages a response passes must each be counted once per response: {:?}",
        metrics.points("prosody.response.stages")?
    );

    let unresolvable = peer(UNPUBLISHED_PEER).to_string();
    for name in ["prosody.response.dropped", "prosody.response.stages"] {
        for (attributes, _) in metrics.points(name)? {
            for (key, value) in attributes {
                ensure!(
                    !key.contains(&unresolvable) && !value.contains(&unresolvable),
                    "{name} carries the peer id in {key}={value}"
                );
            }
        }
    }
    Ok(())
}

/// Every stage and every drop reason count under its
/// own label, so one outcome can never be read as another in a dashboard.
///
/// Each enum is checked in its own namespace. They are different instruments
/// under different attribute keys, so a name they happen to share is not a
/// collision a dashboard could misread.
#[test]
fn every_outcome_has_a_distinct_lowercase_label() -> Result<()> {
    assert_distinct_labels(Stage::VARIANTS.iter().map(|stage| stage.label()))?;
    assert_distinct_labels(DropReason::VARIANTS.iter().map(|reason| reason.label()))
}
