//! What the send path tells an operator, and what it never tells them.
//!
//! The claim these cases pin is a security one: an unresolvable node id arrives
//! in a Kafka header a topic writer controls, so putting it in a label would
//! let that writer choose the metrics pipeline's cardinality. The attribute set
//! is therefore compared **exactly**, not searched for the label it should
//! carry.
//!
//! [`GlobalMetrics`] is installed as the first statement of each case that
//! reads a metric: the instruments bind to whatever meter provider is global
//! when they are first touched, and nextest gives each case its own process.

use super::super::metrics::{DropReason, Stage};
use super::Harness;
use crate::router::loopback::{UNPUBLISHED_NODE, config, node, paused};
use crate::test_util::{GlobalMetrics, assert_distinct_labels, label};
use color_eyre::Result;
use color_eyre::eyre::ensure;
use strum::VariantArray;

/// Destinations the fleet holds: the node that answers, and the one no
/// registration names.
const DESTINATIONS: usize = 2;

/// Responses one destination may hold at once.
const SLOTS: usize = 2;

/// A node every suite router publishes.
const PUBLISHED: u8 = 0;

/// One response to a node no registration names is dropped under a fixed
/// reason, and the node's id appears nowhere in the metrics it moved.
///
/// The published response beside it is what makes the stage counters a
/// progression rather than one number: it reaches `delivered`, and the
/// unresolvable one stops after `enqueued`.
#[test]
fn a_drop_names_its_reason_and_never_the_node() -> Result<()> {
    let metrics = GlobalMetrics::install();
    let drained = paused()?.block_on(async {
        let harness = Harness::new(config(DESTINATIONS, SLOTS))?;
        harness.send(PUBLISHED)?;
        harness.send(UNPUBLISHED_NODE)?;
        harness.drain().await
    })?;
    ensure!(
        (drained.sent, drained.dropped) == (1, 1),
        "one response must reach the listener and one must be dropped, not {} and {}",
        drained.sent,
        drained.dropped
    );

    ensure!(
        metrics.points("prosody.response.dropped")?
            == vec![(label("reason", "unresolvable_node"), 1)],
        "the drop must be counted under its reason alone: {:?}",
        metrics.points("prosody.response.dropped")?
    );
    ensure!(
        metrics.points("prosody.response.stages")?
            == vec![
                (label("stage", "attempted"), 2),
                (label("stage", "delivered"), 1),
                (label("stage", "enqueued"), 2),
                (label("stage", "framed"), 1),
            ],
        "the stages a response passes must each be counted once per response: {:?}",
        metrics.points("prosody.response.stages")?
    );

    let unresolvable = node(UNPUBLISHED_NODE).to_string();
    for name in [
        "prosody.response.dropped",
        "prosody.response.stages",
        "prosody.peer.fleet.destinations",
    ] {
        for (attributes, _) in metrics.points(name)? {
            for (key, value) in attributes {
                ensure!(
                    !key.contains(&unresolvable) && !value.contains(&unresolvable),
                    "{name} carries the node id in {key}={value}"
                );
            }
        }
    }
    Ok(())
}

/// Every stage and every drop reason counts under its own label, so one outcome
/// can never be read as another in a dashboard.
///
/// Each enum is checked in its own namespace. They are different instruments
/// under different attribute keys, so a name they happen to share is not a
/// collision a dashboard could misread.
#[test]
fn every_outcome_has_a_distinct_lowercase_label() -> Result<()> {
    assert_distinct_labels(Stage::VARIANTS.iter().map(|stage| stage.label()))?;
    assert_distinct_labels(DropReason::VARIANTS.iter().map(|reason| reason.label()))
}
