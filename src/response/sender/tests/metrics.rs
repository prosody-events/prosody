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
use super::{CAP_BYTES, Harness};
use crate::router::loopback::{Script, UNPUBLISHED_NODE, config, node, paused};
use crate::router::{Preference, SendFailure};
use crate::test_util::{GlobalMetrics, assert_distinct_labels, label};
use color_eyre::Result;
use color_eyre::eyre::ensure;
use std::collections::BTreeMap;
use strum::VariantArray;

/// Destinations the fleet holds. Each case here addresses two nodes, so two
/// cells hold them all.
const DESTINATIONS: usize = 2;

/// Responses one destination may hold at once.
const SLOTS: usize = 2;

/// A node every suite router publishes.
const PUBLISHED: u8 = 0;

/// The node whose direct endpoint does not answer, so its responses fall back.
const FALLS_BACK: u8 = PUBLISHED;

/// The node neither of whose endpoints answers.
const SILENT: u8 = 1;

/// A body no frame at the cap can carry: the whole cap, before any header.
const OVER_CAP: usize = CAP_BYTES;

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

    ensure!(
        metrics.points("prosody.peer.fleet.destinations")? == vec![(BTreeMap::new(), 2)],
        "both destinations must be counted live, under no attribute at all: {:?}",
        metrics.points("prosody.peer.fleet.destinations")?
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

/// A response the frame cap cannot carry is counted under its own reason and
/// gives its slot back.
///
/// The encoder refuses it, so this is the one drop reason a worker reports
/// without reaching the transport at all. The response queued beside it is
/// delivered, which is what makes the stage counters a progression: both are
/// enqueued and only one is framed.
///
/// Both responses name the same destination, so the free slots read after the
/// drain are that destination's own: a worker that kept the refused response's
/// slot would leave one of them missing.
#[test]
fn a_response_the_cap_refuses_is_counted_and_gives_its_slot_back() -> Result<()> {
    let metrics = GlobalMetrics::install();
    let (fleet, drained) = paused()?.block_on(async {
        let harness = Harness::new(config(DESTINATIONS, SLOTS))?;
        let fleet = harness.fleet();
        harness.send_payload(PUBLISHED, vec![0; OVER_CAP])?;
        harness.send(PUBLISHED)?;
        let drained = harness.drain().await?;
        Ok::<_, color_eyre::Report>((fleet, drained))
    })?;
    ensure!(
        (drained.sent, drained.dropped) == (1, 1),
        "one response must reach the listener and one must be refused, not {} and {}",
        drained.sent,
        drained.dropped
    );

    ensure!(
        metrics.points("prosody.response.dropped")? == vec![(label("reason", "encode_failed"), 1)],
        "the refusal must be counted under its own reason alone: {:?}",
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
        "a response the cap refuses must stop at enqueued: {:?}",
        metrics.points("prosody.response.stages")?
    );
    ensure!(
        fleet.available(node(PUBLISHED)) == Some(SLOTS),
        "every slot must come back, not {:?}",
        fleet.available(node(PUBLISHED))
    );
    Ok(())
}

/// A fallback counts the transition it made, and only when the next candidate
/// answered.
///
/// Three responses drive the whole claim on one meter. The first falls back,
/// and it is the one transition. The second reaches the same node, which now
/// remembers the endpoint that answered. It starts there and counts nothing, so
/// a counter that moved once per delivery would read two. The third reaches a
/// node where nothing answers. Its walk leaves both candidates behind and still
/// counts nothing, so a counter that moved once per candidate would read two as
/// well.
///
/// One worker drains one destination's queue in order, so the second response
/// is dequeued after the first stored its preference. No wait is needed for
/// that.
#[test]
fn a_fallback_counts_the_transition_and_only_when_the_next_candidate_answers() -> Result<()> {
    let metrics = GlobalMetrics::install();
    let drained = paused()?.block_on(async {
        let harness = Harness::dual_homed(config(DESTINATIONS, SLOTS))?;
        harness.script(FALLS_BACK, never_answers());
        harness.script(SILENT, never_answers());
        harness.script_advertised(SILENT, never_answers());
        harness.send(FALLS_BACK)?;
        harness.send(FALLS_BACK)?;
        harness.send(SILENT)?;
        harness.drain().await
    })?;
    ensure!(
        (drained.sent, drained.dropped) == (2, 1),
        "two responses must be accepted and one must be dropped, not {} and {}",
        drained.sent,
        drained.dropped
    );

    ensure!(
        metrics.points("prosody.response.fallback")?
            == vec![(
                BTreeMap::from([
                    ("from".to_owned(), "direct".to_owned()),
                    ("to".to_owned(), "advertised".to_owned()),
                ]),
                1,
            )],
        "only the answered direct-to-advertised transition must be counted: {:?}",
        metrics.points("prosody.response.fallback")?
    );
    Ok(())
}

/// Every stage, every drop reason and every fallback endpoint counts under its
/// own label, so one outcome can never be read as another in a dashboard.
///
/// Each enum is checked in its own namespace. They are different instruments
/// under different attribute keys, so a name they happen to share is not a
/// collision a dashboard could misread.
#[test]
fn every_outcome_has_a_distinct_lowercase_label() -> Result<()> {
    assert_distinct_labels(Stage::VARIANTS.iter().map(|stage| stage.label()))?;
    assert_distinct_labels(
        Preference::VARIANTS
            .iter()
            .map(|preference| preference.label()),
    )?;
    assert_distinct_labels(DropReason::VARIANTS.iter().map(|reason| reason.label()))
}

/// An endpoint that says nothing at all, which is what a wrong network label
/// reaches.
const fn never_answers() -> Script {
    Script::Fail {
        failure: SendFailure::Unreachable,
        times: usize::MAX,
    }
}
