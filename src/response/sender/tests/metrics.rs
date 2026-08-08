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

use super::super::metrics::{DropReason, Stage, record_fallback};
use super::{CAP_BYTES, Harness, attempts};
use crate::router::loopback::{Script, UNPUBLISHED_NODE, config, node, paused};
use crate::router::{Preference, SendFailure};
use crate::test_util::{GlobalMetrics, assert_distinct_labels, label};
use color_eyre::Result;
use color_eyre::eyre::ensure;
use std::collections::BTreeMap;
use strum::VariantArray;

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
/// unresolvable one stops after `attempted`.
#[test]
fn a_drop_names_its_reason_and_never_the_node() -> Result<()> {
    let metrics = GlobalMetrics::install();
    let drained = paused()?.block_on(async {
        let harness = Harness::new(config())?;
        harness.send(PUBLISHED).await?;
        harness.send(UNPUBLISHED_NODE).await?;
        harness.drain().await
    })?;
    ensure!(
        (drained.sent, drained.dropped) == (1, 1),
        "one response must reach the listener and one must be dropped, not {} and {}",
        drained.sent,
        drained.dropped
    );
    ensure!(
        attempts(&drained.deliveries, UNPUBLISHED_NODE) == 0,
        "an unpublished node must reach no address"
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
                (label("stage", "framed"), 2),
            ],
        "the stages a response passes must each be counted once per response: {:?}",
        metrics.points("prosody.response.stages")?
    );

    let unresolvable = node(UNPUBLISHED_NODE).to_string();
    for name in ["prosody.response.dropped", "prosody.response.stages"] {
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

/// A response the frame cap cannot carry is counted under its own reason.
///
/// The encoder refuses it before transport work. The other response completes.
/// Thus, both are attempted and only one is framed.
#[test]
fn a_response_the_cap_refuses_is_counted() -> Result<()> {
    let metrics = GlobalMetrics::install();
    let drained = paused()?.block_on(async {
        let harness = Harness::new(config())?;
        harness.send_payload(PUBLISHED, vec![0; OVER_CAP]).await?;
        harness.send(PUBLISHED).await?;
        harness.drain().await
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
                (label("stage", "framed"), 1),
            ],
        "a response the cap refuses must stop before framing: {:?}",
        metrics.points("prosody.response.stages")?
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
/// The second response starts after the first response stores its preference.
#[test]
fn a_fallback_counts_the_transition_and_only_when_the_next_candidate_answers() -> Result<()> {
    let metrics = GlobalMetrics::install();
    let drained = paused()?.block_on(async {
        let harness = Harness::dual_homed(config())?;
        harness.script(FALLS_BACK, never_answers());
        harness.script(SILENT, never_answers());
        harness.script_advertised(SILENT, never_answers());
        harness.send(FALLS_BACK).await?;
        harness.send(FALLS_BACK).await?;
        harness.send(SILENT).await?;
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

/// A transition is counted under the pair it is given, in that order.
///
/// The case above reaches the direct-to-advertised transition alone. A
/// [`Script`] fails a fixed count of first attempts, so an endpoint that
/// answers and then stops cannot be scripted, and the walk back to direct needs
/// exactly that. A counter that named one fixed pair would therefore pass that
/// case. This one records the other direction directly, so the labels are
/// proved to be read rather than fixed.
#[test]
fn a_fallback_names_the_pair_it_is_given() -> Result<()> {
    let metrics = GlobalMetrics::install();
    record_fallback(Preference::Advertised, Preference::Direct);
    ensure!(
        metrics.points("prosody.response.fallback")?
            == vec![(
                BTreeMap::from([
                    ("from".to_owned(), "advertised".to_owned()),
                    ("to".to_owned(), "direct".to_owned()),
                ]),
                1,
            )],
        "the count must name the pair it was given: {:?}",
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
