//! What the requesting side tells an operator: how many requests wait right
//! now, and how long each one waited for the answers it got.
//!
//! Neither series carries an identity. A request id is minted per call and a
//! subsystem name arrives from the network, so a series keyed by one would let
//! a caller choose the metrics pipeline's cardinality. Each attribute set is
//! therefore compared **exactly**.
//!
//! Each case supplies local instruments to its requester.

use super::{MAX_TIMEOUT, names, register, unanswered_call_with_registry};
use crate::peer::requester::registry::PendingRegistry;
use crate::test_util::{GlobalMetrics, label};
use color_eyre::Result;
use color_eyre::eyre::ensure;
use std::collections::BTreeMap;

/// How many requests this process waits for answers to.
const PENDING: &str = "prosody.request.pending";

/// How long one request waited, by how complete its answers were.
const LATENCY: &str = "prosody.request.duration";

/// The subsystem the registered request awaits.
const SUBSYSTEM: &str = "billing";

/// A live request counts while it waits and stops counting once it is over.
///
/// The count is read at both moments, so an add without its subtract and a
/// subtract without its add each read wrong. The registration is dropped
/// unfinished, which is the removal path a cancelled call takes.
#[tokio::test(start_paused = true)]
async fn a_waiting_request_is_counted_until_it_is_over() -> Result<()> {
    let metrics = GlobalMetrics::install();
    let registry = PendingRegistry::with_metrics(metrics.metrics());
    let awaited = names(&[SUBSYSTEM])?;
    let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
    ensure!(
        metrics.points(PENDING)? == vec![(BTreeMap::new(), 1)],
        "one request must be counted waiting, under no attribute at all: {:?}",
        metrics.points(PENDING)?
    );

    drop(registration);
    ensure!(
        metrics.points(PENDING)? == vec![(BTreeMap::new(), 0)],
        "a request that is over must count no more: {:?}",
        metrics.points(PENDING)?
    );
    Ok(())
}

/// One call that nothing answers records its wait under the completeness it
/// reached, and never under anything it was told.
///
/// A sustained `none` is what says synchrony waiting has stopped working, so
/// the label is the claim here rather than the duration.
#[tokio::test(start_paused = true)]
async fn an_unanswered_call_records_its_wait_as_answered_by_nobody() -> Result<()> {
    let metrics = GlobalMetrics::install();
    unanswered_call_with_registry(PendingRegistry::with_metrics(metrics.metrics())).await?;
    ensure!(
        metrics.points(LATENCY)? == vec![(label("prosody.request.outcome", "none"), 1)],
        "the call must record one wait under the answers it got: {:?}",
        metrics.points(LATENCY)?
    );
    Ok(())
}
