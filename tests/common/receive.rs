//! Receiving events off a channel under a hang-guard.
//!
//! Every deadline here is a hang-guard, never the assertion: a test asserts on
//! what it received, and the timeout exists only so a stalled pipeline fails
//! with a clear message instead of blocking the suite.

use color_eyre::eyre::{Result, bail, eyre};
use serde_json::Value;
use std::fmt::Debug;
use std::time::Duration as StdDuration;
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout;

/// How long [`collect_messages_with_timeout`] waits to confirm no extra message
/// follows the ones it expected.
const NO_EXTRA_WINDOW: StdDuration = StdDuration::from_secs(2);

/// Receives the next event from `rx`, failing if none arrives within
/// `hang_guard`.
///
/// # Errors
///
/// Returns an error on timeout or if the channel closed.
pub(crate) async fn expect_event<T>(rx: &mut Receiver<T>, hang_guard: StdDuration) -> Result<T> {
    timeout(hang_guard, rx.recv())
        .await
        .map_err(|_| eyre!("timed out waiting for event after {hang_guard:?}"))?
        .ok_or_else(|| eyre!("event channel closed unexpectedly"))
}

/// Verifies that no event arrives on `rx` within `window`.
///
/// A closed channel is a failure, not a satisfied expectation: a live
/// consumer's channel staying open is part of what "no extra event" means.
///
/// # Errors
///
/// Returns an error if an event arrives or the channel closed.
pub(crate) async fn expect_no_event<T: Debug>(
    rx: &mut Receiver<T>,
    window: StdDuration,
) -> Result<()> {
    match timeout(window, rx.recv()).await {
        Err(_) => Ok(()),
        Ok(Some(event)) => bail!("expected no event within {window:?} but received: {event:?}"),
        Ok(None) => bail!("event channel closed unexpectedly"),
    }
}

/// Collects exactly `expected_count` messages, allowing `timeout_secs` for
/// each, then confirms no further message arrives.
///
/// # Errors
///
/// Returns an error on timeout, if the channel closes, or if an extra message
/// arrives.
pub(crate) async fn collect_messages_with_timeout(
    receiver: &mut Receiver<(String, Value)>,
    expected_count: usize,
    timeout_secs: u64,
) -> Result<Vec<(String, Value)>> {
    let hang_guard = StdDuration::from_secs(timeout_secs);
    let mut messages = Vec::with_capacity(expected_count);

    for i in 0..expected_count {
        messages.push(
            expect_event(receiver, hang_guard)
                .await
                .map_err(|error| error.wrap_err(format!("waiting for message {}", i + 1)))?,
        );
    }

    expect_no_event(receiver, NO_EXTRA_WINDOW).await?;
    Ok(messages)
}
