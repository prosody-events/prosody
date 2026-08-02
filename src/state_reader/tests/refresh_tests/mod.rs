//! Snapshot acquisition and the three-outcome refresh rule.
//!
//! [`snapshot`] proves the whole rule over random refresh scripts. Withdrawals
//! apply unconditionally. An already-admitted source is never re-validated: the
//! property proves this by counting identity-store reads. A failed routing read
//! keeps the prior snapshot. An emptied routing table fails with
//! `UnknownPublication`. A table whose every new source lacks a frozen identity
//! fails with `IdentityUnavailable`.
//!
//! [`pacing`] proves what a failed refresh leaves behind: a window in which
//! every read serves the held outcome without a store read, and past which the
//! next read re-reads the routing table.
//!
//! [`publication`] owns the deterministic publication schedules: what two
//! callers observe when they race one generation of the shared snapshot. Every
//! schedule observes the overlap through the publication store's read gate,
//! never through elapsed time.
//!
//! The two focused examples below cover what the properties cannot model: a
//! frozen identity that is present but disagrees, and an oversized routing
//! table. Both need precise interval and clock control. Both are the same
//! sticky Permanent fault, so they live together.
//!
//! This module owns what both properties assert against: [`Expect`],
//! [`element`], and [`outcome_matches`].
//!
//! Every reader here refreshes on every operation (`new_eager`), except the
//! sticky example, which sets an explicit interval for the cached fast path.
//! Both properties drive a mocked clock, never a sleep.

mod pacing;
mod publication;
mod snapshot;

use super::support::{GROUP_A, GROUP_B, ScriptedEnv, mock_clock_cache, topic};
use crate::Key;
use crate::codec::JsonCodec;
use crate::error::ErrorCategory;
use crate::state::StateType;
use crate::state::descriptor::{DescriptorIdentity, value_state};
use crate::state::descriptor_identity::DurableDescriptorIdentity;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state_reader::StateReaderError;
use crate::state_reader::source::{MAX_PUBLICATION_SOURCES, PUBLICATION_READ_LIMIT};
use color_eyre::eyre::{Result, bail, eyre};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// The reader outcome one round predicts.
#[derive(Debug, PartialEq, Eq)]
enum Expect {
    /// `Ok(Some(element(idx)))` — the lowest admitted source answers.
    Value(usize),
    /// `Err(UnknownPublication)`.
    UnknownPublication,
    /// `Err(IdentityUnavailable)`.
    IdentityUnavailable,
    /// A failed routing read with no prior snapshot → a store `Err`.
    ReadError,
    /// `Err(RefreshUnavailable)` — a read inside the pacing window left by a
    /// failed refresh, with nothing held to serve.
    RefreshUnavailable,
}

/// The committed value that source `idx` holds.
fn element(idx: usize) -> Value {
    Value::from(idx as i64)
}

/// Whether the reader's observed outcome matches the predicted [`Expect`].
fn outcome_matches(expect: &Expect, observed: &Result<Option<Value>, StateReaderError>) -> bool {
    match expect {
        Expect::Value(idx) => observed.as_ref().ok() == Some(&Some(element(*idx))),
        Expect::UnknownPublication => {
            matches!(observed, Err(StateReaderError::UnknownPublication { .. }))
        }
        Expect::IdentityUnavailable => {
            matches!(observed, Err(StateReaderError::IdentityUnavailable { .. }))
        }
        Expect::ReadError => matches!(observed, Err(StateReaderError::Store { .. })),
        Expect::RefreshUnavailable => {
            matches!(observed, Err(StateReaderError::RefreshUnavailable { .. }))
        }
    }
}
// --- Sticky identity mismatch -----------------------------------------------

/// A frozen identity that disagrees with the descriptor is sticky. Once a
/// source's identity mismatches, every read within the refresh interval
/// surfaces the Permanent `IdentityMismatch`. So does a failed refresh past
/// the interval. Neither ever falls back to the admitted, valid subset.
///
/// `GROUP_A` carries a matching identity, so it stays admitted. `GROUP_B`
/// carries a perturbed identity that disagrees with the descriptor. The
/// property above does not model a mismatched identity: doing so needs
/// precise interval and clock control. This test covers that case instead.
///
/// Falsify: drop the sticky-mismatch cached-path branch in
/// `StateReader::snapshot` → op 2 within the interval serves A's `Ok(None)`.
/// Drop the failed-read sticky arm in `failed` → op 3 serves A's subset.
#[tokio::test]
async fn identity_mismatch_sticky() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("v-sticky");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // A: matching identity → admitted.
    env.publish(GROUP_A, tp_a).await;
    // B: advertised, but its identity is perturbed to disagree with the descriptor.
    env.publications
        .seed(
            &env.sub,
            StateType::Application,
            &env.name,
            &StatePublication {
                group_id: Arc::from(GROUP_B),
                topic: tp_b,
                partition_count: env.count,
            },
        )
        .await;
    let mut perturbed = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        env.name.as_str(),
        &descriptor.structural_identity(),
    );
    perturbed.kind = perturbed.kind.wrapping_add(1);
    env.identities.seed(GROUP_B, &perturbed).await;

    let (cache, mock) = mock_clock_cache(1 << 20);
    // A non-zero interval so op 2 takes the cached-snapshot fast path.
    let reader = env.reader_with_interval(cache, Duration::from_mins(1))?;

    // Op 1, at t=0: the initial refresh detects B's mismatch. A is still admitted.
    match reader.get(key.clone()).await {
        Err(StateReaderError::IdentityMismatch { .. }) => {}
        other => bail!("op 1 expected IdentityMismatch, got {other:?}"),
    }
    let reads_after_first = env.publications.reads();
    // Op 2 (within the interval, cached fast path): the mismatch must still
    // surface, not A's admitted subset, and without a second routing read.
    match reader.get(key.clone()).await {
        Err(StateReaderError::IdentityMismatch { .. }) => {}
        other => bail!("op 2 (within interval) expected a sticky IdentityMismatch, got {other:?}"),
    }
    assert_eq!(
        env.publications.reads(),
        reads_after_first,
        "a sticky fault must not re-read the routing table"
    );
    // Elapse the interval and make every routing read fail.
    mock.increment(Duration::from_mins(2));
    env.publications.fail_reads_with(ErrorCategory::Transient);
    // Op 3 (stale → refresh, read fails): the sticky mismatch outranks the
    // admitted subset even though the routing read failed.
    match reader.get(key).await {
        Err(StateReaderError::IdentityMismatch { .. }) => Ok(()),
        other => bail!("op 3 (failed refresh) expected a sticky IdentityMismatch, got {other:?}"),
    }
}

// --- Oversized routing table ------------------------------------------------

/// An oversized routing table is the other Permanent fault a refresh can find,
/// and it is sticky the same way: every read within the refresh interval
/// surfaces `TooManySources` without re-reading the table. Past the interval
/// the reader re-validates, so withdrawing enough sources clears the fault.
///
/// This is the twin of [`identity_mismatch_sticky`]. Both prove one rule: a
/// Permanent fault is cached like a snapshot, because only an operator can
/// clear it, while a Transient absence re-reads eagerly.
///
/// Falsify: propagate the rejection out of `StateReader::refresh` instead of
/// publishing it as a fault → op 2 re-reads the routing table → the read-count
/// assert reds. Publish it with no fault → op 2 falls through to
/// `UnknownPublication` → the op 2 match reds.
#[tokio::test]
async fn oversized_routing_table_is_sticky() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("v-oversized");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");

    // More rows than the store returns, each with a matching frozen identity,
    // so the reported count proves the read stops at the overflow sentinel.
    for i in 0..PUBLICATION_READ_LIMIT + 5 {
        env.publish(
            &format!("oversized-g{i}"),
            topic(&format!("oversized-t{i}")),
        )
        .await;
    }

    let (cache, mock) = mock_clock_cache(1 << 20);
    let reader = env.reader_with_interval(cache, Duration::from_mins(1))?;

    // Op 1, at t=0: the initial refresh reads the table and rejects it.
    match reader.get(key.clone()).await {
        Err(StateReaderError::TooManySources { found, max }) => {
            assert_eq!(found, PUBLICATION_READ_LIMIT);
            assert_eq!(max, MAX_PUBLICATION_SOURCES);
        }
        other => bail!("op 1 expected TooManySources, got {other:?}"),
    }
    let reads_after_first = env.publications.reads();
    // Op 2 (within the interval): the same Permanent fault, served from the
    // cached state.
    match reader.get(key.clone()).await {
        Err(StateReaderError::TooManySources { .. }) => {}
        other => bail!("op 2 (within interval) expected a sticky TooManySources, got {other:?}"),
    }
    assert_eq!(
        env.publications.reads(),
        reads_after_first,
        "a sticky fault must not re-read the routing table"
    );

    // Op 3, past the interval with enough sources withdrawn: the table is back
    // within the bound, so the re-validation clears the fault.
    mock.increment(Duration::from_mins(2));
    for i in 0_usize..6 {
        env.publications
            .remove_group(
                &env.sub,
                StateType::Application,
                &env.name,
                &format!("oversized-g{i}"),
            )
            .await
            .map_err(|e| eyre!("remove_group: {e}"))?;
    }
    match reader.get(key).await {
        Ok(None) => Ok(()),
        other => bail!("op 3 (withdrawn back within the bound) expected Ok(None), got {other:?}"),
    }
}
