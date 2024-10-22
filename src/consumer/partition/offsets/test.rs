//! Unit tests for the `OffsetTracker` in the consumer partition offsets module.
//!
//! This module contains QuickCheck-based property tests to verify the correct
//! functioning of the `OffsetTracker`, focusing on watermark tracking and
//! committing.

use ahash::{HashMap, HashMapExt, HashSet};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;

use crate::consumer::partition::offsets::{Action, OffsetTracker, Operation};
use crate::Offset;

/// A wrapper for a vector of Actions used in `QuickCheck` tests.
#[derive(Clone, Debug)]
struct Actions(Vec<Action>);

/// Verifies that OffsetTracker correctly tracks and commits watermarks.
///
/// # Arguments
/// * `actions` - An `Actions` instance containing the test actions.
#[quickcheck]
fn tracks_commit_watermark(actions: Actions) -> TestResult {
    let Ok(runtime) = Builder::new_multi_thread().enable_time().build() else {
        return TestResult::error("failed to initialize runtime");
    };

    runtime.block_on(tracks_commit_watermark_impl(actions))
}

/// Implements the test for verifying watermark tracking and committing.
///
/// # Arguments
/// * `actions` - An `Actions` instance containing the test actions.
///
/// # Returns
/// A `TestResult` indicating whether the test passed or failed.
async fn tracks_commit_watermark_impl(Actions(actions): Actions) -> TestResult {
    let version = Arc::default();
    let tracker = OffsetTracker::new(
        "test-topic".into(),
        0,
        actions.len() + 1,
        Duration::from_secs(300),
        version,
    );

    let mut test_offsets = BTreeMap::default();
    let mut commits = HashMap::with_capacity(actions.len());

    // Process each action
    for action in &actions {
        match action.operation {
            Operation::Take(_) => {
                // Take an offset from the tracker
                let commit = match tracker.take(action.offset).await {
                    Ok(offset) => offset,
                    Err(error) => return TestResult::error(format!("tracker failed: {error:#}")),
                };

                commits.insert(action.offset, commit);
                test_offsets.insert(action.offset, action.operation);
            }
            Operation::Commit => {
                // Commit an offset if it was previously taken
                if let Some(commit) = commits.remove(&action.offset) {
                    commit.commit();
                    test_offsets.insert(action.offset, action.operation);
                }
            }
        }
    }

    drop(commits);

    // Determine the expected watermark
    let expected = test_offsets
        .iter()
        .take_while(|(_, &action)| action == Operation::Commit)
        .last()
        .map(|(&offset, _)| offset);

    // Get the actual watermark from the tracker
    let actual = tracker.shutdown().await;

    // Compare expected and actual watermarks
    if expected == actual {
        TestResult::passed()
    } else {
        TestResult::error(format!("{expected:?} != {actual:?} ({test_offsets:?})"))
    }
}

impl Arbitrary for Actions {
    /// Generates an arbitrary `Actions` instance for `QuickCheck` tests.
    ///
    /// # Arguments
    /// * `g` - A mutable reference to a `Gen` instance for random generation.
    fn arbitrary(g: &mut Gen) -> Self {
        let mut offset: Offset = 0;
        let mut takes = HashSet::default();
        let mut actions = Vec::<Action>::arbitrary(g);
        let gaps: Vec<Offset> = (0..4).collect();

        for action in &mut actions {
            if takes.is_empty() || bool::arbitrary(g) {
                // Generate a Take action
                let gap = *g.choose(&gaps).unwrap_or(&0);
                offset += gap as Offset;
                action.offset = offset;
                action.operation = Operation::Take(Instant::now());
                takes.insert(offset);
                offset += 1;
            } else {
                // Generate a Commit action
                let possible_takes = takes.iter().copied().collect::<Vec<_>>();
                let Some(offset) = g.choose(&possible_takes) else {
                    continue;
                };

                action.offset = *offset;
                action.operation = Operation::Commit;
                takes.remove(&action.offset);
            }
        }

        Self(actions)
    }
}

impl Arbitrary for Action {
    /// Generates an arbitrary `Action` for `QuickCheck` tests.
    ///
    /// # Arguments
    /// * `g` - A mutable reference to a `Gen` instance for random generation.
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            offset: u16::arbitrary(g).into(),
            operation: Operation::arbitrary(g),
        }
    }
}

impl Arbitrary for Operation {
    /// Generates an arbitrary `Operation` for `QuickCheck` tests.
    ///
    /// # Arguments
    /// * `g` - A mutable reference to a `Gen` instance for random generation.
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Operation::Take(Instant::now())
        } else {
            Operation::Commit
        }
    }
}
