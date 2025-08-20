//! Unit tests for the `OffsetTracker` in the consumer partition offsets module.
//!
//! This module contains QuickCheck-based property tests to verify the correct
//! functioning of the `OffsetTracker`, focusing on watermark tracking and
//! committing.

use crate::Offset;
use crate::consumer::partition::offsets::{Action, OffsetTracker, Operation, UncommittedOffset};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use crossbeam_utils::CachePadded;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::Instant;

/// A wrapper for a vector of Actions used in `QuickCheck` tests.
#[derive(Clone, Debug)]
struct Actions(Vec<Action>);

/// Verifies that `OffsetTracker` correctly tracks and commits watermarks.
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
        .take_while(|(_, action)| **action == Operation::Commit)
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

// Add this new struct for our stall detection test
#[derive(Clone, Debug)]
struct StallTestCase {
    actions: Vec<StallAction>,
    stall_threshold: Duration,
}

#[quickcheck]
fn detects_stalls(test_case: StallTestCase) -> TestResult {
    let Ok(runtime) = Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
    else {
        return TestResult::error("failed to initialize runtime");
    };

    runtime.block_on(async {
        let StallTestCase {
            actions,
            stall_threshold,
        } = test_case;
        let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
        let tracker = OffsetTracker::new(
            "test-topic".into(),
            0,
            actions.len() + 1,
            stall_threshold,
            version,
        );

        let mut uncommitted_offsets: BTreeMap<Offset, (UncommittedOffset, Instant)> =
            BTreeMap::new();
        let mut committed_offsets: HashSet<Offset> = HashSet::new();
        let mut watermark: Offset = -1; // Start with initial watermark value

        for action in actions {
            match action {
                StallAction::Take(offset) => {
                    if let Ok(uncommitted_offset) = tracker.take(offset).await {
                        let take_time = Instant::now();
                        uncommitted_offsets.insert(offset, (uncommitted_offset, take_time));
                    } else {
                        return TestResult::error("Failed to take offset");
                    }
                }
                StallAction::Commit(offset) => {
                    if let Some((uncommitted_offset, _)) = uncommitted_offsets.remove(&offset) {
                        uncommitted_offset.commit();
                        committed_offsets.insert(offset);
                    }
                }
                StallAction::Wait(duration) => {
                    // Advance the simulated time
                    tokio::time::advance(duration).await;
                    // Yield to allow background tasks to run
                    tokio::task::yield_now().await;
                }
            }

            // Simulate watermark advancement
            while committed_offsets.contains(&(watermark + 1)) {
                watermark += 1;
            }

            // After each action, check for stalls
            let now = Instant::now();

            let stalled_offsets: Vec<_> =
                get_stalled(stall_threshold, &mut uncommitted_offsets, watermark, now);

            let expected_stall = !stalled_offsets.is_empty();

            // Allow background tasks to run
            tokio::task::yield_now().await;

            // Check if the stall state matches our expectation
            if expected_stall != tracker.is_stalled() {
                return TestResult::error(format!(
                    "Stall detection mismatch during actions.\nExpected: {}, Actual: {}\nStalled \
                     Offsets: {:?}\nWatermark: {}\nAction: {:?}",
                    expected_stall,
                    tracker.is_stalled(),
                    stalled_offsets,
                    watermark,
                    action,
                ));
            }
        }

        // Advance time to trigger stall detection if necessary
        let additional_time = stall_threshold + Duration::from_millis(1);
        tokio::time::advance(additional_time).await;
        tokio::task::yield_now().await;

        // Recompute expected_stall after advancing time
        let now = Instant::now();

        let stalled_offsets: Vec<_> =
            get_stalled(stall_threshold, &mut uncommitted_offsets, watermark, now);

        let expected_stall = !stalled_offsets.is_empty();

        // Allow background tasks to run
        tokio::task::yield_now().await;

        // Check if the stall state matches our expectation
        if expected_stall == tracker.is_stalled() {
            TestResult::passed()
        } else {
            TestResult::error(format!(
                "Stall detection mismatch after advancing time.\nExpected: {}, Actual: \
                 {}\nStalled Offsets: {:?}\nWatermark: {}",
                expected_stall,
                tracker.is_stalled(),
                stalled_offsets,
                watermark,
            ))
        }
    })
}

fn get_stalled(
    stall_threshold: Duration,
    uncommitted_offsets: &mut BTreeMap<Offset, (UncommittedOffset, Instant)>,
    watermark: Offset,
    now: Instant,
) -> Vec<Offset> {
    uncommitted_offsets
        .iter()
        .filter_map(|(&offset, &(_, take_time))| {
            (offset > watermark && now.duration_since(take_time) >= stall_threshold)
                .then_some(offset)
        })
        .collect()
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

#[derive(Clone, Debug)]
enum StallAction {
    Take(Offset),
    Commit(Offset),
    Wait(Duration),
}

impl Arbitrary for StallTestCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut actions = Vec::new();
        let mut taken_offsets = HashSet::new();
        let mut offset_counter = 0;
        let max_gap = 2; // Maximum gap between offsets

        // Use smaller stall threshold for faster tests
        let stall_threshold = Duration::from_millis(10 + u64::arbitrary(g) % 20);

        for _ in 0_u8..1u8 {
            match u8::arbitrary(g) % 3 {
                0 => {
                    // Generate offsets with possible small gaps
                    let gap = u8::arbitrary(g) % max_gap;
                    offset_counter += Offset::from(gap);

                    let offset = offset_counter;
                    offset_counter += 1;

                    actions.push(StallAction::Take(offset));
                    taken_offsets.insert(offset);
                }
                1 if !taken_offsets.is_empty() => {
                    let Some(&&offset) = g.choose(&taken_offsets.iter().collect::<Vec<_>>()) else {
                        continue;
                    };
                    actions.push(StallAction::Commit(offset));
                    taken_offsets.remove(&offset);
                }
                _ => {
                    // Use smaller wait times
                    let wait_time = Duration::from_millis(u64::arbitrary(g) % 10);
                    actions.push(StallAction::Wait(wait_time));
                }
            }
        }

        StallTestCase {
            actions,
            stall_threshold,
        }
    }
}
