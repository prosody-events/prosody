#![allow(
    clippy::mutable_key_type,
    reason = "Trigger's ArcSwap field is excluded from hash/equality via Educe"
)]

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::Slab;
use crate::timers::store::tests::TestStoreResult;
use crate::timers::store::tests::common::{
    add_trigger, get_key_triggers, get_slab_triggers, insert_segment,
};
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::{TimerType, Trigger};
use ahash::HashSet;
use std::fmt::Debug;
use tracing::Span;

/// Tests operations across slab boundaries
///
/// This test verifies that:
/// - Triggers are correctly assigned to slabs based on their time
/// - Boundary conditions are handled correctly (times exactly at slab edges)
/// - Triggers can be correctly retrieved from different slabs
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_cross_slab_operations<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Insert the segment
    insert_segment(store, segment).await?;

    // Create a test key for boundary tests
    let key = Key::from("boundary-test-key".to_owned());
    let slab_size_seconds = segment.slab_size.seconds();

    // Can't test with zero slab size
    if slab_size_seconds == 0 {
        return Err("Slab size is zero, can't test boundaries".to_owned());
    }

    // Create times at and around slab boundaries
    let test_times = vec![
        // Start of first slab
        CompactDateTime::from(0u32), // Slab ID 0
        // Start of second slab
        CompactDateTime::from(slab_size_seconds), // Slab ID 1
        // Start of third slab
        CompactDateTime::from(2 * slab_size_seconds), // Slab ID 2
        // Middle of third slab
        CompactDateTime::from(2 * slab_size_seconds + slab_size_seconds / 2), // Slab ID 2
        // Start of fourth slab
        CompactDateTime::from(3 * slab_size_seconds), // Slab ID 3
    ];

    // Add triggers at all test times
    for &time in &test_times {
        let trigger = Trigger::new(key.clone(), time, TimerType::Application, Span::current());

        add_trigger(store, segment, &trigger).await?;
    }

    // Verify all triggers were added correctly
    let retrieved_times = get_key_triggers(store, &segment.id, &key).await?;
    let expected_times: HashSet<_> = test_times.iter().copied().collect();

    if retrieved_times != expected_times {
        return Err(format!(
            "Retrieved times don't match expected times. Expected: {expected_times:?}, Got: \
             {retrieved_times:?}"
        ));
    }

    // Check that each trigger is in its correct slab
    for &time in &test_times {
        let slab = Slab::from_time(segment.slab_size, time);
        let slab_triggers = get_slab_triggers(store, &slab).await?;

        // Find our trigger in this slab
        let found = slab_triggers.iter().any(|t| t.key == key && t.time == time);

        if !found {
            return Err(format!(
                "Trigger with time {time:?} not found in expected slab {slab:?}"
            ));
        }
    }

    // Explicitly test that slabs are different for times in different slabs
    if test_times.len() >= 2 {
        let time1 = test_times[0]; // At boundary
        let time2 = test_times[4]; // Middle of next slab

        let slab1 = Slab::from_time(segment.slab_size, time1);
        let slab2 = Slab::from_time(segment.slab_size, time2);

        if slab1.id() == slab2.id() {
            return Err("Expected different slab IDs for widely separated times".to_owned());
        }

        // Verify each trigger is only in its own slab
        let slab1_triggers = get_slab_triggers(store, &slab1).await?;
        let found_time2_in_slab1 = slab1_triggers
            .iter()
            .any(|t| t.key == key && t.time == time2);

        if found_time2_in_slab1 {
            return Err("Found time2 trigger incorrectly in slab1".to_owned());
        }

        let slab2_triggers = get_slab_triggers(store, &slab2).await?;
        let found_time1_in_slab2 = slab2_triggers
            .iter()
            .any(|t| t.key == key && t.time == time1);

        if found_time1_in_slab2 {
            return Err("Found time1 trigger incorrectly in slab2".to_owned());
        }
    }

    Ok(())
}
