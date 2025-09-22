use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::tests::TestStoreResult;
use crate::timers::store::tests::common::{
    add_trigger, get_key_triggers, insert_segment, remove_trigger,
};
use crate::timers::store::{Segment, TriggerStore};
use ahash::HashSet;
use std::fmt::Debug;
use tracing::Span;

/// Tests high-contention scenarios with many triggers on the same key
///
/// This test verifies that the store can correctly:
/// - Handle a large number of triggers with the same key
/// - Accurately track and retrieve all triggers for a high-contention key
/// - Correctly perform selective deletion on high-volume keys
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_key_contention<S>(store: &S, segment: &Segment) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Set up
    insert_segment(store, segment).await?;

    // Create a single key for contention testing
    let key = Key::from("contention-test-key".to_owned());
    let mut all_times = Vec::new();

    // Add a large number of triggers with the same key
    for i in 0_i32..100_i32 {
        let time = CompactDateTime::from(1_000_i32 + i);
        all_times.push(time);

        let trigger = Trigger::new(key.clone(), time, Span::current());

        add_trigger(store, segment, &trigger).await?;
    }

    // Verify all triggers were added
    let retrieved_times = get_key_triggers(store, &segment.id, &key).await?;
    let expected_times: HashSet<_> = all_times.iter().copied().collect();

    if retrieved_times != expected_times {
        return Err(format!(
            "Not all triggers added successfully. Expected: {expected_times:?}, Got: \
             {retrieved_times:?}"
        ));
    }

    // Test selective removal - remove every other trigger
    for (i, &time) in all_times.iter().enumerate() {
        if i % 2 == 0 {
            remove_trigger(store, segment, &key, time).await?;
        }
    }

    // Verify only expected triggers remain
    let remaining_times = get_key_triggers(store, &segment.id, &key).await?;
    let expected_remaining: HashSet<_> = all_times
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 != 0)
        .map(|(_, &time)| time)
        .collect();

    if remaining_times != expected_remaining {
        return Err(format!(
            "Unexpected triggers after selective removal. Expected: {expected_remaining:?}, Got: \
             {remaining_times:?}"
        ));
    }

    // Test the edge case of adding a trigger that was just deleted
    if let Some(&time) = all_times.first() {
        let trigger = Trigger::new(key.clone(), time, Span::current());

        add_trigger(store, segment, &trigger).await?;

        // Verify it was added back
        let updated_times = get_key_triggers(store, &segment.id, &key).await?;
        if !updated_times.contains(&time) {
            return Err("Failed to add back a previously deleted trigger".to_owned());
        }
    }

    Ok(())
}
