use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::TriggerStore;
use crate::timers::store::tests::common::{
    add_trigger, insert_segment, remove_trigger, verify_store_state,
};
use crate::timers::store::tests::{TestStoreResult, TriggerTestInput};
use ahash::{HashMap, HashSet};
use std::fmt::Debug;

/// Tests sequential interleavings of insert and delete operations
///
/// This test verifies that:
/// - Operations interleaved in any order arrive at the expected state
/// - Both key and slab indices remain consistent after any sequence
/// - Common scenarios like re-insertion and removal work as expected
///
/// # Errors
///
/// Returns an error if the store operation fails.
pub async fn test_sequential_interleavings<S>(
    store: &S,
    input: &TriggerTestInput,
) -> TestStoreResult
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    if input.triggers.is_empty() {
        return Err("No triggers in test input".to_owned());
    }

    // Insert the segment
    insert_segment(store, &input.segment).await?;

    // Track expected state
    let mut expected_state: HashMap<Key, HashSet<CompactDateTime>> = HashMap::default();

    // Process all triggers in a deterministic order
    let shuffled_triggers: Vec<_> = input.triggers.clone();

    // Perform operations in an interleaved pattern
    for (i, trigger) in shuffled_triggers.iter().enumerate() {
        let key_times = expected_state.entry(trigger.key.clone()).or_default();
        let time_exists = key_times.contains(&trigger.time);

        // Use a cycle of 4 operations to test different patterns
        match i % 4 {
            // Insert new
            0 => {
                add_trigger(store, &input.segment, trigger).await?;
                key_times.insert(trigger.time);
            }
            // Delete if exists (or insert if doesn't)
            1 => {
                if time_exists {
                    remove_trigger(store, &input.segment, &trigger.key, trigger.time).await?;
                    key_times.remove(&trigger.time);
                } else {
                    add_trigger(store, &input.segment, trigger).await?;
                    key_times.insert(trigger.time);
                }
            }
            // Re-insert (idempotent operation)
            2 => {
                add_trigger(store, &input.segment, trigger).await?;
                key_times.insert(trigger.time);
            }
            // Delete regardless of existence
            _ => {
                remove_trigger(store, &input.segment, &trigger.key, trigger.time).await?;
                key_times.remove(&trigger.time);
            }
        }
    }

    // Verify the store matches our expected state after all operations
    verify_store_state(store, &input.segment, &expected_state).await?;

    Ok(())
}
