use super::{CassandraConfiguration, CassandraTriggerStore, cassandra_store};
use super::{InlineTimer, TimerState};
use crate::Key;
use crate::cassandra::CassandraStore;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::tests::prop_key_triggers::KeyTriggerTestInput;
use crate::timers::store::{Segment, SegmentId, SegmentVersion};
use crate::tracing::init_test_logging;
use crate::trigger_store_tests;
use color_eyre::Result;
use futures::TryStreamExt;
use futures::pin_mut;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::ops::RangeInclusive;
use std::time::Duration;
use strum::VariantArray;
use uuid::Uuid;

/// Creates a test configuration for Cassandra integration tests.
fn test_cassandra_config(keyspace: &str) -> CassandraConfiguration {
    CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: keyspace.to_owned(),
        user: None,
        password: None,
        retention: Duration::from_secs(10 * 60),
    }
}

// Determine the number of tests to run from an environment variable,
// defaulting to 25 if the variable is not set or invalid.
// Uses INTEGRATION_TESTS since these tests hit a real Cassandra database.
fn get_test_count() -> u64 {
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25)
}

/// Creates a test store and segment, returning `(store, segment_id)`.
async fn setup_test_store(name: &str) -> Result<(CassandraTriggerStore, SegmentId)> {
    setup_test_store_with_version(name, SegmentVersion::V3).await
}

/// Creates a test store and segment with the given version, returning `(store,
/// segment_id)`.
async fn setup_test_store_with_version(
    name: &str,
    version: SegmentVersion,
) -> Result<(CassandraTriggerStore, SegmentId)> {
    let slab_size = CompactDuration::new(60);
    let config = test_cassandra_config("prosody_test");
    let cassandra_store = CassandraStore::new(&config).await?;
    let store =
        CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

    let segment_id = SegmentId::from(Uuid::new_v4());
    let segment = Segment {
        id: segment_id,
        name: name.to_owned(),
        slab_size,
        version,
    };
    store.insert_segment(segment).await?;
    Ok((store, segment_id))
}

// Run the full suite of TriggerStore compliance tests on this implementation.
// Low-level tests use CassandraTriggerStore directly
// High-level tests use TableAdapter<CassandraTriggerStore>
trigger_store_tests!(
    CassandraTriggerStore,
    |slab_size| async move {
        let config = test_cassandra_config("prosody_test");
        let store = CassandraStore::new(&config).await?;
        CassandraTriggerStore::with_store(store, &config.keyspace, slab_size).await
    },
    crate::timers::store::adapter::TableAdapter<CassandraTriggerStore>,
    |slab_size| async move {
        let config = test_cassandra_config("prosody_test");
        cassandra_store(&config, slab_size).await
    },
    get_test_count()
);

#[tokio::test]
async fn test_slab_range_wrap_around_edge_cases() -> Result<()> {
    init_test_logging();

    let slab_size = CompactDuration::new(60); // 1 minute slabs
    let config = test_cassandra_config("prosody_test");
    let cassandra_store = CassandraStore::new(&config).await?;
    let store =
        CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

    let segment_id = SegmentId::from(Uuid::new_v4());
    let segment = Segment {
        id: segment_id,
        name: "test_segment".to_owned(),
        slab_size,
        version: SegmentVersion::V1,
    };

    // Insert the test segment
    store.insert_segment(segment.clone()).await?;

    // Test SlabId values that will cause wrap-around issues
    let boundary = 2_147_483_648u32; // 2^31, becomes negative in i32
    let test_slab_ids = vec![
        boundary - 2,    // 2147483646 -> positive i32
        boundary - 1,    // 2147483647 -> i32::MAX
        boundary,        // 2147483648 -> i32::MIN (negative)
        boundary + 1,    // 2147483649 -> negative i32
        SlabId::MAX - 1, // 4294967294 -> negative i32
        SlabId::MAX,     // 4294967295 -> -1 in i32
    ];

    // Insert test slabs
    for &slab_id in &test_slab_ids {
        let slab = Slab::new(segment_id, slab_id, segment.slab_size);
        store.insert_slab(&segment_id, slab).await?;
    }

    // Test Case 1: Range that crosses the wrap-around boundary
    let cross_boundary_range = RangeInclusive::new(boundary - 1, boundary + 1);
    let result: HashSet<SlabId> = store
        .get_slab_range(&segment_id, cross_boundary_range)
        .try_collect()
        .await?;

    let expected: HashSet<SlabId> = vec![boundary - 1, boundary, boundary + 1]
        .into_iter()
        .collect();
    assert_eq!(result, expected, "Cross-boundary range failed");

    // Test Case 2: Range entirely in "negative" i32 space (high u32 values)
    let high_range = RangeInclusive::new(boundary, SlabId::MAX);
    let result: HashSet<SlabId> = store
        .get_slab_range(&segment_id, high_range)
        .try_collect()
        .await?;

    let expected: HashSet<SlabId> = vec![boundary, boundary + 1, SlabId::MAX - 1, SlabId::MAX]
        .into_iter()
        .collect();
    assert_eq!(result, expected, "High range (negative i32) failed");

    // Test Case 3: Range entirely in "positive" i32 space (low u32 values)
    let low_range = RangeInclusive::new(boundary - 2, boundary - 1);
    let result: HashSet<SlabId> = store
        .get_slab_range(&segment_id, low_range)
        .try_collect()
        .await?;

    let expected: HashSet<SlabId> = vec![boundary - 2, boundary - 1].into_iter().collect();
    assert_eq!(result, expected, "Low range (positive i32) failed");

    // Test Case 4: Single element at boundary
    let single_boundary_range = RangeInclusive::new(boundary, boundary);
    let result: Vec<SlabId> = store
        .get_slab_range(&segment_id, single_boundary_range)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(result, vec![boundary], "Single boundary element failed");

    // Test Case 5: Invalid range (start > end in u32 space)
    let invalid_range = RangeInclusive::new(SlabId::MAX - 1, boundary - 2);
    let result: HashSet<SlabId> = store
        .get_slab_range(&segment_id, invalid_range)
        .try_collect()
        .await?;

    let expected: HashSet<SlabId> = HashSet::new();
    assert_eq!(result, expected, "Invalid range should return empty set");

    // Cleanup
    store.delete_segment(&segment_id).await?;

    Ok(())
}

#[tokio::test]
async fn test_simple_wrap_around() -> Result<()> {
    init_test_logging();

    let slab_size = CompactDuration::new(60);
    let config = test_cassandra_config("prosody_test");
    let cassandra_store = CassandraStore::new(&config).await?;
    let store =
        CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

    let segment_id = SegmentId::from(Uuid::new_v4());
    let segment = Segment {
        id: segment_id,
        name: "simple_test".to_owned(),
        slab_size,
        version: SegmentVersion::V1,
    };

    store.insert_segment(segment.clone()).await?;

    // The critical boundary: 2^31 = 2,147,483,648
    // Values below this are positive i32, values at/above are negative i32
    let boundary = 2_147_483_648u32;
    let test_ids = vec![boundary - 1, boundary, boundary + 1];

    // Insert test slabs
    for &slab_id in &test_ids {
        let slab = Slab::new(segment_id, slab_id, segment.slab_size);
        store.insert_slab(&segment_id, slab).await?;
    }

    // Test the critical range that crosses the wrap-around boundary
    let wrap_range = RangeInclusive::new(boundary - 1, boundary + 1);
    let mut results = Vec::new();

    let stream = store.get_slab_range(&segment_id, wrap_range);
    pin_mut!(stream);
    while let Some(result) = stream.next().await {
        results.push(result?);
    }

    // Sort results for consistent comparison
    results.sort_unstable();
    let mut expected = test_ids.clone();
    expected.sort_unstable();

    assert_eq!(results, expected, "Wrap-around range query failed");

    // Cleanup
    store.delete_segment(&segment_id).await?;

    Ok(())
}

/// Collects sorted times from `get_key_times`.
async fn collect_key_times(
    store: &CassandraTriggerStore,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
) -> Result<Vec<CompactDateTime>> {
    let mut times: Vec<CompactDateTime> = store
        .get_key_times(segment_id, timer_type, key)
        .try_collect()
        .await?;
    times.sort();
    Ok(times)
}

/// Collects sorted times from `get_key_triggers`.
async fn collect_trigger_times(
    store: &CassandraTriggerStore,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
) -> Result<Vec<CompactDateTime>> {
    let mut times: Vec<CompactDateTime> = store
        .get_key_triggers(segment_id, timer_type, key)
        .map_ok(|t| t.time)
        .try_collect()
        .await?;
    times.sort();
    Ok(times)
}

/// Collects sorted times for a specific type from
/// `get_key_triggers_all_types`.
async fn collect_all_types_times(
    store: &CassandraTriggerStore,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
) -> Result<Vec<CompactDateTime>> {
    let mut times: Vec<CompactDateTime> = store
        .get_key_triggers_all_types(segment_id, key)
        .try_filter_map(|t| async move {
            if t.timer_type == timer_type {
                Ok(Some(t.time))
            } else {
                Ok(None)
            }
        })
        .try_collect()
        .await?;
    times.sort();
    Ok(times)
}

/// Asserts that all three read paths return the expected sorted times for
/// a `(segment_id, key, timer_type)`.
async fn assert_key_reads(
    store: &CassandraTriggerStore,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
    expected: &[CompactDateTime],
    phase: &str,
) -> Result<()> {
    assert_eq!(
        collect_key_times(store, segment_id, timer_type, key).await?,
        expected,
        "{phase}: get_key_times"
    );
    assert_eq!(
        collect_trigger_times(store, segment_id, timer_type, key).await?,
        expected,
        "{phase}: get_key_triggers"
    );
    assert_eq!(
        collect_all_types_times(store, segment_id, timer_type, key).await?,
        expected,
        "{phase}: get_key_triggers_all_types"
    );
    Ok(())
}

/// Asserts the timer state matches the expected variant, with reads
/// verification.
///
/// Uses `resolve_state` (which populates the cache) so that the cache
/// assertion below is always valid. Post-V3 all states are cached.
async fn assert_state_and_reads(
    store: &CassandraTriggerStore,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
    expected_state: &TimerState,
    expected_times: &[CompactDateTime],
    phase: &str,
) -> Result<()> {
    let (state, _) = store.resolve_state(segment_id, key, timer_type).await?;
    match expected_state {
        TimerState::Absent => {
            assert_eq!(state, TimerState::Absent, "{phase}: expected Absent");
        }
        TimerState::Inline(expected) => {
            assert!(
                matches!(&state, TimerState::Inline(t) if t.time == expected.time),
                "{phase}: expected Inline({}), got {state:?}",
                expected.time
            );
        }
        TimerState::Overflow => {
            assert_eq!(state, TimerState::Overflow, "{phase}: expected Overflow");
        }
    }

    // Verify the cache entry for this specific type matches the expected state.
    // resolve_state above populated the cache for this type.
    let cache_key = (key.clone(), timer_type);
    let cached = store.state_cache.get(&cache_key);
    match expected_state {
        TimerState::Inline(expected) => {
            assert!(cached.is_some(), "{phase}: cache should have Inline entry");
            assert!(
                matches!(&cached, Some(TimerState::Inline(t)) if t.time == expected.time),
                "{phase}: cached state should be Inline({}), got {cached:?}",
                expected.time,
            );
        }
        TimerState::Overflow => {
            assert!(
                cached.is_some(),
                "{phase}: cache should have Overflow entry"
            );
            assert_eq!(
                cached,
                Some(TimerState::Overflow),
                "{phase}: cached state should be Overflow"
            );
        }
        TimerState::Absent => {
            assert!(cached.is_some(), "{phase}: cache should have Absent entry");
            assert_eq!(
                cached,
                Some(TimerState::Absent),
                "{phase}: cached state should be Absent"
            );
        }
    }

    assert_key_reads(store, segment_id, timer_type, key, expected_times, phase).await
}

/// Absent → Inline → Overflow → demotion via delete → Absent.
///
/// Covers: Absent→Inline (schedule), Inline→Overflow (insert/promote),
/// Overflow→Inline (delete demotion 2→1), Inline→Absent (delete 1→0).
#[tokio::test]
async fn test_state_transitions_schedule_promote_demote() -> Result<()> {
    init_test_logging();
    let (store, segment_id) = setup_test_store("promote_demote").await?;

    let key: Key = format!("state-test-{}", Uuid::new_v4()).into();
    let tt = TimerType::Application;
    let t1 = CompactDateTime::from(1_000_000u32);
    let t2 = CompactDateTime::from(2_000_000u32);
    let absent = TimerState::Absent;
    let inline_t1 = TimerState::Inline(InlineTimer {
        time: t1,
        span: HashMap::new(),
    });
    let inline_t2 = TimerState::Inline(InlineTimer {
        time: t2,
        span: HashMap::new(),
    });

    // Absent (0 timers)
    assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "absent").await?;

    // Absent → Inline via clear_and_schedule_key
    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &inline_t1, &[t1], "schedule").await?;

    // Inline → Overflow via insert_key_trigger (promotion)
    store
        .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t2, tt))
        .await?;
    assert_state_and_reads(
        &store,
        &segment_id,
        tt,
        &key,
        &TimerState::Overflow,
        &[t1, t2],
        "promote",
    )
    .await?;

    // Overflow → Inline(t2) via delete_key_trigger (2→1 demotion)
    store.delete_key_trigger(&segment_id, tt, &key, t1).await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &inline_t2, &[t2], "demote").await?;

    // Inline → Absent via delete_key_trigger (1→0)
    store.delete_key_trigger(&segment_id, tt, &key, t2).await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "delete last").await?;

    store.delete_segment(&segment_id).await?;
    Ok(())
}

/// Overflow→Inline via `clear_and_schedule_key`, `clear_key_triggers`
/// paths, Inline→Inline reschedule.
///
/// Covers: Overflow→Inline (`clear_and_schedule`), Inline→Absent (clear),
/// Overflow→Absent (clear), Inline→Inline (reschedule, 0 tombstones).
#[tokio::test]
async fn test_state_transitions_clear_and_reschedule() -> Result<()> {
    init_test_logging();
    let (store, segment_id) = setup_test_store("clear_reschedule").await?;

    let key: Key = format!("state-test-{}", Uuid::new_v4()).into();
    let tt = TimerType::Application;
    let t1 = CompactDateTime::from(1_000_000u32);
    let t2 = CompactDateTime::from(2_000_000u32);
    let t3 = CompactDateTime::from(3_000_000u32);
    let absent = TimerState::Absent;
    let inline_t2 = TimerState::Inline(InlineTimer {
        time: t2,
        span: HashMap::new(),
    });
    let inline_t3 = TimerState::Inline(InlineTimer {
        time: t3,
        span: HashMap::new(),
    });

    // Overflow → Inline via clear_and_schedule_key
    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    store
        .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t2, tt))
        .await?;
    assert_key_reads(&store, &segment_id, tt, &key, &[t1, t2], "overflow setup").await?;

    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t3, tt))
        .await?;
    assert_state_and_reads(
        &store,
        &segment_id,
        tt,
        &key,
        &inline_t3,
        &[t3],
        "overflow→inline",
    )
    .await?;

    // Inline → Absent via clear_key_triggers
    store.clear_key_triggers(&segment_id, tt, &key).await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "clear inline").await?;

    // Overflow → Absent via clear_key_triggers
    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    store
        .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t2, tt))
        .await?;
    store.clear_key_triggers(&segment_id, tt, &key).await?;
    assert_state_and_reads(
        &store,
        &segment_id,
        tt,
        &key,
        &absent,
        &[],
        "clear overflow",
    )
    .await?;

    // Inline → Inline via clear_and_schedule_key (no tombstone)
    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t2, tt))
        .await?;
    assert_state_and_reads(
        &store,
        &segment_id,
        tt,
        &key,
        &inline_t2,
        &[t2],
        "inline→inline",
    )
    .await?;

    store.delete_segment(&segment_id).await?;
    Ok(())
}

/// Absent → Inline via insert, Inline → Absent via delete.
///
/// Post-V3: inserting on any cold or warm cache with Absent state always
/// goes to `set_state_inline` (no more clustering-only path).
///
/// Covers: Absent→Inline (insert on cold cache), Inline→Absent (delete
/// match), Absent→Inline (insert on warm/cached Absent).
#[tokio::test]
async fn test_state_transitions_insert_and_delete() -> Result<()> {
    init_test_logging();
    let (store, segment_id) = setup_test_store("insert_delete").await?;

    let key: Key = format!("state-test-{}", Uuid::new_v4()).into();
    let tt = TimerType::Application;
    let t1 = CompactDateTime::from(1_000_000u32);
    let absent = TimerState::Absent;
    let inline_t1 = TimerState::Inline(InlineTimer {
        time: t1,
        span: HashMap::new(),
    });

    // Post-V3: cold insert with Absent state → set_state_inline directly.
    // State becomes Inline (not clustering-only).
    store
        .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    assert_state_and_reads(
        &store,
        &segment_id,
        tt,
        &key,
        &inline_t1,
        &[t1],
        "cold insert",
    )
    .await?;

    // Inline → Absent via delete_key_trigger (time match)
    store.delete_key_trigger(&segment_id, tt, &key, t1).await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "delete inline").await?;

    // Absent (cached) → Inline via insert_key_trigger
    store
        .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    assert_state_and_reads(
        &store,
        &segment_id,
        tt,
        &key,
        &inline_t1,
        &[t1],
        "cached absent→inline",
    )
    .await?;

    // Inline → Absent via delete_key_trigger (time match)
    store.delete_key_trigger(&segment_id, tt, &key, t1).await?;
    assert_state_and_reads(
        &store,
        &segment_id,
        tt,
        &key,
        &absent,
        &[],
        "delete inline 2",
    )
    .await?;

    store.delete_segment(&segment_id).await?;
    Ok(())
}

/// Verifies V2→V3 migration backfills key state for clustering-only data.
///
/// Simulates V2 data (clustering rows written without state MAP entry)
/// and verifies `backfill_key_state` correctly sets inline or overflow
/// state. After backfill, reads use the state-optimized paths.
#[tokio::test]
async fn test_pre_migration_reads_and_migration() -> Result<()> {
    init_test_logging();
    let (store, segment_id) = setup_test_store("pre_mig_reads").await?;

    let tt = TimerType::Application;
    let t1 = CompactDateTime::from(1_000_000u32);
    let t2 = CompactDateTime::from(2_000_000u32);

    // Scenario A: 1 clustering row (no state entry) → backfill → Inline.
    let key_a: Key = format!("pre-mig-a-{}", Uuid::new_v4()).into();
    store
        .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_a.clone(), t1, tt))
        .await?;
    // Pre-backfill: state is Absent (no MAP entry).
    // Uses fetch_state (DB-direct): add_key_trigger_clustering does not update the
    // cache.
    let state = store.fetch_state(&segment_id, &key_a, tt).await?;
    assert_eq!(
        state,
        TimerState::Absent,
        "A pre-backfill: state should be Absent"
    );

    // Backfill: 1 row → Inline.
    store.backfill_key_state(&segment_id, &key_a, tt).await?;
    // Uses fetch_state (DB-direct): backfill_key_state does not update the cache.
    let state = store.fetch_state(&segment_id, &key_a, tt).await?;
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t1),
        "A post-backfill: expected Inline(t1), got {state:?}"
    );
    assert_key_reads(&store, &segment_id, tt, &key_a, &[t1], "A backfilled").await?;

    // Scenario B: 2 clustering rows (no state entry) → backfill → Overflow.
    let key_b: Key = format!("pre-mig-b-{}", Uuid::new_v4()).into();
    store
        .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_b.clone(), t1, tt))
        .await?;
    store
        .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_b.clone(), t2, tt))
        .await?;
    // Uses fetch_state (DB-direct): add_key_trigger_clustering does not update the
    // cache.
    let state = store.fetch_state(&segment_id, &key_b, tt).await?;
    assert_eq!(
        state,
        TimerState::Absent,
        "B pre-backfill: state should be Absent"
    );

    // Backfill: 2 rows → Overflow.
    store.backfill_key_state(&segment_id, &key_b, tt).await?;
    // Uses fetch_state (DB-direct): backfill_key_state does not update the cache.
    let state = store.fetch_state(&segment_id, &key_b, tt).await?;
    assert_eq!(
        state,
        TimerState::Overflow,
        "B post-backfill: expected Overflow"
    );
    assert_key_reads(&store, &segment_id, tt, &key_b, &[t1, t2], "B backfilled").await?;

    // Scenario C: already has state (idempotency) → backfill is no-op.
    let key_c: Key = format!("pre-mig-c-{}", Uuid::new_v4()).into();
    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key_c.clone(), t1, tt))
        .await?;
    // State is already Inline(t1). backfill should not change it.
    store.backfill_key_state(&segment_id, &key_c, tt).await?;
    // Uses fetch_state (DB-direct): verify backfill did not overwrite existing
    // state.
    let state = store.fetch_state(&segment_id, &key_c, tt).await?;
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t1),
        "C idempotency: expected Inline(t1) unchanged, got {state:?}"
    );

    store.delete_segment(&segment_id).await?;
    Ok(())
}

/// Verifies V2→V3 migration handles edge cases: stale slab entries and
/// re-running migration (idempotency via version check).
#[tokio::test]
async fn test_pre_migration_mutations() -> Result<()> {
    init_test_logging();
    let (store, segment_id) = setup_test_store("pre_mig_mutations").await?;

    let tt = TimerType::Application;
    let t1 = CompactDateTime::from(1_000_000u32);
    let t2 = CompactDateTime::from(2_000_000u32);
    let t3 = CompactDateTime::from(3_000_000u32);

    // Scenario D: clear_key_triggers correctly removes both state and
    // clustering rows. After clear, Absent state is cached.
    let key_d: Key = format!("pre-mig-d-{}", Uuid::new_v4()).into();
    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key_d.clone(), t1, tt))
        .await?;
    store
        .insert_key_trigger(&segment_id, Trigger::for_testing(key_d.clone(), t2, tt))
        .await?;
    store.clear_key_triggers(&segment_id, tt, &key_d).await?;
    assert_key_reads(&store, &segment_id, tt, &key_d, &[], "D cleared").await?;
    let (state, _) = store.resolve_state(&segment_id, &key_d, tt).await?;
    assert_eq!(
        state,
        TimerState::Absent,
        "D: state should be Absent after clear"
    );

    // Scenario E: delete_key_trigger with Overflow demotion.
    let key_e: Key = format!("pre-mig-e-{}", Uuid::new_v4()).into();
    store
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key_e.clone(), t1, tt))
        .await?;
    store
        .insert_key_trigger(&segment_id, Trigger::for_testing(key_e.clone(), t2, tt))
        .await?;
    store
        .insert_key_trigger(&segment_id, Trigger::for_testing(key_e.clone(), t3, tt))
        .await?;
    assert_key_reads(
        &store,
        &segment_id,
        tt,
        &key_e,
        &[t1, t2, t3],
        "E overflow setup",
    )
    .await?;
    store
        .delete_key_trigger(&segment_id, tt, &key_e, t1)
        .await?;
    assert_key_reads(&store, &segment_id, tt, &key_e, &[t2, t3], "E delete one").await?;
    store
        .delete_key_trigger(&segment_id, tt, &key_e, t2)
        .await?;
    assert_key_reads(&store, &segment_id, tt, &key_e, &[t3], "E demote to inline").await?;
    let (state, _) = store.resolve_state(&segment_id, &key_e, tt).await?;
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t3),
        "E: expected Inline(t3) after demotion, got {state:?}"
    );

    store.delete_segment(&segment_id).await?;
    Ok(())
}

/// Verifies `clear_key_triggers_all_types` clears both inline and
/// overflow states across different timer types simultaneously.
#[tokio::test]
async fn test_clear_all_types_clears_inline_and_overflow() -> Result<()> {
    init_test_logging();
    let (store, segment_id) =
        setup_test_store_with_version("clear_all_types", SegmentVersion::V2).await?;

    let key: Key = format!("clear-all-{}", Uuid::new_v4()).into();
    let t1 = CompactDateTime::from(1_000_000u32);
    let t2 = CompactDateTime::from(2_000_000u32);

    // Set up: Application inline (1 timer), DeferredMessage overflow (2 timers).
    store
        .clear_and_schedule_key(
            &segment_id,
            Trigger::for_testing(key.clone(), t1, TimerType::Application),
        )
        .await?;
    store
        .clear_and_schedule_key(
            &segment_id,
            Trigger::for_testing(key.clone(), t1, TimerType::DeferredMessage),
        )
        .await?;
    store
        .insert_key_trigger(
            &segment_id,
            Trigger::for_testing(key.clone(), t2, TimerType::DeferredMessage),
        )
        .await?;

    // Verify setup.
    assert_key_reads(
        &store,
        &segment_id,
        TimerType::Application,
        &key,
        &[t1],
        "setup app",
    )
    .await?;
    assert_key_reads(
        &store,
        &segment_id,
        TimerType::DeferredMessage,
        &key,
        &[t1, t2],
        "setup dm",
    )
    .await?;

    // Clear all types.
    store
        .clear_key_triggers_all_types(&segment_id, &key)
        .await?;

    // Verify all types are Absent with no data.
    for &variant in TimerType::VARIANTS {
        let (state, _) = store.resolve_state(&segment_id, &key, variant).await?;
        assert_eq!(state, TimerState::Absent, "{variant:?} should be Absent");
        assert_key_reads(
            &store,
            &segment_id,
            variant,
            &key,
            &[],
            &format!("{variant:?}"),
        )
        .await?;
    }

    store.delete_segment(&segment_id).await?;
    Ok(())
}

/// Verifies the inline timer state machine lifecycle:
/// Absent → Inline → Inline (replacement) → Overflow (after promotion)
/// → Inline
///
/// This confirms the tombstone-free optimization actually transitions
/// through the expected states, and that type isolation holds between
/// timer types.
#[tokio::test]
async fn test_inline_state_round_trip() -> Result<()> {
    init_test_logging();
    let (store, segment_id) =
        setup_test_store_with_version("inline_state_round_trip", SegmentVersion::V1).await?;

    let key: Key = format!("inline-test-{}", Uuid::new_v4()).into();
    let t1 = CompactDateTime::from(1_000_000u32);
    let t2 = CompactDateTime::from(2_000_000u32);
    let t3 = CompactDateTime::from(3_000_000u32);
    let t4 = CompactDateTime::from(4_000_000u32);

    // Phase 1: Initial state — no data, state is Absent.
    let (state, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert_eq!(state, TimerState::Absent, "phase 1: expected Absent");

    // Phase 2: clear_and_schedule_key(t1) → Inline(t1)
    let trigger1 = Trigger::for_testing(key.clone(), t1, TimerType::Application);
    store.clear_and_schedule_key(&segment_id, trigger1).await?;

    let (state, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t1),
        "phase 2: expected Inline(t1), got {state:?}"
    );

    // Phase 3: clear_and_schedule_key(t2) → Inline(t2) (Inline→Inline, no
    // tombstone)
    let trigger2 = Trigger::for_testing(key.clone(), t2, TimerType::Application);
    store.clear_and_schedule_key(&segment_id, trigger2).await?;

    let (state, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t2),
        "phase 3: expected Inline(t2), got {state:?}"
    );

    // Phase 4: insert_key_trigger(t3) promotes inline to clustering → state
    // becomes Overflow
    let trigger3 = Trigger::for_testing(key.clone(), t3, TimerType::Application);
    store.insert_key_trigger(&segment_id, trigger3).await?;

    let (state, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert_eq!(
        state,
        TimerState::Overflow,
        "phase 4: expected Overflow after promotion"
    );

    // Phase 5: clear_and_schedule_key(t4) on an overflow key → back to
    // Inline(t4)
    let trigger4 = Trigger::for_testing(key.clone(), t4, TimerType::Application);
    store.clear_and_schedule_key(&segment_id, trigger4).await?;

    let (state, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t4),
        "phase 5: expected Inline(t4), got {state:?}"
    );

    // Phase 6: Verify get_key_times returns exactly [t4].
    let times: Vec<CompactDateTime> = store
        .get_key_times(&segment_id, TimerType::Application, &key)
        .try_collect()
        .await?;
    assert_eq!(
        times,
        vec![t4],
        "phase 6: get_key_times should return exactly [t4]"
    );

    // Phase 7: Type isolation — DeferredMessage state is still Absent.
    let (state, _) = store
        .resolve_state(&segment_id, &key, TimerType::DeferredMessage)
        .await?;
    assert_eq!(
        state,
        TimerState::Absent,
        "phase 7: DeferredMessage state should be Absent"
    );

    // Phase 8: Cleanup — clear_key_triggers_all_types resets everything.
    store
        .clear_key_triggers_all_types(&segment_id, &key)
        .await?;

    let (state, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert_eq!(
        state,
        TimerState::Absent,
        "phase 8: expected Absent after cleanup"
    );

    store.delete_segment(&segment_id).await?;

    Ok(())
}

/// Property test verifying the timer state invariant:
///
/// - **1 timer** for a `(segment_id, key, timer_type)` → state must be `Inline`
///   holding it, no clustering rows.
/// - **>1 timer** → state must be `Overflow`, all timers in clustering rows.
/// - **0 timers** → state must be `Absent`.
///
/// Applies a random sequence of operations then inspects every
/// `(segment_id, key, timer_type)` combination against the reference model.
#[test]
fn test_prop_timer_state_invariant() {
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::{QuickCheck, TestResult};
    use tracing::Instrument;

    fn prop(input: KeyTriggerTestInput) -> TestResult {
        let runtime = &*TEST_RUNTIME;
        let span = tracing::Span::current();

        let slab_size = input.slab_size;
        let store = match runtime.block_on(
            async {
                let config = test_cassandra_config("prosody_test");
                let store = CassandraStore::new(&config).await?;
                CassandraTriggerStore::with_store(store, &config.keyspace, slab_size).await
            }
            .instrument(span.clone()),
        ) {
            Ok(s) => s,
            Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
        };

        match runtime
            .block_on(async { prop_timer_state_invariant(&store, input).await }.instrument(span))
        {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(format!("{e:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(KeyTriggerTestInput) -> TestResult);
}

/// Verifies that `CassandraTriggerStoreProvider` creates stores with
/// independent caches but a shared Cassandra session.
///
/// 1. Create provider, call `create_store` twice.
/// 2. Write via store A → store A cache is warm.
/// 3. Store B cache is cold (no entry for same key).
/// 4. Store B can still read the data via DB (shared session).
#[tokio::test]
async fn test_provider_creates_independent_stores() -> Result<()> {
    use crate::timers::store::TriggerStoreProvider;
    use crate::timers::store::cassandra::CassandraTriggerStoreProvider;

    init_test_logging();

    let slab_size = CompactDuration::new(60);
    let config = test_cassandra_config("prosody_test");
    let provider = CassandraTriggerStoreProvider::with_store(
        CassandraStore::new(&config).await?,
        &config.keyspace,
        slab_size,
    )
    .await?;

    // Create two independent stores from the same provider.
    let store_a = provider.create_store("test-topic".into(), 0, "test-group");
    let store_b = provider.create_store("test-topic".into(), 1, "test-group");

    // Access inner CassandraTriggerStore for direct TriggerOperations use.
    let ops_a = store_a.operations();
    let ops_b = store_b.operations();

    // Set up a shared segment (both stores share the session, so both see it).
    let segment_id = SegmentId::from(Uuid::new_v4());
    let segment = Segment {
        id: segment_id,
        name: "provider_independent".to_owned(),
        slab_size,
        version: SegmentVersion::V2,
    };
    ops_a.insert_segment(segment).await?;

    let key: Key = format!("provider-test-{}", Uuid::new_v4()).into();
    let tt = TimerType::Application;
    let t1 = CompactDateTime::from(1_000_000u32);

    // Write via store A: clear_and_schedule_key populates store A's cache.
    ops_a
        .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
        .await?;

    // Store A cache is warm: Inline(t1).
    let cache_key = (key.clone(), tt);
    let cached_a = ops_a.state_cache.get(&cache_key);
    assert!(
        matches!(&cached_a, Some(TimerState::Inline(timer)) if timer.time == t1),
        "store A cache should have Inline(t1), got {cached_a:?}"
    );

    // Store B cache is cold: no entry for this key.
    let cached_b = ops_b.state_cache.get(&cache_key);
    assert!(
        cached_b.is_none(),
        "store B cache should be cold (None), got {cached_b:?}"
    );

    // Store B can still read the data (proves shared session).
    let times: Vec<CompactDateTime> = ops_b
        .get_key_times(&segment_id, tt, &key)
        .try_collect()
        .await?;
    assert_eq!(times, vec![t1], "store B should read t1 via shared session");

    // After the read, store B's cache should now be warm (Inline cached from DB).
    let warm_b = ops_b.state_cache.get(&cache_key);
    assert!(
        matches!(&warm_b, Some(TimerState::Inline(t)) if t.time == t1),
        "store B cache should be warm after read, got {warm_b:?}"
    );

    // Cleanup.
    ops_a.delete_segment(&segment_id).await?;

    Ok(())
}

/// Applies operations from [`KeyTriggerTestInput`] and verifies the
/// timer state invariant holds for every `(segment_id, key, timer_type)`.
async fn prop_timer_state_invariant(
    store: &CassandraTriggerStore,
    input: KeyTriggerTestInput,
) -> Result<()> {
    use crate::timers::store::tests::prop_key_triggers::{KeyTriggerModel, KeyTriggerOperation};

    let key_pool = ["key-a", "key-b", "key-c"];

    // Clean up before test
    for segment_id in &input.segment_ids {
        for key_str in &key_pool {
            let key = Key::from(*key_str);
            store.clear_key_triggers_all_types(segment_id, &key).await?;
        }
    }

    // Apply all operations to both model and store
    let mut model = KeyTriggerModel::new();
    for op in &input.operations {
        model.apply(op);
        match op {
            KeyTriggerOperation::Insert {
                segment_id,
                trigger,
            } => {
                store
                    .insert_key_trigger(segment_id, trigger.clone())
                    .await?;
            }
            KeyTriggerOperation::Delete {
                segment_id,
                timer_type,
                key,
                time,
            } => {
                store
                    .delete_key_trigger(segment_id, *timer_type, key, *time)
                    .await?;
            }
            KeyTriggerOperation::ClearByType {
                segment_id,
                timer_type,
                key,
            } => {
                store
                    .clear_key_triggers(segment_id, *timer_type, key)
                    .await?;
            }
            KeyTriggerOperation::ClearAllTypes { segment_id, key } => {
                store.clear_key_triggers_all_types(segment_id, key).await?;
            }
            KeyTriggerOperation::ClearAndSchedule {
                segment_id,
                trigger,
            } => {
                store
                    .clear_and_schedule_key(segment_id, trigger.clone())
                    .await?;
            }
            KeyTriggerOperation::GetTimes { .. }
            | KeyTriggerOperation::GetTriggers { .. }
            | KeyTriggerOperation::GetAllTypes { .. } => {}
        }
    }

    // Verify timer state invariant for every (segment_id, key, timer_type)
    for (segment_id, key) in &model.all_keys() {
        for &timer_type in TimerType::VARIANTS {
            let expected_count = model.get_times(segment_id, timer_type, key).len();
            let (timer_state, _) = store.resolve_state(segment_id, key, timer_type).await?;

            match expected_count {
                0 => {
                    assert!(
                        matches!(timer_state, TimerState::Absent),
                        "Invariant violation: 0 timers for ({segment_id}, {key}, {timer_type:?}) \
                         but state is {timer_state:?}"
                    );
                }
                1 => {
                    let expected_time = model.get_times(segment_id, timer_type, key)[0];
                    assert!(
                        matches!(&timer_state, TimerState::Inline(t) if t.time == expected_time),
                        "Invariant violation: exactly 1 timer (time={expected_time:?}) for \
                         ({segment_id}, {key}, {timer_type:?}) but state is {timer_state:?} — \
                         expected Inline"
                    );
                }
                n => {
                    assert!(
                        matches!(timer_state, TimerState::Overflow),
                        "Invariant violation: {n} timers for ({segment_id}, {key}, \
                         {timer_type:?}) but state is {timer_state:?} — expected Overflow"
                    );
                }
            }
        }
    }

    Ok(())
}
