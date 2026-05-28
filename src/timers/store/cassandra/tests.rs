//! Integration tests for [`CassandraTriggerStore`].
//!
//! These tests run against a real Cassandra node and are skipped automatically
//! when one isn't available. They exist because the V3 state-column logic
//! involves conditional write paths (inline vs. overflow vs. absent) and
//! concurrent mutex semantics that are hard to exercise meaningfully with a
//! mock. Running against actual Cassandra also catches serialisation bugs,
//! TTL edge-cases, and UDT schema mismatches that unit tests cannot.
//!
//! [`CassandraTriggerStore`]: super::CassandraTriggerStore

use super::{CassandraConfiguration, CassandraTriggerStore, cassandra_store};
use super::{InlineTimer, TimerState};
use crate::Key;
use crate::cassandra::CassandraStore;
use crate::otel::SpanRelation;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::tests::prop_key_triggers::{KeyTriggerOperation, KeyTriggerTestInput};
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
        retention: Duration::from_mins(10),
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
    let segment_id = SegmentId::from(Uuid::new_v4());
    let segment = Segment {
        id: segment_id,
        name: name.to_owned(),
        slab_size,
        version,
    };
    let config = test_cassandra_config("prosody_test");
    let cassandra_store = CassandraStore::new(&config).await?;
    let store = CassandraTriggerStore::with_store(
        cassandra_store,
        &config.keyspace,
        segment.clone(),
        SpanRelation::default(),
    )
    .await?;
    store.insert_segment().await?;
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
        let segment = Segment {
            id: Uuid::new_v4(),
            name: String::new(),
            slab_size,
            version: SegmentVersion::V3,
        };
        CassandraTriggerStore::with_store(store, &config.keyspace, segment, SpanRelation::default())
            .await
    },
    crate::timers::store::adapter::TableAdapter<CassandraTriggerStore>,
    |slab_size| async move {
        let config = test_cassandra_config("prosody_test");
        let segment = Segment {
            id: Uuid::new_v4(),
            name: String::new(),
            slab_size,
            version: SegmentVersion::V3,
        };
        cassandra_store(&config, segment, SpanRelation::default()).await
    },
    get_test_count()
);

#[tokio::test]
async fn test_slab_range_wrap_around_edge_cases() -> Result<()> {
    init_test_logging();

    let slab_size = CompactDuration::new(60); // 1 minute slabs
    let (store, _segment_id) =
        setup_test_store_with_version("test_segment", SegmentVersion::V1).await?;

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
        let slab = Slab::new(slab_id, slab_size);
        store.insert_slab(slab).await?;
    }

    // Test Case 1: Range that crosses the wrap-around boundary
    let cross_boundary_range = RangeInclusive::new(boundary - 1, boundary + 1);
    let result: HashSet<SlabId> = store
        .get_slab_range(cross_boundary_range)
        .try_collect()
        .await?;

    let expected: HashSet<SlabId> = vec![boundary - 1, boundary, boundary + 1]
        .into_iter()
        .collect();
    assert_eq!(result, expected, "Cross-boundary range failed");

    // Test Case 2: Range entirely in "negative" i32 space (high u32 values)
    let high_range = RangeInclusive::new(boundary, SlabId::MAX);
    let result: HashSet<SlabId> = store.get_slab_range(high_range).try_collect().await?;

    let expected: HashSet<SlabId> = vec![boundary, boundary + 1, SlabId::MAX - 1, SlabId::MAX]
        .into_iter()
        .collect();
    assert_eq!(result, expected, "High range (negative i32) failed");

    // Test Case 3: Range entirely in "positive" i32 space (low u32 values)
    let low_range = RangeInclusive::new(boundary - 2, boundary - 1);
    let result: HashSet<SlabId> = store.get_slab_range(low_range).try_collect().await?;

    let expected: HashSet<SlabId> = vec![boundary - 2, boundary - 1].into_iter().collect();
    assert_eq!(result, expected, "Low range (positive i32) failed");

    // Test Case 4: Single element at boundary
    let single_boundary_range = RangeInclusive::new(boundary, boundary);
    let result: Vec<SlabId> = store
        .get_slab_range(single_boundary_range)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(result, vec![boundary], "Single boundary element failed");

    // Test Case 5: Invalid range (start > end in u32 space)
    let invalid_range = RangeInclusive::new(SlabId::MAX - 1, boundary - 2);
    let result: HashSet<SlabId> = store.get_slab_range(invalid_range).try_collect().await?;

    let expected: HashSet<SlabId> = HashSet::new();
    assert_eq!(result, expected, "Invalid range should return empty set");

    // Cleanup
    store.delete_segment().await?;

    Ok(())
}

#[tokio::test]
async fn test_simple_wrap_around() -> Result<()> {
    init_test_logging();

    let slab_size = CompactDuration::new(60);
    let (store, _segment_id) =
        setup_test_store_with_version("simple_test", SegmentVersion::V1).await?;

    // The critical boundary: 2^31 = 2,147,483,648
    // Values below this are positive i32, values at/above are negative i32
    let boundary = 2_147_483_648u32;
    let test_ids = vec![boundary - 1, boundary, boundary + 1];

    // Insert test slabs
    for &slab_id in &test_ids {
        let slab = Slab::new(slab_id, slab_size);
        store.insert_slab(slab).await?;
    }

    // Test the critical range that crosses the wrap-around boundary
    let wrap_range = RangeInclusive::new(boundary - 1, boundary + 1);
    let mut results = Vec::new();

    let stream = store.get_slab_range(wrap_range);
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
    store.delete_segment().await?;

    Ok(())
}

/// Collects sorted times from `get_key_times`.
async fn collect_key_times(
    store: &CassandraTriggerStore,
    timer_type: TimerType,
    key: &Key,
) -> Result<Vec<CompactDateTime>> {
    let mut times: Vec<CompactDateTime> =
        store.get_key_times(timer_type, key).try_collect().await?;
    times.sort();
    Ok(times)
}

/// Collects sorted times from `get_key_triggers`.
async fn collect_trigger_times(
    store: &CassandraTriggerStore,
    timer_type: TimerType,
    key: &Key,
) -> Result<Vec<CompactDateTime>> {
    let mut times: Vec<CompactDateTime> = store
        .get_key_triggers(timer_type, key)
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
    timer_type: TimerType,
    key: &Key,
) -> Result<Vec<CompactDateTime>> {
    let mut times: Vec<CompactDateTime> = store
        .get_key_triggers_all_types(key)
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
/// a `(key, timer_type)`.
async fn assert_key_reads(
    store: &CassandraTriggerStore,
    timer_type: TimerType,
    key: &Key,
    expected: &[CompactDateTime],
    phase: &str,
) -> Result<()> {
    assert_eq!(
        collect_key_times(store, timer_type, key).await?,
        expected,
        "{phase}: get_key_times"
    );
    assert_eq!(
        collect_trigger_times(store, timer_type, key).await?,
        expected,
        "{phase}: get_key_triggers"
    );
    assert_eq!(
        collect_all_types_times(store, timer_type, key).await?,
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
    let (handle, _) = store.resolve_state(segment_id, key, timer_type).await?;
    let state = handle.lock().await.clone();
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
    let cached = store
        .state_cache
        .get(&cache_key)
        .map(|h| h.try_lock().map(|g| g.clone()));
    match expected_state {
        TimerState::Inline(expected) => {
            assert!(cached.is_some(), "{phase}: cache should have Inline entry");
            assert!(
                matches!(&cached, Some(Ok(TimerState::Inline(t))) if t.time == expected.time),
                "{phase}: cached state should be Inline({}), got {cached:?}",
                expected.time,
            );
        }
        TimerState::Overflow => {
            assert!(
                cached.is_some(),
                "{phase}: cache should have Overflow entry"
            );
            assert!(
                matches!(cached, Some(Ok(TimerState::Overflow))),
                "{phase}: cached state should be Overflow, got {cached:?}"
            );
        }
        TimerState::Absent => {
            assert!(cached.is_some(), "{phase}: cache should have Absent entry");
            assert!(
                matches!(cached, Some(Ok(TimerState::Absent))),
                "{phase}: cached state should be Absent, got {cached:?}"
            );
        }
    }

    assert_key_reads(store, timer_type, key, expected_times, phase).await
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
        tag: 0,
    });
    let inline_t2 = TimerState::Inline(InlineTimer {
        time: t2,
        span: HashMap::new(),
        tag: 0,
    });

    // Absent (0 timers)
    assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "absent").await?;

    // Absent → Inline via clear_and_schedule_key
    store
        .clear_and_schedule_key(Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &inline_t1, &[t1], "schedule").await?;

    // Inline → Overflow via upsert_key_trigger (promotion)
    store
        .upsert_key_trigger(Trigger::for_testing(key.clone(), t2, tt))
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
    store.delete_key_trigger(tt, &key, t1).await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &inline_t2, &[t2], "demote").await?;

    // Inline → Absent via delete_key_trigger (1→0)
    store.delete_key_trigger(tt, &key, t2).await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "delete last").await?;

    store.delete_segment().await?;
    Ok(())
}

/// Regression: `Inline→Overflow` promotion must preserve the old timer's
/// tag, and the tag must survive a subsequent `Overflow→Inline` demotion.
///
/// The commit oracle classifies a WAL entry by comparing its tag against
/// the live row's tag. If promotion zeroes the old tag, `current_tag` will
/// return `0` for a still-pending timer — the oracle reads "tag mismatch"
/// and (incorrectly) concludes the timer was committed-and-rescheduled.
/// Demotion then bakes the wrong tag back into Inline state, making the
/// loss permanent.
#[tokio::test]
async fn test_promote_preserves_tag() -> Result<()> {
    use crate::timers::store::TriggerStore;
    use crate::timers::store::adapter::TableAdapter;
    init_test_logging();
    let (store, _segment_id) = setup_test_store("promote_tag").await?;
    let store = TableAdapter::new(store);

    let key: Key = format!("promote-tag-{}", Uuid::new_v4()).into();
    let tt = TimerType::Application;
    let t1 = CompactDateTime::from(1_000_000u32);
    let t2 = CompactDateTime::from(2_000_000u32);

    // First trigger lands Inline with random tag1.
    let trigger1 = Trigger::new(key.clone(), t1, tt, tracing::Span::current());
    let tag1 = trigger1.tag;
    store.add_trigger(trigger1).await?;
    assert_eq!(
        store.current_tag(&key, t1, tt).await?,
        Some(tag1),
        "tag1 must be queryable while Inline"
    );

    // Second trigger triggers Inline→Overflow promotion. The old (t1)
    // timer is moved into a clustering row; this is where the bug zeroed
    // the tag.
    let trigger2 = Trigger::new(key.clone(), t2, tt, tracing::Span::current());
    let tag2 = trigger2.tag;
    store.add_trigger(trigger2).await?;

    assert_eq!(
        store.current_tag(&key, t1, tt).await?,
        Some(tag1),
        "promotion must preserve tag1 in the clustering row"
    );
    assert_eq!(
        store.current_tag(&key, t2, tt).await?,
        Some(tag2),
        "new clustering row must carry tag2"
    );

    // Demoting back to Inline (delete t2) reads the t1 clustering row's
    // tag into Inline state. If promotion wrote 0 above, that 0 is now
    // permanently stamped into Inline.
    store.remove_trigger(&key, t2, tt).await?;
    assert_eq!(
        store.current_tag(&key, t1, tt).await?,
        Some(tag1),
        "demotion must round-trip tag1 from clustering row back to Inline state"
    );

    store.remove_trigger(&key, t1, tt).await?;
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
        tag: 0,
    });
    let inline_t3 = TimerState::Inline(InlineTimer {
        time: t3,
        span: HashMap::new(),
        tag: 0,
    });

    // Overflow → Inline via clear_and_schedule_key
    store
        .clear_and_schedule_key(Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    store
        .upsert_key_trigger(Trigger::for_testing(key.clone(), t2, tt))
        .await?;
    assert_key_reads(&store, tt, &key, &[t1, t2], "overflow setup").await?;

    store
        .clear_and_schedule_key(Trigger::for_testing(key.clone(), t3, tt))
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
    store.clear_key_triggers(tt, &key).await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "clear inline").await?;

    // Overflow → Absent via clear_key_triggers
    store
        .clear_and_schedule_key(Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    store
        .upsert_key_trigger(Trigger::for_testing(key.clone(), t2, tt))
        .await?;
    store.clear_key_triggers(tt, &key).await?;
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
        .clear_and_schedule_key(Trigger::for_testing(key.clone(), t1, tt))
        .await?;
    store
        .clear_and_schedule_key(Trigger::for_testing(key.clone(), t2, tt))
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

    store.delete_segment().await?;
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
        tag: 0,
    });

    // Post-V3: cold insert with Absent state → set_state_inline directly.
    // State becomes Inline (not clustering-only).
    store
        .upsert_key_trigger(Trigger::for_testing(key.clone(), t1, tt))
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
    store.delete_key_trigger(tt, &key, t1).await?;
    assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "delete inline").await?;

    // Absent (cached) → Inline via upsert_key_trigger
    store
        .upsert_key_trigger(Trigger::for_testing(key.clone(), t1, tt))
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
    store.delete_key_trigger(tt, &key, t1).await?;
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

    store.delete_segment().await?;
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
    assert_key_reads(&store, tt, &key_a, &[t1], "A backfilled").await?;

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
    assert_key_reads(&store, tt, &key_b, &[t1, t2], "B backfilled").await?;

    // Scenario C: already has state (idempotency) → backfill is no-op.
    let key_c: Key = format!("pre-mig-c-{}", Uuid::new_v4()).into();
    store
        .clear_and_schedule_key(Trigger::for_testing(key_c.clone(), t1, tt))
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

    store.delete_segment().await?;
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
        .clear_and_schedule_key(Trigger::for_testing(key_d.clone(), t1, tt))
        .await?;
    store
        .upsert_key_trigger(Trigger::for_testing(key_d.clone(), t2, tt))
        .await?;
    store.clear_key_triggers(tt, &key_d).await?;
    assert_key_reads(&store, tt, &key_d, &[], "D cleared").await?;
    let (handle, _) = store.resolve_state(&segment_id, &key_d, tt).await?;
    assert_eq!(
        *handle.lock().await,
        TimerState::Absent,
        "D: state should be Absent after clear"
    );

    // Scenario E: delete_key_trigger with Overflow demotion.
    let key_e: Key = format!("pre-mig-e-{}", Uuid::new_v4()).into();
    store
        .clear_and_schedule_key(Trigger::for_testing(key_e.clone(), t1, tt))
        .await?;
    store
        .upsert_key_trigger(Trigger::for_testing(key_e.clone(), t2, tt))
        .await?;
    store
        .upsert_key_trigger(Trigger::for_testing(key_e.clone(), t3, tt))
        .await?;
    assert_key_reads(&store, tt, &key_e, &[t1, t2, t3], "E overflow setup").await?;
    store.delete_key_trigger(tt, &key_e, t1).await?;
    assert_key_reads(&store, tt, &key_e, &[t2, t3], "E delete one").await?;
    store.delete_key_trigger(tt, &key_e, t2).await?;
    assert_key_reads(&store, tt, &key_e, &[t3], "E demote to inline").await?;
    let (handle, _) = store.resolve_state(&segment_id, &key_e, tt).await?;
    let state = handle.lock().await.clone();
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t3),
        "E: expected Inline(t3) after demotion, got {state:?}"
    );

    store.delete_segment().await?;
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
        .clear_and_schedule_key(Trigger::for_testing(
            key.clone(),
            t1,
            TimerType::Application,
        ))
        .await?;
    store
        .clear_and_schedule_key(Trigger::for_testing(
            key.clone(),
            t1,
            TimerType::DeferredMessage,
        ))
        .await?;
    store
        .upsert_key_trigger(Trigger::for_testing(
            key.clone(),
            t2,
            TimerType::DeferredMessage,
        ))
        .await?;

    // Verify setup.
    assert_key_reads(&store, TimerType::Application, &key, &[t1], "setup app").await?;
    assert_key_reads(
        &store,
        TimerType::DeferredMessage,
        &key,
        &[t1, t2],
        "setup dm",
    )
    .await?;

    // Clear all types.
    store.clear_key_triggers_all_types(&key).await?;

    // Verify all types are Absent with no data.
    for &variant in TimerType::VARIANTS {
        let (handle, _) = store.resolve_state(&segment_id, &key, variant).await?;
        assert_eq!(
            *handle.lock().await,
            TimerState::Absent,
            "{variant:?} should be Absent"
        );
        assert_key_reads(&store, variant, &key, &[], &format!("{variant:?}")).await?;
    }

    store.delete_segment().await?;
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
    let (handle, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert_eq!(
        *handle.lock().await,
        TimerState::Absent,
        "phase 1: expected Absent"
    );

    // Phase 2: clear_and_schedule_key(t1) → Inline(t1)
    let trigger1 = Trigger::for_testing(key.clone(), t1, TimerType::Application);
    store.clear_and_schedule_key(trigger1).await?;

    let (handle, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    let state = handle.lock().await.clone();
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t1),
        "phase 2: expected Inline(t1), got {state:?}"
    );

    // Phase 3: clear_and_schedule_key(t2) → Inline(t2) (Inline→Inline, no
    // tombstone)
    let trigger2 = Trigger::for_testing(key.clone(), t2, TimerType::Application);
    store.clear_and_schedule_key(trigger2).await?;

    let (handle, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    let state = handle.lock().await.clone();
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t2),
        "phase 3: expected Inline(t2), got {state:?}"
    );

    // Phase 4: upsert_key_trigger(t3) promotes inline to clustering → state
    // becomes Overflow
    let trigger3 = Trigger::for_testing(key.clone(), t3, TimerType::Application);
    store.upsert_key_trigger(trigger3).await?;

    let (handle, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert_eq!(
        *handle.lock().await,
        TimerState::Overflow,
        "phase 4: expected Overflow after promotion"
    );

    // Phase 5: clear_and_schedule_key(t4) on an overflow key → back to
    // Inline(t4)
    let trigger4 = Trigger::for_testing(key.clone(), t4, TimerType::Application);
    store.clear_and_schedule_key(trigger4).await?;

    let (handle, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    let state = handle.lock().await.clone();
    assert!(
        matches!(&state, TimerState::Inline(t) if t.time == t4),
        "phase 5: expected Inline(t4), got {state:?}"
    );

    // Phase 6: Verify get_key_times returns exactly [t4].
    let times: Vec<CompactDateTime> = store
        .get_key_times(TimerType::Application, &key)
        .try_collect()
        .await?;
    assert_eq!(
        times,
        vec![t4],
        "phase 6: get_key_times should return exactly [t4]"
    );

    // Phase 7: Type isolation — DeferredMessage state is still Absent.
    let (handle, _) = store
        .resolve_state(&segment_id, &key, TimerType::DeferredMessage)
        .await?;
    assert_eq!(
        *handle.lock().await,
        TimerState::Absent,
        "phase 7: DeferredMessage state should be Absent"
    );

    // Phase 8: Cleanup — clear_key_triggers_all_types resets everything.
    store.clear_key_triggers_all_types(&key).await?;

    let (handle, _) = store
        .resolve_state(&segment_id, &key, TimerType::Application)
        .await?;
    assert_eq!(
        *handle.lock().await,
        TimerState::Absent,
        "phase 8: expected Absent after cleanup"
    );

    store.delete_segment().await?;

    Ok(())
}

/// Regression test: `current_tag` must return the correct tag for Inline
/// timers (single trigger stored in `state` static column).
///
/// The quickcheck property test found that after `upsert_key_trigger` (which
/// stores the trigger as Inline in the `state` column), `current_tag` returned
/// `None` (only checked clustering rows). This test pins the fix: Inline
/// triggers must be queryable via `current_tag`.
#[tokio::test]
async fn test_current_tag_inline_trigger() -> Result<()> {
    use crate::timers::store::TriggerStore;
    use crate::timers::store::adapter::TableAdapter;
    init_test_logging();
    let (store, _segment_id) = setup_test_store("current_tag_inline").await?;
    let store = TableAdapter::new(store);

    let key: Key = format!("tag-inline-{}", Uuid::new_v4()).into();
    let time = CompactDateTime::from(1_500_000u32);
    let timer_type = TimerType::Application;
    let trigger = Trigger::new(key.clone(), time, timer_type, tracing::Span::current());
    let expected_tag = trigger.tag;

    // add_trigger produces an Inline state (first trigger for this key/type).
    store.add_trigger(trigger).await?;

    // current_tag must return Some(expected_tag), not None.
    let actual_tag = store.current_tag(&key, time, timer_type).await?;
    assert_eq!(
        actual_tag,
        Some(expected_tag),
        "current_tag must return the tag for an Inline trigger"
    );

    // update_tag must update the tag even in Inline mode.
    let new_tag = expected_tag.wrapping_add(1);
    store.update_tag(&key, time, timer_type, new_tag).await?;
    let updated = store.current_tag(&key, time, timer_type).await?;
    assert_eq!(
        updated,
        Some(new_tag),
        "update_tag must rotate the tag for an Inline trigger"
    );

    let all_type_triggers: Vec<Trigger> = store
        .operations()
        .get_key_triggers_all_types(&key)
        .try_collect()
        .await?;
    assert_eq!(all_type_triggers.len(), 1);
    assert_eq!(
        all_type_triggers[0].tag, new_tag,
        "get_key_triggers_all_types must preserve the stored Inline tag"
    );

    let slab_id = Slab::from_time(store.slab_size(), time).id();
    let slab_triggers: Vec<Trigger> = store
        .get_slab_triggers_all_types(slab_id)
        .try_collect()
        .await?;
    let slab_tag = slab_triggers
        .iter()
        .find(|t| t.key == key && t.time == time && t.timer_type == timer_type)
        .map(|t| t.tag);
    assert_eq!(
        slab_tag,
        Some(new_tag),
        "update_tag must rotate the tag in the slab index"
    );

    store.remove_trigger(&key, time, timer_type).await?;
    Ok(())
}

#[tokio::test]
async fn test_key_triggers_all_types_preserves_inline_tags() -> Result<()> {
    use crate::timers::store::TriggerStore;
    use crate::timers::store::adapter::TableAdapter;
    init_test_logging();
    let (store, _segment_id) = setup_test_store("all_types_inline_tags").await?;
    let store = TableAdapter::new(store);

    let key: Key = format!("all-types-inline-tags-{}", Uuid::new_v4()).into();
    let base_time = 1_600_000u32;
    let mut expected_tags = HashMap::new();

    for (idx, &timer_type) in TimerType::VARIANTS.iter().enumerate() {
        let idx_seconds = u32::try_from(idx)?;
        let time = CompactDateTime::from(base_time + idx_seconds);
        let trigger = Trigger::new(key.clone(), time, timer_type, tracing::Span::current());
        expected_tags.insert(timer_type, trigger.tag);
        store.add_trigger(trigger).await?;
    }

    let actual: Vec<Trigger> = store
        .operations()
        .get_key_triggers_all_types(&key)
        .try_collect()
        .await?;
    assert_eq!(
        actual.len(),
        expected_tags.len(),
        "get_key_triggers_all_types must return one inline trigger per timer type"
    );

    for trigger in &actual {
        assert_eq!(trigger.key, key);
        assert_eq!(
            expected_tags.get(&trigger.timer_type).copied(),
            Some(trigger.tag),
            "get_key_triggers_all_types must preserve inline tags"
        );
    }

    for trigger in actual {
        store
            .remove_trigger(&trigger.key, trigger.time, trigger.timer_type)
            .await?;
    }
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
                let segment = Segment {
                    id: Uuid::new_v4(),
                    name: String::new(),
                    slab_size,
                    version: SegmentVersion::V3,
                };
                CassandraTriggerStore::with_store(
                    store,
                    &config.keyspace,
                    segment,
                    SpanRelation::default(),
                )
                .await
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

/// Verifies that two `CassandraTriggerStore` instances sharing a session have
/// independent caches.
///
/// Both stores are scoped to the same segment so that store B can observe
/// data written by store A through the shared Cassandra session.
///
/// 1. Build store A (insert segment, write a trigger) → cache is warm.
/// 2. Build store B with the same segment but a fresh cache → cache is cold.
/// 3. Store B reads the same key via the shared session → returns t1.
/// 4. After the read, store B's cache is now warm.
#[tokio::test]
async fn test_provider_creates_independent_stores() -> Result<()> {
    init_test_logging();

    let slab_size = CompactDuration::new(60);
    let segment = Segment {
        id: SegmentId::from(Uuid::new_v4()),
        name: "provider_independent".to_owned(),
        slab_size,
        version: SegmentVersion::V3,
    };
    let config = test_cassandra_config("prosody_test");

    // Build store A with the chosen segment.
    let base_a = CassandraStore::new(&config).await?;
    let ops_a = CassandraTriggerStore::with_store(
        base_a,
        &config.keyspace,
        segment.clone(),
        SpanRelation::default(),
    )
    .await?;
    ops_a.insert_segment().await?;

    // Build store B with the same segment but a fresh (cold) cache, sharing
    // the same prepared queries.
    let base_b = CassandraStore::new(&config).await?;
    let ops_b = CassandraTriggerStore::with_store(
        base_b,
        &config.keyspace,
        segment.clone(),
        SpanRelation::default(),
    )
    .await?;

    let key: Key = format!("provider-test-{}", Uuid::new_v4()).into();
    let tt = TimerType::Application;
    let t1 = CompactDateTime::from(1_000_000u32);

    // Write via store A: clear_and_schedule_key populates store A's cache.
    ops_a
        .clear_and_schedule_key(Trigger::for_testing(key.clone(), t1, tt))
        .await?;

    // Store A cache is warm: Inline(t1).
    let cache_key = (key.clone(), tt);
    let cached_a = ops_a.state_cache.get(&cache_key);
    assert!(cached_a.is_some(), "store A cache should have Inline(t1)");
    let cached_a_state = cached_a.as_ref().map(|h| h.try_lock().map(|g| g.clone()));
    assert!(
        matches!(&cached_a_state, Some(Ok(TimerState::Inline(timer))) if timer.time == t1),
        "store A cache should have Inline(t1), got {cached_a_state:?}"
    );

    // Store B cache is cold: no entry for this key.
    let cached_b = ops_b.state_cache.get(&cache_key);
    assert!(cached_b.is_none(), "store B cache should be cold (None)");

    // Store B can still read the data via shared keyspace (same segment ID).
    let times: Vec<CompactDateTime> = ops_b.get_key_times(tt, &key).try_collect().await?;
    assert_eq!(times, vec![t1], "store B should read t1 via shared session");

    // After the read, store B's cache should now be warm (Inline cached from DB).
    let warm_b = ops_b.state_cache.get(&cache_key);
    assert!(warm_b.is_some(), "store B cache should be warm after read");
    let warm_b_state = warm_b.as_ref().map(|h| h.try_lock().map(|g| g.clone()));
    assert!(
        matches!(&warm_b_state, Some(Ok(TimerState::Inline(t))) if t.time == t1),
        "store B cache should be warm after read, got {warm_b_state:?}"
    );

    // Cleanup.
    ops_a.delete_segment().await?;

    Ok(())
}

/// Asserts `current_tag` matches `expected_tags` for every entry on the
/// given `(key, timer_type)`. Used by the property test to catch any
/// write path that drops or rewrites a tag.
async fn verify_tags_for_key_type(
    store: &CassandraTriggerStore,
    expected_tags: &HashMap<(Key, TimerType, CompactDateTime), i32>,
    key: &Key,
    timer_type: TimerType,
) -> Result<()> {
    for ((k, tt, time), expected_tag) in expected_tags {
        if k != key || *tt != timer_type {
            continue;
        }
        let observed = store.current_tag(key, *time, timer_type).await?;
        assert_eq!(
            observed,
            Some(*expected_tag),
            "current_tag mismatch: key={key:?} time={time:?} type={timer_type:?} expected \
             Some({expected_tag}) got {observed:?}",
        );
    }
    Ok(())
}

/// Applies a single property-test operation to `store`, updates
/// `expected_tags`, and re-verifies every known tag on the affected
/// `(key, timer_type)` pair after writes.
///
/// We snapshot the tag the store *actually* holds right after each write
/// (rather than the trigger's constructed tag) so the invariant pins
/// tag-stability between writes — catching any path that silently drops
/// or rewrites the tag of an unrelated, untouched timer (e.g. the OLD
/// inline timer during Inline→Overflow promotion). Recording the
/// trigger-constructed tag instead would over-specify against the
/// production contract: duplicate `(key, time, type)` writes are upserts, so
/// this records the replacement metadata chosen by the store.
async fn apply_op_and_verify_tags(
    store: &CassandraTriggerStore,
    op: &KeyTriggerOperation,
    expected_tags: &mut HashMap<(Key, TimerType, CompactDateTime), i32>,
) -> Result<()> {
    match op {
        KeyTriggerOperation::Insert { trigger, .. } => {
            store.upsert_key_trigger(trigger.clone()).await?;
            snapshot_tag(
                store,
                expected_tags,
                &trigger.key,
                trigger.timer_type,
                trigger.time,
            )
            .await?;
            verify_tags_for_key_type(store, expected_tags, &trigger.key, trigger.timer_type)
                .await?;
        }
        KeyTriggerOperation::Delete {
            timer_type,
            key,
            time,
            ..
        } => {
            store.delete_key_trigger(*timer_type, key, *time).await?;
            expected_tags.remove(&(key.clone(), *timer_type, *time));
            verify_tags_for_key_type(store, expected_tags, key, *timer_type).await?;
        }
        KeyTriggerOperation::ClearByType {
            timer_type, key, ..
        } => {
            store.clear_key_triggers(*timer_type, key).await?;
            expected_tags.retain(|(k, tt, _), _| !(k == key && *tt == *timer_type));
        }
        KeyTriggerOperation::ClearAllTypes { key, .. } => {
            store.clear_key_triggers_all_types(key).await?;
            expected_tags.retain(|(k, ..), _| k != key);
        }
        KeyTriggerOperation::ClearAndSchedule { trigger, .. } => {
            store.clear_and_schedule_key(trigger.clone()).await?;
            expected_tags.retain(|(k, tt, _), _| !(k == &trigger.key && *tt == trigger.timer_type));
            snapshot_tag(
                store,
                expected_tags,
                &trigger.key,
                trigger.timer_type,
                trigger.time,
            )
            .await?;
            verify_tags_for_key_type(store, expected_tags, &trigger.key, trigger.timer_type)
                .await?;
        }
        KeyTriggerOperation::GetTimes { .. }
        | KeyTriggerOperation::GetTriggers { .. }
        | KeyTriggerOperation::GetAllTypes { .. } => {}
    }
    Ok(())
}

/// Reads the store's current tag for `(key, timer_type, time)` and
/// records it in `expected_tags`. Used immediately after a write so the
/// invariant pins what the store actually has, not what the caller asked
/// for.
async fn snapshot_tag(
    store: &CassandraTriggerStore,
    expected_tags: &mut HashMap<(Key, TimerType, CompactDateTime), i32>,
    key: &Key,
    timer_type: TimerType,
    time: CompactDateTime,
) -> Result<()> {
    let tag = store
        .current_tag(key, time, timer_type)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("current_tag returned None right after write"))?;
    expected_tags.insert((key.clone(), timer_type, time), tag);
    Ok(())
}

/// Applies operations from [`KeyTriggerTestInput`] and verifies the
/// timer state invariant holds for every `(segment_id, key, timer_type)`.
async fn prop_timer_state_invariant(
    store: &CassandraTriggerStore,
    input: KeyTriggerTestInput,
) -> Result<()> {
    use crate::timers::store::tests::prop_key_triggers::KeyTriggerModel;

    let key_pool = ["key-a", "key-b", "key-c"];

    // Clean up before test
    for _segment_id in &input.segment_ids {
        for key_str in &key_pool {
            let key = Key::from(*key_str);
            store.clear_key_triggers_all_types(&key).await?;
        }
    }

    // Apply all operations to both model and store, verifying tags as we go.
    let mut model = KeyTriggerModel::new();
    let mut expected_tags: HashMap<(Key, TimerType, CompactDateTime), i32> = HashMap::new();
    for op in &input.operations {
        model.apply(op);
        apply_op_and_verify_tags(store, op, &mut expected_tags).await?;
    }

    // Verify timer state invariant for every (segment_id, key, timer_type)
    for (segment_id, key) in &model.all_keys() {
        for &timer_type in TimerType::VARIANTS {
            let expected_count = model.get_times(segment_id, timer_type, key).len();
            let (handle, _) = store.resolve_state(segment_id, key, timer_type).await?;
            let timer_state = handle.lock().await.clone();

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

/// Regression test for `tombstone_warn_threshold` warnings emitted by
/// `get_segment` and `get_slab_watermark` on actor startup.
///
/// Both queries select only static columns from `timer_segments` with
/// `LIMIT 1`. With no clustering predicate, Cassandra walks the iterator
/// from the bottom up to materialise the static row — straight through
/// the tombstone graveyard the load-driven sweeper (PR #34) leaves at
/// low `slab_id`. Appending `ORDER BY slab_id DESC` resolves on the
/// live tail and skips the graveyard entirely.
///
/// This test seeds that exact partition shape — a dense band of
/// tombstones at low `slab_id`, a small set of live rows above the
/// watermark, and `slab_watermark` raised to the boundary — and asserts
/// the two reads return the correct values. The assertion is purely
/// behavioural; the explicit `ORDER BY slab_id DESC` in
/// `queries.rs` is itself the durable statement about scan direction.
#[tokio::test]
async fn test_segment_reads_skip_low_slab_tombstones() -> Result<()> {
    /// Density of the tombstone band — chosen to mimic the post-sweeper
    /// graveyard observed in production (~5k cells per segment partition).
    const TOMBSTONE_COUNT: u32 = 5_000;

    init_test_logging();

    let segment_name = "tombstone_skip_test";
    let (store, segment_id) = setup_test_store(segment_name).await?;
    let slab_size = store.segment().slab_size;

    for slab_id in 0..TOMBSTONE_COUNT {
        let slab = Slab::new(slab_id, slab_size);
        store.insert_slab(slab).await?;
        store.delete_slab(slab_id).await?;
    }

    // Seed live rows above the sweeper's reach.
    let live_slabs: Vec<SlabId> = (TOMBSTONE_COUNT + 1..=TOMBSTONE_COUNT + 10).collect();
    for &slab_id in &live_slabs {
        store.insert_slab(Slab::new(slab_id, slab_size)).await?;
    }

    // Raise the watermark to the boundary — same as the sweeper would.
    let watermark = TOMBSTONE_COUNT;
    store.set_slab_watermark(Some(watermark)).await?;

    // `get_segment`: forward scan walked the graveyard; reverse scan
    // resolves on a live row at the top of the partition.
    let segment = store
        .get_segment()
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("segment missing after insert"))?;
    assert_eq!(segment.id, segment_id);
    assert_eq!(segment.name, segment_name);
    assert_eq!(segment.slab_size, slab_size);

    // `get_slab_watermark`: same partition, same problem, same fix.
    let observed_watermark = store.get_slab_watermark().await?;
    assert_eq!(
        observed_watermark,
        Some(watermark),
        "watermark roundtrip should ignore low-slab tombstones",
    );

    store.delete_segment().await?;
    Ok(())
}
