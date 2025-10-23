# Timer Types Migration Design

## Overview

This document describes the design for adding support for multiple timer types per key in the timer system. The migration enables concurrent scheduling of different timer types (e.g., application timers and defer retry timers) for the same key while maintaining backward compatibility.

## Motivation

The defer middleware requires the ability to schedule retry timers independently from application timers for the same key. The current timer system only supports one timer per key within a segment, causing conflicts when both timer types need to be scheduled simultaneously.

## Design Goals

1. **Multiple timer types per key**: Support concurrent timers of different types for the same key
2. **Lazy migration**: Migrate segments on-demand when accessed, no full table scans
3. **Slab size flexibility**: Enable changing slab sizes through the same migration mechanism
4. **Zero downtime**: Old and new schemas coexist during migration
5. **Automatic cleanup**: Remove old data after successful migration
6. **Idempotent**: Safe to retry migrations if they fail partway through
7. **Clean separation**: Migration complexity isolated to store layer

## Key Design Decisions

### Timer Type Representation

Timer types are represented as a `tinyint` (i8) in Cassandra for storage efficiency and performance:

```rust
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(i8)]
pub enum TimerType {
    Application = 0,  // User-scheduled timers (default)
    DeferRetry = 1,   // Defer middleware retry timers
    // Future types: 2-127 available
}
```

**Rationale:**
- **Storage efficiency**: 1 byte vs 11+ bytes for strings
- **Clustering key performance**: Smaller keys improve compression and query performance
- **Type safety**: Rust enum provides compile-time validation
- **Controlled namespace**: Prevents arbitrary types, requires explicit registration

### API Design

Timer types are internal to Prosody infrastructure and not exposed to applications:

```rust
// Public API - applications use default Application type
pub async fn schedule(&self, key: Key, time: CompactDateTime) -> Result<PendingTimer>;

// Internal API - middleware specifies type explicitly
pub(crate) async fn schedule_typed(
    &self,
    key: Key,
    time: CompactDateTime,
    timer_type: TimerType
) -> Result<PendingTimer>;
```

Applications remain unaware of timer types and always implicitly use `TimerType::Application`. Middleware components use `schedule_typed()` to specify non-default types.

### Core Type Changes

The `Trigger` struct gains a `timer_type` field:

```rust
pub struct Trigger {
    pub key: Key,
    pub time: CompactDateTime,
    pub timer_type: TimerType,  // NEW
    span: Arc<ArcSwap<Span>>,
}
```

**Breaking change**: `Trigger::new()` signature changes to include `timer_type` parameter.

## Schema Design

### Version 1 (Current)

**Tables:**
- `segments`: Tracks segment metadata and active slabs
- `slabs`: Stores triggers indexed by `(segment_id, slab_id, key, time)`
- `keys`: Secondary index for key-based lookups

**Limitation:** Only one timer per `(segment_id, key, time)` tuple.

**Complete V1 Schema:**

```cql
-- Segment metadata and active slab tracking
CREATE TABLE segments (
    id        uuid,
    name      text static,
    slab_size int static,
    slab_id   int,
    PRIMARY KEY ((id), slab_id)
) WITH compression = { 'class': 'ZstdCompressor' }
  AND compaction = { 'class': 'UnifiedCompactionStrategy' };

-- Triggers indexed by slab (primary access pattern)
CREATE TABLE slabs (
    segment_id uuid,
    slab_size  int,                    -- Part of partition key for safe slab size migration
    id         int,
    key        text,
    time       int,
    span       frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, slab_size, id), key, time)
) WITH compression = { 'class': 'ZstdCompressor' }
  AND compaction = { 'class': 'UnifiedCompactionStrategy' };

-- Triggers indexed by key (secondary access pattern)
CREATE TABLE keys (
    segment_id uuid,
    key        text,
    time       int,
    span       frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, key), time)
) WITH compression = { 'class': 'ZstdCompressor' }
  AND compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' };
```

**Why `slab_size` in partition key:**
- Enables safe slab size migration by preventing partition conflicts
- Old slabs: `(segment_id, old_slab_size, slab_id)`
- New slabs: `(segment_id, new_slab_size, slab_id)`
- Both can coexist during migration, then old partitions are deleted

### Version 2 (Target)

**Tables:**
- `segments`: Enhanced with `version` field (static column)
- `timer_typed_slabs`: New table with `type` in clustering key
- `timer_typed_keys`: New table with `type` in clustering key

**Key differences:**
- Clustering key includes `type`: `PRIMARY KEY ((segment_id, id), key, time, type)`
- This allows multiple timers with different types for the same (key, time) tuple
- Segments track version to enable migration detection

**Capability:** Multiple timers per key, differentiated by `type`.

**Complete V2 Schema:**

```cql
-- Segment metadata (enhanced with version field)
-- Note: segments table is shared between V1 and V2
CREATE TABLE segments (
    id        uuid,
    name      text static,
    slab_size int static,
    version   int static,           -- NEW: tracks schema version
    slab_id   int,
    PRIMARY KEY ((id), slab_id)
) WITH compression = { 'class': 'ZstdCompressor' }
  AND compaction = { 'class': 'UnifiedCompactionStrategy' };

-- Triggers with timer type support (replaces 'slabs')
CREATE TABLE timer_typed_slabs (
    segment_id uuid,
    slab_size  int,                  -- Part of partition key for safe slab size migration
    id         int,
    key        text,
    time       int,
    type       tinyint,              -- NEW: timer type (0=application, 1=defer_retry)
    span       frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, slab_size, id), key, time, type)
) WITH compression = { 'class': 'ZstdCompressor' }
  AND compaction = { 'class': 'UnifiedCompactionStrategy' }
  AND COMMENT = 'Timer types: 0=application, 1=defer_retry';

-- Triggers by key with timer type support (replaces 'keys')
CREATE TABLE timer_typed_keys (
    segment_id uuid,
    key        text,
    time       int,
    type       tinyint,              -- NEW: timer type (0=application, 1=defer_retry)
    span       frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, key), time, type)
) WITH compression = { 'class': 'ZstdCompressor' }
  AND compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' }
  AND COMMENT = 'Timer types: 0=application, 1=defer_retry';
```

**Why `slab_size` in partition key:**
- Critical for safe slab size migration
- During Phase 2, old and new slabs with different sizes coexist in separate partitions
- Example: `(segment_X, 3600, 0)` and `(segment_X, 7200, 0)` are different partitions
- After migration completes, old partitions `(segment_X, 3600, *)` are deleted

### Migration CQL

**File:** `src/cassandra/migrations/20250124_add_timer_types.cql`

This migration file will be applied by the existing Cassandra migration framework:

```cql
-- Migration: Add timer type support to timer system
-- Timestamp: 20250124
-- Description: Adds version tracking to segments and creates new timer_typed tables

-- Step 1: Add version tracking to existing segments table
ALTER TABLE {{KEYSPACE}}.segments ADD version int;

-- Step 2: Create new slabs table with type field and slab_size in partition key
CREATE TABLE IF NOT EXISTS {{KEYSPACE}}.timer_typed_slabs (
    segment_id uuid,
    slab_size  int,
    id         int,
    key        text,
    time       int,
    type       tinyint,
    span       frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, slab_size, id), key, time, type)
) WITH compression = { 'class': 'ZstdCompressor' }
  AND compaction = { 'class': 'UnifiedCompactionStrategy' }
  AND COMMENT = 'Timer types: 0=application, 1=defer_retry. slab_size in partition key enables safe slab size migration';

-- Step 3: Create new keys table with type field
CREATE TABLE IF NOT EXISTS {{KEYSPACE}}.timer_typed_keys (
    segment_id uuid,
    key        text,
    time       int,
    type       tinyint,
    span       frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, key), time, type)
) WITH compression = { 'class': 'ZstdCompressor' }
  AND compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' }
  AND COMMENT = 'Timer types: 0=application, 1=defer_retry';

-- Note: Old tables (slabs, keys) are NOT dropped
-- They remain for backward compatibility and lazy migration
-- After all segments migrate, they can be dropped manually
```

**Key Schema Properties:**

1. **Additive Migration**: Only adds columns and creates new tables, doesn't modify or drop existing structures
2. **Backward Compatible**: Old code can continue using V1 tables
3. **Template Substitution**: `{{KEYSPACE}}` is replaced by the Cassandra migration framework
4. **Idempotent**: `IF NOT EXISTS` allows safe re-application
5. **Comments**: Document timer type enum values for operational clarity
6. **slab_size in Partition Key**: Critical design decision enabling safe slab size migration
   - Old partitions: `(segment_id, old_slab_size, slab_id)`
   - New partitions: `(segment_id, new_slab_size, slab_id)`
   - No conflicts during migration, clean deletion after completion

## Migration Strategy

### Architecture Principle

**Migration complexity is isolated to the store layer.** The rest of the codebase only knows about v2 (current version).

- **Core timer system** (`Trigger`, `TimerManager`, `TimerScheduler`): Works exclusively with v2 structures
- **Store implementations** (Cassandra, Memory): Support both v1 (read-only for migration) and v2 (current operations)
- **Migration module**: Orchestrates migration using store v1/v2 methods

### Store Interface Extensions

The store trait gains methods for migration:

```rust
pub trait TriggerStore {
    // === V2 methods (primary API) ===
    // All existing methods operate on v2 schema
    // Methods like get_slab_triggers() now require slab_size parameter
    // to construct partition key: (segment_id, slab_size, id)

    // === V1 methods (migration only, read-only) ===
    fn get_slabs_v1(&self, segment_id: &SegmentId, slab_size: CompactDuration)
        -> impl Stream<Item = Result<SlabId>>;

    fn get_slab_triggers_v1(&self, segment_id: &SegmentId, slab_size: CompactDuration, slab_id: SlabId)
        -> impl Stream<Item = Result<TriggerV1>>;

    fn delete_slab_v1(&self, segment_id: &SegmentId, slab_size: CompactDuration, slab_id: SlabId)
        -> impl Future<Output = Result<()>>;

    fn clear_slab_triggers_v1(&self, segment_id: &SegmentId, slab_size: CompactDuration, slab_id: SlabId)
        -> impl Future<Output = Result<()>>;

    // === Segment version management ===
    fn update_segment_version(&self, segment_id: &SegmentId, version: i32, slab_size: CompactDuration)
        -> impl Future<Output = Result<()>>;
}
```

**Key points:**
- `TriggerV1` is a temporary structure without `timer_type`, used only during migration reads
- All slab operations now require `slab_size` parameter to construct correct partition keys
- Cassandra queries: `WHERE segment_id = ? AND slab_size = ? AND id = ?`

### Migration Phases

**Important:** Slab size migration is only supported for v2 tables. If both version and slab size need to change, two sequential migrations occur:

1. **Phase 1 - Version Migration (v1 → v2)**:
   - Triggered when `segment.version ≠ 2`
   - Preserves current `slab_size`
   - Reads from v1 tables, writes to v2 tables
   - All migrated timers get `type = Application`
   - Cleans up v1 tables after success

2. **Phase 2 - Slab Size Migration (v2 → v2)**:
   - Triggered when `segment.slab_size ≠ desired_slab_size`
   - Only runs after Phase 1 completes (segment must be v2)
   - Reads from v2 tables, writes to v2 tables
   - Recalculates slab IDs based on new slab size
   - Preserves `timer_type` for each trigger
   - Cleans up old slabs after success

### Migration Algorithm

**High-level flow:**

```
ensure_segment_current(segment_id, desired_slab_size):
    Load segment metadata (includes current_slab_size)

    // Phase 1: Version migration (v1 → v2)
    if segment.version != V2:
        // Read from v1 partitions: (segment_id, current_slab_size, slab_id)
        for each slab_id in get_slabs_v1(segment_id, current_slab_size):
            triggers_v1 = get_slab_triggers_v1(segment_id, current_slab_size, slab_id)

            for each trigger_v1 in triggers_v1:
                trigger_v2 = Trigger {
                    key: trigger_v1.key,
                    time: trigger_v1.time,
                    timer_type: Application,  // Default for migrated triggers
                    span: trigger_v1.span
                }

                // Write to v2 partitions: (segment_id, current_slab_size, slab_id)
                // Note: slab_size unchanged, slab_id unchanged
                write_to_v2_tables(segment_id, current_slab_size, slab_id, trigger_v2)

        update_segment(version = V2, slab_size = current_slab_size)

        // Delete v1 partitions: (segment_id, current_slab_size, *)
        delete_v1_partitions(segment_id, current_slab_size)

        reload_segment()  // Get updated metadata

    // Phase 2: Slab size migration (v2 → v2)
    if segment.slab_size != desired_slab_size:
        old_slab_size = segment.slab_size

        // Read from old v2 partitions: (segment_id, old_slab_size, slab_id)
        for each slab_id in get_active_slabs(segment_id, old_slab_size):
            triggers = get_slab_triggers(segment_id, old_slab_size, slab_id)

            for each trigger in triggers:
                new_slab_id = floor(trigger.time / desired_slab_size)

                // Write to new v2 partitions: (segment_id, desired_slab_size, new_slab_id)
                // Note: timer_type preserved
                write_to_v2_tables(segment_id, desired_slab_size, new_slab_id, trigger)

        update_segment(version = V2, slab_size = desired_slab_size)

        // Delete old v2 partitions: (segment_id, old_slab_size, *)
        delete_old_v2_partitions(segment_id, old_slab_size)
```

**Key properties:**
- **Idempotent**: Can retry from any point
- **Non-atomic but safe**: Uses version field to track completion
- **No data loss**: Old data only deleted after successful migration
- **Isolated by partition**: Prosody's partition ownership prevents concurrent migrations
- **Partition key safety**: `slab_size` in partition key ensures old/new slabs never conflict

### Slab ID Recalculation

When slab size changes, slab IDs must be recalculated:

```
new_slab_id = floor(trigger_time / new_slab_size)
```

**Example** (3600s → 7200s):
- Old slab 0 (times 0-3599) → New slab 0 (times 0-7199)
- Old slab 1 (times 3600-7199) → New slab 0 (times 0-7199)
- Old slab 2 (times 7200-10799) → New slab 1 (times 7200-14399)

Multiple old slabs may merge into a single new slab, or split across new slabs.

## Migration Safety

### Atomicity

Migration is not atomic across all operations but is designed to be safe:

1. **Read phase**: Non-destructive, can be retried
2. **Write phase**: Idempotent writes to new tables
3. **Metadata update**: Single static column update (atomic per segment)
4. **Delete phase**: Only after successful metadata update

### Failure Handling

**Migration fails during copy:**
- Old data remains intact
- Partial new data exists but segment version not updated
- Retry will re-copy (overwrites previous partial data)

**Migration fails after metadata update:**
- Segment version updated, indicating migration complete
- Old data deletion can be retried separately
- Orphaned old data is harmless (version indicates v2 tables should be used)

**Concurrent migrations:**
- Prevented by Prosody's partition ownership model
- Only one consumer processes a partition at a time
- Only one TimerManager instance per segment

### Backward Compatibility

**Reading:** V2 code queries v2 tables exclusively after migration

**Writing:**
- V1 code (pre-migration) writes to v1 tables
- V2 code (post-migration) writes to v2 tables

**Routing:** `on_timer()` uses `timer_type` to route:
```rust
match timer.timer_type() {
    TimerType::DeferRetry => defer_middleware.handle_retry_timer(timer).await,
    TimerType::Application => inner_handler.on_timer(timer).await,
}
```

## Performance Characteristics

### Migration Cost

Per segment migration:
- **Reads**: All slabs in segment (sequential)
- **Writes**: All triggers to new tables (batch-friendly)
- **Deletes**: All old slabs (sequential)

Typical segment:
- 100-1000 slabs
- 10-100 triggers per slab
- Migration time: seconds to minutes depending on data volume

### Lazy Migration Benefits

- **No full table scans**: Only accessed segments migrate
- **Bounded work**: Each segment migration is independent
- **Priority to active**: Hot segments migrate first, cold segments migrate on-demand or never
- **Gradual rollout**: Load spreads over time as segments are accessed

## Memory Store Considerations

The in-memory store must mirror Cassandra's behavior exactly, including `slab_size` in the partition key:

**Structure:**
```rust
pub struct MemoryTriggerStore {
    segments: Arc<DashMap<SegmentId, Segment>>,        // Shared

    // V2 storage (with timer_type, slab_size in key)
    slabs_v2: Arc<DashMap<(SegmentId, SlabSize, SlabId), BTreeSet<Trigger>>>,
    keys_v2: Arc<DashMap<(SegmentId, Key), BTreeSet<Trigger>>>,

    // V1 storage (without timer_type, slab_size in key)
    slabs_v1: Arc<DashMap<(SegmentId, SlabSize, SlabId), BTreeSet<TriggerV1>>>,
    keys_v1: Arc<DashMap<(SegmentId, Key), BTreeSet<TriggerV1>>>,
}
```

**Why this matters:**
- Enables testing migration logic with memory store
- Correctly simulates Cassandra's separate v1/v2 tables
- **Tests can verify slab size migration**: Old and new slabs with different sizes are in different map entries
- Example: `(segment_X, 3600, 0)` and `(segment_X, 7200, 0)` are separate entries
- Tests can verify data moves from v1 to v2 correctly

## Testing Strategy

### Testing Approach

The dual v1/v2 store support enables comprehensive migration testing. Tests can:

1. **Setup**: Write triggers to v1 tables using v1 methods
2. **Execute**: Run migration logic
3. **Verify**: Read from v2 tables and verify correctness
4. **Validate**: Confirm v1 tables cleaned up appropriately

This approach works with both Cassandra and memory stores, enabling fast unit tests and comprehensive integration tests.

### Unit Tests

**Timer Type Functionality:**
1. `TimerType::from_i8()` with valid values (0, 1)
2. `TimerType::from_i8()` with invalid values (negative, > 127)
3. `TimerType::as_i8()` round-trip conversion
4. Default implementation returns `Application`

**Slab ID Calculation:**
1. Same slab size preserves slab IDs
2. Larger slab size merges slabs correctly
3. Smaller slab size splits slabs correctly
4. Boundary cases (time = 0, time at slab boundaries)

**Version Detection:**
1. `version = None` detected as v1
2. `version = Some(1)` detected as v1
3. `version = Some(2)` detected as v2
4. Slab size mismatch detected correctly

### Migration Tests

These tests use the memory store for speed and setup v1 data explicitly.

**Scenario 1: Basic V1 → V2 Migration**
- Setup: Insert triggers into v1 tables (various keys, times, spans)
- Execute: Run Phase 1 migration
- Verify:
  - All triggers present in v2 tables with `type = Application`
  - Slab IDs unchanged
  - Segment version = 2
  - V1 tables empty
  - Spans preserved correctly

**Scenario 2: V1 → V2 with Multiple Slabs**
- Setup: Insert triggers across 10+ slabs in v1
- Execute: Run Phase 1 migration
- Verify:
  - All slabs migrated
  - Trigger count matches
  - Each trigger in correct slab
  - V1 slabs all deleted

**Scenario 3: V1 → V2 with No Data**
- Setup: Create segment with version 1, no triggers
- Execute: Run Phase 1 migration
- Verify:
  - Segment version = 2
  - No errors
  - V1 tables empty

**Scenario 4: V2 Slab Size Increase**
- Setup: Insert triggers into v2 tables (slab_size = 3600)
- Execute: Run Phase 2 migration (slab_size = 7200)
- Verify:
  - Slabs merged correctly (old slabs 0,1 → new slab 0)
  - All triggers present
  - Timer types preserved (test with mixed Application/DeferRetry)
  - Old slabs deleted

**Scenario 5: V2 Slab Size Decrease**
- Setup: Insert triggers into v2 tables (slab_size = 7200)
- Execute: Run Phase 2 migration (slab_size = 3600)
- Verify:
  - Slabs split correctly (old slab 0 → new slabs 0,1)
  - All triggers present
  - Correct new slab IDs calculated
  - Old slabs deleted

**Scenario 6: Sequential Migration (V1 + Slab Size Change)**
- Setup: Insert triggers into v1 tables (slab_size = 3600)
- Execute: Run migration with desired_slab_size = 7200
- Verify:
  - Phase 1 completes: version = 2, slab_size still 3600
  - Phase 2 completes: version = 2, slab_size = 7200
  - All triggers present with correct slab IDs
  - Type = Application for all triggers
  - V1 tables empty, old v2 slabs deleted

**Scenario 7: Migration Idempotency**
- Setup: Insert triggers into v1 tables
- Execute: Run Phase 1 migration twice
- Verify:
  - Second migration is no-op (detects version = 2)
  - Data correct after both runs
  - No duplicate triggers

**Scenario 8: Partial Migration Retry**
- Setup: Insert triggers into v1 tables
- Execute: Run Phase 1 but simulate failure after copying data (before version update)
- Execute: Retry migration
- Verify:
  - Retry completes successfully
  - No duplicate triggers (writes are idempotent)
  - V1 tables cleaned up

**Scenario 9: Multiple Timer Types Per Key (Post-Migration)**
- Setup: Migrate segment to v2
- Execute: Schedule both Application and DeferRetry timers for same (key, time)
- Verify:
  - Both timers stored
  - Both timers fired correctly
  - Routing to correct handlers works

**Scenario 10: Migration with Spans**
- Setup: Insert triggers with various span contexts into v1
- Execute: Run Phase 1 migration
- Verify:
  - Span data preserved correctly
  - Span deserialization works in v2 tables

**Scenario 11: Empty Slab Migration**
- Setup: Create slabs in v1 with no triggers (deleted triggers scenario)
- Execute: Run Phase 1 migration
- Verify:
  - Empty slabs handled gracefully
  - No errors
  - Slab metadata cleaned up

**Scenario 12: Large Dataset Migration**
- Setup: Insert 10,000 triggers across 100 slabs in v1
- Execute: Run Phase 1 migration
- Verify:
  - All 10,000 triggers migrated
  - Performance acceptable (< 5 seconds for memory store)
  - Memory usage reasonable

### Integration Tests (Cassandra)

These tests use a real Cassandra cluster to verify behavior with actual database semantics.

**Test 1: V1 → V2 with Real Cassandra**
- Verify LWT behavior for segment version updates
- Verify TTL handling across migration
- Verify compaction doesn't interfere

**Test 2: Concurrent Access (Partition Ownership)**
- Verify only one process can migrate a segment
- Verify partition ownership prevents concurrent migration

**Test 3: Schema Migration Deployment**
- Apply Cassandra migration `.cql` file
- Verify `version` column added to segments table
- Verify new tables created with correct schema
- Verify old tables still accessible

**Test 4: Long-Running Migration**
- Large dataset (100K+ triggers)
- Monitor migration progress
- Verify system remains operational during migration

**Test 5: Rollback and Retry**
- Start migration
- Kill process mid-migration
- Restart and verify retry succeeds

### Property Tests

Use QuickCheck to verify invariants across random inputs:

**Property 1: Migration Preserves All Triggers**
```
∀ triggers in v1_tables:
    After migration → ∃ equivalent trigger in v2_tables
```

**Property 2: Slab ID Calculation is Deterministic**
```
∀ trigger, slab_size:
    slab_id = floor(trigger.time / slab_size)
    Recalculation always produces same result
```

**Property 3: Migration Idempotency**
```
∀ segment state:
    migrate(segment) = migrate(migrate(segment))
```

**Property 4: Type Preservation During Slab Resize**
```
∀ trigger in v2 with timer_type = T:
    After slab resize → trigger.timer_type = T
```

**Property 5: No Trigger Reordering**
```
∀ triggers sorted by (key, time, type):
    After migration → same sort order maintained
```

### Test Utilities

**Helper Functions:**

1. **`setup_v1_segment()`** - Create segment with v1 data
2. **`setup_v2_segment()`** - Create segment with v2 data
3. **`insert_v1_trigger()`** - Insert trigger into v1 tables
4. **`insert_v2_trigger()`** - Insert trigger into v2 tables
5. **`verify_migration()`** - Assert expected migration results
6. **`assert_v1_empty()`** - Verify v1 tables cleaned up
7. **`assert_triggers_equal()`** - Compare trigger lists ignoring spans

**Test Fixtures:**

1. **Small dataset**: 10 triggers, 3 slabs
2. **Medium dataset**: 1000 triggers, 50 slabs
3. **Large dataset**: 10000 triggers, 500 slabs
4. **Span variety**: Different span contexts, empty spans
5. **Time distribution**: Clustered times, sparse times, boundary times

### Continuous Testing

**Pre-commit:**
- All unit tests
- Basic migration scenarios (memory store)
- Property tests (100 iterations)

**CI Pipeline:**
- All unit tests
- All migration scenarios (memory + Cassandra)
- Property tests (1000 iterations)
- Integration tests with real Cassandra

**Performance Benchmarks:**
- Track migration speed for standard datasets
- Alert on regression > 20%

## Deployment Strategy

### Deployment Steps

1. **Deploy Cassandra schema**: Run migration to add `version` field and create new tables
2. **Deploy application code**: Code supports both v1 and v2, performs lazy migration
3. **Monitor migrations**: Track migration events in logs/metrics
4. **Verify data**: Confirm migrated segments function correctly
5. **Eventual cleanup**: After all segments migrated, can drop v1 tables (optional)

### Monitoring

Track these metrics:
- Number of segments migrated (by phase)
- Migration duration per segment
- Migration failures and retry rates
- New timer type usage (Application vs DeferRetry)

### Rollback Strategy

If issues arise:
- Application code can be rolled back (supports v1)
- Schema changes are additive (non-breaking)
- Segments track version, can be re-migrated
- Old tables remain until explicitly dropped

## Breaking Changes

### Public API

1. **`Trigger::new()` signature**: Now requires `timer_type` parameter
2. **`UncommittedTimer` trait**: New methods `timer_type()` and `trigger()`

### Internal API

1. **Store trait**: New v1 methods added (implementations required)
2. **Segment struct**: Now includes `version` field
3. **All Trigger constructors**: Require `timer_type` parameter

## Implementation Strategy

### Incremental Approach

**Key Principle**: Each step must result in compilable code with passing clippy checks.

After each step:
1. `cargo build` must succeed
2. `cargo clippy` must pass with no warnings
3. `cargo clippy --tests` must pass with no warnings
4. Existing tests should continue to pass (or be updated in that step)

This approach ensures the codebase remains in a working state throughout the migration, enabling safe iterative development and easy rollback if needed.

### Implementation Steps

#### Step 1: Add TimerType Enum (Non-Breaking)
**Files**: `src/timers/mod.rs`, `src/timers/error.rs`

**Changes**:
- Add `TimerType` enum with `Application = 0`, `DeferRetry = 1`
- Add `Default` impl returning `Application`
- Add `as_i8()` and `from_i8()` methods
- Add `ParseError::UnknownTimerType` variant
- Re-export `TimerType`

**Validation**:
- Code compiles
- Clippy passes
- New enum is available but not yet used
- No breaking changes yet

#### Step 2: Update Trigger Struct (Breaking Change)
**Files**: `src/timers/mod.rs`

**Changes**:
- Add `pub timer_type: TimerType` field to `Trigger`
- Update `Trigger::new()` to accept `timer_type: TimerType` parameter
- Add `Trigger::timer_type()` accessor method
- Update equality/hashing (already handled by `Educe` derive)

**Validation**:
- Compilation will fail - expected
- Fix ALL compilation errors by updating call sites with `TimerType::Application`
- Once fixed, clippy must pass
- This is the most disruptive step - do it early

**Files to update**:
- All test files in `src/timers/store/tests/`
- `src/timers/manager.rs`
- `src/timers/loader.rs`
- `src/timers/scheduler.rs`
- Any other `Trigger::new()` call sites

#### Step 3: Update Segment Struct (Non-Breaking)
**Files**: `src/timers/store/mod.rs`

**Changes**:
- Add `pub version: Option<i32>` field to `Segment`
- Add `effective_version()` method
- Add `SEGMENT_VERSION_V1` and `SEGMENT_VERSION_V2` constants
- Add `TriggerV1` struct (without `timer_type`)

**Validation**:
- Code compiles
- Clippy passes
- New field defaults to `None`, treated as v1
- No breaking changes

#### Step 4: Extend Store Trait with V1 Methods
**Files**: `src/timers/store/mod.rs`

**Changes**:
- Add v1 method signatures to `TriggerStore` trait:
  - `get_slabs_v1()` - returns stream of slab IDs
  - `get_slab_triggers_v1()` - returns stream of `TriggerV1`
  - `delete_slab_v1()` - deletes v1 slab
  - `clear_slab_triggers_v1()` - clears v1 triggers
- Add `update_segment_version()` method

**Validation**:
- Compilation will fail - trait methods not implemented
- This is expected and will be fixed in next steps

#### Step 5: Implement V1 Methods for Memory Store
**Files**: `src/timers/store/memory.rs`

**Changes**:
- Add `slabs_v1` and `keys_v1` fields with `(SegmentId, SlabSize, SlabId)` keys
- Implement v1 methods (initially as no-ops or returning empty streams)
- Implement `update_segment_version()`
- Update constructor to initialize v1 storage

**Validation**:
- Code compiles
- Clippy passes
- Tests pass (v1 methods not used yet)

#### Step 6: Implement V1 Methods for Cassandra Store
**Files**: `src/timers/store/cassandra/mod.rs`

**Changes**:
- Add table name constants: `TABLE_SLABS_V1`, `TABLE_KEYS_V1`
- Implement `get_slabs_v1()` - query old `slabs` table
- Implement `get_slab_triggers_v1()` - query old `slabs` table, return `TriggerV1`
- Implement `delete_slab_v1()` - DELETE from old `segments` and `slabs`
- Implement `clear_slab_triggers_v1()` - DELETE from old `slabs` and `keys`
- Implement `update_segment_version()` - UPDATE `segments` SET version, slab_size

**Validation**:
- Code compiles
- Clippy passes
- Integration tests pass (if any exist for basic store operations)

#### Step 7: Update Cassandra Store to Use V2 Tables
**Files**: `src/timers/store/cassandra/mod.rs`

**Changes**:
- Update table name constants to point to new tables:
  - `TABLE_SLABS_V2 = "timer_typed_slabs"`
  - `TABLE_KEYS_V2 = "timer_typed_keys"`
- Update all INSERT/SELECT/DELETE queries to:
  - Include `slab_size` in partition key
  - Include `type` in clustering key
  - Handle `timer_type` field in serialization/deserialization

**Validation**:
- Code compiles
- Clippy passes
- Tests will fail (v2 tables don't exist yet) - expected
- Schema migration needed

#### Step 8: Deploy Cassandra Schema Migration
**Files**: `src/cassandra/migrations/20250124_add_timer_types.cql` (new)

**Changes**:
- Create migration file with ALTER and CREATE statements
- Deploy to test environment
- Verify tables created correctly

**Validation**:
- Schema migration applies cleanly
- Can query new tables
- Old tables still exist
- Tests pass after schema deployed

#### Step 9: Update Memory Store to Use Slab Size in Keys
**Files**: `src/timers/store/memory.rs`

**Changes**:
- Update `slabs_v2` key from `(SegmentId, SlabId)` to `(SegmentId, SlabSize, SlabId)`
- Update all methods to use new key structure
- Ensure v1 storage also uses `(SegmentId, SlabSize, SlabId)` keys

**Validation**:
- Code compiles
- Clippy passes
- All tests pass

#### Step 10: Add Migration Error Variants
**Files**: `src/timers/error.rs`

**Changes**:
- Add `SegmentNotFound` error variant
- Add `UnknownSegmentVersion` error variant
- Add `MigrationFailed` error variant

**Validation**:
- Code compiles
- Clippy passes
- No breaking changes (new variants)

#### Step 11: Create Migration Module
**Files**: `src/timers/migration.rs` (new), `src/timers/mod.rs`

**Changes**:
- Create new module with `ensure_segment_current()` function
- Implement Phase 1: v1 → v2 migration logic
- Implement Phase 2: v2 slab size migration logic
- Add module declaration in `src/timers/mod.rs`

**Validation**:
- Code compiles
- Clippy passes
- Module exists but not yet called

#### Step 12: Integrate Migration into TimerManager
**Files**: `src/timers/manager.rs`

**Changes**:
- Update `TimerManager::new()` to call `ensure_segment_current()`
- Add `schedule_typed()` method (pub(crate))
- Update `schedule()` to call `schedule_typed()` with `Application` type

**Validation**:
- Code compiles
- Clippy passes
- Migration runs on manager initialization

#### Step 13: Update Segment Creation
**Files**: `src/timers/loader.rs`

**Changes**:
- Update `get_or_create_segment()` to set `version = Some(SEGMENT_VERSION_V2)` for new segments

**Validation**:
- Code compiles
- Clippy passes
- New segments created as v2

#### Step 14: Add UncommittedTimer Methods
**Files**: `src/timers/uncommitted.rs`

**Changes**:
- Add `timer_type()` method to `UncommittedTimer` trait
- Add `trigger()` method to `UncommittedTimer` trait
- Implement methods for `PendingTimer`

**Validation**:
- Code compiles
- Clippy passes
- New methods available to middleware

#### Step 15: Write Migration Tests
**Files**: `src/timers/store/tests/migration.rs` (new)

**Changes**:
- Add test utilities: `setup_v1_segment()`, `insert_v1_trigger()`, etc.
- Implement all 12 migration test scenarios
- Add property tests for migration invariants

**Validation**:
- Code compiles
- Clippy --tests passes
- All migration tests pass

#### Step 16: Update All Existing Tests
**Files**: All files in `src/timers/store/tests/`

**Changes**:
- Review each test file for any remaining `Trigger::new()` calls
- Ensure all tests work with v2 schema
- Update test utilities as needed

**Validation**:
- Code compiles
- Clippy --tests passes
- ALL tests pass (unit + integration)

#### Step 17: Final Validation
**Changes**: None (validation only)

**Validation**:
- `cargo build --release` succeeds
- `cargo clippy` passes with zero warnings
- `cargo clippy --tests` passes with zero warnings
- `cargo test` passes all tests
- `cargo doc` builds without warnings
- Integration tests with Cassandra pass

### Notes on Breaking Changes

**Step 2** is the only truly breaking step. The strategy is:

1. Make the change to `Trigger::new()`
2. Let compiler find all call sites (compilation errors)
3. Update each call site systematically
4. Group by file to minimize context switching
5. Use `TimerType::Application` as default everywhere
6. Only after ALL call sites updated should code compile again

This ensures we don't miss any call sites and the compiler enforces correctness.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Breaking change to `Trigger::new()` affects many call sites | Compiler will catch all call sites, systematic update |
| Migration could fail midway | Migration is idempotent and retryable, segments track version |
| Performance degradation from larger clustering keys | `tinyint` (1 byte) minimal overhead, lazy migration spreads load |
| Memory store behavior differs from Cassandra | Separate v1/v2 storage mirrors Cassandra's table separation |

## Post-Migration Cleanup

After all segments have been migrated (weeks/months):

1. Remove v1 methods from store trait
2. Drop old Cassandra tables (`slabs`, `keys`)
3. Remove migration module
4. Simplify code that handled version checking

## References

- Cassandra lightweight transactions: Used for segment updates
- Prosody partition ownership: Ensures single-process segment access
- Timer system architecture: `/src/timers/mod.rs`
- Cassandra migration framework: `/src/cassandra/migrator/mod.rs`

## Document Status: Ready for Implementation

### Completeness Checklist

- ✅ **Schema Design**: Complete V1 and V2 schemas with full CQL
- ✅ **Migration CQL**: Deployable migration file with all ALTER/CREATE statements
- ✅ **Core Type Changes**: `TimerType` enum and `Trigger` struct updates defined
- ✅ **API Design**: Public vs internal API boundaries clear
- ✅ **Migration Strategy**: Two-phase sequential approach fully specified
- ✅ **Store Interface**: V1/V2 method signatures with `slab_size` parameters
- ✅ **Migration Algorithm**: Pseudocode with explicit partition key handling
- ✅ **Safety Analysis**: Atomicity, failure scenarios, and recovery documented
- ✅ **Testing Strategy**: 12 migration scenarios + property tests + integration tests
- ✅ **Implementation Steps**: 17 incremental steps with validation criteria
- ✅ **Memory Store Design**: Mirrors Cassandra with separate v1/v2 storage
- ✅ **Performance Analysis**: Migration cost and lazy migration benefits
- ✅ **Deployment Strategy**: Steps, monitoring, rollback plan
- ✅ **Risk Analysis**: Identified risks with mitigations

### What Makes This Document Implementation-Ready

1. **Complete Schema**: Every table, column, and constraint is specified in valid CQL
2. **Partition Key Critical Design**: `slab_size` in partition key enables safe migrations
3. **Incremental Steps**: 17 steps where each leaves code compilable with passing clippy
4. **Breaking Change Strategy**: Explicit approach for handling `Trigger::new()` signature change
5. **Test-First Approach**: Comprehensive test scenarios defined before implementation
6. **Store Abstraction**: Both Cassandra and memory store designs specified
7. **Migration Isolation**: Complexity isolated to store layer, core code stays clean
8. **Validation Criteria**: Each step has clear validation requirements

### Implementation Readiness

This document provides sufficient detail to begin implementation. The 17-step incremental approach ensures:

- Code remains compilable after each step
- Clippy warnings are addressed immediately
- Tests can be written and validated progressively
- Rollback is possible at any point
- Multiple developers can work in parallel on different steps

The most critical decisions are documented:
- Why `tinyint` for timer types (storage efficiency)
- Why `slab_size` in partition key (migration safety)
- Why sequential phases (simplicity and testability)
- Why lazy migration (gradual rollout, bounded work)

**Recommendation**: Begin with Step 1 (Add TimerType Enum) and proceed sequentially through Step 17.
