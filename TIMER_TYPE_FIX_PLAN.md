# Timer Type Partitioning Fix - Implementation Plan

## Executive Summary

**Problem**: Current v2 schema does not include `timer_type` in primary keys, making it impossible to efficiently query, delete, or partition operations by timer type. This is a critical design flaw that must be fixed before v2 rollout.

**Impact**: Breaking changes to schema and trait methods, but v2 hasn't rolled out yet so this is safe.

**Timeline**: Must be completed before v2 production deployment.

---

## Current Schema Flaws

### typed_keys (BROKEN)
```sql
CREATE TABLE typed_keys (
    segment_id uuid,
    key text,
    time int,
    type tinyint,           -- NOT IN PRIMARY KEY ❌
    span map<text, text>,
    PRIMARY KEY ((segment_id, key), time)
)
```

**Problems**:
- ❌ Cannot efficiently query by type (requires `ALLOW FILTERING`)
- ❌ Cannot efficiently delete by type
- ❌ `clear_key_triggers(segment, key)` destroys ALL types
- ❌ `delete_key_trigger(segment, key, time)` is ambiguous - which type?
- ❌ Multiple rows can have same (segment, key, time) but different types

### typed_slabs (BROKEN)
```sql
CREATE TABLE typed_slabs (
    segment_id uuid,
    slab_size int,
    id int,
    key text,
    time int,
    type tinyint,           -- NOT IN PRIMARY KEY ❌
    span map<text, text>,
    PRIMARY KEY ((segment_id, slab_size, id), key, time)
)
```

**Problems**:
- ❌ Cannot efficiently query by type
- ❌ Cannot efficiently delete by type
- ❌ `delete_slab_trigger(slab, key, time)` is ambiguous - which type?
- ❌ Multiple rows can have same (slab, key, time) but different types

---

## Corrected Schema Design

### typed_keys (CORRECT)
```sql
CREATE TABLE typed_keys (
    segment_id uuid,
    key text,
    timer_type tinyint,
    time int,
    span frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, key), timer_type, time)
)
WITH compression = { 'class': 'ZstdCompressor' }
AND compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' };
```

**Benefits**:
- ✅ Partition key: `(segment_id, key)` - we always know both when querying
- ✅ Clustering key: `(timer_type, time)` - efficient filtering by type
- ✅ Efficient queries: `WHERE segment_id = ? AND key = ? AND timer_type = ?`
- ✅ Both timer types for a key in same partition but efficiently separable
- ✅ Fewer partitions than separating by type (better for low-cardinality keys)
- ✅ `(segment, key, type, time)` uniquely identifies a trigger

**Why timer_type in clustering key, not partition key**:
- We **always know** the key when querying typed_keys
- Clustering key filter `WHERE ... AND timer_type = ?` is efficient
- Keeps both types for a key together (fewer partitions, better for small keys)
- No benefit to separate partitions per type since we always provide the key

### typed_slabs (CORRECT)
```sql
CREATE TABLE typed_slabs (
    segment_id uuid,
    slab_size int,
    id int,
    timer_type tinyint,
    key text,
    time int,
    span frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, slab_size, id), timer_type, key, time)
)
WITH compression = { 'class': 'ZstdCompressor' }
AND compaction = { 'class': 'UnifiedCompactionStrategy' };
```

**Benefits**:
- ✅ Each slab is a partition (as before)
- ✅ `timer_type` FIRST in clustering key for efficient range scans by type
- ✅ Efficient queries: `WHERE segment_id = ? AND slab_size = ? AND id = ? AND timer_type = ?`
- ✅ Can delete all triggers of specific type from slab
- ✅ `(slab, type, key, time)` uniquely identifies a trigger

**Why timer_type comes BEFORE key in clustering key**:
- Allows efficient range scans: `WHERE ... AND timer_type = ?` (gets only defer timers)
- Without it, would need to filter on client side (slow)
- Order: type → key → time (from general to specific)

---

## Trait Method Changes

### Methods Requiring timer_type Parameter

| Method | Current Signature | New Signature | Reason |
|--------|------------------|---------------|--------|
| `get_key_times` | `(segment_id, key)` | `(segment_id, timer_type, key)` | Query specific type only |
| `get_key_triggers` | `(segment_id, key)` | `(segment_id, timer_type, key)` | Query specific type only |
| `clear_key_triggers` | `(segment_id, key)` | `(segment_id, timer_type, key)` | Clear specific type only |
| `delete_key_trigger` | `(segment_id, key, time)` | `(segment_id, timer_type, key, time)` | Unique identification |
| `get_slab_triggers` | `(segment_id, slab_id)` | `(slab, timer_type)` | Query specific type only; slab contains slab_size |
| `delete_slab_trigger` | `(slab, key, time)` | `(slab, timer_type, key, time)` | Unique identification |

### Methods NOT Requiring Changes

| Method | Signature | Reason |
|--------|-----------|--------|
| `insert_slab_trigger` | `(slab, trigger)` | Trigger contains timer_type ✅ |
| `insert_key_trigger` | `(segment_id, trigger)` | Trigger contains timer_type ✅ |
| `clear_slab_triggers` | `(slab)` | Clears ALL types (used in slab_size migration) ✅ |

### Updated Trait Signatures

```rust
// === Key trigger operations ===

fn get_key_times(
    &self,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send;

fn get_key_triggers(
    &self,
    segment_id: &SegmentId,
    timer_type: TimerType,
    key: &Key,
) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

fn insert_key_trigger(
    &self,
    segment_id: &SegmentId,
    trigger: Trigger,  // Contains timer_type - NO CHANGE
) -> impl Future<Output = Result<(), Self::Error>> + Send;

fn delete_key_trigger(
    &self,
    segment_id: &SegmentId,
    timer_type: TimerType,  // NEW PARAMETER
    key: &Key,
    time: CompactDateTime,
) -> impl Future<Output = Result<(), Self::Error>> + Send;

fn clear_key_triggers(
    &self,
    segment_id: &SegmentId,
    timer_type: TimerType,  // NEW PARAMETER
    key: &Key,
) -> impl Future<Output = Result<(), Self::Error>> + Send;

// === Slab trigger operations ===

fn get_slab_triggers(
    &self,
    slab: &Slab,  // CHANGED: Now takes Slab reference (contains segment_id, slab_size, slab_id)
    timer_type: TimerType,  // NEW PARAMETER
) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

fn insert_slab_trigger(
    &self,
    slab: Slab,
    trigger: Trigger,  // Contains timer_type - NO CHANGE
) -> impl Future<Output = Result<(), Self::Error>> + Send;

fn delete_slab_trigger(
    &self,
    slab: &Slab,
    timer_type: TimerType,  // NEW PARAMETER
    key: &Key,
    time: CompactDateTime,
) -> impl Future<Output = Result<(), Self::Error>> + Send;

fn clear_slab_triggers(
    &self,
    slab: &Slab,  // NO CHANGE - clears ALL types
) -> impl Future<Output = Result<(), Self::Error>> + Send;
```

---

## Memory Store Changes

### Current Structure (BROKEN)
```rust
struct Inner {
    // Does NOT partition by timer_type ❌
    slab_triggers: HashMap<(SegmentId, CompactDuration, SlabId), BTreeSet<Trigger>>,
    key_triggers: HashMap<(SegmentId, Key), BTreeSet<Trigger>>,
}
```

### Corrected Structure
```rust
struct Inner {
    // Partitioned by timer_type ✅
    slab_triggers: HashMap<(SegmentId, CompactDuration, SlabId, TimerType), BTreeSet<Trigger>>,
    key_triggers: HashMap<(SegmentId, TimerType, Key), BTreeSet<Trigger>>,
}
```

**Implementation Notes**:
- Memory store includes `TimerType` in HashMap keys for O(1) lookup, even though Cassandra has it in clustering key
- This is a valid optimization: Memory store doesn't need to exactly mirror Cassandra's structure
- When filtering by timer_type, directly access the correct HashMap entry
- clear_slab_triggers must iterate over ALL TimerType values (Application + Defer) and clear each HashMap entry
- BTreeSet<Trigger> still works since Trigger has Ord trait

**Why Memory store differs from Cassandra**:
- Cassandra: `typed_keys` has `PRIMARY KEY ((segment_id, key), timer_type, time)` - timer_type in clustering key
- Memory: HashMap key is `(SegmentId, TimerType, Key)` - timer_type in map key for direct lookup
- This gives Memory store O(1) access by type, while Cassandra does clustering key filtering
- Both approaches are correct for their respective storage models

---

## Cassandra Query Changes

### Insert Queries (TTL Required!)

**insert_slab_trigger** (UPDATED):
```sql
INSERT INTO typed_slabs (segment_id, slab_size, id, timer_type, key, time, span)
VALUES (?, ?, ?, ?, ?, ?, ?)
USING TTL ?
```

**insert_key_trigger** (UPDATED):
```sql
INSERT INTO typed_keys (segment_id, key, timer_type, time, span)
VALUES (?, ?, ?, ?, ?)
USING TTL ?
```

### Query Operations (UPDATED)

**get_key_times**:
```sql
SELECT time
FROM typed_keys
WHERE segment_id = ? AND key = ? AND timer_type = ?
```

**get_key_triggers**:
```sql
SELECT key, time, type, span
FROM typed_keys
WHERE segment_id = ? AND key = ? AND timer_type = ?
```

**get_slab_triggers** (slab parameter provides segment_id, slab_size, id):
```sql
SELECT key, time, type, span
FROM typed_slabs
WHERE segment_id = ? AND slab_size = ? AND id = ? AND timer_type = ?
-- All values extracted from slab parameter
```

### Delete Operations (UPDATED)

**delete_key_trigger**:
```sql
DELETE FROM typed_keys
WHERE segment_id = ? AND key = ? AND timer_type = ? AND time = ?
```

**clear_key_triggers**:
```sql
DELETE FROM typed_keys
WHERE segment_id = ? AND key = ? AND timer_type = ?
```

**delete_slab_trigger**:
```sql
DELETE FROM typed_slabs
WHERE segment_id = ? AND slab_size = ? AND id = ? AND timer_type = ? AND key = ? AND time = ?
```

**clear_slab_triggers** (NO CHANGE):
```sql
DELETE FROM typed_slabs
WHERE segment_id = ? AND slab_size = ? AND id = ?
```
Note: Deletes entire partition (all types) - used in slab_size migration.

---

## Migration File Changes

### File to Modify
`src/cassandra/migrations/20251023_add_timer_types.cql`

### New Schema (Replaces Existing)

```sql
-- Migration: add_timer_types
-- Timestamp: 20251023

-- Add version column to segments table
ALTER TABLE {{KEYSPACE}}.{{TABLE_SEGMENTS}} ADD version tinyint static;

-- V2 time-based index with timer type support
-- CORRECTED: timer_type in clustering key, before key
CREATE TABLE {{KEYSPACE}}.{{TABLE_TYPED_SLABS}}
(
    segment_id uuid,
    slab_size  int,
    id         int,
    timer_type tinyint,
    key        text,
    time       int,
    span       frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, slab_size, id), timer_type, key, time)
)
WITH compression = { 'class': 'ZstdCompressor' }
AND compaction = { 'class': 'UnifiedCompactionStrategy' };

-- V2 key-based index with timer type support
-- CORRECTED: timer_type in clustering key (not partition key)
CREATE TABLE {{KEYSPACE}}.{{TABLE_TYPED_KEYS}}
(
    segment_id uuid,
    key        text,
    timer_type tinyint,
    time       int,
    span       frozen<map<text, text>>,
    PRIMARY KEY ((segment_id, key), timer_type, time)
)
WITH compression = { 'class': 'ZstdCompressor' }
AND compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' };
```

**Key Changes**:
1. `typed_slabs`: Added `timer_type` to clustering key, **before** `key`
2. `typed_keys`: Added `timer_type` to clustering key (NOT partition key)
3. Both tables use `frozen<map<text, text>>` for `span` (was missing `frozen`)

---

## Implementation Checklist

### Phase 1: Schema (CRITICAL - DO FIRST)
- [ ] Update `src/cassandra/migrations/20251023_add_timer_types.cql` with corrected schema
- [ ] Verify migration can be applied cleanly to test database
- [ ] Drop any existing v2 test data if needed

### Phase 2: Trait Definition
- [ ] Update `TriggerStore` trait in `src/timers/store/mod.rs`:
  - [ ] Add `timer_type` parameter to `get_key_times`
  - [ ] Add `timer_type` parameter to `get_key_triggers`
  - [ ] Add `timer_type` parameter to `clear_key_triggers`
  - [ ] Add `timer_type` parameter to `delete_key_trigger`
  - [ ] Add `timer_type` parameter to `get_slab_triggers`
  - [ ] Add `timer_type` parameter to `delete_slab_trigger`
  - [ ] Update documentation for all changed methods

### Phase 3: Memory Store Implementation
- [ ] Update `Inner` struct in `src/timers/store/memory.rs`:
  - [ ] Change `slab_triggers` HashMap key to include `TimerType`
  - [ ] Change `key_triggers` HashMap key to include `TimerType`
- [ ] Update all Memory store method implementations:
  - [ ] `get_key_times` - filter by timer_type
  - [ ] `get_key_triggers` - filter by timer_type
  - [ ] `insert_key_trigger` - use trigger.timer_type in HashMap key
  - [ ] `delete_key_trigger` - use timer_type in HashMap key
  - [ ] `clear_key_triggers` - use timer_type in HashMap key
  - [ ] `get_slab_triggers` - use slab.segment_id, slab.slab_size, slab.id, timer_type in HashMap key
  - [ ] `insert_slab_trigger` - use trigger.timer_type in HashMap key
  - [ ] `delete_slab_trigger` - use timer_type in HashMap key
  - [ ] `clear_slab_triggers` - iterate ALL timer types (both Application + Defer)

### Phase 4: Cassandra Queries
- [ ] Update query preparations in `src/timers/store/cassandra/queries.rs`:
  - [ ] `prepare_get_key_times` - add timer_type to WHERE clause
  - [ ] `prepare_get_key_triggers` - add timer_type to WHERE clause
  - [ ] `prepare_insert_key_trigger` - add timer_type to INSERT (verify TTL!)
  - [ ] `prepare_delete_key_trigger` - add timer_type to WHERE clause
  - [ ] `prepare_clear_key_triggers` - add timer_type to WHERE clause
  - [ ] `prepare_get_slab_triggers` - add timer_type to WHERE clause
  - [ ] `prepare_insert_slab_trigger` - add timer_type to INSERT (verify TTL!)
  - [ ] `prepare_delete_slab_trigger` - add timer_type to WHERE clause
  - [ ] Verify `prepare_clear_slab_triggers` does NOT include timer_type (clears all)

### Phase 5: Cassandra Store Implementation
- [ ] Update all Cassandra store method implementations in `src/timers/store/cassandra/mod.rs`:
  - [ ] `get_key_times` - pass timer_type to query
  - [ ] `get_key_triggers` - pass timer_type to query
  - [ ] `insert_key_trigger` - extract timer_type from trigger, verify TTL calculation
  - [ ] `delete_key_trigger` - pass timer_type to query
  - [ ] `clear_key_triggers` - pass timer_type to query
  - [ ] `get_slab_triggers` - extract segment_id, slab_size, id from slab; pass timer_type to query
  - [ ] `insert_slab_trigger` - extract timer_type from trigger, verify TTL calculation
  - [ ] `delete_slab_trigger` - pass timer_type to query
  - [ ] `clear_slab_triggers` - verify it clears ALL types (no timer_type in query)

### Phase 6: Test Updates (Largest Effort)
- [ ] Update all test helper functions in `src/timers/store/tests/common.rs`:
  - [ ] `get_key_triggers` - add timer_type parameter
  - [ ] `get_slab_triggers` - add timer_type parameter
  - [ ] `clear_key_triggers` - add timer_type parameter
  - [ ] Any other affected helpers
- [ ] Update all test files:
  - [ ] `src/timers/store/tests/cleanup_operations.rs`
  - [ ] `src/timers/store/tests/primitive_operations.rs`
  - [ ] `src/timers/store/tests/trigger_consistency.rs`
  - [ ] `src/timers/store/tests/trigger_operations.rs`
  - [ ] `src/timers/store/tests/cross_slab.rs`
  - [ ] `src/timers/store/tests/contention.rs`
  - [ ] `src/timers/store/tests/sequential_interleavings.rs`
  - [ ] `src/timers/store/tests/migration.rs` (especially v2 tests)
  - [ ] Any other test files calling affected methods

### Phase 7: Migration Code Updates
- [ ] Update `src/timers/migration.rs`:
  - [ ] `migrate_triggers` - ensure it migrates Application type (default for v1)
  - [ ] Any calls to updated trait methods
  - [ ] Verify TTL is calculated correctly during migration

### Phase 8: Integration and Verification
- [ ] Run full test suite: `cargo test --lib`
- [ ] Run clippy: `cargo clippy --lib --tests`
- [ ] Verify both Memory and Cassandra store tests pass
- [ ] Test Cassandra migration on clean database
- [ ] Test Cassandra migration with existing v1 data
- [ ] Verify all inserts have TTL (except segments)

---

## Call Site Estimate

Based on grep analysis, approximate call sites to update:

| Method | Estimated Call Sites |
|--------|---------------------|
| `get_key_times` | ~5 |
| `get_key_triggers` | ~15 |
| `clear_key_triggers` | ~10 |
| `delete_key_trigger` | ~20 |
| `get_slab_triggers` | ~25 |
| `delete_slab_trigger` | ~15 |
| **Total** | **~90 call sites** |

---

## Testing Strategy

### Unit Tests
- [ ] Each trait method has dedicated test in test suite
- [ ] Test both Application and Defer timer types
- [ ] Test that types are properly isolated (no cross-contamination)

### Integration Tests
- [ ] Test get_key_triggers with Application type returns only Application
- [ ] Test get_key_triggers with Defer type returns only Defer
- [ ] Test clear_key_triggers with Application doesn't affect Defer
- [ ] Test clear_slab_triggers clears ALL types

### Migration Tests
- [ ] Test v1→v2 migration creates Application timers
- [ ] Test migrated timers queryable by type
- [ ] Test mixed types in same segment

---

## Rollback Plan

Since v2 hasn't rolled out:
1. Drop typed_slabs and typed_keys tables
2. Revert migration file
3. Reapply old migration
4. No data loss (v1 data still exists)

---

## Success Criteria

- [ ] All 313+ tests passing
- [ ] Clippy clean (lib + tests)
- [ ] Can query triggers by timer_type efficiently
- [ ] Can delete triggers of specific type without affecting others
- [ ] clear_slab_triggers still works for slab_size migration
- [ ] All inserts include TTL (except segments)
- [ ] No performance regression

---

## Risk Assessment

**Low Risk** because:
- v2 hasn't rolled out yet
- Can completely break v2 schema
- v1→v2 migration logic already exists
- All changes are internal to storage layer

**Highest Risk Areas**:
1. Missing a call site (compile-time safety helps here)
2. TTL not applied to inserts (manual verification needed)
3. clear_slab_triggers breaking if not updated correctly

---

## Timeline Estimate

- Phase 1 (Schema): 30 minutes
- Phase 2 (Trait): 1 hour
- Phase 3 (Memory Store): 2 hours
- Phase 4-5 (Cassandra): 3 hours
- Phase 6 (Tests): 4-6 hours (largest effort)
- Phase 7 (Migration): 1 hour
- Phase 8 (Verification): 2 hours

**Total: 13-15 hours** of focused work

---

## Notes

- **TTL Critical**: Every insert must have TTL except segments
- **clear_slab_triggers**: Must clear ALL types (used in slab_size migration)
- **slab_size in partition key**: Intentional for slab_size migration support
- **frozen<map>**: Required for nested types in Cassandra
- **timer_type ordering**: In typed_slabs clustering key, timer_type comes BEFORE key for efficient filtering
