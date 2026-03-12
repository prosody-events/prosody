# Data Model: Inline/Overflow Timer State

**Feature Branch**: `001-reduce-timer-tombstones`
**Date**: 2026-02-20

## Entities

### 1. `InlineTimer` (replaces `TimerSlot`)

Timer data for a single inlined timer. Only exists when the state is `Inline`.

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `time` | `CompactDateTime` (i32) | Required | Timer trigger time |
| `span` | `HashMap<String, String>` | Required (may be empty) | OpenTelemetry span context for trace continuity |

**Location**: `src/timers/store/cassandra/mod.rs`
**Derives**: `Clone, Debug, PartialEq, Eq`
**Note**: Unlike `TimerSlot`, this does NOT derive `SerializeValue`/`DeserializeValue` — it is the resolved domain type. The raw Cassandra serde type is `RawTimerState`.

### 2. `TimerState` (replaces `SlotState`)

Resolved three-state enum for a `(key, timer_type)` pair within a partition.

| Variant | Fields | Meaning |
|---------|--------|---------|
| `Absent` | None | No timers for this key/type |
| `Inline(InlineTimer)` | Timer data | Exactly 1 timer, stored in state column |
| `Overflow` | None | >1 timers, stored in clustering rows |

**Location**: `src/timers/store/cassandra/mod.rs`
**Derives**: `Clone, Debug, PartialEq, Eq`
**Cache key**: `(Key, TimerType)` — `SegmentId` is implicit in the store instance.

### 3. `RawTimerState` (new, private)

Cassandra UDT serde type. Used only at the module boundary for serialization/deserialization.

| Field | Type | CQL Type | Description |
|-------|------|----------|-------------|
| `inline` | `Option<bool>` | `boolean` | `true` = inline data present; `false`/`null` = overflow marker |
| `time` | `Option<CompactDateTime>` | `int` | Timer time (present only when `inline = true`) |
| `span` | `Option<HashMap<String, String>>` | `frozen<map<text, text>>` | Span context (present only when `inline = true`) |

**Location**: `src/timers/store/cassandra/mod.rs` (private)
**Derives**: `DeserializeValue, SerializeValue`

**Conversion to `TimerState`**:

```
None                          → Absent
Some(inline=true, time=Some)  → Inline(InlineTimer { time, span })
Some(inline=true, time=None)  → Overflow (corrupt data, safe fallback)
Some(inline=false/null, ...)  → Overflow
```

### 4. `CassandraTriggerStoreProvider` (new)

Factory holding shared Cassandra resources. Creates per-partition stores.

| Field | Type | Sharing | Description |
|-------|------|---------|-------------|
| `store` | `CassandraStore` | Shared (Arc internally) | Cassandra session |
| `queries` | `Arc<Queries>` | Shared | Prepared statements |
| `slab_size` | `CompactDuration` | Copied | Time partitioning size |

**Location**: `src/timers/store/cassandra/mod.rs`
**Derives**: `Clone`
**Trait**: `TriggerStoreProvider`

### 5. `CassandraTriggerStore` (modified)

Per-partition store with its own state cache.

| Field | Type | Sharing | Description |
|-------|------|---------|-------------|
| `store` | `CassandraStore` | Arc clone from provider | Cassandra session |
| `queries` | `Arc<Queries>` | Arc clone from provider | Prepared statements |
| `slab_size` | `CompactDuration` | Copy from provider | Time partitioning size |
| `state_cache` | `Arc<Cache<(Key, TimerType), TimerState>>` | Per-partition | Timer state cache |

**Changes from current**:
- `singleton_keys: Arc<Cache<SingletonCacheKey, ()>>` → `state_cache: Arc<Cache<(Key, TimerType), TimerState>>`
- Cache key loses `SegmentId` (implicit in store instance)
- Cache value changes from `()` (presence-only) to full `TimerState` (including `InlineTimer` data)

### 6. `TriggerStoreProvider` trait (new)

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_store` | `fn create_store(&self, topic: Topic, partition: Partition, consumer_group: &str) -> Self::Store` | Creates a per-partition store with fresh cache |

**Associated type**: `Store: TriggerStore` (via `TableAdapter`)
**Bounds**: `Clone + Send + Sync + 'static`
**Location**: `src/timers/store/mod.rs`

### 7. `InMemoryTriggerStoreProvider` (new)

Trivial provider for tests. Creates fresh `InMemoryTriggerStore` per call.

**Location**: `src/timers/store/memory.rs`

## CQL Schema Changes

### UDT: `key_timer_state` (replaces `timer_slot`)

```sql
CREATE TYPE IF NOT EXISTS {{KEYSPACE}}.key_timer_state (
    inline boolean,
    time int,
    span frozen<map<text, text>>
);
```

### Column: `state` (replaces `singleton_timers`)

```sql
ALTER TABLE {{KEYSPACE}}.timer_typed_keys ADD (
    state map<tinyint, frozen<key_timer_state>> static
);
```

**Migration file**: `src/cassandra/migrations/20260217_add_singleton_slots.cql` (modified in place; branch is unreleased).

## State Transitions

```
                 insert (first)
    Absent ─────────────────────────► Inline(timer)
      ▲                                   │
      │ clear / delete(last)              │ insert (second)
      │                                   ▼
      └──────────────── Overflow ◄────────┘
                           │
                           │ delete (→1 remaining)
                           ▼
                      Inline(remaining)
```

| From | Operation | To | Write Strategy |
|------|-----------|-----|---------------|
| Absent (cache) | `insert_key_trigger` | `Inline(new)` | Plain UPDATE (set inline state) |
| Absent (DB) | `insert_key_trigger` | N/A (not cached) | Write clustering only (pre-migration safe) |
| Absent (any) | `clear_and_schedule_key` | `Inline(new)` | BATCH if DB Absent, UPDATE if cache Absent |
| Inline(old) | `clear_and_schedule_key` | `Inline(new)` | Plain UPDATE (0 tombstones) |
| Inline(A) | `insert_key_trigger` | `Overflow` | BATCH: promote A to clustering + write new + set overflow |
| Inline(A) | `delete_key_trigger` (time matches) | `Absent` | Remove state entry |
| Inline(A) | `delete_key_trigger` (time mismatch) | `Inline(A)` | No-op |
| Overflow | `clear_and_schedule_key` | `Inline(new)` | BATCH: DELETE clustering + set inline |
| Overflow | `insert_key_trigger` | `Overflow` | Write clustering only |
| Overflow | `delete_key_trigger` (→0) | `Absent` | Remove state entry |
| Overflow | `delete_key_trigger` (→1) | `Inline(remaining)` | BATCH: demote remaining to inline |
| Overflow | `delete_key_trigger` (→2+) | `Overflow` | No state change |
| Any | `clear_key_triggers` | `Absent` | Delete state entry + clear clustering |
| Any | `clear_key_triggers_all_types` | `Absent` (all types) | Delete state column + clear clustering |

## Cache Population Rules

| Event | Cache Action |
|-------|-------------|
| Successful write (any mutation) | Update cache with resulting state |
| DB read → `Inline(timer)` | Cache `Inline(timer)` |
| DB read → `Overflow` | Cache `Overflow` |
| DB read → `Absent` | **Do NOT cache** (ambiguous: 0 timers or pre-migration) |
| Write failure | **Do NOT update cache** (only cache on success) |

## Prepared Statements (New/Modified)

| Name | CQL | Purpose |
|------|-----|---------|
| `get_state` | `SELECT state FROM timer_typed_keys WHERE segment_id = ? AND key = ? LIMIT 1` | Read state MAP for a key |
| `set_state_inline` | `UPDATE timer_typed_keys SET state[?] = ? WHERE segment_id = ? AND key = ?` (+TTL variant) | Write inline timer state |
| `set_state_overflow` | `UPDATE timer_typed_keys SET state[?] = ? WHERE segment_id = ? AND key = ?` | Write overflow marker |
| `remove_state_entry` | `DELETE state[?] FROM timer_typed_keys WHERE segment_id = ? AND key = ?` | Remove single type's state entry |
| `clear_state` | `DELETE state FROM timer_typed_keys WHERE segment_id = ? AND key = ?` | Clear entire state column |
| `batch_clear_and_set_inline` | BATCH: DELETE clustering + UPDATE state[?] = ? (+TTL variant) | Atomic clear+set for Overflow→Inline or Absent→Inline |
| `count_key_triggers` | `SELECT time FROM timer_typed_keys WHERE segment_id = ? AND key = ? AND timer_type = ? LIMIT 2` | Demotion check (count remaining after delete) |
