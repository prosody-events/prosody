# Feature Specification: Inline/Overflow Timer State

**Feature Branch**: `001-reduce-timer-tombstones`
**Created**: 2026-02-20
**Status**: Draft
**Input**: User description: "Implement inline/overflow state design for strict singleton invariant enforcement in the Cassandra timer store to reduce tombstones"

## Scenarios & Testing *(mandatory)*

### Scenario 1 - Singleton Timer Reschedule Without Tombstones (Priority: P1)

The dominant production workload is the timer reschedule cycle: a key has one timer, it fires, and is immediately rescheduled. Today, every reschedule creates a range tombstone because the system cannot distinguish "one timer in singleton slot" from "multiple timers in clustering rows." With the inline/overflow state column, the system knows the key is inline and issues a plain UPDATE instead of a BATCH DELETE + INSERT.

**Why this priority**: This is the hot path. Eliminating tombstones on every reschedule directly addresses the core problem — Cassandra tombstone accumulation causing read amplification and compaction pressure.

**Independent Test**: Can be tested by calling `clear_and_schedule_key` on a key that already has one timer and verifying the system issues a plain UPDATE (no range tombstone), and that the timer data is correct on subsequent reads.

**Acceptance Criteria**:

1. **Given** a key with exactly one timer (inline state), **When** `clear_and_schedule_key` is called with a new timer, **Then** the system writes via plain UPDATE (no BATCH DELETE), and subsequent reads return the new timer.
2. **Given** a key in inline state, **When** the state cache is warm, **Then** `clear_and_schedule_key` performs 0 DB reads and 1 DB write.
3. **Given** a key in inline state, **When** `clear_and_schedule_key` succeeds, **Then** the cache is updated to `Inline(new_timer)`.

---

### Scenario 2 - State Transitions Across All Mutations (Priority: P1)

The system must correctly track the timer count state (Absent/Inline/Overflow) through every possible mutation sequence. Inserting a second timer must promote from Inline to Overflow (moving the inline timer to clustering). Deleting down to one timer must demote from Overflow to Inline. Clearing must reset to Absent.

**Why this priority**: Correctness of the three-state invariant is the foundation the entire tombstone reduction relies on. If state transitions are wrong, the system either loses timer data or creates unnecessary tombstones.

**Independent Test**: Can be tested by executing a sequence of mutations (insert, insert, delete, clear_and_schedule, clear) and verifying the state column and clustering rows match the expected invariant at each step.

**Acceptance Criteria**:

1. **Given** a key with state `Absent`, **When** a timer is inserted via `insert_key_trigger`, **Then** the timer is written inline and state becomes `Inline(timer)`.
2. **Given** a key with state `Inline(timer_A)`, **When** a second timer is inserted, **Then** timer_A is promoted to clustering, the new timer is written to clustering, and state becomes `Overflow`.
3. **Given** a key with state `Overflow` and exactly 2 timers, **When** one timer is deleted, **Then** the remaining timer is demoted to inline and state becomes `Inline(remaining_timer)`.
4. **Given** a key with state `Overflow`, **When** `delete_key_trigger` removes the last remaining timer, **Then** state becomes `Absent`.
5. **Given** a key in any state, **When** `clear_key_triggers` is called, **Then** all clustering rows and the state entry are removed, and state becomes `Absent`.
6. **Given** a key in any state, **When** `clear_key_triggers_all_types` is called, **Then** all types' clustering rows and the entire state column are removed, and all types become `Absent`.

---

### Scenario 3 - Pre-Migration Data Compatibility (Priority: P1)

At deployment, every existing timer in production has data in clustering rows with `state = null`. The system must handle this gracefully: reads must find timers, writes must not lose data, and the dominant reschedule path must naturally migrate keys to the new format.

**Why this priority**: Deployment safety is non-negotiable. If pre-migration data is mishandled, timers are lost in production.

**Independent Test**: Can be tested by directly writing clustering rows without any state column entries (simulating production data), then verifying all read and write operations behave correctly.

**Acceptance Criteria**:

1. **Given** a key with 1 timer in clustering and `state = null`, **When** `get_key_times` or `get_key_triggers` is called, **Then** the timer is found via clustering scan.
2. **Given** a key with 2 timers in clustering and `state = null`, **When** `get_key_triggers` is called, **Then** both timers are returned.
3. **Given** a key with timers in clustering and `state = null`, **When** `clear_and_schedule_key` is called, **Then** old clustering data is deleted (BATCH path) and the new timer is written inline, migrating the key to the new format.
4. **Given** a key with timers in clustering and `state = null`, **When** `insert_key_trigger` is called, **Then** the new timer is added to clustering without data loss and without setting state (state remains `null`).
5. **Given** a key with timers in clustering and `state = null`, **When** `delete_key_trigger` is called, **Then** the targeted timer is deleted without data loss.

---

### Scenario 4 - Per-Partition Cache Lifecycle (Priority: P2)

Each partition must have its own isolated state cache, created when the partition is assigned and destroyed when the partition is revoked. The cache must never leak state across partition ownership changes.

**Why this priority**: Cache isolation prevents stale data from causing incorrect state decisions. Necessary for correctness but lower priority because the system falls back to DB reads on cache misses (safe by default).

**Independent Test**: Can be tested by simulating partition assignment, performing operations that warm the cache, revoking the partition, re-assigning it, and verifying the cache starts cold.

**Acceptance Criteria**:

1. **Given** a partition is assigned, **When** the store is created via the provider, **Then** the store has a fresh, empty cache.
2. **Given** two partitions assigned simultaneously, **When** operations are performed on both, **Then** each partition's cache is independent (mutations on one do not appear in the other).
3. **Given** a partition is revoked, **When** the partition manager shuts down and the task completes, **Then** the cache is destroyed (no dangling references).
4. **Given** the per-partition state cache is implemented, **Then** the `CachedTriggerStore` global cache wrapper is removed and all its functionality is subsumed by the per-partition cache.

---

### Scenario 5 - Read Path Cache Optimization (Priority: P2)

Once a key's state is cached (after any write), subsequent reads for that key should be served from cache without any DB queries. This is significant during slab loading where many keys are read in sequence.

**Why this priority**: Performance optimization that reduces DB load. Important for slab loading performance but not critical for correctness.

**Independent Test**: Can be tested by performing a `clear_and_schedule_key`, then calling `get_key_times` and `get_key_triggers` and verifying the results are returned without DB reads (from cache).

**Acceptance Criteria**:

1. **Given** a key with cached state `Inline(timer)`, **When** `get_key_times` is called, **Then** the time is served from cache with 0 DB queries.
2. **Given** a key with cached state `Inline(timer)`, **When** `get_key_triggers` is called, **Then** the full trigger (time + span) is served from cache with 0 DB queries.
3. **Given** a key with cached state `Overflow`, **When** `get_key_triggers` is called, **Then** the state read is skipped and only clustering rows are scanned (1 DB query instead of 2).
4. **Given** a key with cached state `Absent`, **When** `get_key_times` is called, **Then** nothing is returned with 0 DB queries.

---

### Scenario 6 - Provider Pattern for Store Creation (Priority: P2)

The store must be created via a provider/factory pattern where shared resources (Cassandra session, prepared statements) are held by the provider and per-partition resources (cache) are created fresh per partition. This aligns with the existing `TimerDeferStoreProvider` pattern in the codebase.

**Why this priority**: Architectural consistency with existing patterns. Enables the per-partition cache lifecycle without changing the global session management.

**Independent Test**: Can be tested by creating a provider, calling `create_store` multiple times, and verifying that stores share the session/queries but have independent caches.

**Acceptance Criteria**:

1. **Given** a `CassandraTriggerStoreProvider`, **When** `create_store` is called, **Then** the returned store shares the session and prepared statements with the provider.
2. **Given** a `CassandraTriggerStoreProvider`, **When** `create_store` is called twice, **Then** the two stores have independent caches.
3. **Given** an `InMemoryTriggerStoreProvider`, **When** `create_store` is called, **Then** it returns a fresh in-memory store suitable for testing.

---

### Edge Cases

- What happens when `inline = true` but `time` is null (corrupted UDT)? System treats it as `Overflow` and falls back to clustering scan.
- What happens when the cache is evicted due to capacity? System falls through to a DB read, which is the same as a cold cache. No correctness impact. The `quick_cache` crate handles eviction automatically using a frequency-based policy.
- What happens when a write fails after the DB operation but before cache update? Cache is not updated (only updated on success). Next read will hit the DB and get the correct state.
- What happens when `delete_key_trigger` is called on an `Inline` key where the time does not match? The clustering DELETE is a no-op, the inline timer remains unchanged.
- What happens when `Absent` is read from DB (ambiguous: 0 timers or pre-migration)? It is NOT cached. All code paths handle it safely by falling through to clustering scan or BATCH writes.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST maintain the three-state invariant for every `(segment_id, key, timer_type)`: 0 timers = Absent, 1 timer = Inline (data in state column), >1 timers = Overflow (data in clustering rows, overflow marker in state column).
- **FR-002**: System MUST encode the timer state as a tagged UDT with an explicit `inline` boolean discriminator inside a `MAP<TINYINT, FROZEN<key_timer_state>>` static column, replacing the current `singleton_timers` column.
- **FR-003**: System MUST treat `inline = null` as `false` (overflow/scan) as the safe default — only trust inline data when explicitly marked `inline = true`.
- **FR-004**: System MUST handle pre-migration data (timers in clustering with `state = null`) correctly on all code paths: reads scan clustering, `clear_and_schedule_key` takes the BATCH path, `insert_key_trigger` writes clustering only.
- **FR-005**: System MUST naturally migrate pre-migration keys to the new format through the dominant `clear_and_schedule_key` path without requiring a bulk migration.
- **FR-006**: System MUST cache `TimerState` per-partition with the cache key `(Key, TimerType)` (no `SegmentId` — implicit in store instance). Each per-partition cache MUST have a capacity of 8,192 entries.
- **FR-007**: System MUST only cache `Absent` after its own write operations (clear/delete), never after a DB read that returns `Absent` (which is ambiguous for pre-migration data).
- **FR-008**: System MUST implement the provider/factory pattern (`TriggerStoreProvider` trait) where shared resources (session, prepared statements) are held by the provider and per-partition caches are created fresh per `create_store` call.
- **FR-009**: System MUST demote from Overflow to Inline when a delete leaves exactly 1 timer remaining, reading the remaining row and writing it as inline state in a BATCH.
- **FR-010**: System MUST destroy the per-partition cache when the partition is revoked (via Rust ownership — drop the store when the task completes).
- **FR-011**: System MUST skip the DB read for state when the cache has a hit, for all write and read operations.
- **FR-012**: System MUST NOT disable or remove any existing test invariants. Test coverage MUST only increase.
- **FR-013**: System MUST implement `InMemoryTriggerStoreProvider` for the in-memory store used in tests.
- **FR-014**: System MUST instrument new store methods with the existing tracing pattern (`#[instrument(level = "debug", skip(self), err)]`, `info_span!` for per-trigger spans, OpenTelemetry context propagation). No new metrics counters.

### Key Entities

- **TimerState**: The resolved three-state enum (Absent, Inline, Overflow) representing the timer count for a given key and timer type.
- **InlineTimer**: The data for a single inlined timer (time + span), stored directly in the state column UDT.
- **RawTimerState**: The Cassandra serde type for the `key_timer_state` UDT (inline boolean, optional time, optional span). Private to the Cassandra module.
- **CassandraTriggerStoreProvider**: Shared factory holding session and prepared statements, creating per-partition stores.
- **State Cache**: Per-partition `Cache<(Key, TimerType), TimerState>` inside each `CassandraTriggerStore` instance.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The dominant timer reschedule path (`clear_and_schedule_key` for a key with 1 timer) produces 0 tombstones when the cache is warm (steady state: after the first `clear_and_schedule_key` call for a given key).
- **SC-002**: All existing tests continue to pass without modification to their invariant assertions. No tests are disabled or weakened.
- **SC-003**: New tests cover all 6 state transitions (Absent→Inline, Inline→Overflow, Overflow→Inline, Overflow→Absent, Any→Absent via clear, pre-migration handling) and verify the invariant at each step.
- **SC-004**: Property tests verify the three-state invariant holds after arbitrary sequences of operations on any (segment, key, type).
- **SC-005**: Pre-migration data (timers in clustering with no state column) is readable and writable without data loss across all operations.
- **SC-006**: Cache-warm read paths serve inline timer data with 0 DB queries.

## Clarifications

### Session 2026-02-20

- Q: Should the new inline/overflow state system emit observability signals for cache hit rate, state transitions, or migration progress? → A: Follow existing trace instrumentation patterns already in the codebase (`#[instrument]` on store methods, `info_span!` for per-trigger trace continuity, OpenTelemetry context propagation). No new metrics counters — traces only, consistent with the current approach.

## Assumptions

- The branch is unreleased, so the existing `20260217_add_singleton_slots.cql` migration can be modified in place rather than adding a new migration file.
- The `timer_slot` UDT will be replaced by `key_timer_state` in the same migration file.
- The `singleton_timers` column will be renamed to `state` in the schema.
- Per-partition cache capacity of 8,192 entries is sufficient for the working set.
- The `InlineTimer` span HashMap typically contains ~2 OTEL key-value pairs (~200 bytes per entry).
- No bulk migration is needed — natural convergence through the reschedule cycle is acceptable.
