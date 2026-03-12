# Tasks: Inline/Overflow Timer State

**Input**: Design documents from `/specs/001-reduce-timer-tombstones/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md

**Tests**: The spec requires tests (SC-002, SC-003, SC-004, SC-005, FR-012). Test tasks are included.

**Organization**: Tasks are grouped by scenario to enable independent implementation and testing of each scenario.

## Format: `[ID] [P?] [S#] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[S#]**: Which scenario this task belongs to (e.g., S1, S2, S3)
- Include exact file paths in descriptions

## Phase 1: Setup (Schema & Type Foundation)

**Purpose**: Modify the CQL migration, define the new Rust types, and prepare the serialization boundary. No behavioral
changes yet — existing code continues to work against the old column names until Phase 2 rewires it.

- [x] T001 Update CQL migration to replace `timer_slot` UDT with `key_timer_state` and rename `singleton_timers` to
  `state` in `src/cassandra/migrations/20260217_add_singleton_slots.cql`
- [x] T002 Define `InlineTimer` struct (replaces `TimerSlot` as domain type) with `Clone, Debug, PartialEq, Eq` in
  `src/timers/store/cassandra/mod.rs`
- [x] T003 Define `TimerState` enum (`Absent`, `Inline(InlineTimer)`, `Overflow`) with `Clone, Debug, PartialEq, Eq` in
  `src/timers/store/cassandra/mod.rs`
- [x] T004 Define private `RawTimerState` UDT serde struct with `DeserializeValue, SerializeValue` and `Option<bool>`,
  `Option<CompactDateTime>`, `Option<HashMap<String, String>>` fields in `src/timers/store/cassandra/mod.rs`
- [x] T005 Implement `into_timer_state(Option<RawTimerState>) -> TimerState` conversion function with safe defaults (
  `inline: null` → `false` → `Overflow`; `inline: true, time: None` → `Overflow` as corrupt fallback) in
  `src/timers/store/cassandra/mod.rs`

---

## Phase 2: Foundational (Queries, Provider Trait, Store Restructure)

**Purpose**: Replace prepared statements, introduce the `TriggerStoreProvider` trait, restructure
`CassandraTriggerStore` to use per-partition `state_cache`, and implement `InMemoryTriggerStoreProvider`. After this
phase, the store compiles with the new types but existing `TriggerOperations` methods are not yet rewired.

**CRITICAL**: No scenario work can begin until this phase is complete.

- [x] T006 Replace all singleton slot queries with new state queries in `src/timers/store/cassandra/queries.rs`: rename
  `get_singleton_slot` → `get_state`, `set_singleton_slot`/`_no_ttl` → `set_state_inline`/`_no_ttl`,
  `remove_singleton_entry` → `remove_state_entry`, `clear_singleton_slots` → `clear_state`,
  `batch_clear_and_set_singleton`/`_no_ttl` → `batch_clear_and_set_inline`/`_no_ttl`; update all CQL strings to
  reference `state` column and `key_timer_state` UDT; add new `set_state_overflow` query (writes
  `{inline: false, time: null, span: null}`); add new `count_key_triggers` query (
  `SELECT time FROM ... WHERE segment_id = ? AND key = ? AND timer_type = ? LIMIT 2`)
- [x] T007 Define `TriggerStoreProvider` trait with `type Store: TriggerStore` and
  `fn create_store(&self, topic: Topic, partition: Partition, consumer_group: &str) -> Self::Store` in
  `src/timers/store/mod.rs`
- [x] T008 Restructure `CassandraTriggerStore`: replace `singleton_keys: Arc<Cache<SingletonCacheKey, ()>>` with
  `state_cache: Arc<Cache<(Key, TimerType), TimerState>>` (capacity 8,192); remove `SingletonCacheKey` type alias;
  update `with_store` constructor to accept cache; update `Clone` and `Debug` derives in
  `src/timers/store/cassandra/mod.rs`
- [x] T009 Implement `CassandraTriggerStoreProvider` struct with `store: CassandraStore`, `queries: Arc<Queries>`,
  `slab_size: CompactDuration` fields, `Clone` derive, and `TriggerStoreProvider` impl that creates a new
  `CassandraTriggerStore` with fresh `Arc<Cache>` per call in `src/timers/store/cassandra/mod.rs`
- [x] T010 [P] Implement `InMemoryTriggerStoreProvider` struct that creates fresh `InMemoryTriggerStore` per
  `create_store` call, implementing `TriggerStoreProvider` in `src/timers/store/memory.rs`
- [x] T011 Replace private singleton helper methods on `CassandraTriggerStore`: rename `get_singleton_slot` →
  `get_timer_state` (returns `TimerState` via `into_timer_state`); rename `get_singleton_slot_map` →
  `get_timer_state_map` (returns `HashMap<TimerType, TimerState>`); rename `clear_and_set_singleton_slot` →
  `batch_clear_and_set_inline`; rename `set_singleton_slot` → `set_state_inline`; rename `remove_singleton_entry` →
  `remove_state_entry`; add `set_state_overflow` method; add `peek_trigger_times` method (executes `count_key_triggers`
  query, returns count ≤ 2); remove old `TimerSlot` and `SlotState` types in `src/timers/store/cassandra/mod.rs`

**Checkpoint**: Store compiles with new types, queries, and provider. Existing `TriggerOperations` methods need
rewiring.

---

## Phase 3: Scenario 1 — Singleton Timer Reschedule Without Tombstones (Priority: P1) MVP

**Goal**: The dominant reschedule path (`clear_and_schedule_key` on Inline state) produces zero tombstones via plain
UPDATE. Cache-warm path skips DB reads entirely.

**Independent Test**: Call `clear_and_schedule_key` on a key with one timer, verify plain UPDATE write path and correct
read-back.

### Implementation for Scenario 1

- [x] T012 [S1] Rewrite `clear_and_schedule_key` in `TriggerOperations` impl: check `state_cache` for `(Key, TimerType)`
  hit; on `Inline` hit → `set_state_inline` (plain UPDATE, 0 tombstones); on `Overflow` hit →
  `batch_clear_and_set_inline` (BATCH); on cache `Absent` hit (from our own clear/delete — known 0 timers) →
  `set_state_inline` (plain UPDATE); on cache miss → call `get_timer_state` from DB, branch on result: DB `Inline` →
  UPDATE, DB `Overflow` → BATCH, DB `Absent` (ambiguous: 0 timers or pre-migration) → BATCH (safe default); on success
  cache `Inline(new_timer)` in `src/timers/store/cassandra/mod.rs`
- [x] T013 [S1] Update `clear_key_triggers` in `TriggerOperations` impl: replace `remove_singleton_entry` with
  `remove_state_entry`; replace `clear_singleton_slots` references with `clear_state`; after success cache `Absent` for
  `(key, timer_type)` in `state_cache` in `src/timers/store/cassandra/mod.rs`
- [x] T014 [S1] Update `clear_key_triggers_all_types` in `TriggerOperations` impl: replace `clear_singleton_slots` with
  `clear_state`; replace `singleton_keys` eviction loop with `state_cache` eviction for all `TimerType::VARIANTS`; after
  success cache `Absent` for all types in `src/timers/store/cassandra/mod.rs`

### Tests for Scenario 1

- [x] T015 [S1] Update `test_singleton_slot_round_trip` integration test: rename to reflect new naming (
  `test_inline_state_round_trip`); update type references from `TimerSlot`/`SlotState` to `InlineTimer`/`TimerState`;
  verify `clear_and_schedule_key` on inline key uses UPDATE path and reads back correctly in
  `src/timers/store/cassandra/mod.rs`. NOTE: This test covers the basic inline round-trip only (write + read). T018
  covers the full state transition matrix. Ensure no duplication — if T018 subsumes this test's coverage, consider
  merging.

**Checkpoint**: Dominant reschedule path (Inline → Inline) is tombstone-free. `clear_key_triggers` and
`clear_key_triggers_all_types` work with new state column.

---

## Phase 4: Scenario 2 — State Transitions Across All Mutations (Priority: P1)

**Goal**: All mutation methods correctly transition between Absent/Inline/Overflow states. `insert_key_trigger` handles
promotion (Inline → Overflow). `delete_key_trigger` handles demotion (Overflow → Inline) and removal (Overflow → Absent,
Inline → Absent).

**Independent Test**: Execute a sequence of insert/insert/delete/clear_and_schedule/clear operations and verify the
state column matches the expected invariant at each step.

### Implementation for Scenario 2

- [x] T016 [S2] Rewrite `insert_key_trigger` in `TriggerOperations` impl: check `state_cache`; on `Inline(old)` hit →
  BATCH: promote old timer to clustering + write new to clustering + `set_state_overflow`, cache `Overflow`; on
  `Overflow` hit → write clustering only (1 query), cache stays `Overflow`; on `Absent` hit (from our clear, known 0
  timers) → `set_state_inline` with new timer, cache `Inline(new)`; on cache miss (DB `Absent`, pre-migration safe) →
  write clustering only, do NOT cache; on DB `Inline`/`Overflow` → same as cache hit logic, cache result in
  `src/timers/store/cassandra/mod.rs`
- [x] T017 [S2] Rewrite `delete_key_trigger` in `TriggerOperations` impl: check `state_cache`; on `Inline(timer)` hit
  where time matches → delete clustering row (no-op) + `remove_state_entry`, cache `Absent`; on `Inline(timer)` hit
  where time does NOT match → delete clustering row (no-op), cache unchanged; on `Overflow` hit → delete clustering
  row + `peek_trigger_times` (LIMIT 2): 0 remaining → `remove_state_entry`, cache `Absent`; 1 remaining → BATCH: read
  remaining trigger, `set_state_inline` + delete remaining clustering row, cache `Inline(remaining)`; 2+ remaining → no
  state change, cache stays `Overflow`; on `Absent` hit → delete clustering only, cache unchanged; on cache miss →
  `get_timer_state` from DB, then branch as above in `src/timers/store/cassandra/mod.rs`

### Tests for Scenario 2

- [x] T018 [S2] Update `test_state_transitions_one_and_two_timers` integration test: update all type references from
  `SlotState`/`TimerSlot` to `TimerState`/`InlineTimer`; verify all 6 transitions (Absent→Inline, Inline→Overflow,
  Overflow→Inline, Overflow→Absent, Any→Absent via clear, Any→Absent via clear_all_types); verify state column and
  clustering rows match invariant at each step; verify all 3 read paths (`get_key_times`, `get_key_triggers`,
  `get_key_triggers_all_types`) return correct data at each step in `src/timers/store/cassandra/mod.rs`

**Checkpoint**: All state transitions are correct. Invariant is maintained through arbitrary mutation sequences.

---

## Phase 5: Scenario 3 — Read Path Rewrite & Pre-Migration Data Compatibility (Priority: P1)

**Goal**: Rewrite all read paths with cache-first logic and ensure keys with timers in clustering rows and
`state = null` (pre-migration data) are handled correctly on all code paths without data loss. Natural migration through
`clear_and_schedule_key`. This phase also implements the read path cache optimization required by Scenario 5.

**Independent Test**: Write clustering rows directly (without state column), verify reads find timers and
`clear_and_schedule_key` migrates to inline format.

### Implementation for Scenario 3

- [x] T019 [S3] Update `get_key_times` read path in `TriggerOperations` impl: check `state_cache`; on `Inline(timer)`
  hit → yield timer.time from cache (0 DB queries); on `Overflow` hit → skip state read, scan clustering only; on
  `Absent` hit → yield nothing (0 DB queries); on cache miss → `get_timer_state` from DB: if `Inline` → yield from
  state + cache; if `Overflow` → scan clustering + cache; if `Absent` → scan clustering (pre-migration safe), do NOT
  cache in `src/timers/store/cassandra/mod.rs`
- [x] T020 [S3] Update `get_key_triggers` read path in `TriggerOperations` impl: same cache-first logic as T019 but
  yields full `Trigger` objects (time + span context from `InlineTimer`); for cache hit `Inline`, create span from
  stored HashMap using existing propagator pattern; on cache miss follow same DB fallback as T019 in
  `src/timers/store/cassandra/mod.rs`
- [x] T021 [S3] Update `get_key_triggers_all_types` read path: reads state map from DB (not cache — this reads all types
  in one query), distinguishes `Inline` entries from `Overflow` entries per type, merges with clustering stream;
  maintain existing merge logic but use `TimerState` instead of `SlotState` in `src/timers/store/cassandra/mod.rs`
- [x] T022 [S3] Audit cache population rules across all mutation and read methods (T012, T016, T017, T019, T020): verify
  that after writes `Inline`/`Overflow`/`Absent` are cached correctly; verify that DB reads cache `Inline` and
  `Overflow` but NOT `Absent` (ambiguous for pre-migration data); verify write failures do NOT update cache. This is a
  code review pass, not new implementation — the rules are embedded in each method's rewrite in
  `src/timers/store/cassandra/mod.rs`
- [x] T023 [P] [S3] Add `add_key_trigger_clustering` public test helper method on `CassandraTriggerStore` (gated behind
  `#[cfg(test)]`) that inserts a trigger directly into clustering columns without touching the state column, for
  simulating pre-migration data in tests in `src/timers/store/cassandra/mod.rs`

### Tests for Scenario 3

- [x] T024 [S3] Add `test_pre_migration_reads_and_migration` integration test: use `add_key_trigger_clustering` to write
  1 and 2 timers with `state = null`; verify `get_key_times` and `get_key_triggers` find them via clustering scan;
  verify `clear_and_schedule_key` migrates key to inline format; verify subsequent reads serve from cache in
  `src/timers/store/cassandra/mod.rs`
- [x] T025 [S3] Add `test_pre_migration_mutations` integration test: use `add_key_trigger_clustering` for pre-migration
  data; verify `insert_key_trigger` adds to clustering without data loss (state stays null); verify `delete_key_trigger`
  removes targeted timer without data loss; verify `clear_key_triggers` clears everything in
  `src/timers/store/cassandra/mod.rs`

**Checkpoint**: Pre-migration data is fully safe. All code paths handle `state = null` correctly. Natural migration
works.

---

## Phase 6: Scenario 4 — Per-Partition Cache Lifecycle (Priority: P2)

**Goal**: Each partition gets its own isolated state cache via the provider pattern. Caches are created fresh per
partition and destroyed on revocation via Rust ownership.

**Independent Test**: Create a provider, call `create_store` twice, verify stores have independent caches.

### Implementation for Scenario 4

- [X] T026 [S4] Refactor `StorePair` to use providers: in `StorePair::Cassandra` variant, replace
  `trigger: TableAdapter<CassandraTriggerStore>` with `trigger_provider: CassandraTriggerStoreProvider`; in
  `StorePair::Memory` variant, replace `trigger: TableAdapter<InMemoryTriggerStore>` with
  `trigger_provider: InMemoryTriggerStoreProvider`; update `StorePair::new()` to construct providers instead of stores
  in `src/consumer/storage.rs`
- [X] T027 [S4] Update `PartitionConfiguration` to hold a trigger store provider instead of a store: replace the
  `trigger_store: T` field with a provider field; update `handle_messages` to call
  `provider.create_store(topic, partition, &group_id)` at the point where `segment_id` is known to create a
  per-partition store instance in `src/consumer/partition/mod.rs`
- [X] T028 [S4] Update all call sites that construct `PartitionConfiguration` or pattern-match on `StorePair` to pass
  the provider instead of the store; update generic type parameters where `T: TriggerStore` becomes
  `P: TriggerStoreProvider` at the partition level in `src/consumer/partition/mod.rs` and
  `src/consumer/kafka_context.rs`. Audit all compilation errors after the type parameter change and fix any additional
  call sites beyond `partition/mod.rs` and `kafka_context.rs` — the type change may ripple into other consumer modules
  that reference `TriggerStore` generics
- [X] T029 [S4] Remove `CachedTriggerStore` wrapper: delete `src/timers/store/cached.rs`; remove its `mod cached`
  declaration from `src/timers/store/mod.rs`; remove all `CachedTriggerStore` usage from `StorePair` and
  `PartitionConfiguration`; the per-partition `state_cache` inside `CassandraTriggerStore` replaces this

### Tests for Scenario 4

- [X] T030 [S4] Update `CachedTriggerStore` test suite: migrate the `trigger_store_tests!` macro invocation from
  `src/timers/store/cached.rs` to use `InMemoryTriggerStore` directly (or via `InMemoryTriggerStoreProvider`); ensure
  all existing compliance tests still pass; verify the `trigger_store_tests!` macro works with the new provider-created
  stores in `src/timers/store/tests/mod.rs` and `src/timers/store/cached.rs`

**Checkpoint**: Per-partition cache lifecycle works. Caches are isolated. `CachedTriggerStore` removed.

---

## Phase 7: Scenario 5 — Read Path Cache Optimization (Priority: P2)

**Goal**: Cache-warm reads serve data directly from cache with 0 DB queries. `Inline` cache hit returns time+span from
memory. `Overflow` cache hit skips state read. `Absent` cache hit returns empty.

**Independent Test**: Perform `clear_and_schedule_key`, then call `get_key_times` and `get_key_triggers` — verify
results come from cache.

### Implementation for Scenario 5

Note: The read path cache logic was already implemented in T019, T020, T021, T022 (Phase 5). This phase verifies and
tests the optimization is working correctly.

- [x] T031 [P] [S5] Verify all read paths in `TriggerOperations` impl check `state_cache` before DB reads:
  `get_key_times` (from T019), `get_key_triggers` (from T020); ensure `Inline` cache hit creates span from stored
  HashMap and yields trigger without DB query; ensure `Overflow` cache hit skips state read; ensure `Absent` cache hit
  yields nothing immediately in `src/timers/store/cassandra/mod.rs`

### Tests for Scenario 5

- [x] T032 [P] [S5] Add read path cache verification to `test_state_transitions_one_and_two_timers`: after each mutation
  that caches state, verify reads return correct data (implicitly from cache since state was just written); this extends
  T018 rather than creating a separate test in `src/timers/store/cassandra/mod.rs`

**Checkpoint**: Cache-warm reads work. `Inline` hits serve 0-query reads. `Overflow` hits save 1 query.

---

## Phase 8: Scenario 6 — Provider Pattern for Store Creation (Priority: P2)

**Goal**: Provider pattern works end-to-end. Shared resources (session, queries) are held by provider. Per-partition
stores have independent caches.

**Independent Test**: Create provider, call `create_store` multiple times, verify shared session but independent caches.

### Implementation for Scenario 6

Note: Provider struct and trait were implemented in T007, T009, T010 (Phase 2) and integrated in T026–T028 (Phase 6).
This phase adds tracing instrumentation and the end-to-end test.

- [x] T033 [S6] Add `#[instrument]` tracing to all new and modified methods per FR-014. This is a single-pass task that
  adds instrumentation AFTER all method rewrites (T012–T021) are complete: `get_timer_state`, `set_state_inline`,
  `set_state_overflow`, `remove_state_entry`, `batch_clear_and_set_inline`, `peek_trigger_times`; use existing pattern (
  `#[instrument(level = "debug", skip(self), err)]`); add `info_span!` for per-trigger spans in read paths where
  triggers are served from cache in `src/timers/store/cassandra/mod.rs`. NOTE: Phases 3–5 intentionally defer tracing to
  this task to avoid churn during method rewrites.

### Tests for Scenario 6

- [x] T034 [S6] Add `test_provider_creates_independent_stores` integration test: create `CassandraTriggerStoreProvider`;
  call `create_store` twice; perform `clear_and_schedule_key` on store A; verify store B cache is cold (reads hit DB);
  verify both stores share session (same connection pool) in `src/timers/store/cassandra/mod.rs`

**Checkpoint**: Provider pattern works end-to-end. Stores share session, have independent caches.

---

## Phase 9: Property Tests & Cross-Cutting Verification

**Purpose**: Property tests verify the three-state invariant holds after arbitrary operation sequences. Existing tests
pass. Lints clean.

- [X] T035 [P] Update `test_prop_singleton_invariant` property test: rename to `test_prop_timer_state_invariant`; after
  each operation sequence, verify the three-state invariant by reading state from DB and comparing to reference model (
  `0 timers = Absent, 1 timer = Inline, >1 timers = Overflow`); update type references from `SlotState`/`TimerSlot` to
  `TimerState`/`InlineTimer` in `src/timers/store/cassandra/mod.rs`
- [X] T036 [P] Add `test_clear_all_types_clears_inline_and_overflow` integration test: set key type A to `Inline`, key
  type B to `Overflow`; call `clear_key_triggers_all_types`; verify both types become `Absent` and all clustering rows
  are gone in `src/timers/store/cassandra/mod.rs`
- [X] T037 [P] Update `trigger_store_tests!` macro invocations: ensure InMemoryTriggerStore tests still pass with
  `clear_and_schedule_key` behavior (memory store has no state column but must maintain same observable behavior);
  update any test helpers that reference `SlotState` or `TimerSlot` in `src/timers/store/tests/mod.rs`
- [X] T038 Run `cargo clippy`, `cargo clippy --tests`, `cargo doc`, `cargo +nightly fmt` — fix any warnings or errors
  across all modified files
- [X] T039 Run full test suite `cargo test 2>&1 | tee /tmp/test_output.log` — verify all existing tests pass, no tests
  disabled or weakened (FR-012), and all new tests pass

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all scenarios
- **Phase 3 (S1)**: Depends on Phase 2
- **Phase 4 (S2)**: Depends on Phase 3 (S1 rewires `clear_and_schedule_key` which S2 extends)
- **Phase 5 (S3)**: Depends on Phase 4 (S2 rewires `insert_key_trigger`/`delete_key_trigger` which S3 uses)
- **Phase 6 (S4)**: Depends on Phase 5 (needs all store methods finalized before provider integration)
- **Phase 7 (S5)**: Depends on Phase 5 (read path cache logic implemented in Phase 5)
- **Phase 8 (S6)**: Depends on Phase 6 (provider integration from Phase 6)
- **Phase 9 (Polish)**: Depends on all previous phases

### Scenario Dependencies

- **S1 (Reschedule)**: Foundation only — MVP
- **S2 (State Transitions)**: Requires S1 (`clear_and_schedule_key` must be rewired first)
- **S3 (Pre-Migration)**: Requires S2 (all mutation methods must be rewired)
- **S4 (Cache Lifecycle)**: Requires S3 (all store methods finalized)
- **S5 (Read Cache)**: Requires S3 (read paths rewired in S3); can run parallel with S4
- **S6 (Provider Pattern)**: Requires S4 (provider integration)

### Within Each Scenario

- Implementation before tests (tests verify implementation)
- Core mutation logic before read path optimizations
- Store-level changes before consumer-level integration

### Parallel Opportunities

- T002, T003, T004 can run in parallel (independent type definitions)
- T007, T010 can run in parallel (different files: `mod.rs` vs `memory.rs`)
- T023 can run in parallel with other S3 tasks (test helper, `#[cfg(test)]`)
- T035, T036, T037 can run in parallel (independent test files/concerns)
- Phase 7 (S5) and Phase 6 (S4) can overlap: S5 verifies read paths (already implemented in S3), S4 does provider
  integration
- T031, T032 (S5) can run in parallel with T026–T029 (S4) — read path verification is independent of provider
  integration

---

## Implementation Strategy

### MVP First (Scenario 1 Only)

1. Complete Phase 1: Setup (T001–T005)
2. Complete Phase 2: Foundational (T006–T011)
3. Complete Phase 3: Scenario 1 (T012–T015)
4. **STOP and VALIDATE**: The dominant reschedule path is tombstone-free
5. Run `cargo test` — all existing tests must still pass

### Incremental Delivery

1. Phase 1 + 2 → Foundation ready (compiles, types defined, queries prepared)
2. Phase 3 (S1) → Test: `clear_and_schedule_key` on Inline uses plain UPDATE
3. Phase 4 (S2) → Test: All state transitions verified via `test_state_transitions`
4. Phase 5 (S3) → Test: Pre-migration data safe, natural migration works
5. Phase 6 (S4) → Test: Per-partition cache isolation, `CachedTriggerStore` removed
6. Phase 7 (S5) → Test: Cache-warm reads serve 0-query results
7. Phase 8 (S6) → Test: Provider creates stores with shared session, independent caches
8. Phase 9 → Full property tests, lint clean, all tests green

---

## Notes

- [P] tasks = different files, no dependencies
- [S#] label maps task to specific scenario for traceability
- All query rename/restructuring is in Phase 2 to avoid partial states
- `CachedTriggerStore` removal (T029) is deferred to Phase 6 to avoid breaking compilation during Phase 3–5 rewiring
- Pre-migration safety (S3) is P1 because it affects deployment; it is sequenced after S2 because the mutation rewiring
  in S2 is a prerequisite
- Property test update (T035) runs after all mutations are rewired to avoid testing intermediate states
