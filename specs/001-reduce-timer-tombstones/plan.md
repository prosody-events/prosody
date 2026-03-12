# Implementation Plan: Inline/Overflow Timer State

**Branch**: `001-reduce-timer-tombstones` | **Date**: 2026-02-20 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-reduce-timer-tombstones/spec.md`
**Design Reference**: [docs/inline-overflow-design.md](/docs/inline-overflow-design.md)

## Summary

Replace the current `singleton_timers` static column and `SlotState`/`TimerSlot` types with a tagged UDT (`key_timer_state`) and three-state `TimerState` enum (Absent/Inline/Overflow) that strictly tracks per-key timer count. Introduce a per-partition state cache inside `CassandraTriggerStore` (replacing both the global `singleton_keys` cache and the `CachedTriggerStore` wrapper) and a `TriggerStoreProvider` factory pattern. The result: the dominant reschedule path produces zero tombstones when the cache is warm, pre-migration data is handled safely on all code paths, and cache lifecycle is tied to partition ownership via Rust's drop semantics.

## Technical Context

**Language/Version**: Rust Edition 2024 (stable)
**Primary Dependencies**: scylla 1.2 (Cassandra driver), tokio 1.45, parking_lot 0.12, quick_cache 0.6, scc 3.0, tracing 0.1, tracing-opentelemetry 0.32, opentelemetry 0.31, thiserror 2.0, async-stream 0.3, smallvec 1.15, strum 0.27
**Storage**: Apache Cassandra via scylla-rust-driver — `timer_typed_keys` and `timer_typed_slabs` tables
**Testing**: cargo test (unit), quickcheck 1.0 + quickcheck_macros 1.1 (property), color-eyre 0.6 (test assertions), Cassandra integration tests (`#[ignore]`)
**Target Platform**: Linux server (Kafka consumer library)
**Project Type**: Single Rust crate (library)
**Performance Goals**: Dominant reschedule path (cache-warm `clear_and_schedule_key`) = 0 DB reads, 1 DB write, 0 tombstones. Cache-warm reads = 0 DB queries.
**Constraints**: Per-partition cache ≤8,192 entries (~1.6 MB/partition). No `ALLOW FILTERING`, no secondary indices, no materialized views.
**Scale/Scope**: ~15 files modified/created. Core changes in `src/timers/store/cassandra/`, `src/timers/store/`, and `src/consumer/storage.rs`.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Error Handling (NON-NEGOTIABLE) | PASS | All new code propagates errors with `?`. `thiserror` for `CassandraTriggerStoreError`. No `unwrap`/`expect`/`panic`. UDT field `inline: Option<bool>` uses `unwrap_or_default()` (returns `false`, not panic). |
| II. Code Quality (NON-NEGOTIABLE) | PASS | All changes must pass `cargo clippy`, `cargo clippy --tests`, `cargo doc`, `cargo +nightly fmt`. |
| III. Debugging Discipline | PASS | No claims without evidence. Design proven via property tests and integration tests. |
| IV. Testing Standards | PASS | No `sleep` in tests. Channel-based synchronization. `assert`/`color_eyre::Result` only. Integration test output preserved via `tee`. |
| V. Simplicity | PASS | No over-engineering. Single per-partition cache replaces two separate caches. No bulk migration (natural convergence). Provider pattern matches existing `TimerDeferStoreProvider`. |
| VI. Trait-Based Interfaces | PASS | New `TriggerStoreProvider` trait for factory pattern. Existing `TriggerStore`/`TriggerOperations` traits unchanged. `InMemoryTriggerStoreProvider` for test mocking. |
| VII. Documentation Independence (NON-NEGOTIABLE) | PASS | No references to spec numbers, tickets, or scenarios in code. All code documentation describes technical contracts. |
| Cassandra Anti-Patterns | PASS | No `ALLOW FILTERING`, no secondary indices, no materialized views. Proper partition keys and clustering columns. `Option<T>` for NULLs filtered in application code. |

**Pre-Phase 0 Gate: PASS** — No violations. Proceeding to research.

## Project Structure

### Documentation (this feature)

```text
specs/001-reduce-timer-tombstones/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
src/timers/store/
├── mod.rs                    # TriggerStore trait (9 methods, unchanged)
├── operations.rs             # TriggerOperations trait (20 methods, unchanged)
├── adapter.rs                # TableAdapter<T> (unchanged)
├── cached.rs                 # CachedTriggerStore — REMOVED (replaced by per-partition cache)
├── memory.rs                 # InMemoryTriggerStore + InMemoryTriggerStoreProvider (modified)
├── cassandra/
│   ├── mod.rs                # CassandraTriggerStore + CassandraTriggerStoreProvider (major changes)
│   ├── queries.rs            # Prepared CQL statements (new/modified queries)
│   ├── migration.rs          # V1→V2 migration (unchanged)
│   └── v1/
│       └── tests/            # V1 Cassandra tests (unchanged)
└── tests/
    ├── mod.rs                # Test framework + Arbitrary impls (update macro)
    ├── common.rs             # Shared test helpers
    └── prop_key_triggers.rs  # Property tests (update for state verification)

src/cassandra/migrations/
└── 20260217_add_singleton_slots.cql  # Modified: timer_slot → key_timer_state, singleton_timers → state

src/consumer/
├── storage.rs                # StorePair (refactor to use providers)
└── partition/
    └── mod.rs                # PartitionConfiguration (trigger_store_provider instead of trigger_store)
```

**Structure Decision**: Single Rust crate. All changes are within the existing `src/timers/store/` subtree, the CQL migration, and `src/consumer/` integration points. No new modules or crates needed.

## Complexity Tracking

No constitution violations to justify.
