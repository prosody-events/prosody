# Research: Inline/Overflow Timer State

**Feature Branch**: `001-reduce-timer-tombstones`
**Date**: 2026-02-20

## R1: Cassandra MAP NULL Semantics and UDT Discriminant Encoding

**Decision**: Use a tagged UDT (`key_timer_state`) with an explicit `inline: boolean` discriminator inside a `MAP<TINYINT, FROZEN<key_timer_state>>` static column.

**Rationale**: Cassandra MAP entries cannot hold NULL values. `UPDATE ... SET map[key] = null` is equivalent to `DELETE map[key]`. This means the current `singleton_timers MAP<TINYINT, FROZEN<timer_slot>>` column can only encode two states per type: absent (no entry) and present (entry with data). The `SlotState::Overflow` branch (`Some(None)`) in the existing code is unreachable dead code. The tagged UDT solves this by encoding the discriminant inside the frozen value: `{inline: false, time: null, span: null}` is a valid non-NULL UDT value that Cassandra stores correctly.

**Alternatives Considered**:
- **Separate `SET<TINYINT>` overflow column**: Rejected because two independent columns permit the invalid state where a type appears in both inline and overflow simultaneously.
- **Sentinel values in time field** (e.g., `time = -1` for overflow): Rejected because it conflates data with metadata and risks collision with valid time values.

## R2: scylla-rust-driver 1.2 UDT Serialization with Option Fields

**Decision**: Use `#[derive(DeserializeValue, SerializeValue)]` on a `RawTimerState` struct with `Option<bool>`, `Option<CompactDateTime>`, and `Option<HashMap<String, String>>` fields.

**Rationale**: The scylla 1.2 driver supports `Option<T>` in UDT structs to handle nullable Cassandra fields. Field names must match the CQL UDT field names. The driver handles serialization of `None` as CQL NULL within the UDT. For deserialization, `None` is produced when the CQL field is NULL.

**Alternatives Considered**:
- **Manual `impl SerializeValue`/`DeserializeValue`**: Rejected per Simplicity principle; derive macros handle the common case correctly.

## R3: Per-Partition Cache vs Global Cache

**Decision**: Per-partition `Arc<Cache<(Key, TimerType), TimerState>>` inside `CassandraTriggerStore`, replacing both the global `singleton_keys: Arc<Cache<SingletonCacheKey, ()>>` and the global `CachedTriggerStore` wrapper.

**Rationale**: Partition ownership guarantees that while we own a partition, we see every read and write for every key in it. A per-partition cache is automatically consistent without cross-partition coordination. When a partition is revoked, Rust's ownership model drops the store and its cache — no manual cleanup or epoch counters needed. The global cache survived partition revocation, which meant stale entries could persist across ownership changes.

**Alternatives Considered**:
- **Keep global cache with revocation hook**: Rejected because it requires explicit cleanup code and risks stale entries if cleanup is missed. Per-partition cache gets cleanup for free via drop.
- **Keep `CachedTriggerStore` wrapper with per-partition scoping**: Rejected because `CachedTriggerStore` caches `opentelemetry::Context` per trigger, while `TimerState` cache is per key — different granularity. Merging them adds complexity.

## R4: Pre-Migration Data Safety

**Decision**: Never cache `Absent` from a DB read. Only cache `Absent` after our own write operations (clear/delete). All code paths treat DB-read `Absent` as potentially pre-migration (scan clustering as fallback).

**Rationale**: At deployment, every existing timer has data in clustering rows with `state = null`. A DB read returning `Absent` is ambiguous: either 0 timers or N pre-migration timers. Caching this would cause `clear_and_schedule_key` to skip the BATCH DELETE, leaving stale clustering rows. Natural convergence through the dominant `clear_and_schedule_key` path migrates active keys without a bulk migration.

**Alternatives Considered**:
- **Bulk migration on deployment**: Rejected per Simplicity principle; natural convergence handles active keys automatically, and all code paths are safe without migration.
- **Schema version column per key**: Rejected because it adds write amplification for marginal benefit — `clear_and_schedule_key` already handles the migration path correctly.

## R5: `TriggerStoreProvider` Factory Pattern

**Decision**: New `TriggerStoreProvider` trait with `create_store(topic, partition, consumer_group) -> Store` method. `CassandraTriggerStoreProvider` holds shared resources (session, prepared statements). `InMemoryTriggerStoreProvider` creates fresh stores for tests.

**Rationale**: Follows the existing `TimerDeferStoreProvider` and `MessageDeferStoreProvider` patterns already in the codebase. Separates shared resources (session, queries) from per-partition resources (cache). Enables per-partition cache lifecycle without changing global session management.

**Alternatives Considered**:
- **Pass cache externally to `CassandraTriggerStore::new()`**: Rejected because the caller would need to manage cache lifecycle, violating encapsulation.
- **Keep `StorePair` creating stores directly**: Rejected because `StorePair` currently creates one `CassandraTriggerStore` that is shared across all partitions via `Clone`. With per-partition cache, each partition needs its own store instance from a factory.

## R6: Overflow Demotion Strategy

**Decision**: On `delete_key_trigger` when state is `Overflow`, issue a `LIMIT 2` query on the clustering rows to count remaining timers. If 1 remains, demote to `Inline` in a BATCH (read remaining row, write as inline state, delete clustering row). If 0 remain, set `Absent`.

**Rationale**: The `LIMIT 2` query is lightweight (scans at most 2 rows in a single partition). Strict invariant enforcement ensures the cache is always accurate, which prevents unnecessary tombstones on subsequent `clear_and_schedule_key` calls. The extra query cost only applies in the Overflow path, which is the uncommon case.

**Alternatives Considered**:
- **Lazy demotion** (leave as Overflow until `clear_and_schedule_key`): Rejected because it means `clear_and_schedule_key` would take the BATCH path (creating a range tombstone) when only 1 timer exists. Strict enforcement avoids this.
- **Counter-based tracking**: Rejected because Cassandra lightweight transactions would be needed for accuracy, adding latency and complexity.

## R7: CQL Query Design

**Decision**: 7 new/modified prepared statements. `set_state_inline` with/without TTL. `set_state_overflow`. `remove_state_entry`. `clear_state`. `get_state`. `batch_clear_and_set_inline` with/without TTL. `count_key_triggers` (lightweight `SELECT time ... LIMIT 2`).

**Rationale**: Each query maps to a specific state transition. The `count_key_triggers` query uses `SELECT time` (a clustering column, lightweight) with `LIMIT 2` rather than `COUNT(*)` to avoid full partition scans. Batch queries use `UNLOGGED BATCH` for atomicity within a single partition (consistent with existing `batch_clear_and_set_singleton`).

**Alternatives Considered**:
- **`COUNT(*)` for demotion check**: Rejected because `COUNT(*)` scans all rows in the partition slice, while `LIMIT 2` reads at most 2.
- **Single parameterized query for both inline and overflow writes**: Rejected because the parameter combinations differ (inline has time+span, overflow has nulls) and separate statements are clearer.
