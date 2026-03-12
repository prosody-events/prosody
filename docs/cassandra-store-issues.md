# Cassandra Trigger Store: Known Issues

Identified via audit of `src/timers/store/cassandra/mod.rs` and `queries.rs`.
All issues were verified by agent investigation. Migration context: by the time
any `TriggerOperations` method executes, the segment is guaranteed to be V3
(migration runs synchronously inside `get_segment`, which is always called
before `TimerManager::new()` returns).

**All issues have been fixed. This document is preserved for reference.**

---

## Issue 1 — Double round-trip in Overflow→Inline demotion ✅ Fixed

**Location:** `delete_key_trigger`, `TimerState::Overflow` arm

**Was:** Two sequential Cassandra reads when deleting the last-but-one timer:

1. `SELECT time ... LIMIT 2` (count only)
2. `SELECT key, time, timer_type, span ...` (unbounded scan to get the span)

The time from the second query was discarded; only the span was used.

**Fix:** Split into two queries with minimal data each:

- `count_key_triggers`: `SELECT time ... LIMIT 2` — cheap, no span allocation,
  decides 0/1/2+ branch.
- `peek_first_key_trigger`: `SELECT time, span ... LIMIT 1` — only executed on
  the `1`-remaining path, returns exactly the data needed.

The `2+` branch no longer allocates any span data. The `0` branch allocates
nothing beyond 2 `CompactDateTime` values.

---

## Issue 2 — No-op DELETE in `Absent` arm of `delete_key_trigger` ✅ Fixed

**Location:** `delete_key_trigger`, `TimerState::Absent` arm

**Was:** A `DELETE` was issued unconditionally even when state was `Absent`.
The inline comment already stated "post-V3 Absent is unambiguous: 0 timers, no
clustering rows," but the code did not act on its own assertion.

**Fix:** The `Absent` arm is now a no-op. V3 migration is guaranteed before any
key operations run; `Absent` always means zero clustering rows.

---

## Issue 3 — `execute_iter` for a `LIMIT 2` query ✅ Fixed (absorbed by Issue 1)

**Location:** `peek_trigger_times`

**Was:** `execute_iter` allocates a tokio task, a oneshot channel, and a full
`QueryPager` state machine — all unnecessary overhead for a bounded 2-row read.
Every other bounded SELECT in the file used `execute_unpaged`.

**Fix:** `peek_trigger_times` now uses `execute_unpaged` +
`into_rows_result()` + `rows::<(CompactDateTime,)>()`, matching the pattern used
by all other bounded SELECTs.

---

## Issue 4 — `state_cached` span field always records `true` ✅ Fixed

**Location:** `get_key_times`, `get_key_triggers`, `insert_key_trigger`,
`delete_key_trigger`, `clear_and_schedule_key`

**Was:** `resolve_state` returned `Ok(TimerState)` from both the cache-hit and
DB-miss branches. All call sites unconditionally recorded `state_cached = true`.
The field was always `true` on success and absent on error — useless for
measuring cache effectiveness.

**Fix:** `resolve_state` now returns `(TimerState, bool)` where the `bool` is
`true` on a cache hit and `false` on a DB read. All call sites destructure the
result and record the actual value.

---

## Issue 5 — `state_cached = true` after a guaranteed DB read in `get_key_triggers_all_types` ✅ Fixed

**Location:** `get_key_triggers_all_types`

**Was:** `fetch_state_map` is a direct `execute_unpaged` against the database
with no cache involvement. The code recorded `state_cached = true` after it —
the inverse of the truth. Additionally, the bulk state map was discarded without
populating `self.state_cache`, so any subsequent per-type operations on the same
key would pay a separate DB round-trip each.

**Fix:**

1. After building `state_map` from the bulk read, each `(key, TimerType)` entry
   is inserted into `self.state_cache`, warming the cache for all timer types in
   one DB read.
2. `state_cached` is now recorded as `false` (this path is always a DB read).

---

## Issue 6 — Redundant `state_cache.insert` in `insert_key_trigger` Overflow arm ✅ Fixed

**Location:** `insert_key_trigger`, `TimerState::Overflow` arm

**Was:** After writing the clustering row, the code re-inserted
`TimerState::Overflow` into the cache with the comment "re-insert to refresh."
`quick_cache` v0.6 (S3-FIFO, no TTL) is not refreshed by re-insertion.
Frequency counters are incremented by `.get()`, not `.insert()`. The preceding
`resolve_state` call already performed the `.get()`. The re-insert was
cargo-culted from the `Inline` arm above, where an insert is genuinely required
(state transitions from `Inline` to `Overflow`).

**Fix:** Removed the redundant `state_cache.insert` from the `Overflow` arm.

---

## Non-issue: UNLOGGED BATCH partition safety

The `batch_clear_and_set_inline` and `batch_clear_and_set_inline_no_ttl`
queries were audited. Both statements target the same `(segment_id, key)`
partition of `timer_typed_keys` (`timer_type` in the DELETE is a clustering
column predicate, not a partition key change). UNLOGGED BATCH is the correct
choice. Parameter binding is correct in both TTL and no-TTL paths.

---

## Issue 7 — Inline→Overflow transition is not atomic ✅ Fixed

**Location:** `insert_key_trigger`, `TimerState::Inline` arm (lines ~1360–1374)

**Problem:** The Inline→Overflow transition fires three independent Cassandra writes
concurrently via `try_join!`:

1. `add_key_trigger_clustering` — write promoted (old) timer to clustering rows
2. `add_key_trigger_clustering` — write new timer to clustering rows
3. `set_state_overflow` — update state column to `Overflow`

These are separate CQL statements with no atomicity guarantees. If either clustering
write is sent to the Cassandra coordinator but `set_state_overflow` then fails (e.g.,
`WriteTimeoutException`), the state column remains `Inline(old_timer)` while one or
two orphaned clustering rows now exist for that key/type.

**Consequences of diverged state:**

- `get_key_triggers` and `get_key_times` skip clustering rows entirely when state
  is `Inline`, so the orphaned rows are permanently invisible through all normal
  read paths.
- `clear_and_schedule_key` takes the `Inline` fast path (`set_state_inline` only,
  no DELETE) when state reads as `Inline`, so it does not heal the state.
- If the inline timer is subsequently deleted via `delete_key_trigger` (case:
  `Inline(timer), timer.time == time`), state transitions to `Absent` and the
  orphaned clustering rows are stranded with no cleanup path until TTL expiry.

**Self-healing:** A caller retry of `insert_key_trigger` re-reads state as
`Inline(old_timer)` and re-attempts the promotion. The duplicate clustering INSERTs
are idempotent (Cassandra upsert semantics), so a successful retry converges to a
correct `Overflow` state. Recovery is caller-driven, not automatic.

**Fix:** Use an `UNLOGGED BATCH` for all three writes, following the same pattern
as `batch_clear_and_set_inline`. All three statements target the same
`(segment_id, key)` partition of `timer_typed_keys`, so an unlogged batch is
correct and carries no cross-partition coordination overhead. The batch is applied
atomically at the partition level: either all three writes land or none do, closing
the partial-write window entirely. New queries needed:

- `batch_promote_and_set_overflow` (with TTL on both clustering INSERTs)
- `batch_promote_and_set_overflow_no_ttl`

---

## Issue 8 — `StateCacheKey` does not include `SegmentId`

**Location:** `type StateCacheKey = (Key, TimerType)` (line ~48)

**Problem:** The per-partition state cache is keyed by `(Key, TimerType)` with no
`SegmentId` component. If the same `CassandraTriggerStore` instance were ever called
with two different `SegmentId`s for the same `(Key, TimerType)` pair, state cached
from segment A would be returned for segment B reads, causing the wrong write path
to execute.

**Current safety:** This is safe today because `create_store` always allocates a
fresh `Arc<Cache<...>>` (each call to `with_shared` constructs `Arc::new(Cache::new(...))`),
and in practice every `CassandraTriggerStore` is owned by exactly one `TimerManager`,
which is pinned to one `SegmentId` for its lifetime. No production code path calls
the same store with two different segment IDs.

**Latent risk:** `CassandraTriggerStore` derives `Clone` and the `TriggerOperations`
trait accepts arbitrary `segment_id`s at every call site. Nothing at the type level
prevents a future caller from sharing a store across segments. If that happened, the
cache poisoning would be completely silent — no error, just wrong state-transition
paths (e.g., skipping a `batch_clear_and_set_inline` that is required).

**Fix:** Change `StateCacheKey` to `(SegmentId, Key, TimerType)`. The added cost is
one `Uuid` (16 bytes) per cache entry — negligible at `STATE_CACHE_CAPACITY = 8_192`.

---

## Out of scope: `set_is_idempotent(true)` applied globally

The `cassandra_queries!` macro (macros.rs) applies `set_is_idempotent(true)` to
every prepared statement, including the batch queries that contain range
tombstones and TTL-recomputed writes. Range tombstone DELETEs and
TTL-parameterised UPDATEs are not strictly idempotent under retries (a retried
TTL write recalculates the TTL from the current time, potentially producing a
different expiry). This is a pre-existing macro-level concern affecting all
queries uniformly and is tracked separately from the issues above.
