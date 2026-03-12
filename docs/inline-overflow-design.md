# Inline/Overflow State Design

Design for strict singleton invariant enforcement in the Cassandra timer store.

## Invariant

For a given `(segment_id, key, timer_type)`:

| Timer count | State column | Clustering rows |
|---|---|---|
| 0 | Entry absent | None |
| 1 | `{inline: true, time: T, span: S}` | None |
| >1 | `{inline: false, time: null, span: null}` | All timers |

The invariant is strictly upheld by every mutation. No ambiguous states.

## Schema Changes

### UDT: `key_timer_state` (replaces `timer_slot`)

```sql
CREATE TYPE key_timer_state (
    inline boolean,
    time int,
    span frozen<map<text, text>>
);
```

- `inline = true` with `time` and `span` present: inline timer data
- `inline = false/null` with `time = null` and `span = null`: overflow marker
- Single UDT encodes both states; no invalid state is representable per type
- **Safe default:** `inline = null` → `unwrap_or_default()` → `false` → scan clustering. The code only trusts inline data when explicitly marked `true`.

### Column: `state` (replaces `singleton_timers`)

```sql
ALTER TABLE timer_typed_keys ADD state MAP<TINYINT, FROZEN<key_timer_state>> STATIC;
```

Existing `singleton_timers` column is renamed to `state`. Since the branch is unreleased, we modify the existing migration directly.

### Removed: separate overflow column

Earlier we considered a `SET<TINYINT>` overflow column alongside the inline MAP. Rejected because two independent columns permit the invalid state where a type appears in both inline and overflow simultaneously. The single MAP with tagged UDT (`inline` boolean discriminator) eliminates this degree of freedom.

### Why the `inline` boolean is necessary (Cassandra MAP NULL semantics)

The current `singleton_timers MAP<TINYINT, FROZEN<timer_slot>>` column cannot encode a three-state value per type. One might expect that a MAP entry with a NULL frozen UDT value could serve as an overflow marker, but **Cassandra MAP entries cannot hold NULL values**. In CQL, `UPDATE ... SET map[key] = null` is equivalent to `DELETE map[key]` — it removes the entry entirely. The scylla driver never deserializes a "present but NULL" MAP value.

This means the current schema only supports two states per type: absent (no entry) and present (entry with data). The `SlotState::Overflow` branch in the existing code (`Some(None) => Ok(SlotState::Overflow)`) is unreachable dead code — no Cassandra operation produces this state.

The tagged UDT solves this by encoding the discriminant *inside* the frozen value. An overflow marker is `{inline: false, time: null, span: null}` — a non-NULL UDT value that Cassandra stores and returns correctly. This gives us the three states we need within a single MAP column.

## Store Architecture

### Provider pattern: shared queries, per-partition cache

The store follows the same provider/factory pattern used by the defer middleware (`MessageDeferStoreProvider`, `TimerDeferStoreProvider`). A **provider** holds shared Cassandra resources (session, prepared statements) and is cloned cheaply across partitions. Each partition calls `create_store` to get a fresh store instance with its own cache.

```rust
/// Shared across all partitions. Constructed once in `StorePair::new()`.
/// Holds the Cassandra session and prepared statements.
#[derive(Clone)]
pub struct CassandraTriggerStoreProvider {
    store: CassandraStore,       // Arc<Inner> — single session
    queries: Arc<Queries>,       // prepared statements, constructed once
    slab_size: CompactDuration,
}
```

```rust
/// Per-partition. Created fresh by the provider for each partition assignment.
/// Owns its own state cache; destroyed when the partition is revoked.
#[derive(Clone)]
pub struct CassandraTriggerStore {
    store: CassandraStore,       // Arc clone from provider
    queries: Arc<Queries>,       // Arc clone from provider
    slab_size: CompactDuration,  // Copy from provider
    state_cache: Arc<Cache<(Key, TimerType), TimerState>>,  // per-partition
}
```

The provider implements a new `TriggerStoreProvider` trait:

```rust
pub trait TriggerStoreProvider: Clone + Send + Sync + 'static {
    type Store: TriggerStore;

    fn create_store(&self, topic: Topic, partition: Partition, consumer_group: &str)
        -> Self::Store;
}
```

`InMemoryTriggerStore` (used in tests) also needs a provider. A trivial `InMemoryTriggerStoreProvider` that clones a fresh store per partition satisfies the trait.

`PartitionConfiguration` holds the provider (not the store). In `handle_messages`, the per-partition store is created at the point where `segment_id` is already known:

```rust
// handle_messages (per-partition scope)
let segment_id = Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes());
let store = config.trigger_store_provider.create_store(topic, partition, &group_id);

TimerManager::new(segment_id, ..., store, ...).await
```

### Why `Arc<Cache>` inside the store

`CassandraTriggerStore` derives `Clone` because `TriggerStore: Clone` is required — migration code (`migrate_segment_version`) clones the store into concurrent `try_for_each_concurrent` futures for parallel slab processing. These clones are always within the same partition (migrations run inside `TimerManager::new` → `get_segment` → `migrate_segment_if_needed`), so all clones share the same `Arc<Cache>`. This is correct: within a partition, all operations should see the same cache.

Across partitions, each `create_store` call produces a new `Arc<Cache>`, so different partitions never share a cache.

### Cache key: no `SegmentId`

The cache key is `(Key, TimerType)`, not `(SegmentId, Key, TimerType)`. The `SegmentId` is implicit in the store instance — each partition's store only caches entries for its own segment. This saves 16 bytes (one UUID) per cache key.

### Partition lifecycle

When a partition is revoked, the following sequence ensures the cache is destroyed:

1. `kafka_context.rs` removes `PartitionManager` from the managers map.
2. `manager.shutdown()` consumes `self`:
   - Drops the message channel (stops accepting messages).
   - Sends shutdown signal via `watch::Sender`.
   - **Awaits the task handle** — blocks until `handle_messages` completes.
3. When `handle_messages` returns, all local variables are dropped, including the `TimerManager` and its `Arc<TimerManagerInner<CassandraTriggerStore>>`.
4. The `CassandraTriggerStore` inside is dropped. The `Arc<Cache>` reference count reaches zero. The cache is destroyed.

The `shutdown()` method consumes `self` and awaits the task, so there is no window where the task outlives the shutdown. The cache is structurally destroyed — no cleanup hook, no iteration, no epoch counter. Rust's ownership model enforces it.

### What replaces `CachedTriggerStore`

The previous architecture had two cache layers:

- **`singleton_keys: Arc<Cache<..., ()>>`** inside `CassandraTriggerStore` — global, shared across all partitions, write-path only, survived partition revocation.
- **`CachedTriggerStore`** wrapper — global, shared across all partitions, read-path caching of full trigger data, survived partition revocation.

Both are replaced by the single per-partition `state_cache` inside `CassandraTriggerStore`. This cache handles both write-path optimization (discriminant selects UPDATE vs BATCH) and read-path optimization (inline timer data served directly from cache). One cache with clear ownership semantics replaces two caches with unclear lifecycle.

## Rust Type Changes

### `TimerSlot` renamed to `InlineTimer`

The data struct that holds a real timer's inline data. Non-optional fields; only exists when the data is real.

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineTimer {
    pub time: CompactDateTime,
    pub span: HashMap<String, String>,
}
```

### `SlotState` renamed to `TimerState`

The resolved state enum. Consumers never see the raw UDT.

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerState {
    Absent,
    Inline(InlineTimer),
    Overflow,
}
```

### Raw UDT serde type (private)

Used only for Cassandra serialization/deserialization at the module boundary.

```rust
#[derive(DeserializeValue, SerializeValue)]
struct RawTimerState {
    inline: Option<bool>,
    time: Option<CompactDateTime>,
    span: Option<HashMap<String, String>>,
}
```

Conversion to `TimerState`:

```rust
fn into_timer_state(raw: Option<RawTimerState>) -> TimerState {
    match raw {
        None => TimerState::Absent,
        Some(r) if r.inline.unwrap_or_default() => match r.time {
            Some(time) => TimerState::Inline(InlineTimer {
                time,
                span: r.span.unwrap_or_default(),
            }),
            None => TimerState::Overflow, // inline = true but no time — treat as corrupt, fall back to scan
        },
        Some(_) => TimerState::Overflow,
    }
}
```

`inline: null` → `unwrap_or_default()` → `false` → `Overflow` (scan clustering). This is the safe default: the code only trusts inline data when it has been explicitly marked `inline = true`. If a map entry exists but the `inline` field is null (unexpected state), the fallback is to scan clustering, which always works.

## Cache

The `state_cache` inside `CassandraTriggerStore` stores the full resolved `TimerState`, including `InlineTimer` data (time + span) for inline entries. This enables both write-path optimization (choose UPDATE vs BATCH without a DB read) and read-path optimization (serve reads directly from cache without any DB query).

### Core design principle: the cache never contains pre-migration data

Pre-migration keys (timers in clustering, `state = null`) always produce cache **misses**, which fall through to a DB read, which returns `Absent`, which correctly scans clustering. The cache is only populated after our code writes a known state or reads a definitive one (see population rules below). This means the cache cannot lie about pre-migration data — it simply doesn't know about it.

### Population rules

**After writes (always populate):** Every successful write establishes a known state. Cache it.

| Operation | Outcome | Cache after |
|---|---|---|
| `clear_and_schedule_key` | Sets inline entry | `Inline(new_timer)` |
| `insert_key_trigger` (Cache Absent → Inline) | First timer for known-empty key | `Inline(new_timer)` |
| `insert_key_trigger` (Inline → Overflow) | Promotes to clustering | `Overflow` |
| `clear_key_triggers` | Clears everything | `Absent` |
| `clear_key_triggers_all_types` | Clears everything | `Absent` (all types) |
| `delete_key_trigger` (Overflow → 0) | Empties key | `Absent` |
| `delete_key_trigger` (Overflow → 1, demote) | Demotes to inline | `Inline(remaining)` |

**After DB reads (selective):**

| DB state column | Cache? | Reason |
|---|---|---|
| `Inline(timer)` | Yes | `inline = true` was explicitly written by our code. Trustworthy. |
| `Overflow` | Yes | `inline = false` was explicitly written. Trustworthy. |
| `Absent` | **No** | Ambiguous. Could be 0 timers or pre-migration data with N timers in clustering. Caching this would cause `clear_and_schedule_key` to skip the BATCH DELETE, potentially leaving stale clustering rows. |

This asymmetry is the key to pre-migration safety: `Absent` from our own writes (after clearing) is safe to cache, but `Absent` from a DB read is not.

### Cache consistency guarantees

Three properties ensure the cache is always consistent:

1. **Partition ownership:** While we own a partition, we see every read and write for every key in it. No other process mutates our keys. The cache is the authoritative in-memory view.
2. **Per-key serialization:** `KeyManager` guarantees at most one concurrent operation per `(segment_id, key)`. No races between read-modify-write sequences on the same cache entry.
3. **Partition revocation destroys the cache.** The provider pattern ensures each partition gets its own store with its own cache. When the partition's `handle_messages` task completes during revocation, the store and its cache are dropped. The new owner starts with a fresh store and cold cache created by a new `create_store` call.

These three properties mean the cache is always consistent for keys we own, and never stale across ownership changes.

### Eviction

On write failure: do not update the cache (only cache on success). Since writes that change state always overwrite the cache entry on success, explicit pre-eviction is not needed.

### Memory considerations

Each per-partition cache stores up to 8,192 `TimerState` entries. `InlineTimer` data (time + span HashMap) is typically ~200 bytes per entry (the span is usually 2 OTEL key-value pairs).

| Partitions | Capacity/partition | Total entries | Memory estimate |
|---|---|---|---|
| 1 | 8,192 | 8,192 | ~1.6 MB |
| 16 | 8,192 | 131,072 | ~25 MB |
| 64 | 8,192 | 524,288 | ~100 MB |

If memory becomes a concern, we can downgrade to caching just the discriminant (`Inline`/`Overflow`/`Absent` without data), reducing per-entry cost to ~1 byte and total memory to under 1 MB at any partition count. This loses the zero-query read optimization but keeps the write-path optimization.

### Query savings by operation

**`clear_and_schedule_key` (dominant hot path):**

| Cache state | DB reads | Write strategy | Tombstones |
|---|---|---|---|
| Hit `Inline` | 0 | Plain UPDATE | None |
| Hit `Overflow` | 0 | BATCH (DELETE + UPDATE) | Range tombstone (necessary) |
| Hit `Absent` | 0 | Plain UPDATE | None |
| Miss, DB = Inline | 1 | Plain UPDATE | None |
| Miss, DB = Overflow | 1 | BATCH | Range tombstone |
| Miss, DB = Absent | 1 | BATCH (safe for pre-migration) | Range tombstone (possibly unnecessary) |

The last row is the pre-migration penalty: a truly-empty key with `state = null` takes the BATCH path on first access, creating a range tombstone over zero rows. This only happens once — after the write, the key is cached as `Inline` and all subsequent accesses are tombstone-free.

**Steady-state for the dominant path:** Most users call `clear_and_schedule_key` repeatedly (the timer reschedule cycle). After the first call warms the cache, every subsequent call is: cache hit `Inline` → plain UPDATE → 1 query, 0 reads, 0 tombstones. This is the optimal path and the common case.

**`insert_key_trigger`:**

| Cache state | DB reads | Action |
|---|---|---|
| Hit `Inline(slot)` | 0 | BATCH: promote + write + set overflow |
| Hit `Overflow` | 0 | Write clustering only (1 query) |
| Miss | 1 | Read DB, then branch |

**`delete_key_trigger` (Overflow case):**

| Cache state | DB reads | Action |
|---|---|---|
| Hit `Inline(slot)`, time matches | 0 | Remove state entry |
| Hit `Overflow` | 1 (`LIMIT 2` count) | Delete + check remaining, demote if needed |
| Miss | 1-2 | Read state + potentially count |

**Read paths (`get_key_times`, `get_key_triggers`):**

| Cache state | DB queries | Action |
|---|---|---|
| Hit `Inline(timer)` | 0 | Serve time/span directly from cache |
| Hit `Overflow` | 1 | Skip state read, scan clustering |
| Hit `Absent` | 0 | Yield nothing |
| Miss | 1-2 | Read state, then branch |

Serving reads from cache (Inline hit = 0 queries) is significant during slab loading, where many keys are read in sequence. After `clear_and_schedule_key` warms the cache, subsequent reads for those keys are free.

## Mutation Changes

### `clear_and_schedule_key`

**Before:** Reads singleton slot, branches on Singleton vs Absent/Overflow.
**After:** Cache hit skips DB read. Writes `{inline: true, time: T, span: S}`. Caches `Inline(new_timer)`.

| State source | Write path | Tombstone |
|---|---|---|
| Cache/DB `Inline` | Plain UPDATE | None |
| Cache/DB `Overflow` | BATCH (DELETE clustering + set inline) | Range (necessary) |
| Cache `Absent` (from our clear) | Plain UPDATE | None |
| DB `Absent` (pre-migration or unknown) | BATCH (safe default) | Range (possibly unnecessary) |

### `insert_key_trigger`

**Before:** Reads singleton slot. If Singleton, promotes to clustering and removes entry (sets Absent). If Absent/Overflow, just writes clustering.

**After:** Cache hit skips DB read.

| State source | Action | Cache after |
|---|---|---|
| Cache/DB `Inline(slot)` | BATCH: promote old + write new + set `inline = false` | `Overflow` |
| Cache/DB `Overflow` | Write clustering only (1 query) | `Overflow` (unchanged) |
| Cache `Absent` (from our clear — 0 timers) | BATCH: write clustering + set `inline = true` | `Inline(new_timer)` |
| DB `Absent` (pre-migration — unknown timers) | Write clustering only | Not cached (Absent from read) |

Two sub-cases of `Absent`:

- **Cache `Absent`** means our own `clear_key_triggers` or `delete_key_trigger` set it — we know for certain there are 0 existing timers. This is the first timer for this key, so we write it inline and cache the result.
- **DB `Absent`** means a cache miss fell through to a DB read that returned no state entry. This is ambiguous: either 0 timers or pre-migration data with N timers in clustering. We write the clustering row only and don't set state. `clear_and_schedule_key` will fix the state on the next cycle. `Absent` is never in the cache from a DB read, so this path always involves a DB read first.

### `delete_key_trigger` (strict invariant)

**Before:** Reads singleton slot concurrently with deleting clustering row. If Singleton matches time, removes entry.

**After:** Cache hit skips state read.

1. Delete the clustering row (may be a no-op if the timer is inline).
2. If state is `Inline` and time matches: remove state entry. Cache `Absent`. Done (1 → 0).
3. If state is `Inline` and time does **not** match: no-op. The clustering DELETE hit nothing (the timer is inline with a different time), and the inline timer remains. Cache unchanged.
4. If state is `Overflow`: read remaining clustering rows with `LIMIT 2`.
   - 0 remaining: remove state entry. Cache `Absent`.
   - 1 remaining: **demote** — read remaining row, write as `{inline: true, time: T, span: S}`, delete clustering row. (BATCH). Cache `Inline(remaining)`.
   - >1 remaining: no state change. Cache stays `Overflow`.
5. If state is `Absent` (cache miss, DB read): delete clustering row only. Don't cache.

The `LIMIT 2` read after delete is the cost of strict invariant enforcement. Only applies to the Overflow case (1 extra query).

### `clear_key_triggers`

**Before:** Concurrently removes singleton entry and clears clustering.
**After:** Same. Remove state entry and clear clustering. Cache `Absent`.

### `clear_key_triggers_all_types`

**Before:** Clears clustering and singleton slots for all types.
**After:** Same. Clears clustering and state column entirely. Cache `Absent` for all types.

## Read Path Changes

### `get_key_times` / `get_key_triggers`

**Before:** Reads `SlotState` from DB. If `Singleton`, yield from slot. If `Absent`/`Overflow`, scan clustering.

**After:** Cache hit skips DB read entirely.

| Cache state | DB queries | Action |
|---|---|---|
| Hit `Inline(timer)` | 0 | Serve time/span from cache |
| Hit `Overflow` | 1 | Skip state read, scan clustering only |
| Hit `Absent` | 0 | Yield nothing |
| Miss | 1-2 | Read state from DB, then branch |

Since we own the partition and see every write, the cache is warm for all active keys after their first `clear_and_schedule_key`. The dominant steady-state path is: cache hit `Inline` → 0 DB queries for reads.

### `get_key_triggers_all_types`

**Before:** Reads singleton map, merges with clustering stream.

**After:** Same logic. Reads state map, distinguishes `Inline` entries from `Overflow` entries per type. `Overflow` types are skipped in the singleton source and served from clustering only. Cache is not used for this path (it reads the full map across all types in one query).

## Query Changes

New/modified prepared statements:

| Query | Purpose |
|---|---|
| `get_state` | `SELECT state FROM ... WHERE segment_id = ? AND key = ? LIMIT 1` |
| `set_state_inline` | `UPDATE ... SET state[?] = ? WHERE ...` (writes `{inline: true, time, span}`, with/without TTL) |
| `set_state_overflow` | `UPDATE ... SET state[?] = ? WHERE ...` (writes `{inline: false, time: null, span: null}`) |
| `remove_state_entry` | `DELETE state[?] FROM ... WHERE ...` |
| `clear_state` | `DELETE state FROM ... WHERE ...` |
| `batch_clear_and_set_inline` | BATCH: DELETE clustering + set inline state |
| `count_key_triggers` | `SELECT time FROM ... WHERE segment_id = ? AND key = ? AND timer_type = ? LIMIT 2` (demotion check — selects a lightweight column and counts rows client-side; not `COUNT(*)`) |

## Initial State: Invariant Violations in Production

When this change is first deployed, **every existing timer in production violates the invariant.** All existing timers live in clustering columns with `state = null`. The invariant says 1-timer keys should have inline state entries and >1-timer keys should have overflow markers, but none of them will.

Concretely, after the schema migration adds the `state` column:

| Production state | Invariant expects | Actual `state` column |
|---|---|---|
| 1 timer in clustering | `Inline(time, span)` | `null` (Absent) |
| 3 timers in clustering | `Overflow` marker | `null` (Absent) |
| 0 timers | Absent | `null` (Absent) -- correct by coincidence |

**The design must be robust to this.** Every code path that reads `TimerState::Absent` must handle the possibility that timers exist in clustering columns. This is not a transient edge case -- it is the universal state of every key at deployment time.

### How each code path handles Absent-with-clustering-data

**Reads** (`get_key_times`, `get_key_triggers`, `get_key_triggers_all_types`): `Absent` falls through to clustering column scan, same as `Overflow`. Timers are found. No data loss.

**`clear_and_schedule_key`**: `Absent` takes the BATCH path (DELETE clustering + set inline). This atomically clears old clustering data and writes the inline entry, migrating the key to the new format. Correct.

**`insert_key_trigger`**: `Absent` means no singleton to promote. The new timer is written to clustering. If there were already N timers in clustering, there are now N+1. No data loss. The state column remains null -- this key stays in the pre-migration state until a `clear_and_schedule_key` fixes it.

**`delete_key_trigger`**: `Absent` with timers in clustering. The clustering row is deleted. The state column remains null. If 1 timer remains, it stays in clustering (invariant still violated). No data loss -- just not yet migrated.

**`clear_key_triggers` / `clear_key_triggers_all_types`**: Deletes clustering rows and clears state. Correct regardless of state column contents.

### Natural convergence

The dominant write path in production is `clear_and_schedule_key` (the timer system's normal reschedule cycle). Every call to this method migrates one key to the new format. Over time, all active keys converge to invariant-compliant state without a bulk migration.

Keys that are never rescheduled (abandoned/expired timers) remain in clustering with `state = null`. This is harmless -- reads still find them, and TTL will eventually expire them.

### No version migration required

A bulk migration (scan all keys, read clustering, write state entries) would be correct but unnecessary:

1. Natural convergence handles active keys automatically.
2. `Absent` is handled correctly by every code path (reads scan clustering, writes take the safe path).
3. The extra complexity and operational risk of a bulk migration is not justified when the system is already correct without it.

If performance profiling later reveals that cold-cache reads on un-migrated keys are a bottleneck (extra DB read to scan clustering instead of cache hit), a version migration can be added at that point.

### Test coverage for pre-migration data

Pre-migration data is tested explicitly using `add_key_trigger_clustering` to write clustering rows without touching the state column, simulating production data that existed before deployment:

- `test_pre_migration_reads_and_migration`: 1 and 2 timers in clustering with `state = null`. Verifies reads work and `clear_and_schedule_key` migrates to inline.
- `test_pre_migration_mutations`: `insert_key_trigger`, `delete_key_trigger`, `clear_key_triggers` on pre-migration data. Verifies no data loss.
- `test_state_transitions_one_and_two_timers` phase 1b: `insert_key_trigger` on a fresh key creates 1 clustering row with `state = null`, verifies reads find it.

## Tests

| Test | Covers |
|---|---|
| `test_state_transitions_one_and_two_timers` | All state transitions: Absent/Inline/Overflow via insert, delete, clear, clear_and_schedule. Verifies state + all 3 read paths at each step. |
| `test_pre_migration_reads_and_migration` | 1 and 2 pre-migration clustering timers: reads work, `clear_and_schedule_key` migrates to inline. |
| `test_pre_migration_mutations` | `insert_key_trigger`, `delete_key_trigger`, `clear_key_triggers` on pre-migration data. |
| `test_clear_all_types_clears_inline_and_overflow` | Multi-type: inline + overflow cleared simultaneously. |
| `test_singleton_slot_round_trip` | Original lifecycle test (will be updated to new naming). |
| `test_prop_singleton_invariant` | Property test: random operation sequences, verify invariant holds for all (segment, key, type). |
