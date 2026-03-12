# Futurelock Audit: Synthesis

Audit context: [jacko.io/snooze.html](https://jacko.io/snooze.html) defines a "snoozed" future as one that is ready to make progress but is not being polled. A **futurelock** occurs when a snoozed future holds a lock guard, blocking other tasks that need the same lock.

---

## 1. Executive Summary

The audit identified **five confirmed issues** across two distinct subsystems. There are no false alarms: every verdict is "confirmed". The distribution by severity is:

| Severity | Count | Issues |
|---|---|---|
| High | 2 | 1 (`load_slabs`), 2 (`scheduled_times` / `unschedule_all`) |
| Medium | 1 | 3 (`ConsumerStateView` / `subscribe()`) |
| Low | 2 | 4 (`subscribe()` mutex hold), 5 (`unschedule_all` read starvation) |

Issues 1, 2, and 5 are three facets of a single architectural problem: `SlabLock`'s `tokio::sync::RwLock` is held across unbounded I/O in the timer system's write path. Issues 3 and 4 are a separate but related pattern: the high-level consumer API returns live lock guards to callers and holds a mutex across async initialization.

None of the confirmed issues are caught by the existing heartbeat system in the way the system was designed to work. The heartbeat detects slab loader staleness only after the fact, and only for the most severe stalls. It is blind to the reader-starvation effects that the write-preferring `RwLock` creates for other tasks.

---

## 2. Cross-Cutting Analysis

### 2.1 The `SlabLock` RwLock — issues 1, 2, and 5

All three timer-system findings converge on a single lock: `SlabLock<State<T>>` in `src/timers/slab_lock.rs`.

```
src/timers/slab_lock.rs:34–37
pub struct SlabLock<T> {
    inner: Arc<RwLock<T>>,   // tokio::sync::RwLock — write-preferring
}

src/timers/slab_lock.rs:97–98   trigger_lock() → RwLock::read()   (shared)
src/timers/slab_lock.rs:105–106 slab_lock()    → RwLock::write()  (exclusive)
```

The unified picture:

- The **write path** (`load_slabs`, `remove_completed_slabs` in `src/timers/loader.rs`) acquires the exclusive write lock and holds it across multi-step Cassandra I/O and mpsc/oneshot channel operations — potentially hundreds of milliseconds per call with `LOAD_CONCURRENCY = 16` concurrent futures in flight.

- The **read path** (`scheduled_times` in `src/timers/manager.rs`) acquires the shared read lock inside a `try_stream!` generator and holds it for the entire lifetime of the stream, across Cassandra reads, `scc::HashMap` awaits, and `yield` points where the generator is parked by its consumer (`try_buffer_unordered`).

- **Tokio's write-preferring `RwLock`** is the amplifier. The docs state: once a write request is queued, new `read()` calls block even though existing readers are still valid. This means:
  - When the write path is active, all trigger operations (`schedule`, `unschedule`, `fire`, `complete`, `abort`) block immediately, not just when the write lock is granted.
  - When the read path holds the lock and the write path queues, subsequent trigger-operation readers also block — the `unschedule_all` stall is self-amplifying.

The three issues are not independent bugs. They are three manifestations of the same design: a single `RwLock` serializes structurally-different operations that have very different required lock durations.

### 2.2 The guard-returning API pattern — issues 3 and 4

Issues 3 and 4 share a structural pattern: a `tokio::sync` lock guard is either returned directly to the caller or held inside a public method across async I/O.

`src/high_level/state.rs:19`:
```rust
pub struct ConsumerStateView<'a, T>(pub(crate) MutexGuard<'a, ConsumerState<T>>);
```

`src/high_level/mod.rs:82–84`:
```rust
pub async fn consumer_state(&self) -> ConsumerStateView<'_, T> {
    ConsumerStateView(self.consumer.lock().await)
}
```

`src/high_level/mod.rs:207` (subscribe):
```rust
let mut guard = self.consumer.lock().await;
// ... multi-second Cassandra init ...
// guard dropped at end of function
```

The API design problem: `tokio::sync::MutexGuard<T: Send>` is `Send`, which means holding it across an `.await` compiles without warning or error. The Clippy `await_holding_lock` lint targets `std::sync` guards only. There is no static enforcement preventing callers from writing:

```rust
let view = client.consumer_state().await;
some_async_fn().await;   // guard held — mutex locked for full duration — no compiler warning
drop(view);
```

`unsubscribe()` (`src/high_level/mod.rs:292–298`) is the positive example of the correct pattern: it extracts the consumer inside a scoped block, drops the guard, then calls `consumer.shutdown().await` outside the lock. Issues 3 and 4 should converge on the same pattern.

---

## 3. Issue Table

| # | Location | Verdict | Severity | Root Cause | Heartbeat Detects? | Test Written? |
|---|---|---|---|---|---|---|
| 1 | `src/timers/loader.rs:295` (`load_slabs`) | Confirmed | High | Wide critical section — write lock held across Cassandra I/O + channel sends | No — loader is progressing; reader starvation is invisible to heartbeat | Yes |
| 2 | `src/timers/manager.rs:134` (`scheduled_times`) | Confirmed | High | Wide critical section — read lock held across Cassandra reads + scc awaits + generator yields | Partial — only after stall exceeds heartbeat timeout (typically 30 s); root cause not surfaced | Yes |
| 3 | `src/high_level/mod.rs:82` (`consumer_state()`) | Confirmed | Medium | Guard escape — live `MutexGuard` returned to caller; API cannot prevent await-across-guard misuse | No | Yes |
| 4 | `src/high_level/mod.rs:207` (`subscribe()`) | Confirmed | Low | Wide critical section — `tokio::sync::Mutex` held across multi-second Cassandra + Kafka init | No | Yes |
| 5 | `src/timers/manager.rs:410` (`unschedule_all`) | Confirmed | Low | Composite — snoozed `scheduled_times` generator holds read lock; write-preferring RwLock starves slab loader | Indirect only; short stalls not detected | Yes |

Note: Issues 2 and 5 describe different aspects of the same call chain. Issue 2 focuses on the read guard held inside `scheduled_times`; issue 5 focuses on what that means for `unschedule_all`'s interaction with the slab loader write path.

---

## 4. Root Cause Taxonomy

### 4.1 Wide critical section — lock held across I/O

The lock scope spans I/O operations with unbounded latency. The guard is held not because the protected data is needed during the I/O, but because the code reads from state at the start and writes back at the end, and the current structure holds the lock for the entire window between those two points.

Affected:
- `load_slabs` (`src/timers/loader.rs:286–314`): write lock held across `try_buffer_unordered(16)` of Cassandra reads + scheduler channel sends.
- `remove_completed_slabs` (`src/timers/loader.rs:331–358`): write lock held across `buffer_unordered(16)` of Cassandra deletes.
- `scheduled_times` (`src/timers/manager.rs:133–153`): read lock held across Cassandra reads, `scc::HashMap` awaits, and `yield` points inside a `try_stream!` generator.
- `subscribe()` (`src/high_level/mod.rs:207–282`): `tokio::sync::Mutex` held across Cassandra TCP connect, DDL migration, and prepared statement compilation.

**Fix pattern**: snapshot the needed state, release the lock, perform I/O, re-acquire the lock only for the final write-back. `TriggerStore` and `TriggerScheduler` are both `Clone + Send + Sync + 'static` and support this pattern directly.

### 4.2 Guard escape — live guard returned to caller

The lock is not held for too long by any specific internal caller today, but the API makes it structurally impossible to prevent callers from holding the guard across awaits.

Affected:
- `consumer_state()` (`src/high_level/mod.rs:82`): returns `ConsumerStateView<'_, T>` which wraps a live `tokio::sync::MutexGuard`.

**Fix pattern**: do not return the guard. Either return a snapshot of the data (clone the relevant fields), or expose targeted accessor methods that acquire and release the lock internally.

### 4.3 Write-preferring amplification

This is not a root cause on its own, but it turns category 4.1 bugs from "the locked operation is slow" into "all concurrent readers are also blocked". Every instance of a wide write critical section in the timer system blocks not only new write attempts but all `trigger_lock()` readers the moment the write is queued.

The write-preferring behavior is documented and intentional in Tokio; the right response is to eliminate wide write critical sections rather than switching lock implementations.

---

## 5. Prioritized Fix List

### Fix 1 — Restructure `load_slabs` and `remove_completed_slabs` to hold write lock only for `extend_ownership` (Issue 1)

**What to fix**: `src/timers/loader.rs`, functions `load_slabs` (lines 286–314) and `remove_completed_slabs` (lines 331–358).

**Recommended approach**:
1. Acquire write lock, clone `store` handle and `scheduler` handle, record current `max_owned`, release write lock immediately.
2. Perform all Cassandra I/O and channel sends lock-free using the cloned handles.
3. Re-acquire write lock only to call `extend_ownership` (O(1) in-memory operation).

Both `TriggerStore` (bounded by `Clone + Send + Sync + 'static`) and `TriggerScheduler` (`Clone`) already support this. The write lock would be held for microseconds instead of hundreds of milliseconds.

**Blast radius**: Isolated to `src/timers/loader.rs`. No public API surface changes. The `State<T>` struct fields become accessible via cloned handles rather than through the guard, which is already the access pattern inside `load_triggers`. Low risk.

### Fix 2 — Collect scheduled times before releasing the read lock in `scheduled_times` (Issue 2 / 5)

**What to fix**: `src/timers/manager.rs`, `scheduled_times` (lines 167–210) and `unschedule_all` (lines 410–425).

**Recommended approach**: Change `scheduled_times` to collect all times into a `Vec<CompactDateTime>` while holding the read lock, drop the lock, then yield from the collected `Vec`. The read lock is then held only for the duration of the `get_key_times` stream plus two `scc::HashMap` lookups per item — not for the entire stream consumption lifetime. `unschedule_all` can then drive `unschedule` calls without any lock held on the source stream.

**Trade-off**: The snapshot may be stale by the time `unschedule` calls execute. Given at-least-once delivery semantics and idempotent `unschedule` behavior, a stale snapshot is safe. A timer that was added after the snapshot will not be unscheduled in this call — which is the same race condition that exists today.

**Blast radius**: `scheduled_times` return type changes from a lazy `Stream` to something backed by an eagerly-collected `Vec`. All callers (`unschedule_all`, `scheduled_triggers`, any external uses via `EventContext`) need review. Medium scope but contained to the timer manager.

### Fix 3 — Restructure `subscribe()` to hold the mutex only for state transitions (Issues 3 and 4)

**What to fix**: `src/high_level/mod.rs`, `subscribe()` (lines 203–282).

**Recommended approach** (documented in issue 04):
1. Acquire mutex, extract `ModeConfiguration` from `ConsumerState::Configured`, release mutex immediately.
2. Perform all async init (Kafka, Cassandra) without holding the lock.
3. Acquire mutex briefly to write `ConsumerState::Running`.
4. On init failure, re-acquire mutex to restore `ConsumerState::Configured`.

This requires handling the window where state is `Unconfigured` between step 1 and step 3, but callers that check state during init already must handle `Unconfigured`.

**Blast radius**: Contained to `src/high_level/mod.rs`. The existing `AlreadySubscribed` guard logic needs adjustment since a second concurrent `subscribe()` call would race on the `Configured → Running` transition. A `ConsumerState::Initializing` variant would close this race cleanly and also serve as a visible signal to readers.

### Fix 4 — Replace `consumer_state()` guard-returning API (Issue 3)

**What to fix**: `src/high_level/mod.rs:82`, `src/high_level/state.rs:19`.

**Recommended approach**: Remove or seal `consumer_state()`. Replace with dedicated, lock-scoped accessor methods that acquire the mutex, read the needed value, and return a plain (non-guard-wrapping) type. Example: `assigned_partition_count()` and `is_stalled()` already do this correctly — they call `consumer_state()` but drop the guard before any `await`. Extend this pattern to any remaining callers and eliminate the `ConsumerStateView` public API.

**Blast radius**: `ConsumerStateView` is a public type. This is a breaking API change. If callers outside the crate use `consumer_state()`, a deprecation cycle is needed. Internal callers are limited to `mod.rs:317` and `mod.rs:329`, both already correct.

---

## 6. Gaps — What the Heartbeat Does Not Catch

The heartbeat system calls `heartbeat.beat()` at the **top** of each `slab_loader_iteration` (`src/timers/loader.rs:172`), before entering `load_slabs` or `remove_completed_slabs`. This creates two distinct blind spots:

**Blind spot 1 — Active writer, blocked readers.** When `load_slabs` holds the write lock and is actively progressing through Cassandra I/O, the slab loader heartbeat continues to beat normally after each iteration. `HeartbeatRegistry::any_stalled()` returns `false`. Meanwhile, every task calling `trigger_lock()` is fully blocked. The heartbeat correctly reports the slab loader is healthy because it is healthy — the starvation is experienced entirely by the other tasks.

**Blind spot 2 — Blocked writer, active readers.** When the slab loader is blocked at `state.slab_lock().await` because `scheduled_times` holds the read lock, the heartbeat does not fire. After the configured stall threshold (typically 30 seconds), `any_stalled()` returns `true`. This is detection, but it is passive: it requires an external health probe to observe the flag, does not identify the cause, and fires only for stalls that exceed the threshold — short stalls (seconds) during normal `unschedule_all` calls are invisible.

**What the heartbeat cannot catch by design**: the heartbeat is per-task and measures task liveness. Futurelocks are a cross-task phenomenon — one task's guard blocks another task's progress. A heartbeat on the blocked task would detect the stall, but there is no heartbeat on the `TimerManager` caller tasks (schedule, unschedule, fire, complete). Adding per-operation timeouts at the `trigger_lock()` and `slab_lock()` call sites would provide direct detection, but the current architecture has no such instrumentation.

---

## 7. Architectural Recommendation

The single most impactful structural change is to **decouple the `SlabLock` write path from I/O**.

All five issues reduce to a common failure mode: a `tokio::sync` lock guard is live while the holding task is suspended at an I/O await. The `SlabLock` cases (issues 1, 2, 5) are the highest-severity instances because the write-preferring `RwLock` converts a slow write critical section into a system-wide read stall.

The fix is not to change the lock type or add more locks. It is to enforce a discipline that is already naturally expressed by the `SlabLock` abstraction's intended semantics: `slab_lock()` protects structural mutations (ownership ranges, loaded slab metadata); `trigger_lock()` protects trigger scheduling state. Neither write path actually requires holding the lock during I/O — they need the lock only to read the starting state and write the final result. The I/O in between is entirely lock-independent.

Concretely: `load_slabs` needs the write lock for one reason — to call `state.extend_ownership(range_end)` at the end. Everything before that (`get_slab_range`, `load_triggers`, `scheduler.schedule`) uses cloneable handles that can be captured before the lock is acquired. Restructuring to "snapshot → drop lock → I/O → re-acquire lock → commit" would reduce write lock hold time from O(Cassandra round-trips × LOAD_CONCURRENCY) to O(1 in-memory operation), eliminating the starvation window entirely for issues 1, 2, and 5.

The `subscribe()` fix (issues 3 and 4) follows the same principle and is already demonstrated by `unsubscribe()` in the same file.
