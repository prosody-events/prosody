# Snooze Analysis: `load_slabs` Futurelock

**Verdict: CONFIRMED**

`load_slabs` (and `remove_completed_slabs`) hold a `tokio::sync::RwLock` **write
guard** across multiple `.await` yield points that perform I/O and channel sends.
Tokio's `RwLock` is write-preferring: once a write lock request is queued, all
subsequent `read()` calls block until the write lock is both acquired and
released.  This creates a futurelock (snooze hazard) where any concurrent call
to `trigger_lock()` — used by `schedule`, `unschedule`, `scheduled_times`,
`complete`, and `fire` on `TimerManager` — is blocked for the full duration of
the I/O inside `load_slabs`.

---

## Background: The "Snooze" Pattern

A future is "snoozed" (per https://jacko.io/snooze.html) when it is logically
ready to make progress but is not being polled.  A futurelock occurs when a
snoozed future holds a lock guard, preventing other futures from acquiring that
lock.

Tokio's write-preferring `RwLock` adds a second dimension: a queued write lock
request blocks **new readers** even before the write lock is acquired.  This
turns a "hold across await" pattern into a liveness hazard for any concurrent
reader.

---

## The Codepath

### Lock abstraction: `src/timers/slab_lock.rs`

```rust
// slab_lock.rs, lines 34–37
pub struct SlabLock<T> {
    inner: Arc<RwLock<T>>,  // tokio::sync::RwLock
}

// line 97–99
pub async fn trigger_lock(&self) -> SlabLockTriggerGuard<'_, T> {
    SlabLockTriggerGuard(self.inner.read().await)  // RwLock::read()
}

// line 105–107
pub async fn slab_lock(&self) -> SlabLockSlabGuard<'_, T> {
    SlabLockSlabGuard(self.inner.write().await)    // RwLock::write()
}
```

`trigger_lock` = shared read; `slab_lock` = exclusive write.  Both operate on
the same underlying `Arc<RwLock<T>>`.

### Task A — The writer: `src/timers/loader.rs`, `load_slabs` (lines 286–314)

```rust
async fn load_slabs<T>(
    state: &SlabLock<State<T>>,
    segment: &Segment,
    slab_range: RangeInclusive<SlabId>,
) -> Result<Vec<SlabId>, TimerManagerError<T::Error>>
where
    T: TriggerStore,
{
    let range_end = *slab_range.end();
    let mut state = state.slab_lock().await;          // LINE 295 — write lock acquired
    let store_ref = &state.store;
    let scheduler_ref = &state.scheduler;

    let loaded = state
        .store
        .get_slab_range(&segment.id, slab_range)      // stream of Cassandra rows
        .map_err(TimerManagerError::Store)
        .map_ok(|slab_id| async move {
            let slab = Slab::new(segment.id, slab_id, segment.slab_size);
            load_triggers(store_ref, scheduler_ref, slab).await?;  // LINE 305 — I/O + channel sends
            Ok(slab_id)
        })
        .try_buffer_unordered(LOAD_CONCURRENCY)        // up to 16 concurrent futures, each yields
        .try_collect()
        .await?;                                       // LINE 310 — write lock still held here

    state.extend_ownership(range_end);
    Ok(loaded)                                         // LINE 313 — write lock released on drop
}
```

The write guard (`state`) is held from **line 295** all the way through
`try_buffer_unordered(...).try_collect().await?` on **line 310**.  Each
iteration of the stream calls `load_triggers`, which does:

1. `store.get_slab_triggers_all_types(&slab)` — Cassandra network round-trip
   (unbounded latency).
2. For each trigger: `scheduler.schedule(trigger).await` — mpsc channel send +
   awaiting a oneshot response from the scheduler background task.

Every `.await` in these calls is a yield point where the executor may run other
tasks — including tasks that call `trigger_lock()`.

### `remove_completed_slabs` has the same pattern: `src/timers/loader.rs` (lines 331–358)

```rust
async fn remove_completed_slabs<T>(...) {
    let state = state.slab_lock().await;         // write lock acquired
    let store_ref = &state.store;
    // ...
    let deleted_slab_ids = iter(completed_slab_ids)
        .map(|slab_id| async move {
            store_ref.delete_slab(&segment.id, slab_id).await?;  // Cassandra I/O
            Ok(slab_id)
        })
        .buffer_unordered(DELETE_CONCURRENCY)    // up to 16 concurrent futures, each yields
        .try_collect::<Vec<_>>()
        .await?;                                  // write lock still held
}
```

### Task B — The reader: `src/timers/manager.rs`, `schedule` (lines 260–314)

```rust
pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerManagerError<T::Error>> {
    let slab = Slab::from_time(...);
    let state = self.0.state.trigger_lock().await;  // LINE 264 — RwLock::read()
    // ... store.add_trigger, scheduler.schedule
}
```

Same pattern for `unschedule` (line 348), `scheduled_times` (line 178),
`complete` (line 484), and `fire` (line 449) — all call `trigger_lock()`.

### Tokio write-preferring fairness (confirmed)

From Tokio 1.49.0 docs:

> "The priority policy of Tokio's read-write lock is *fair* (or
> *write-preferring*), in order to ensure that readers cannot starve writers.
> [...] if a task that wishes to acquire the write lock is at the head of the
> queue, read locks will not be given out until the write lock has been
> released."

> "Therefore deadlock may occur if a read lock is held by the current task, a
> write lock attempt is made, and then a subsequent read lock attempt is made by
> the current task."

This means: as soon as `slab_lock().await` (the write lock request) is *queued*
— even before it is granted — any subsequent `trigger_lock()` call **blocks**.
So even in the window between Task A queuing the write and actually acquiring it,
Task B is already blocked.

---

## Self-Deadlock Check: Does `load_triggers` Re-acquire the Lock?

`load_triggers` (`src/timers/loader.rs`, lines 373–392) calls:

- `store.get_slab_triggers_all_types(&slab)` — calls into `TriggerStore`, which
  in the in-memory implementation uses an `scc::HashMap` (lock-free); in the
  Cassandra implementation does a network I/O.  Neither acquires `SlabLock`.
- `scheduler.schedule(trigger).await` — sends to an `mpsc` channel and awaits a
  `oneshot` response.  `TriggerScheduler` does **not** hold or acquire any
  `SlabLock`; it only uses its own internal `ActiveTriggers` (an `scc::HashMap`)
  and `TriggerQueue` (a `tokio_util::time::DelayQueue` wrapped in a background
  task).

There is **no self-deadlock**: `load_triggers` does not attempt to re-acquire
either `slab_lock` or `trigger_lock`.

The hazard is strictly a **cross-task lock starvation**: Task A (slab loader)
holds write lock while Task B (TimerManager user) is blocked waiting for read
lock.

---

## Heartbeat Detection

The heartbeat is called in `slab_loader_iteration` at the **top** of each loop
iteration:

```rust
// src/timers/loader.rs, line 172
heartbeat.beat();
```

The heartbeat fires **before** calling `load_slabs`.  Once `load_slabs` is
entered and begins awaiting Cassandra I/O, the loop does not reach `heartbeat.beat()`
again until the entire call returns.

The waiting loop at the end of `slab_loader_iteration` (lines 237–241) does
pulse the heartbeat via `heartbeat.next()`, but this is only reached **after**
`load_slabs` completes.  There is no heartbeat call inside `load_slabs` or
`remove_completed_slabs`.

Furthermore, the heartbeat **does not detect the reader starvation** at all:
the slab loader itself is progressing fine (it is actively holding the lock and
doing I/O).  The starvation is experienced only by the tasks blocked on
`trigger_lock()`.  `HeartbeatRegistry::any_stalled()` would return `false` even
while `TimerManager::schedule()` is completely blocked, because the slab loader
heartbeat keeps beating normally.

**Conclusion: the heartbeat system does not detect this class of futurelock.**

---

## The Starvation Window in Practice

With Cassandra backend and `LOAD_CONCURRENCY = 16`:

- Each `load_triggers` call executes `get_slab_triggers_all_types` (1+ network
  round-trips) followed by up to N `scheduler.schedule()` calls (each is an
  mpsc send + oneshot await).
- With `try_buffer_unordered(16)` there are up to 16 such futures in flight
  simultaneously, all while the write lock is held.
- For a busy segment with many triggers, `load_slabs` can hold the write lock
  for hundreds of milliseconds or more.
- During that entire window, every call to `schedule()`, `unschedule()`,
  `scheduled_times()`, `complete()`, or `fire()` on the `TimerManager` blocks
  completely.

---

## Test

The following test was added to `src/timers/loader.rs` under `#[cfg(test)]`
(function `test_write_lock_blocks_trigger_lock_readers`):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_lock_blocks_trigger_lock_readers() -> Result<()> {
    let store = memory_store();
    let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
    let state = SlabLock::new(State::new(store, scheduler));

    let gate = Arc::new(Notify::new());
    let gate_clone = gate.clone();
    let state_clone = state.clone();

    // Task A: acquire write lock, signal that it is held, then sleep 200ms
    // (simulating Cassandra I/O) before releasing.
    let writer = tokio::spawn(async move {
        let _guard = state_clone.slab_lock().await;
        gate_clone.notify_one();
        sleep(Duration::from_millis(200)).await;
        // guard dropped here
    });

    // Wait until Task A holds the write lock.
    gate.notified().await;

    // Task B: try to acquire a read lock within 50ms.
    // Must fail because Task A holds the write lock for 200ms.
    let read_result = timeout(Duration::from_millis(50), state.trigger_lock()).await;
    assert!(read_result.is_err(), "trigger_lock() must be blocked by the held write lock");

    // Release Task A.
    writer.await.map_err(|e| eyre!("writer task panicked: {e}"))?;

    // Now the read lock succeeds.
    let read_result = timeout(Duration::from_millis(50), state.trigger_lock()).await;
    assert!(read_result.is_ok(), "trigger_lock() must succeed after the write lock is released");

    Ok(())
}
```

This test is **multi-threaded** (2 workers) because the writer task must be
polled on a different OS thread while the test task blocks attempting the read
lock — single-threaded (`current_thread`) would deadlock immediately since the
writer could never be scheduled.

The test uses `Notify` (not sleep) to synchronize the "lock is held" moment.
The `sleep` inside the writer is explicitly simulating I/O backpressure latency,
which is the permitted use per CLAUDE.md.

---

## Related Issues

### `remove_completed_slabs` has the same pattern

`remove_completed_slabs` acquires the write lock and holds it across
`buffer_unordered(DELETE_CONCURRENCY)` with Cassandra `delete_slab` calls.
This is the same futurelock with the same fix required.

### `unschedule_all` — nested lock concern

`TimerManager::unschedule_all` (`src/timers/manager.rs`, lines 410–425) calls
`scheduled_times()` (which acquires `trigger_lock()`) and then for each result
calls `unschedule()` (which also acquires `trigger_lock()`), all via
`try_buffer_unordered(DELETE_CONCURRENCY)`.  Both `trigger_lock()` calls are
read locks, so they don't conflict with each other.  However, they both block
behind any queued write lock from the slab loader, which amplifies the stall
since `unschedule_all` itself is doing concurrent I/O while holding no lock.
This is not a separate futurelock — it is another reader starved by the same
writer.

### Heartbeat granularity

The stall threshold used by the `HeartbeatRegistry` in the slab loader is
propagated from the caller (the consumer partition manager).  Even if the stall
threshold were lowered to detect the I/O latency inside `load_slabs`, the
heartbeat would still report the slab loader as **healthy** — because `beat()`
is called at the top of each iteration, before the write lock is acquired.
A heartbeat inside `load_slabs` would fix the detection gap, but the root cause
(holding write lock across I/O) would remain.

---

## Recommended Fix (Not Implemented Here)

The fix is to restructure `load_slabs` so the write lock is **not held across
I/O**.  One approach:

1. Acquire the write lock, clone or snapshot the relevant configuration (store
   handle, scheduler handle, segment metadata).
2. Release the write lock immediately.
3. Perform all I/O (Cassandra reads, scheduler channel sends) lock-free, using
   the cloned handles.
4. Re-acquire the write lock only to call `extend_ownership`.

Both `TriggerStore` (`Clone + Send + Sync + 'static`) and `TriggerScheduler`
(`Clone`) already support this pattern.  The write lock would then be held only
for the microsecond duration of `extend_ownership`, eliminating the starvation
window.

The same restructuring applies to `remove_completed_slabs`.
