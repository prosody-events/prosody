# Futurelock Investigation: `unschedule_all` in `src/timers/manager.rs`

## Executive Summary

**Hypothesis A is disproved.** `unschedule_all` does not cause a self-deadlock because both
`scheduled_times` and `unschedule` acquire only `trigger_lock()` (the read lock). Neither
escalates to the write lock (`slab_lock()`). There is no lock-ordering deadlock.

**Hypothesis B is confirmed as a latent risk**, but is mitigated by `unschedule_all`'s
implementation: the stream returned by `scheduled_times` holds the read lock for its _entire
lifetime_, and that stream is polled interleaved with `unschedule` futures via
`try_buffer_unordered`. Because tokio's `RwLock` is write-preferring, any concurrent write-lock
acquisition attempt (from `slab_loader`) will block all subsequent readers once the current
`scheduled_times` stream finishes. However, because both paths only need the read lock, the
write-lock starvation is one-sided (reads stall writes, not vice versa).

**The real finding is a different futurelock risk**: `scheduled_times` holds `trigger_lock()`
(a `tokio::sync::RwLockReadGuard`) across multiple `.await` points inside a `try_stream!` body.
This means the future that owns the read guard can be snoozed — polled infrequently or not at
all if the consumer (the `try_buffer_unordered` combinator) is busy with in-flight
`unschedule` futures — while the read lock remains held, blocking any write-lock attempt
from the background `slab_loader` task indefinitely. This is a classic snooze-induced lock
starvation.

---

## Background: Snoozing and Futurelocks

Per [jacko.io/snooze.html](https://jacko.io/snooze.html):

> A future is "snoozed" when it is ready to make progress but is not being polled.

A **futurelock** occurs when:
1. Future A holds an async lock guard.
2. Future A is not polled (snoozed).
3. Future B attempts to acquire the same lock.
4. Future B cannot make progress; neither can A because A is not being scheduled.

The risk is distinct from cancellation and from thread-level mutex deadlocks because it
arises from the cooperative scheduling model of async Rust.

---

## Files and Types Involved

| File | Type / Function | Role |
|---|---|---|
| `src/timers/slab_lock.rs` | `SlabLock<T>` | Wraps `Arc<RwLock<T>>` |
| `src/timers/slab_lock.rs` | `trigger_lock()` | Acquires read lock (`RwLock::read()`) |
| `src/timers/slab_lock.rs` | `slab_lock()` | Acquires write lock (`RwLock::write()`) |
| `src/timers/manager.rs` | `scheduled_times()` | Returns stream; holds read lock for entire stream lifetime |
| `src/timers/manager.rs` | `unschedule()` | Acquires read lock, does store + scheduler ops |
| `src/timers/manager.rs` | `unschedule_all()` | Drives `scheduled_times` + `unschedule` concurrently |
| `src/timers/loader.rs` | `load_slabs()` | Acquires **write lock** |
| `src/timers/loader.rs` | `remove_completed_slabs()` | Acquires **write lock** |

---

## Lock Hierarchy

`SlabLock<State<T>>` wraps a single `tokio::sync::RwLock<State<T>>`.

- `trigger_lock()` → `RwLock::read()` — shared, multiple concurrent holders allowed.
- `slab_lock()` → `RwLock::write()` — exclusive, no concurrent readers or writers.

`tokio::sync::RwLock` is write-preferring: once a writer is waiting, new readers are blocked
even if existing readers still hold the lock. This is the mechanism behind Hypothesis B.

---

## Complete Lock Acquisition Graph

### `TimerManager` public/crate methods

| Method | Lock Acquired | Lock Type | Scope |
|---|---|---|---|
| `scheduled_times()` [line 167] | `trigger_lock()` | Read | Held for lifetime of returned stream (across all `.await` points in `try_stream!`) |
| `scheduled_triggers()` [line 222] | `trigger_lock()` | Read | Held across `try_collect().await` |
| `schedule()` [line 260] | `trigger_lock()` | Read | Released before method returns |
| `unschedule()` [line 339] | `trigger_lock()` | Read | Released before method returns |
| `unschedule_all()` [line 410] | (none directly) | — | Drives `scheduled_times` + `unschedule`; see below |
| `fire()` [line 441] | `trigger_lock()` | Read | Released before method returns |
| `complete()` [line 475] | `trigger_lock()` | Read | Released before method returns |
| `abort()` [line 537] | `trigger_lock()` | Read | Released before method returns |

### `slab_loader` background task

| Function | Lock Acquired | Lock Type |
|---|---|---|
| `load_slabs()` [loader.rs:295] | `slab_lock()` | Write (exclusive) |
| `remove_completed_slabs()` [loader.rs:339] | `slab_lock()` | Write (exclusive) |

**No method in `TimerManager` or `slab_loader` acquires both locks. The lock hierarchy has exactly two levels: read (trigger ops) and write (slab ops). They are never held simultaneously by the same task.**

---

## Detailed Code Path: `unschedule_all`

### `unschedule_all` — `src/timers/manager.rs`, lines 410–425

```rust
pub async fn unschedule_all(
    &self,
    key: &Key,
    timer_type: TimerType,
) -> Result<(), TimerManagerError<T::Error>> {
    let span = Span::current();

    self.scheduled_times(key, timer_type)           // [1] creates stream, acquires read lock inside
        .map_ok(|time| {
            self.unschedule(key, time, timer_type)  // [2] each unschedule acquires read lock independently
                .instrument(span.clone())
        })
        .try_buffer_unordered(DELETE_CONCURRENCY)   // [3] up to DELETE_CONCURRENCY unschedule futures in flight
        .try_collect::<()>()
        .await
}
```

### `scheduled_times` — lines 167–210

```rust
pub fn scheduled_times(
    &self,
    key: &Key,
    timer_type: TimerType,
) -> impl Stream<Item = Result<CompactDateTime, TimerManagerError<T::Error>>> + Send + 'static
{
    let slab_lock = self.0.state.clone();
    let key = key.clone();
    let segment_id = self.0.segment.id;

    try_stream! {
        let state = slab_lock.trigger_lock().await;  // [A] READ LOCK ACQUIRED HERE
        // ... 'state' lives for the rest of this generator body:
        let stream = state
            .store
            .get_key_times(&segment_id, timer_type, &key)
            .map_err(TimerManagerError::Store);

        pin_mut!(stream);
        while let Some(time) = cooperative(stream.try_next()).await? {  // [B] AWAIT ACROSS READ LOCK
            let active_triggers = state.scheduler.active_triggers();
            let is_scheduled = active_triggers.is_scheduled(&key, time, timer_type).await;  // [C] AWAIT ACROSS READ LOCK
            let not_active = !active_triggers.contains(&key, time, timer_type).await;       // [D] AWAIT ACROSS READ LOCK

            if is_scheduled || not_active {
                yield time;  // [E] YIELD (suspension point) WHILE HOLDING READ LOCK
            }
        }
        // [F] READ LOCK RELEASED HERE (when 'state' drops)
    }
}
```

**The critical observation**: `state` (a `SlabLockTriggerGuard<'_, State<T>>` wrapping a
`RwLockReadGuard`) is created at line A and dropped at line F. Between A and F there are
multiple `.await` points (B, C, D) and a `yield` point (E). Each of these is a point where
the generator future can be suspended while the read lock remains held.

### `unschedule` — lines 339–394

```rust
pub async fn unschedule(
    &self,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
) -> Result<(), TimerManagerError<T::Error>> {
    let slab = Slab::from_time(...);
    let slab_id = slab.id();
    let state = self.0.state.trigger_lock().await;  // ACQUIRES READ LOCK

    let current_state = state
        .scheduler
        .active_triggers()
        .get_state(key, time, timer_type)
        .await;                                     // AWAIT WHILE HOLDING READ LOCK

    // ... match on current_state, possible further awaits ...
    // state DROP = READ LOCK RELEASED
}
```

`unschedule` acquires only `trigger_lock()` (read lock). It does **not** call `slab_lock()`.
**Hypothesis A is disproved**: there is no attempt to acquire the write lock while the read
lock is held.

---

## Hypothesis Analysis

### Hypothesis A — Self-deadlock: DISPROVED

**Claim**: `unschedule_all` holds read lock → calls `unschedule()` → `unschedule()` tries to
acquire write lock → deadlock.

**Evidence against**:
- `unschedule()` at line 348 calls `self.0.state.trigger_lock().await` — this is
  `RwLock::read()`, not `RwLock::write()`.
- `unschedule()` never calls `slab_lock()`.
- Multiple concurrent read locks can be held simultaneously; tokio's `RwLock` allows this.
- The `scheduled_times` stream holds a read lock; `unschedule` concurrently acquires another
  read lock. Both succeed because they are both readers.

**Verdict**: Not a deadlock.

### Hypothesis B — Write-lock starvation: CONFIRMED (latent)

**Claim**: `unschedule_all` holds read lock for a long time → slab loader waiting for write
lock → tokio's write-preferring RwLock blocks new readers → starvation.

**Evidence for**:

1. `scheduled_times` holds `trigger_lock()` for the entire stream lifetime (lines 178–208,
   across every `await` and `yield`).
2. The stream is driven by `try_buffer_unordered(DELETE_CONCURRENCY)` in `unschedule_all`.
   When the concurrent unschedule futures fill the buffer, the driver stops polling the
   `scheduled_times` stream. The stream future is snoozed. The read lock stays held.
3. `load_slabs()` in `src/timers/loader.rs` line 295 calls `state.slab_lock().await`
   (write lock). While any reader holds the lock, this write acquisition blocks.
4. `remove_completed_slabs()` at line 339 similarly calls `state.slab_lock().await`.
5. tokio's `RwLock` is write-preferring: once `slab_lock()` is waiting, subsequent
   `trigger_lock()` calls will also block. This means new calls to `schedule()`,
   `unschedule()`, `fire()`, `complete()`, etc. from other tasks will stall.

**The snooze pattern**:

```
Task: slab_loader
  → slab_loader_iteration()
  → load_slabs()
  → state.slab_lock().await         ← BLOCKED (write waiter)

Task: consumer calling unschedule_all
  → scheduled_times stream (future S) holds trigger_lock() (read)
  → try_buffer_unordered has DELETE_CONCURRENCY futures in flight
  → S is snoozed (not polled) while buffer is full
  → S holds read lock without making progress
  → slab_loader write attempt blocks

Task: other consumer calling schedule()
  → self.0.state.trigger_lock().await  ← BLOCKED (write-preferring RwLock
                                          blocks new readers once writer waits)
```

**Severity**: The deadlock resolves when the `unschedule` futures drain enough to allow the
`scheduled_times` stream to be polled again, releasing the read guard. So this is **not an
infinite deadlock** but a **bounded stall** whose duration is proportional to the number of
timers being unscheduled and `DELETE_CONCURRENCY`. Under normal operation (small `DELETE_CONCURRENCY`,
short-lived store operations) this is brief. Under adverse conditions (slow store, many timers,
large `DELETE_CONCURRENCY`) it can stall the slab loader and all scheduling calls for seconds.

### Hypothesis C — Double read, safe: PARTIALLY TRUE

Both `scheduled_times` and `unschedule` use only `trigger_lock()`. There is no deadlock between
them because tokio's `RwLock` permits concurrent readers. Hypothesis C is correct in that no
deadlock occurs between `scheduled_times` and `unschedule`. The issue is the interaction with
the `slab_lock()` write path, which Hypothesis C does not address.

---

## The Actual Snooze Pattern (Precise)

The generator returned by `scheduled_times` is a `try_stream!` macro-generated future.
At every `yield` in that generator, control returns to the combinator
(`try_buffer_unordered`). When the combinator has `DELETE_CONCURRENCY` futures already in
flight, it will not poll the input stream again until one completes. During this window:

- The generator future is parked (snoozed in the snooze.html sense).
- The `SlabLockTriggerGuard` — which wraps `RwLockReadGuard<'_, State<T>>` — is still alive
  inside the generator's captured state.
- The read lock is held.
- Any attempt by `slab_loader` to call `state.slab_lock().await` is blocked.
- Due to write-preferring semantics, any subsequent `state.trigger_lock().await` in other
  tasks is also blocked.

This is a textbook futurelock: the snoozed generator holds a resource (the read lock) that
blocks other work (write lock acquisition, then new read lock acquisitions).

---

## Does Heartbeat Detect This?

The heartbeat in `slab_loader` calls `heartbeat.beat()` at the top of each iteration
(`loader.rs`, line 172). If the loader is blocked waiting for the write lock, it will not
call `beat()`. Whether this triggers a heartbeat alert depends on the configured heartbeat
timeout versus the duration of the stall.

- **Short stalls** (milliseconds to a few seconds): likely not detected because heartbeat
  timeouts are typically tens of seconds.
- **Long stalls** (store operations are slow or very many timers are being unscheduled):
  the heartbeat will fire, alerting to a stalled loader. However, the root cause
  (`unschedule_all` holding the read lock) is not directly surfaced.

**Conclusion**: Heartbeat provides an indirect safety net for extreme cases but will not
reliably detect the common, short-duration stall.

---

## `uncommitted.rs` Interaction

`src/timers/uncommitted.rs` does not interact with `SlabLock` directly. It delegates all
operations to `TimerManager` methods:

- `UncommittedTrigger::fire()` → `manager.fire()` (acquires read lock, releases before return)
- `UncommittedTrigger::commit()` → `manager.complete()` (acquires read lock, releases before return)
- `UncommittedTrigger::abort()` → `manager.abort()` (acquires read lock, releases before return)

None of these hold the lock across suspension points because they are regular `async fn`s, not
generators. The lock is acquired and released within a single logical async operation. No
snooze risk here.

---

## `context.rs` Interaction

`src/consumer/middleware/defer/timer/context.rs` uses `EventContext` trait methods
(`schedule`, `unschedule`, `clear_scheduled`, `clear_and_schedule`, `scheduled`). These
delegate to the underlying `TimerManager` methods. The `merge_scheduled_streams` function
uses `cooperative()` yielding, which is correct. No additional lock exposure is introduced.

The `scheduled()` method returns a stream that internally uses `scheduled_times()` via the
event context chain. If this merged stream is driven by a combinator that can snooze it
(e.g., `try_buffer_unordered`), the same read-lock-holding snooze risk applies transitively.

---

## `select!` Loops Holding Lock Guards

There are no `select!` loops in `manager.rs` that hold a `SlabLockTriggerGuard` or
`SlabLockSlabGuard`. The `scheduler.rs` `process_commands` loop uses `select!` but does not
hold a `SlabLock` guard — it operates on `TriggerQueue` and `mpsc` channels, which are
independent of `SlabLock`.

The `slab_loader` function at `loader.rs:237` uses `select!` while the write lock is **not**
held — the `slab_lock()` call is inside `load_slabs()` and `remove_completed_slabs()`, both
of which complete before the wait `select!` at line 237 is reached.

---

## Minimal Reproduction Test

The starvation (Hypothesis B) can be demonstrated by:
1. Holding a read lock in a snoozed async context.
2. Attempting a write lock in a separate task.
3. Observing that the write lock cannot be acquired until the read lock holder is polled.

The following test demonstrates the write-lock starvation caused by a snoozed read-lock
holder. Place this in a new test module in `src/timers/slab_lock.rs` or a test file.

```rust
#[cfg(test)]
mod futurelock_tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Demonstrates that a snoozed future holding a read lock starves
    /// a write-lock attempt: the write lock cannot be acquired until
    /// the read-lock holder is polled again and drops its guard.
    #[tokio::test]
    async fn test_read_lock_snooze_starves_writer() {
        let lock: SlabLock<i32> = SlabLock::new(0);
        let lock2 = lock.clone();

        // Acquire the read lock and immediately suspend (yield without dropping guard).
        // This simulates a snoozed generator holding trigger_lock().
        let (reader_ready_tx, reader_ready_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let reader_task = tokio::spawn(async move {
            let _guard = lock2.trigger_lock().await;
            // Signal that we hold the lock
            let _ = reader_ready_tx.send(());
            // Wait until told to release (simulates snoozing)
            let _ = release_rx.await;
            // guard drops here, releasing read lock
        });

        // Wait until reader holds the lock
        reader_ready_rx.await.ok();

        // Now attempt a write lock — must time out because the reader is "snoozed"
        let write_result = timeout(
            Duration::from_millis(100),
            lock.slab_lock(),
        )
        .await;

        // Write lock acquisition should have timed out (reader is snoozed/holding lock)
        assert!(
            write_result.is_err(),
            "write lock should not be acquirable while snoozed reader holds read lock"
        );

        // Release the reader
        let _ = release_tx.send(());
        reader_task.await.ok();

        // Now write lock should succeed immediately
        let write_result = timeout(
            Duration::from_millis(100),
            lock.slab_lock(),
        )
        .await;
        assert!(
            write_result.is_ok(),
            "write lock should succeed after reader releases"
        );
    }

    /// Demonstrates the write-preferring behavior: once a writer is waiting,
    /// new readers are blocked even though existing readers are compatible.
    /// This is the Hypothesis B starvation mechanism.
    #[tokio::test]
    async fn test_write_preferring_blocks_new_readers() {
        let lock: SlabLock<i32> = SlabLock::new(0);
        let lock_for_reader1 = lock.clone();
        let lock_for_writer = lock.clone();
        let lock_for_reader2 = lock.clone();

        let (r1_held_tx, r1_held_rx) = tokio::sync::oneshot::channel::<()>();
        let (w_waiting_tx, w_waiting_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_r1_tx, release_r1_rx) = tokio::sync::oneshot::channel::<()>();

        // Reader 1: holds read lock for a long time
        let reader1 = tokio::spawn(async move {
            let _guard = lock_for_reader1.trigger_lock().await;
            let _ = r1_held_tx.send(());
            let _ = release_r1_rx.await;
        });

        // Wait for reader1 to hold the lock
        r1_held_rx.await.ok();

        // Writer: attempts to acquire write lock (will block behind reader1)
        let writer = tokio::spawn(async move {
            let _ = w_waiting_tx.send(());
            let _guard = lock_for_writer.slab_lock().await;
        });

        // Wait a moment for writer to be queued
        w_waiting_rx.await.ok();
        tokio::task::yield_now().await;

        // Reader 2: attempts to acquire read lock while writer is waiting.
        // With write-preferring semantics, this should be blocked.
        let reader2_result = timeout(
            Duration::from_millis(50),
            lock_for_reader2.trigger_lock(),
        )
        .await;

        assert!(
            reader2_result.is_err(),
            "new reader should be blocked when a writer is waiting (write-preferring)"
        );

        // Release reader1, allowing writer to proceed
        let _ = release_r1_tx.send(());
        reader1.await.ok();
        writer.await.ok();
    }
}
```

---

## Severity Assessment

| Aspect | Assessment |
|---|---|
| **Hypothesis A (self-deadlock)** | Not present. Both paths use read locks. |
| **Hypothesis B (write starvation)** | Latent. Present whenever `unschedule_all` is called with many timers and `DELETE_CONCURRENCY > 1`. |
| **Infinite deadlock risk** | Low. The stall resolves once all `unschedule` futures drain. |
| **Slab loader stall duration** | Proportional to `DELETE_CONCURRENCY` × average store round-trip time × number of timer deletions needed to drain buffer. |
| **Heartbeat detection** | Indirect only; short stalls not detected. |
| **Production impact** | Low-to-moderate. Visible as increased latency for `schedule`/`fire`/`complete` calls and delayed slab loading during bulk unschedule operations. |
| **Correctness risk** | No data corruption. State remains consistent because the read-lock scope in `scheduled_times` prevents concurrent writes from changing the `State<T>` snapshot mid-stream. |

**The design tradeoff**: Holding `trigger_lock()` across the entire `scheduled_times` stream
provides a consistent snapshot of the store and scheduler state, preventing timers from being
added or removed between the listing and the unschedule operations. The cost is holding the
read lock longer than necessary, creating the starvation window described above.

**Recommendation**: Collect all scheduled times into a `Vec` before releasing the read lock,
then issue unschedule calls without holding the lock. This eliminates the snooze window at the
cost of a snapshot being potentially stale. Given the at-least-once semantics and idempotent
unschedule behavior, a stale snapshot is safe here.
