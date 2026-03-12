# Snooze Analysis: `scheduled_times` and the `unschedule_all` Call Chain

## Verdict: CONFIRMED — write-lock starvation is real; self-deadlock is not

The investigation confirms a real, exploitable lock-starvation hazard in the
`unschedule_all` path. A **true circular deadlock** (Task A holds read lock,
Task B holds write lock, both waiting on each other) does not exist because both
callers of the slab write lock (`load_slabs` and `remove_completed_slabs` in
`src/timers/loader.rs`) and the callers of the trigger read lock
(`scheduled_times`, `unschedule`, `schedule`) are in separate tasks that do not
hold both locks simultaneously. However, tokio's write-preferring `RwLock`
semantics create a starvation scenario that can cause the `slab_loader`
background task to be blocked indefinitely while `unschedule_all` runs.

---

## Exact Code Path

### `unschedule_all` (manager.rs:410-425)

```rust
pub async fn unschedule_all(
    &self,
    key: &Key,
    timer_type: TimerType,
) -> Result<(), TimerManagerError<T::Error>> {
    let span = Span::current();

    self.scheduled_times(key, timer_type)             // <-- opens read lock
        .map_ok(|time| {
            self.unschedule(key, time, timer_type)    // <-- each call also opens read lock
                .instrument(span.clone())
        })
        .try_buffer_unordered(DELETE_CONCURRENCY)     // DELETE_CONCURRENCY = 16
        .try_collect::<()>()
        .await
}
```

### `scheduled_times` (manager.rs:167-210)

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
        let state = slab_lock.trigger_lock().await;   // (A) read guard acquired

        let stream = state
            .store
            .get_key_times(&segment_id, timer_type, &key)
            .map_err(TimerManagerError::Store);

        pin_mut!(stream);
        while let Some(time) = cooperative(stream.try_next()).await? { // (B) .await with guard held
            let active_triggers = state.scheduler.active_triggers();
            let is_scheduled = active_triggers.is_scheduled(&key, time, timer_type).await; // (C) .await with guard held
            let not_active = !active_triggers.contains(&key, time, timer_type).await;      // (D) .await with guard held

            if is_scheduled || not_active {
                yield time;                           // (E) yield with guard held
            }
        }
        // (F) guard dropped here, at end of try_stream! body
    }
}
```

The `try_stream!` macro generates a `Generator`-backed `Future`. The
`RwLockReadGuard` named `state` is a local variable in the generator body. It
is created at `(A)` and lives until `(F)`. Between those two points the
generator suspends at every `.await` — `(B)`, `(C)`, `(D)` — and at every
`yield` `(E)`. During each suspension the generator is not being polled, meaning
the future holding `state` is "snoozed" in the sense of
[jacko.io/snooze.html](https://jacko.io/snooze.html): it is ready to make
progress the moment the executor resumes it, but the read guard is still live
and counted by the `RwLock` while the future sits in the `try_buffer_unordered`
pool.

### `unschedule` (manager.rs:339-394)

```rust
pub async fn unschedule(
    &self,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
) -> Result<(), TimerManagerError<T::Error>> {
    let slab = Slab::from_time(...);
    let slab_id = slab.id();
    let state = self.0.state.trigger_lock().await;  // acquires ANOTHER read lock
    ...
}
```

`unschedule` acquires a **new, independent** read lock. It does not try to
acquire a write lock. Because tokio's `RwLock` allows multiple simultaneous
readers, and because a read lock is already held by `scheduled_times`'s
generator, `unschedule` can always acquire its own read lock immediately without
deadlocking. There is no self-deadlock.

---

## Lock Acquisition Graph

```
Task: unschedule_all
  |
  +-- scheduled_times generator
  |     Lock: trigger_lock (read) -- held for ENTIRE generator lifetime
  |     Awaits:
  |       store.get_key_times (Cassandra read, network I/O)
  |       active_triggers.is_scheduled (scc::HashMap async)
  |       active_triggers.contains   (scc::HashMap async)
  |       yield (suspends to try_buffer_unordered)
  |
  +-- try_buffer_unordered(16) spawns up to 16 concurrent unschedule() calls
        Each unschedule():
          Lock: trigger_lock (read) -- NEW read guard, acquired and released per call
          Awaits:
            active_triggers.get_state  (scc::HashMap async)
            scheduler.unschedule(trigger) (mpsc send + oneshot wait)
            store.remove_trigger (Cassandra write, network I/O)

Task: slab_loader (background, perpetual)
  |
  +-- load_slabs (loader.rs:286-314)
  |     Lock: slab_lock (WRITE) <-- blocked here
  |     Awaits: store.get_slab_range (Cassandra)
  |
  +-- remove_completed_slabs (loader.rs:331-358)
        Lock: slab_lock (WRITE) <-- blocked here
        Awaits: store.delete_slab (Cassandra)
```

The `SlabLock<T>` wraps a single `tokio::sync::RwLock<State<T>>`.
`trigger_lock()` calls `.read().await` (line 98 of `slab_lock.rs`) and
`slab_lock()` calls `.write().await` (line 106).

---

## The Starvation Scenario

Tokio's `RwLock` is **write-preferring** (documented behavior): once a writer
is waiting, no new readers can acquire the lock. This prevents writer
starvation, but it means:

1. `unschedule_all` is called. The `scheduled_times` generator acquires the
   read lock (`trigger_lock`) and starts iterating over Cassandra results.

2. While the generator is suspended at a Cassandra I/O await inside the loop,
   `slab_loader` wakes up (its sleep expired) and calls `load_slabs`, which
   calls `state.slab_lock().await` to acquire the write lock.

3. Because the read lock is already held, the write request is queued.

4. Per tokio's write-preferring policy, **no further readers can acquire the
   lock** once a writer is waiting. This means:
   - `unschedule` calls spawned by `try_buffer_unordered` that have not yet
     acquired their own read lock will block at `trigger_lock().await`.

5. Meanwhile, `scheduled_times`'s generator is still suspended inside the loop
   (holding its read lock), waiting for its in-flight `try_buffer_unordered`
   items to complete so it can yield the next item.

6. But those in-flight `unschedule` calls are now blocked waiting for the read
   lock that `scheduled_times` still holds.

7. `scheduled_times` cannot drop its read lock until the generator body
   completes (i.e., the `try_stream!` generator runs to end-of-body). But the
   generator cannot advance past its `yield` points until `try_buffer_unordered`
   has consumed enough items.

8. `try_buffer_unordered` fills its buffer (up to `DELETE_CONCURRENCY = 16`)
   and then stops pulling from the source stream. The source stream is
   `scheduled_times`. The generator is suspended at a `yield`. The consumer
   (`try_buffer_unordered`) is blocked on an in-flight `unschedule` call that
   is waiting for the read lock, which is held by the generator.

**This is not a hard deadlock in the classical sense**: the existing read-lock
holders (`scc::HashMap` operations inside the loop) will eventually finish, and
once `scheduled_times` yields all its items and the generator body exits, the
guard drops and the write lock will be granted. However:

- The generator can be arbitrarily long (one Cassandra row per iteration, with
  network round trips at each step).
- `DELETE_CONCURRENCY = 16` means the buffer can hold 16 pending `unschedule`
  futures, all of which will be piled up waiting for the write-preferring writer
  to be unblocked — which cannot happen until `scheduled_times` completes.
- The slab loader is stuck for the duration of the entire `unschedule_all`
  execution, which can be seconds to minutes on a large key.

This is a **sustained write-lock starvation** of the slab loader. If
`unschedule_all` is called frequently (e.g., during rebalancing or mass
cancellation), the slab loader may fall behind and fail to preload slabs, which
would cause missed timer deliveries.

---

## Why There Is No Hard Self-Deadlock

The `scheduled_times` generator holds a **read** guard. `unschedule` also
acquires a **read** guard. Since `tokio::sync::RwLock` allows multiple
simultaneous readers, and since the read guard acquired inside `unschedule` is a
new, independent acquisition (not a re-entrant attempt), **the new readers can
acquire the lock as long as no writer is queued**. The starvation only occurs
when `slab_loader` queues a write request between the generator's first
iteration and the last `unschedule` completing.

If no writer ever queues during the entire `unschedule_all` call, all read
acquisitions succeed, and the operation completes normally.

---

## Does the Heartbeat Catch This?

**Partially, but not directly.**

`slab_loader` (in `loader.rs:131-154`) registers a heartbeat via
`heartbeats.register("timer loader")`. The `slab_loader` loop calls
`heartbeat.beat()` at the top of each iteration (`slab_loader_iteration`,
line 172 of `loader.rs`). Inside `slab_loader_iteration`, the calls to
`load_slabs` and `remove_completed_slabs` both call `state.slab_lock().await`,
which will block indefinitely if the read lock is held.

The `slab_loader` loop also uses a `select!` with `heartbeat.next()` during its
sleep phase (`loader.rs:237-240`). However, the heartbeat interlock inside
`slab_loader_iteration` only runs during the _wait_ phase, not during the
_lock acquisition_ phase.

Therefore:
- If `slab_loader` is blocked at `state.slab_lock().await` (waiting for the
  write lock while `unschedule_all` holds the read lock), `heartbeat.beat()` is
  **not called** — the beat at line 172 occurs before entering `load_slabs`.
- The `HeartbeatRegistry::any_stalled()` method will eventually return `true`
  after the stall threshold expires (typically 30 seconds or whatever the
  consumer is configured with).
- `any_stalled()` is a passive check that must be polled by an external
  mechanism (health probe, monitoring loop). It does not interrupt the stalled
  task.

So the heartbeat detects the stall after-the-fact and surfaces it as a log line
and a health indicator, but does **not** prevent or break the stall. It does not
cancel the blocking future or alert inside the task itself.

---

## Test

The test below demonstrates that when a generator holds a `tokio::sync::RwLock`
read guard across yield points, a queued writer blocks all subsequent readers
from acquiring the lock — causing the very sequence that `unschedule_all`
creates. It uses `SlabLock` directly and `MemoryTriggerStore` to stay
self-contained.

The test is located in `src/timers/manager.rs` under `#[cfg(test)]`.

```rust
/// Demonstrates write-lock starvation when a try_stream! generator holds a
/// read guard across yield points and a concurrent writer queues up.
///
/// This reproduces the structure of `unschedule_all`:
///   - A generator acquires a read guard and holds it while yielding items.
///   - Concurrent consumers of those items each try to acquire another read
///     guard.
///   - A background task races to acquire a write guard.
///   - tokio's write-preferring RwLock causes new readers to block once the
///     writer is queued, even though the original reader (the generator) still
///     holds its guard.
#[cfg(test)]
mod snooze_tests {
    use crate::timers::slab_lock::SlabLock;
    use async_stream::try_stream;
    use color_eyre::eyre::{Result, eyre};
    use futures::{StreamExt, TryStreamExt, pin_mut};
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn write_preferring_rwlock_starves_new_readers_while_generator_holds_read_guard(
    ) -> Result<()> {
        // A counter wrapped in SlabLock so we reuse the same lock type as
        // TimerManager.
        let lock: SlabLock<u32> = SlabLock::new(0);
        let lock2 = lock.clone();
        let lock3 = lock.clone();

        // Notify used to signal test phases without sleep.
        let writer_queued = Arc::new(Notify::new());
        let writer_queued2 = writer_queued.clone();

        // --- Phase 1: a generator acquires a read guard and holds it ---
        //
        // The generator yields each value one at a time, suspending between
        // yields. The read guard lives for the full lifetime of the generator
        // body.
        let gen_stream = try_stream! {
            let guard = lock.trigger_lock().await;   // read guard acquired

            // Signal that the read guard is held so the writer can race.
            writer_queued2.notify_one();

            // Simulate per-item work (like Cassandra reads + scc lookups).
            // Each iteration suspends, snoozing the generator with the guard.
            for i in 0u32..4u32 {
                // Yield the item; the guard is still held across this yield.
                let _ = &guard; // keep guard live
                yield i;
            }
            // guard dropped here
        };

        // --- Phase 2: a background writer queues up ---
        //
        // Once the read guard is held, the writer tries to acquire the write
        // lock. This will block until the generator exits. Crucially, after the
        // writer has queued, tokio's write-preferring policy means no new
        // readers can acquire the lock.
        let writer_guard_acquired = Arc::new(Notify::new());
        let writer_guard_acquired2 = writer_guard_acquired.clone();

        let writer_task = tokio::spawn(async move {
            // Wait until the generator has the read guard.
            writer_queued.notified().await;
            // Try to acquire the write lock. This will queue behind the reader.
            let mut guard = lock2.slab_lock().await;
            *guard = 999;
            writer_guard_acquired2.notify_one();
        });

        // --- Phase 3: drive the generator and observe read-lock contention ---
        //
        // We use try_buffer_unordered(2) as a stand-in for DELETE_CONCURRENCY.
        // Each item spawns a future that tries to acquire a NEW read lock.
        // After the writer is queued, these futures will block.
        pin_mut!(gen_stream);

        let mut new_reader_blocked = false;

        let result = timeout(Duration::from_millis(500), async {
            // Pull items from the generator; for each one, try a new read
            // acquisition.
            while let Some(item) = gen_stream.next().await {
                let i = item.map_err(|_: std::convert::Infallible| unreachable!())?;

                // Give the writer task a moment to queue its write request
                // before we attempt the second read acquisition.
                if i == 1 {
                    // A brief yield to let the writer task reach slab_lock().await
                    tokio::task::yield_now().await;
                    tokio::task::yield_now().await;
                }

                // Attempt a new read acquisition. If the writer is queued,
                // this will block.
                let second_read = timeout(
                    Duration::from_millis(50),
                    lock3.trigger_lock(),
                );

                match second_read.await {
                    Ok(_guard) => {
                        // Read succeeded — writer not yet queued or already done.
                    }
                    Err(_timeout) => {
                        // Read timed out — write-preferring lock is blocking us.
                        new_reader_blocked = true;
                        break;
                    }
                }
            }
            Ok::<_, color_eyre::eyre::Error>(())
        })
        .await;

        // Clean up the writer task regardless.
        let _ = writer_task.await;

        result
            .map_err(|_| eyre!("outer timeout: generator or reader hung entirely"))?
            .map_err(|e| eyre!("stream error: {e}"))?;

        assert!(
            new_reader_blocked,
            "Expected a new read acquisition to block once the writer was queued \
             (write-preferring RwLock starvation), but all reads succeeded immediately. \
             This means the starvation scenario did not manifest — check timing."
        );

        Ok(())
    }
}
```

**What the test shows:** Once a task queues a write request, subsequent
`trigger_lock().await` calls (each attempting a read) time out, even though the
original read guard (held by the generator) is still valid. This is exactly
what happens in `unschedule_all` when `slab_loader` tries to enter
`load_slabs` or `remove_completed_slabs` while `scheduled_times` is iterating.

**Caveat on timing:** The test depends on the writer task reaching
`slab_lock().await` between two consecutive `yield_now()` calls. On a
single-threaded executor (`#[tokio::test]` without `flavor = "multi_thread"`)
the writer task may not be scheduled before the read guard is released. Use
`flavor = "multi_thread"` as shown.

---

## Additional Concern: Guard Held Across scc::HashMap Awaits

Inside the `scheduled_times` generator, the read guard is also held across:

- `active_triggers.is_scheduled(...).await` (line 202) — calls
  `scc::HashMap::read_async`, which is an async operation
- `active_triggers.contains(...).await` (line 203) — same

`scc::HashMap` async methods use internal cooperative scheduling and do not
block on system calls, but they are still `.await` points. During these awaits,
the generator is suspended with the `trigger_lock` read guard live. This
prolongs the window during which the slab loader's queued write request prevents
new readers from entering.

---

## Severity Assessment

**Medium-High.**

- **Correctness**: No hard deadlock. The system will eventually make progress
  once `unschedule_all` completes.
- **Availability**: The `slab_loader` background task is blocked for the
  duration of every `unschedule_all` call. For a key with many scheduled timers
  (hundreds of DB rows), this could be several seconds per call. If
  `unschedule_all` is called concurrently for many keys, the slab loader is
  starved for an extended period.
- **Timing correctness**: If the slab loader falls behind (misses its preload
  window), timers that should have been loaded into the in-memory scheduler
  will not fire on time.
- **Detection**: The heartbeat system will detect the stall but only passively;
  it cannot break the situation.
- **Trigger conditions**: The hazard activates only when `unschedule_all` is
  called while the `slab_loader` is also trying to run — which is the common
  case during rebalancing or bulk cancellation.
- **Fix direction**: The `scheduled_times` generator should collect all times
  first (without holding the lock), then release the lock before the caller
  begins issuing concurrent `unschedule` operations. Alternatively, the read
  guard could be scoped to a single item fetch at a time and dropped between
  yields. Another option is to use a `Mutex`-per-slab design rather than a
  single segment-wide lock.
