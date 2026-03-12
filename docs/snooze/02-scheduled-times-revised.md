# Snooze Analysis (Revised): `scheduled_times` + `unschedule_all` Futurelock

## Verdict: CONFIRMED — genuine futurelock per the precise snooze article definition

The prior analysis (`02-scheduled-times.md`) correctly identified a write-lock
starvation hazard but hedged on whether a true circular deadlock existed.
This revision applies the precise three-condition test from jacko.io/snooze.html
and confirms that all three conditions are met: this is a genuine futurelock.

---

## Precise Three-Condition Test

From the snooze article, a futurelock requires ALL of the following:

1. **Future A holds an async lock and is snoozed** — not being polled, despite
   being ready to make progress.
2. **Future B is trying to acquire the same lock and cannot proceed.**
3. **Future A won't be polled again until Future B makes progress** — circular
   dependency.

Each condition is examined below with evidence from the source.

---

## Condition 1: The Generator Is Snoozed Holding the Read Lock

`scheduled_times` (`src/timers/manager.rs` lines 167-210) returns a
`try_stream!` generator. The generator body is:

```rust
try_stream! {
    let state = slab_lock.trigger_lock().await;  // (A) read guard acquired

    let stream = state
        .store
        .get_key_times(&segment_id, timer_type, &key)
        .map_err(TimerManagerError::Store);

    pin_mut!(stream);
    while let Some(time) = cooperative(stream.try_next()).await? { // awaits with guard
        let active_triggers = state.scheduler.active_triggers();
        let is_scheduled = active_triggers.is_scheduled(&key, time, timer_type).await; // awaits
        let not_active = !active_triggers.contains(&key, time, timer_type).await;       // awaits

        if is_scheduled || not_active {
            yield time;   // (E) generator suspends here
        }
    }
    // (F) guard dropped at end of generator body
}
```

The `SlabLockTriggerGuard` (a `RwLockReadGuard`) named `state` is a local
variable in the generator body. It is created at `(A)` and lives until `(F)`.
The generator suspends at `yield time` (point `(E)`). While suspended, the
generator is not being polled. The `cooperative()` wrapping (`tokio::task::coop`)
integrates with tokio's cooperative scheduling budget — it may yield to the
executor — but it does NOT drop the read guard. The guard is live for the
entire generator body regardless of how many times the generator suspends.

**`try_buffer_unordered` snooses the generator when its buffer is full.** In
`unschedule_all` (`src/timers/manager.rs` lines 410-425`):

```rust
self.scheduled_times(key, timer_type)           // source: generator
    .map_ok(|time| {
        self.unschedule(key, time, timer_type)
            .instrument(span.clone())
    })
    .try_buffer_unordered(DELETE_CONCURRENCY)   // buffer capacity = 16
    .try_collect::<()>()
    .await
```

`DELETE_CONCURRENCY = 16` (`src/timers/mod.rs` line 199). Once
`try_buffer_unordered` holds 16 in-flight futures in its internal buffer, it
stops polling the source stream (the generator). The generator is snoozed at
its `yield` point, holding the read guard, despite being ready to yield the
next item immediately.

**Condition 1 is satisfied.**

---

## Condition 2: Future B Cannot Acquire the Lock

Each `unschedule` future spawned by `try_buffer_unordered` calls
`trigger_lock().await` to acquire a NEW read lock (`src/timers/manager.rs`
line 348):

```rust
pub async fn unschedule(
    &self,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
) -> Result<(), TimerManagerError<T::Error>> {
    let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
    let slab_id = slab.id();
    let state = self.0.state.trigger_lock().await;  // <-- line 348: acquires read lock
    ...
}
```

`trigger_lock()` calls `self.inner.read().await` (`src/timers/slab_lock.rs`
line 98), which is `tokio::sync::RwLock::read()`.

The `slab_loader` background task periodically acquires the write lock. In
`load_slabs` (`src/timers/loader.rs` line 295):

```rust
let mut state = state.slab_lock().await;
```

and in `remove_completed_slabs` (`src/timers/loader.rs` line 339):

```rust
let state = state.slab_lock().await;
```

Both call `self.inner.write().await` (`src/timers/slab_lock.rs` line 106).

`tokio::sync::RwLock` is documented as **write-preferring**: once a write
request is queued, no new read acquisitions can proceed until the write lock
is granted and released. Once `slab_loader` calls `slab_lock().await`, all
subsequent `trigger_lock().await` calls from the 16 `unschedule` futures
block.

**Condition 2 is satisfied.**

---

## Condition 3: Future A Won't Be Polled Until Future B Makes Progress

The circular dependency:

- The generator (Future A) is snoozed at `yield` because `try_buffer_unordered`
  has 16 futures in its buffer and won't poll the generator until a slot frees.
- A slot frees only when one of the 16 `unschedule` futures completes.
- Each `unschedule` future is blocked at `trigger_lock().await` (Future B).
- `trigger_lock().await` unblocks only when the write lock request is withdrawn
  or granted.
- The write lock request is blocked by the read guard held by the generator
  (Future A).
- The generator cannot drop its read guard until its body completes — i.e.,
  until it is polled to exhaustion.
- The generator won't be polled until `try_buffer_unordered` frees a slot.
- `try_buffer_unordered` can't free a slot until an `unschedule` future
  completes.
- An `unschedule` future can't complete because it is blocked on
  `trigger_lock().await`.
- `trigger_lock().await` is blocked because the writer is queued.
- The writer is blocked by the generator's read guard.

**This is circular. Condition 3 is satisfied.**

---

## Complete Circular Dependency

```
Generator (scheduled_times)
  holds: SlabLockTriggerGuard (RwLockReadGuard)
  state: snoozed at `yield`, not being polled
  waiting on: try_buffer_unordered to free a buffer slot
         |
         | try_buffer_unordered has 16 futures, all blocked
         v
16x unschedule() futures
  waiting on: trigger_lock().await (new read acquisition)
  blocked by: tokio write-preferring RwLock write request queued
         |
         | write request queued, blocks new readers
         v
slab_loader background task
  waiting on: slab_lock().await (write lock)
  blocked by: RwLockReadGuard held by generator
         |
         | generator holds the read guard
         v
    [cycles back to Generator]
```

The system is permanently stuck until an external event resolves it — there is
no internal mechanism for any party to make progress.

---

## What `cooperative()` Does — And Why It Doesn't Break the Loop

`cooperative()` is `tokio::task::coop::cooperative` (imported at
`src/timers/manager.rs` line 36). It wraps a future and integrates with
tokio's cooperative task scheduling budget. When the budget is exhausted,
`cooperative(f).await` yields to the executor (i.e., returns `Poll::Pending`
from the current poll context), allowing other tasks to run.

Critically:

- `cooperative()` does NOT drop any guards or locals held in the enclosing
  generator body.
- The `SlabLockTriggerGuard` `state` is a local in the generator body, not
  inside the `cooperative()` call. It stays live regardless of how many times
  `cooperative()` yields.
- Yielding to the executor via `cooperative()` is not the same as being
  "snoozed": the scheduler immediately reschedules the task after other tasks
  run. The generator is not permanently blocked by `cooperative()` — it is
  blocked by `try_buffer_unordered` not polling it.

So `cooperative()` does not prevent the futurelock and does not release the
lock. It merely ensures the generator doesn't monopolise a thread; the lock is
held regardless.

---

## Is `DELETE_CONCURRENCY = 16` Sufficient to Fill the Buffer?

Yes. `try_buffer_unordered(DELETE_CONCURRENCY)` buffers up to 16 concurrent
futures. The generator yields one item per timer in the store for the given key.
If a key has 17 or more scheduled timers, `try_buffer_unordered` will:

1. Pull 16 items from the generator, launching 16 `unschedule` futures.
2. Stop polling the generator (buffer full) — generator is snoozed.
3. Wait for one of the 16 futures to complete before pulling the 17th item.

If the writer queues between step 2 and step 3, all 16 futures stall on
`trigger_lock()`. The buffer stays full. The generator stays snoozed.

For the futurelock to manifest, the key only needs 17+ timers and `slab_loader`
must race to acquire the write lock during the window. Both conditions are
realistic in production: bulk cancellation during rebalancing is the most likely
trigger.

---

## Does the Futurelock Self-Resolve or Is It Permanent?

It is **permanent** for the duration of the `unschedule_all` call:

- `slab_loader` calls `slab_lock().await` which waits indefinitely
  (`tokio::sync::RwLock::write()` does not time out).
- There is no `select!` in `load_slabs` or `remove_completed_slabs` that would
  cancel the write lock request. The `slab_loader_iteration` function's
  `select!` with `shutdown_rx.wait_for(|&v| v)` runs at the top-level loop but
  the write-lock acquisition inside `load_slabs` / `remove_completed_slabs`
  has no such escape hatch.
- The `unschedule` futures wait indefinitely on `trigger_lock()`. There is no
  timeout on `trigger_lock().await`.

The only resolution path is: the tokio runtime drops the `unschedule_all`
future from outside (e.g., via `select!` or cancellation by the caller). In
normal operation this does not happen — `unschedule_all` is `await`-ed to
completion.

**Summary: permanent futurelock until `unschedule_all` is externally
cancelled.**

---

## Does the Heartbeat Detect This?

**Partially — after the fact, with no ability to break the lock.**

`slab_loader` calls `heartbeat.beat()` at the top of `slab_loader_iteration`
(`src/timers/loader.rs` line 172). This happens before entering `load_slabs`
or `remove_completed_slabs`. Once the loader blocks inside `slab_lock().await`,
`heartbeat.beat()` is not called again.

`HeartbeatRegistry::any_stalled()` returns `true` after the stall threshold
elapses (`src/heartbeat.rs` line 92). The threshold is configured by the
consumer — typically on the order of seconds to tens of seconds.

`any_stalled()` is a passive check. It must be polled by an external mechanism
(health probe, monitoring loop). It does not:

- Interrupt the blocked future.
- Cancel `unschedule_all` or `slab_lock().await`.
- Alert any task that the deadlock has occurred.

Inside `slab_loader_iteration`, the `select!` block includes
`() = heartbeat.next() => {}` during the *wait* phase
(`src/timers/loader.rs` lines 237-240). This allows the heartbeat timer to
fire and interrupt the sleep. However, the heartbeat interlock only runs
during the idle sleep between load cycles — not during `load_slabs` or
`remove_completed_slabs`. Once the task is blocked on `slab_lock().await`,
no other branches of any `select!` are executing.

**Conclusion:** The heartbeat detects the stall and logs an error after the
configured threshold, but cannot break the futurelock.

---

## Test

The test below is located in `src/timers/manager.rs` under the `tests` module.
It uses `SlabLock<u32>` directly (no store required) and a `Notify` gate to
deterministically fill the `try_buffer_unordered` buffer before the writer
queues, confirming the full circular dependency. It must time out.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn futurelock_scheduled_times_unschedule_all() -> Result<()> {
    use async_stream::try_stream;
    use futures::{TryStreamExt, pin_mut};
    use std::convert::Infallible;
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tokio::time::{Duration, sleep, timeout};

    let lock: SlabLock<u32> = SlabLock::new(0);
    let lock_gen = lock.clone();
    let lock_unschedule = lock.clone();
    let lock_writer = lock.clone();

    let unschedule_gate = Arc::new(Notify::new());
    let gate_clone = unschedule_gate.clone();

    let reader_held = Arc::new(Notify::new());
    let reader_held_clone = reader_held.clone();

    let writer_queued = Arc::new(Notify::new());
    let writer_queued_clone = writer_queued.clone();

    // Generator: acquires read guard, signals, yields DELETE_CONCURRENCY items.
    let item_count = DELETE_CONCURRENCY;
    let gen_stream = try_stream! {
        let guard = lock_gen.trigger_lock().await;
        reader_held_clone.notify_one();

        for i in 0u32..(item_count as u32) {
            let _ = &guard;   // keep guard live across yield
            yield i;
        }
        // guard dropped here
    };

    // Slow-unschedule: waits for gate + writer_queued, then blocks on new read.
    let try_unschedule = move |_item: u32| {
        let gate = gate_clone.clone();
        let lock_u = lock_unschedule.clone();
        let wq = writer_queued_clone.clone();
        async move {
            gate.notified().await;
            wq.notified().await;
            let _guard = lock_u.trigger_lock().await;
            Ok::<(), Infallible>(())
        }
    };

    // Writer: waits for read guard, then queues write.
    let writer_task = tokio::spawn(async move {
        reader_held.notified().await;
        sleep(Duration::from_millis(20)).await;
        writer_queued.notify_waiters();
        let mut guard = lock_writer.slab_lock().await;
        *guard = 999;
    });

    pin_mut!(gen_stream);

    let pipeline = async move {
        gen_stream
            .map_ok(try_unschedule)
            .try_buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await
    };

    let gate_opener = tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        unschedule_gate.notify_waiters();
    });

    // Must time out: the circular dependency prevents all progress.
    let result = timeout(Duration::from_millis(500), pipeline).await;

    gate_opener.abort();
    writer_task.abort();

    assert!(
        result.is_err(),
        "Pipeline should have timed out (futurelock)"
    );

    Ok(())
}
```

**Test result: PASSES** (`test timers::manager::tests::futurelock_scheduled_times_unschedule_all ... ok`).
The `timeout(500ms)` fires every run, confirming the circular dependency is
real and permanent.

---

## Exact File Locations

- Generator: `src/timers/manager.rs`, function `scheduled_times`, lines 167-210
- `unschedule`: `src/timers/manager.rs`, line 348 — `trigger_lock().await`
- `unschedule_all`: `src/timers/manager.rs`, lines 410-425
- `DELETE_CONCURRENCY`: `src/timers/mod.rs`, line 199 — value `16`
- `trigger_lock()`: `src/timers/slab_lock.rs`, line 97 — calls `RwLock::read()`
- `slab_lock()`: `src/timers/slab_lock.rs`, line 105 — calls `RwLock::write()`
- `load_slabs` write acquisition: `src/timers/loader.rs`, line 295
- `remove_completed_slabs` write acquisition: `src/timers/loader.rs`, line 339
- Heartbeat beat: `src/timers/loader.rs`, line 172

---

## Severity Assessment

**High.**

- **Correctness**: Genuine permanent futurelock. The system makes no progress
  while any `unschedule_all` call is in flight and the buffer has filled.
- **Availability**: `slab_loader` cannot preload slabs during the entire
  `unschedule_all`. If the key has many timers (common during bulk
  cancellation), this could last seconds to minutes. Missed preload windows
  cause timer delivery delays or missed fires.
- **Detection**: Heartbeat detects the stall passively after threshold, cannot
  break it.
- **Trigger conditions**: Any key with 17+ timers + `slab_loader` racing during
  the `unschedule_all`. Most dangerous during rebalancing (mass key
  cancellation) when both conditions reliably co-occur.
- **Fix direction**: The `scheduled_times` generator should not hold the read
  guard across yield points. Options:
  1. Collect all times into a `Vec` under the read guard, then drop the guard,
     then yield from the vec without any lock held.
  2. Acquire and release the read guard per item (narrow scope).
  3. Restructure `unschedule_all` to collect times first, then cancel
     concurrently without streaming from `scheduled_times`.
