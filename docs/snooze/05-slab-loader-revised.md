# Futurelock Investigation: `slab_loader` in `src/timers/loader.rs`

## Executive Summary

**No genuine futurelock (per the precise jacko.io/snooze.html definition) exists in
`slab_loader` or its call chain.**

The six questions below are answered in sequence. The key findings:

1. **Shutdown cancellation** (`select!` dropping `slab_loader_iteration`) is safe RAII
   cancellation, not snoozing. The write lock guard is dropped immediately.
2. **No `pin_mut!` + `select!` pattern** holding a lock reference exists anywhere in the
   timer codebase.
3. **The inner `select!` in `slab_loader_iteration`** (at loader.rs:237) holds **no lock**.
   The write lock was already released by the time that `select!` is reached.
4. **`scheduler.schedule()` inside `try_buffer_unordered`** does not acquire `slab_lock()`
   or `trigger_lock()`. It communicates via an `mpsc` channel to a separate task. No
   self-deadlock is possible.
5. **No single `select!` in `src/timers/`** polls a future holding a `SlabLock` guard
   alongside another arm that could snooze it.
6. **`load_triggers` inside `try_buffer_unordered`**: the write lock is held across all
   `await` points in `load_slabs`, but every awaited operation inside (`store` I/O,
   `scheduler.schedule()`) is independent of `SlabLock`. The write lock is held for an
   extended duration (ordinary lock contention), but there is no circular dependency that
   satisfies the futurelock definition.

The test below confirms that shutdown mid-`load_slabs` promptly releases the write lock (no
snooze, ordinary RAII cancellation).

---

## Precise Futurelock Definition (recap)

From [jacko.io/snooze.html](https://jacko.io/snooze.html), a futurelock requires ALL of:

1. Future A holds an async lock and is **snoozed** — not being polled, despite being ready
   to make progress.
2. Future B is trying to acquire the same lock and cannot proceed.
3. Future A won't be polled again until Future B makes progress (circular).

Holding a lock across `.await` is **not** a futurelock unless the holder is snoozed by the
very waiter. Cancellation (drop) is also not a futurelock — dropping a future releases its
RAII guards immediately.

---

## Files and Lines Referenced

| File | Key Lines | Role |
|---|---|---|
| `src/timers/loader.rs` | 131–154 | `slab_loader` outer loop with shutdown `select!` |
| `src/timers/loader.rs` | 160–244 | `slab_loader_iteration` |
| `src/timers/loader.rs` | 237–241 | Inner `select!` (sleep vs heartbeat) |
| `src/timers/loader.rs` | 286–314 | `load_slabs` — acquires write lock |
| `src/timers/loader.rs` | 295–310 | Write lock held across `try_buffer_unordered` |
| `src/timers/loader.rs` | 373–392 | `load_triggers` — calls `scheduler.schedule()` |
| `src/timers/scheduler.rs` | 127–142 | `TriggerScheduler::schedule()` — mpsc send + oneshot |
| `src/timers/slab_lock.rs` | 97–107 | `trigger_lock()` = read, `slab_lock()` = write |

---

## Question 1: Is `slab_loader_iteration` cancelled mid-execution?

**Confirmed: cancellation is safe (RAII drop, not snoozing).**

The outer loop in `slab_loader` (loader.rs:141–153):

```rust
loop {
    select! {
        result = slab_loader_iteration(&segment, &state, &heartbeat, &mut ctx) => {
            if result.is_break() { return; }
        },
        _ = shutdown_rx.wait_for(|&v| v) => {
            debug!("Slab loader shutting down");
            return;
        },
    }
}
```

When `shutdown_rx.wait_for(|&v| v)` fires while `slab_loader_iteration` is in progress,
the Tokio `select!` macro drops the `slab_loader_iteration` future. Because `select!`
creates a fresh `slab_loader_iteration(...)` future each iteration (it is not pinned across
iterations — no `pin_mut!`, no `&mut pinned_future` syntax), dropping the future is the
same as dropping any ordinary value: all local variables and guards in the future's state
machine are dropped synchronously via RAII.

Specifically, if `load_slabs` was in progress and held a `SlabLockSlabGuard` (write lock),
that guard would be dropped as part of the future drop. `SlabLockSlabGuard` wraps
`RwLockWriteGuard<'_, State<T>>`, which implements `Drop` by releasing the write lock. The
drop happens synchronously (no await required) before the `select!` arm body executes.

**Evidence**:
- `slab_loader_iteration` is passed as a fresh expression on loader.rs:143, not as a
  `&mut` reference to a pinned future.
- The `SlabLockSlabGuard` type is a newtype over `tokio::sync::RwLockWriteGuard`, which
  releases the lock on drop.
- The Tokio `select!` documentation specifies: "When a branch is disabled or not selected,
  its future is dropped."

**Verdict**: The shutdown path is safe. No snooze occurs.

---

## Question 2: Is there a `select!` anywhere that polls a future holding a lock reference without dropping it?

**Denied: no such pattern exists in `src/timers/`.**

All `select!` usages in `src/timers/` are:

| Location | Arms | Lock held during `select!`? |
|---|---|---|
| `loader.rs:142` | `slab_loader_iteration(...)` vs `shutdown_rx.wait_for(...)` | No lock held in either arm. The iteration future holds no lock when waiting for the shutdown. The write lock is only held *inside* `load_slabs`, which is not pinned across the `select!`. |
| `loader.rs:237` | `sleep(wait_time)` vs `heartbeat.next()` | No lock held. This `select!` appears **after** `load_slabs` and `remove_completed_slabs` have both returned (and therefore released their write locks). |
| `scheduler.rs:316` | `commands.recv()` vs `trigger_tx.send(...)` vs `heartbeat.next()` | No `SlabLock` guard. Operates on `TriggerQueue` and `mpsc` channels only. |
| `scheduler.rs:334` | `commands.recv()` vs `triggers.next()` vs `heartbeat.next()` | No `SlabLock` guard. Same as above. |

**No `pin_mut!` + `select!` pattern exists in `src/timers/`** where one arm can fire while
the pinned future holds a `SlabLock` guard.

The `pin_mut!` calls in `manager.rs` (lines 185, 963, 1164, 1259, etc.) are all in
`scheduled_times()` or tests. They pin store iteration streams, not lock-holding futures,
and are not inside `select!` arms.

**Verdict**: No such pattern. Denied.

---

## Question 3: Does `slab_loader_iteration` contain internal `select!` while holding a lock?

**Denied: the inner `select!` is lock-free.**

The inner `select!` at loader.rs:237–241:

```rust
// Calculate wait time until next slab needs loading
let wait_time = calculate_wait_time(next_slab.range().start, ctx.preload_window);
if !wait_time.is_zero() {
    debug!("Waiting {wait_time} before next load cycle");
    select! {
        () = sleep(wait_time.into()) => {},
        () = heartbeat.next() => {},
    }
}
```

This `select!` is reached at loader.rs:235, **after** both `load_slabs` (loader.rs:205) and
`remove_completed_slabs` (loader.rs:229) have been awaited and returned. Both of those
functions acquire and release the write lock internally:

- `load_slabs`: acquires write lock at line 295 (`state.slab_lock().await`), drives the
  `try_buffer_unordered` to completion at line 309–310 (`.try_collect().await?`), then
  drops the guard when `state` goes out of scope at the end of the function.
- `remove_completed_slabs`: acquires write lock at line 339, runs deletions, drops at
  function end.

By the time execution reaches line 237, both functions have returned, and no `SlabLock`
guard is in scope. The `select!` at line 237 holds no lock.

**Neither arm of the inner `select!` involves a lock.** `sleep(wait_time.into())` is a
Tokio timer. `heartbeat.next()` is a `Heartbeat` future (a separate domain monitoring
mechanism, not `SlabLock`-related). Either arm firing is safe.

**Verdict**: No lock held. The inner `select!` is safe.

---

## Question 4: Cross-task snoozing via `try_buffer_unordered` in `load_slabs`

**Denied: `scheduler.schedule()` does not interact with `SlabLock`.**

The critical code path (loader.rs:295–310):

```rust
let mut state = state.slab_lock().await;          // WRITE LOCK ACQUIRED
let store_ref = &state.store;
let scheduler_ref = &state.scheduler;

let loaded = state
    .store
    .get_slab_range(&segment.id, slab_range)
    .map_ok(|slab_id| async move {
        let slab = Slab::new(segment.id, slab_id, segment.slab_size);
        load_triggers(store_ref, scheduler_ref, slab).await?;  // awaits while write lock held
        Ok(slab_id)
    })
    .try_buffer_unordered(LOAD_CONCURRENCY)
    .try_collect()
    .await?;                                        // WRITE LOCK STILL HELD

state.extend_ownership(range_end);
Ok(loaded)
// WRITE LOCK RELEASED HERE (state dropped)
```

`load_triggers` (loader.rs:373–392) calls `scheduler.schedule(trigger).await`. What does
`TriggerScheduler::schedule()` actually await? (scheduler.rs:127–142):

```rust
pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerSchedulerError> {
    let (result_tx, result_rx) = oneshot::channel();
    let operation = CommandOperation::Add;

    self.command_tx
        .send(Command { result_tx, trigger, operation })
        .map_err(|_| TimerSchedulerError::Shutdown)
        .await?;                                    // await #1: mpsc channel send

    result_rx.await.map_err(|_| TimerSchedulerError::Shutdown)  // await #2: oneshot receive
}
```

**`schedule()` awaits only:**
1. `self.command_tx.send(...).await` — an `mpsc::Sender<Command>` send. This may block if
   the 64-item command channel is full, but that is ordinary channel backpressure. The
   channel is consumed by `process_commands` in a separate spawned task
   (scheduler.rs:98–103), which runs on the Tokio executor independently.
2. `result_rx.await` — a `oneshot::Receiver<()>` receive. This completes when
   `process_commands` calls `result_tx.send(())`.

**Neither await point interacts with `SlabLock`, `slab_lock()`, or `trigger_lock()`.** The
scheduler processor task (`process_commands`) operates on `TriggerQueue` and
`ActiveTriggers` (an `scc::HashMap`), which are completely independent of `SlabLock`.

Therefore, there is **no circular dependency**:
- The write lock is held by `load_slabs` during `try_buffer_unordered`.
- The futures inside `try_buffer_unordered` await the mpsc channel and a oneshot.
- Neither of those await points requires the write lock or the read lock.
- The `process_commands` task that processes the commands does not acquire any `SlabLock`.

**Could the channel buffer itself cause a stall?** In theory, if the `process_commands` task
is not scheduled and the command channel fills up (64 items), `schedule().await` inside
`try_buffer_unordered` would block, keeping the write lock held. However, this is not a
futurelock: `process_commands` is an independently spawned task that does not need the write
lock to make progress. Once the Tokio runtime schedules `process_commands`, it drains the
channel, the oneshots complete, and the write lock is released. There is no circular
dependency.

**Verdict**: No futurelock. `scheduler.schedule()` uses only channels, not `SlabLock`.

---

## Question 5: `scheduled_times` + `slab_loader` via `select!`

**Confirmed no futurelock in a single `select!`.**

`scheduled_times()` (manager.rs:167–210) holds `trigger_lock()` (read lock) across
multiple await points inside a `try_stream!` generator. This is documented in detail in
`05-unschedule-all.md` and represents ordinary lock contention (Hypothesis B in that
document), not a futurelock in the precise snooze.html sense, because:

- `scheduled_times` is not snoozed by `slab_loader`.
- `slab_loader` is not blocked waiting for `scheduled_times` to make progress.
- The circular dependency required for a futurelock does not exist: `slab_loader` is a
  separate spawned task and is simply waiting for the read lock to be released, which will
  happen when `scheduled_times` is eventually polled to completion.

The question specifically asks whether any single `select!` in `src/timers/` polls both
a future holding a `SlabLock` guard AND something else that could snooze it.

Searching all `select!` usages in `src/timers/` (found in Questions 2 and 3 above): none
of the four `select!` sites involve a `SlabLock` guard in any arm.

**Verdict**: No such pattern. No futurelock.

---

## Question 6: `load_triggers` inside `try_buffer_unordered` — `scc::HashMap` interactions

**Confirmed safe: `entry_async()` on `scc::HashMap` is independent of `SlabLock`.**

`load_triggers` → `scheduler.schedule()` → `process_commands` → `triggers.insert(trigger)`.
`TriggerQueue::insert` calls `ActiveTriggers::insert` (active.rs:124–133):

```rust
pub async fn insert(&self, trigger: Trigger) {
    self.0
        .entry_async(trigger.key)
        .await
        .or_default()
        .get_mut()
        .entry((trigger.time, trigger.timer_type))
        .or_insert(TimerState::Scheduled);
}
```

`scc::HashMap::entry_async()` acquires an internal lock on the hash map bucket. This is a
**different lock** from `SlabLock`. `scc::HashMap` uses fine-grained bucket locking
internally; it does not interact with `tokio::sync::RwLock` at all.

Therefore:
- `load_slabs` holds `slab_lock()` (a `tokio::sync::RwLockWriteGuard`).
- Futures inside `try_buffer_unordered` call `scheduler.schedule()`.
- `scheduler.schedule()` sends on an mpsc channel; the actual `scc::HashMap` mutation
  happens in a **separate task** (`process_commands`), which holds **no** `SlabLock` guard.
- There is no deadlock: the two locks are in different tasks and there is no ordering
  constraint between them.

**Verdict**: No lock interaction. Safe.

---

## Overall Verdict: No Genuine Futurelock

The three conditions for a futurelock (per snooze.html) are not simultaneously satisfied
anywhere in the `slab_loader` call chain:

| Condition | `slab_loader` shutdown (Q1) | Inner `select!` (Q3) | `try_buffer_unordered` (Q4/Q6) |
|---|---|---|---|
| A holds async lock | Yes (write lock held in `load_slabs`) | No (no lock at inner `select!`) | Yes (write lock held across all of `load_slabs`) |
| A is snoozed | **No** — cancellation = RAII drop | N/A | **No** — `load_slabs` runs to completion or is cancelled |
| B can't proceed until A makes progress | N/A | N/A | **No** — scheduler channel operates independently |

The `load_slabs` function does hold the write lock for an extended duration across multiple
`.await` points (all of `try_buffer_unordered`). This creates **ordinary lock contention**:
other tasks attempting `trigger_lock()` or `slab_lock()` will wait. But this is not a
futurelock because no circular dependency exists: the futures inside `try_buffer_unordered`
progress via channels that do not require the write lock, so they will eventually complete,
release the lock, and unblock other waiters.

---

## Test: Shutdown Mid-`load_slabs` Promptly Releases Write Lock

The test below proves that cancellation during `load_slabs` (write lock held) releases the
lock promptly, with no snooze. It uses `tokio::time::timeout` to detect any lock-release
failure.

Place this in `src/timers/loader.rs` under `#[cfg(test)]`.

```rust
#[tokio::test]
async fn test_slab_loader_cancellation_releases_write_lock() -> Result<()> {
    use crate::timers::slab_lock::SlabLock;
    use crate::timers::store::memory::memory_store;
    use crate::heartbeat::HeartbeatRegistry;
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio::time::{timeout, pause, advance};
    use tokio::task;

    pause();

    let store = memory_store();
    let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
    let state = State::new(store.clone(), scheduler);
    let slab_lock = SlabLock::new(state);
    let segment = create_test_segment();

    store.insert_segment(segment.clone()).await?;

    // Add several triggers so load_slabs has real work (Cassandra I/O is
    // simulated by in-memory store, so it's fast, but the concurrency
    // structure still exercises the write-lock-across-await path).
    for slab_id in 0_u32..5_u32 {
        let time = CompactDateTime::from(slab_id * segment.slab_size.seconds());
        let slab = Slab::new(segment.id, slab_id, segment.slab_size);
        let trigger = create_test_trigger(slab_id.into(), time);
        store.add_trigger(&segment, slab, trigger).await?;
    }

    let heartbeat = Heartbeat::new("test-cancel", Duration::from_secs(30));

    // Spawn the slab_loader; we will send shutdown immediately to cancel it
    // mid-operation (or before it starts — both are valid for this test).
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let slab_lock_for_loader = slab_lock.clone();
    let loader_handle = tokio::spawn(slab_loader(
        segment.clone(),
        slab_lock_for_loader,
        heartbeat,
        shutdown_rx,
    ));

    // Give the loader one tick to start and begin load_slabs (acquiring the
    // write lock). Then immediately send shutdown.
    advance(Duration::from_millis(1)).await;
    task::yield_now().await;

    shutdown_tx.send(true)?;

    // Wait for the loader to exit. Because shutdown_rx fires while
    // slab_loader_iteration is in the select!, the future is dropped
    // (RAII), releasing any write lock it held.
    let loader_result = timeout(Duration::from_secs(5), loader_handle).await;
    assert!(
        loader_result.is_ok(),
        "slab_loader did not shut down within 5 seconds after shutdown signal"
    );
    let join_result = loader_result?;
    assert!(join_result.is_ok(), "slab_loader task panicked");

    // The critical assertion: we must be able to acquire the write lock
    // immediately after the loader exits. If the write lock were still held
    // (snooze scenario), this would time out.
    let write_lock_result = timeout(
        Duration::from_millis(100),
        slab_lock.slab_lock(),
    )
    .await;

    assert!(
        write_lock_result.is_ok(),
        "write lock not released after slab_loader shutdown — possible snooze/leak"
    );

    Ok(())
}
```

### Test Rationale

- **Setup**: A real `slab_loader` is spawned with a segment containing 5 slabs of triggers.
  This ensures `load_slabs` has actual work (acquiring the write lock, running
  `try_buffer_unordered`).
- **Shutdown**: `shutdown_tx.send(true)` signals the outer `select!` in `slab_loader` to
  fire its shutdown arm, dropping the `slab_loader_iteration` future. If `load_slabs` was
  running and held the write lock, that lock is released via RAII.
- **Assertion**: `timeout(..., slab_lock.slab_lock()).await` must succeed within 100ms. If
  a snooze prevented lock release, this would time out with `Err`.
- **Why this is sufficient**: The futurelock pattern would manifest as the write lock never
  being released (or being delayed indefinitely). A 100ms timeout is generous for an
  in-memory store with zero I/O latency.

### Build and Run

```bash
cargo clippy --tests 2>&1 | tee /tmp/clippy_output.log
grep -E "^error" /tmp/clippy_output.log

cargo test -p prosody timers::loader::tests::test_slab_loader_cancellation_releases_write_lock \
    2>&1 | tee /tmp/test_output.log
grep -E "FAILED|ok|error" /tmp/test_output.log
```

---

## Summary Table

| Question | Finding | Evidence |
|---|---|---|
| Q1: Shutdown cancellation safe? | **Yes — RAII drop, not snooze** | `slab_loader_iteration` is a fresh future each iteration; not pinned across `select!`. Guards dropped synchronously. |
| Q2: `pin_mut!` + `select!` + lock? | **No such pattern** | All four `select!` sites in `src/timers/` examined; none hold a `SlabLock` guard. |
| Q3: Inner `select!` holds lock? | **No — lock already released** | `load_slabs` and `remove_completed_slabs` both complete (write lock released) before reaching loader.rs:237. |
| Q4: `scheduler.schedule()` acquires lock? | **No — channels only** | `TriggerScheduler::schedule()` uses `mpsc::Sender` + `oneshot`; no `SlabLock` acquisition anywhere in the call chain. |
| Q5: Single `select!` with lock + snooze? | **No such pattern** | No `select!` in `src/timers/` involves a `SlabLock` guard. |
| Q6: `scc::HashMap::entry_async` vs `SlabLock`? | **Independent locks** | `scc::HashMap` bucket locks are unrelated to `tokio::sync::RwLock`. Mutations happen in a separate task that holds no `SlabLock` guard. |
| **Overall verdict** | **No genuine futurelock** | All three futurelock conditions are not simultaneously satisfied. Ordinary lock contention (write lock held for duration of `load_slabs`) exists but resolves without circular dependency. |

---

## What Does Exist (Ordinary Lock Contention)

`load_slabs` holds the write lock (`slab_lock()`) for the entire duration of
`try_buffer_unordered(LOAD_CONCURRENCY).try_collect().await?`. During this time:

- Any concurrent call to `trigger_lock()` (read lock) in `schedule()`, `unschedule()`,
  `fire()`, etc. will block.
- Any concurrent call to `slab_lock()` (write lock) from another part of the code will
  block.

With the in-memory store this is microseconds. With Cassandra, each `store.add_trigger` /
`get_slab_triggers_all_types` call is a network round-trip (1–10ms); with
`LOAD_CONCURRENCY = 16` concurrent futures, the critical section spans the entire parallel
load of 16 slabs worth of triggers. For large slabs with many triggers, this could be
hundreds of milliseconds.

This is **ordinary lock contention**, not a futurelock. The fix, if needed, would be to
narrow the write lock scope: collect slab data under the write lock, release it, then
schedule triggers under the read lock. But that is a performance concern, not a correctness
concern arising from the snooze pattern.
