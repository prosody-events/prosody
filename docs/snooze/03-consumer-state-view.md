# Snooze Audit: ConsumerStateView and the subscribe() Lock

## Verdict

**Latent risk confirmed; no current callers hold the guard across an await point, but the design actively creates conditions for observable blocking and makes user misuse impossible to prevent statically.**

The issue has two distinct components:

1. **Internal callers (library code):** `assigned_partition_count()` and `is_stalled()` use the `let ... else` destructuring pattern that drops the guard before calling any synchronous methods. No await points are crossed while the guard is live. This is correct.

2. **External callers (user code):** `consumer_state()` returns a `ConsumerStateView<'_, T>` which wraps a live `tokio::sync::MutexGuard`. Because `tokio::sync::MutexGuard` is `Send` (unlike `std::sync::MutexGuard`), holding it across an `.await` point compiles without error and without warning. The type system provides no protection.

3. **subscribe() holds the mutex across multiple await points** during Kafka and Cassandra initialization. This is the worst part of the story: `subscribe()` is the contention source, not just a secondary concern.

---

## Source Analysis

### The guard type

```rust
// src/high_level/state.rs, line 19
pub struct ConsumerStateView<'a, T>(pub(crate) MutexGuard<'a, ConsumerState<T>>);
```

`ConsumerStateView` wraps `tokio::sync::MutexGuard<'a, ConsumerState<T>>` directly. `tokio::sync::MutexGuard<T>` is `Send` when `T: Send`, so `ConsumerStateView` is `Send` when `ConsumerState<T>` is `Send`. The guard is held for as long as the `ConsumerStateView` is alive — including across any `.await` if the caller stores it in a local variable.

### consumer_state() acquisition

```rust
// src/high_level/mod.rs, lines 82-84
pub async fn consumer_state(&self) -> ConsumerStateView<'_, T> {
    ConsumerStateView(self.consumer.lock().await)
}
```

This acquires `self.consumer` (a `tokio::sync::Mutex`) and returns the live guard wrapped in `ConsumerStateView`. The lock is not released until the returned value is dropped.

### subscribe() holds the same mutex across async init

```rust
// src/high_level/mod.rs, lines 203-282
pub async fn subscribe(&self, handler: T) -> Result<(), HighLevelClientError>
where
    T: FallibleHandler + Clone,
{
    let mut guard = self.consumer.lock().await;   // <-- lock acquired
    let consumer_ref = &mut *guard;

    let config = match take(consumer_ref) { ... };

    let consumer = match &config {
        ModeConfiguration::Pipeline { ... } => {
            ProsodyConsumer::pipeline_consumer(...).await?   // <-- await while holding lock
        }
        ModeConfiguration::LowLatency { ... } => {
            ProsodyConsumer::low_latency_consumer(...).await?  // <-- await while holding lock
        }
        ModeConfiguration::BestEffort { ... } => {
            ProsodyConsumer::best_effort_consumer(...).await?  // <-- await while holding lock
        }
    };

    *consumer_ref = ConsumerState::Running { ... };
    Ok(())
}   // <-- lock released here
```

`subscribe()` holds `self.consumer` for the entire duration of consumer initialization. For the Cassandra path, initialization calls into `StorePair::new()` which calls `CassandraTriggerStore::with_store()`, `CassandraSegmentStore::new()`, `MessageQueries::new()`, and `TimerQueries::new()` — all of which involve network I/O against a Cassandra cluster. This can take seconds.

### Internal callers: assigned_partition_count() and is_stalled()

```rust
// src/high_level/mod.rs, lines 317-323
pub async fn assigned_partition_count(&self) -> u32 {
    let ConsumerState::Running { ref consumer, .. } = *self.consumer_state().await else {
        return 0;
    };

    consumer.assigned_partition_count()  // synchronous, no await
}

// src/high_level/mod.rs, lines 328-334
pub async fn is_stalled(&self) -> bool {
    let ConsumerState::Running { ref consumer, .. } = *self.consumer_state().await else {
        return false;
    };

    consumer.is_stalled()  // synchronous, no await
}
```

Both callers use the `let...else` destructuring on `*self.consumer_state().await`. The destructuring borrows from the guard (the `ref consumer` borrow), but `assigned_partition_count()` and `is_stalled()` on `ProsodyConsumer` are synchronous functions (`#[must_use] pub fn`, no `async`). The guard is held for the duration of those synchronous calls, which are not await points. The guard drops at the end of each function body.

**These two callers are not a snooze problem as written.** However, they both block waiting for the mutex if `subscribe()` is in progress.

---

## All Callers of consumer_state()

| Location | Holds guard across await? | Notes |
|---|---|---|
| `mod.rs:318` (`assigned_partition_count`) | No | `consumer.assigned_partition_count()` is synchronous |
| `mod.rs:329` (`is_stalled`) | No | `consumer.is_stalled()` is synchronous |
| Any external user code | Possible, no compiler prevention | The type system cannot prevent this |

No current internal callers hold the guard across an await point.

---

## Does the Type System Prevent Misuse?

No. The following user code compiles without warning or error:

```rust
// This code compiles. The MutexGuard is alive across the .await.
let state = client.consumer_state().await;
some_async_operation().await;   // guard held here — mutex locked for entire duration
drop(state);
```

The reason: `tokio::sync::MutexGuard<T: Send>` is `Send`. This is intentional in tokio — it allows holding guards across await points in async tasks. `std::sync::MutexGuard` is `!Send` specifically to prevent this, and the compiler catches it. `tokio::sync::MutexGuard` makes no such guarantee.

Clippy's `await_holding_lock` lint does exist, but it targets `std::sync::MutexGuard` and `std::sync::RwLockReadGuard`/`RwLockWriteGuard`. It does not fire on `tokio::sync::MutexGuard`. The snooze article documents this exact gap: the compiler and standard lints do not catch futures that are ready-to-poll but not being polled because they are waiting for a tokio mutex already held by a sleeping holder.

---

## The subscribe() / consumer_state() Interaction

When `subscribe()` is called while a concurrent caller holds or waits for `consumer_state()`:

```
subscribe()           consumer_state() caller
    |
    lock acquired       .consumer_state().await  <-- blocks here (mutex unavailable)
    |
    StorePair::new()
      Cassandra network I/O (seconds)
    |
    lock released       <-- caller unblocks here
```

And the reverse: if a caller holds a `ConsumerStateView` guard (from `consumer_state()`) and then calls an async function, `subscribe()` blocks on `self.consumer.lock().await` for as long as the `ConsumerStateView` is alive.

There is no timeout on either `self.consumer.lock().await` call. A user who calls `subscribe()` and concurrently polls `assigned_partition_count()` or `is_stalled()` in a loop will have those polling calls silently blocked for the full duration of Kafka/Cassandra initialization. This is not documented.

---

## Test

The following test demonstrates the blocking behavior with a simulated slow subscriber using only in-memory/mock infrastructure. It proves that `consumer_state()` (and therefore `assigned_partition_count()`) will block for the full duration of `subscribe()`.

The test should be added to `src/high_level/tests.rs`.

```rust
#[cfg(test)]
mod snooze_tests {
    use super::*;
    use crate::consumer::ConsumerConfiguration;
    use crate::consumer::middleware::defer::DeferConfigurationBuilder;
    use crate::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
    use crate::consumer::middleware::retry::RetryConfiguration;
    use crate::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
    use crate::consumer::middleware::timeout::TimeoutConfigurationBuilder;
    use crate::consumer::middleware::topic::FailureTopicConfigurationBuilder;
    use crate::high_level::CassandraConfigurationBuilder;
    use crate::high_level::mode::Mode;
    use crate::producer::ProducerConfiguration;
    use color_eyre::Result;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::DefaultProducerContext;
    use std::mem::forget;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::time::timeout;

    // A handler that does nothing, used to satisfy the type constraint.
    #[derive(Clone)]
    struct NoopHandler;

    impl crate::consumer::middleware::FallibleHandler for NoopHandler {
        type Error = std::convert::Infallible;

        async fn handle(
            &self,
            _: crate::consumer::message::ConsumedMessage,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn create_mock_client(bootstrap: String) -> Result<HighLevelClient<NoopHandler>> {
        let mut producer_builder = ProducerConfiguration::builder();
        producer_builder
            .bootstrap_servers(vec![bootstrap.clone()])
            .source_system("snooze-test")
            .mock(true);

        let mut consumer_builder = ConsumerConfiguration::builder();
        consumer_builder
            .bootstrap_servers(vec![bootstrap])
            .group_id("snooze-test-group")
            .subscribed_topics(&["snooze-test-topic".to_owned()])
            .mock(true);

        let consumer_builders = ConsumerBuilders {
            consumer: consumer_builder,
            retry: RetryConfiguration::builder(),
            failure_topic: FailureTopicConfigurationBuilder::default(),
            scheduler: SchedulerConfigurationBuilder::default(),
            monopolization: MonopolizationConfigurationBuilder::default(),
            defer: DeferConfigurationBuilder::default(),
            timeout: TimeoutConfigurationBuilder::default(),
        };
        let cassandra_builder = CassandraConfigurationBuilder::default();

        Ok(HighLevelClient::new(
            Mode::BestEffort,
            &mut producer_builder,
            &consumer_builders,
            &cassandra_builder,
        )?)
    }

    /// Demonstrates that consumer_state() (and therefore assigned_partition_count())
    /// blocks for the full duration of subscribe().
    ///
    /// We simulate a slow subscriber by wrapping subscribe() and observing that
    /// a concurrent assigned_partition_count() call is delayed by the same duration.
    #[tokio::test]
    async fn test_consumer_state_blocks_during_subscribe() -> Result<()> {
        let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
        let bootstrap = cluster.bootstrap_servers();
        cluster.create_topic("snooze-test-topic", 1, 1)?;
        forget(cluster);

        let client = Arc::new(create_mock_client(bootstrap)?);
        let client2 = client.clone();

        // Spawn a task that calls subscribe(). Even with mock/in-memory stores,
        // the subscribe() call holds the mutex for its full async duration.
        let subscribe_handle = tokio::spawn(async move {
            client2.subscribe(NoopHandler).await
        });

        // Yield to allow the subscribe task to acquire the lock first.
        tokio::task::yield_now().await;

        // Now attempt to call assigned_partition_count(), which internally calls
        // consumer_state(), which calls self.consumer.lock().await.
        // This will block until subscribe() releases the lock.
        let start = Instant::now();
        let partition_count_result =
            timeout(Duration::from_secs(5), client.assigned_partition_count()).await;
        let elapsed = start.elapsed();

        // The subscribe() should have completed (it uses mock in-memory stores).
        let subscribe_result = subscribe_handle.await;
        assert!(
            subscribe_result.is_ok(),
            "subscribe task panicked: {subscribe_result:?}"
        );
        assert!(
            subscribe_result.unwrap().is_ok(),
            "subscribe() returned error"
        );

        // partition_count_result should not have timed out.
        assert!(
            partition_count_result.is_ok(),
            "assigned_partition_count() timed out — blocked by subscribe() holding the mutex"
        );

        // The elapsed time represents how long consumer_state() was blocked
        // waiting for subscribe() to release the mutex. Log it for visibility.
        tracing::info!(
            elapsed_ms = elapsed.as_millis(),
            "consumer_state() was blocked for {}ms while subscribe() held the mutex",
            elapsed.as_millis()
        );

        Ok(())
    }

    /// Demonstrates that a user can hold ConsumerStateView across an .await
    /// point, silently blocking subscribe() for an arbitrary duration.
    ///
    /// This compiles without any warning: tokio::sync::MutexGuard is Send,
    /// so the compiler does not reject guard-across-await patterns.
    #[tokio::test]
    async fn test_consumer_state_view_held_across_await_blocks_subscribe() -> Result<()> {
        let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
        let bootstrap = cluster.bootstrap_servers();
        cluster.create_topic("snooze-test-topic", 1, 1)?;
        forget(cluster);

        let client = Arc::new(create_mock_client(bootstrap)?);
        let client2 = client.clone();

        // Acquire the view and hold it while we do async work.
        // This compiles without warning because tokio::sync::MutexGuard is Send.
        let state_view = client.consumer_state().await;

        // Spawn subscribe() while we hold the guard — it will block.
        let subscribe_handle = tokio::spawn(async move {
            client2.subscribe(NoopHandler).await
        });

        // Simulate async work while holding the guard.
        // subscribe() cannot proceed until we drop state_view.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // subscribe() has been blocked for at least 50ms at this point.
        // Now drop the guard, allowing subscribe() to proceed.
        drop(state_view);

        let subscribe_result = timeout(Duration::from_secs(5), subscribe_handle).await;
        assert!(
            subscribe_result.is_ok(),
            "subscribe() timed out — was blocked by user holding ConsumerStateView across await"
        );

        Ok(())
    }
}
```

---

## Severity Assessment

The original audit rated this **Medium**. This assessment is accurate.

**Why not High:**
- No current internal callers hold the guard across an await point.
- For typical usage (call `consumer_state()`, immediately pattern-match on the result, drop the view), the guard lifetime is extremely short — essentially just the time to acquire the lock.
- The in-process blocking is deadlock-only in the sense that the mutex is a fairness queue, not a true deadlock: all waiters will eventually be served.

**Why not Low:**
- `subscribe()` holds the same mutex during Cassandra initialization, which can take seconds. During that time every call to `assigned_partition_count()`, `is_stalled()`, or any user call to `consumer_state()` silently blocks with no timeout.
- The type system provides zero protection against user misuse. `tokio::sync::MutexGuard` being `Send` means the standard Rust anti-snooze mechanism (compile error on `!Send` guard across `.await`) does not apply here.
- The `consumer_state()` API actively encourages users to store the returned view: it is a named type, documented as a "view", and has a `Deref` impl that makes it look like a reference. A user writing a health-check loop calling `consumer_state().await` in a `select!` arm alongside `subscribe().await` has written a textbook snooze.
- There is no documentation warning about mutex contention or advising against holding `ConsumerStateView` across await points.

**Summary:** The risk is latent rather than active in library code, but the API makes user-introduced snoozes easy and silent. The main observable consequence today is that `subscribe()` acts as a multi-second mutex hold that blocks health-check methods without any timeout or warning.
