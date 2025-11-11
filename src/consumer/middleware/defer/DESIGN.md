# Defer Middleware Design Document

## Executive Summary

This document proposes a persistent, per-key retry mechanism (Defer Middleware) for Prosody's **pipeline mode** that
provides long-term retry capabilities with Cassandra-backed persistence. The middleware wraps the inner handler stack,
enabling graceful degradation from immediate retries to persistent deferred retries.

The middleware leverages Prosody's built-in timer system with typed timers (defer vs. business timers) to schedule
retries, eliminating the need for a separate timer manager. Defer timers trigger on_timer() calls that reload deferred
messages from Kafka and retry them through the handler pipeline. The `clear_and_schedule()` context method provides
atomic timer replacement to maintain continuous coverage during retries.

Pipeline mode prioritizes high throughput, durability, and eventual success over latency.

## Context

### Current State

Prosody's current pipeline mode consumer (`src/consumer/mod.rs::pipeline_consumer`):

**Execution order (outermost to innermost):**

1. RetryMiddleware (outermost)
2. MonopolizationMiddleware
3. ShutdownMiddleware
4. SchedulerMiddleware
5. TimeoutMiddleware
6. TelemetryMiddleware
7. Handler (innermost - business logic)

**Limitations:**

- `RetryMiddleware` performs immediate retries with exponential backoff (default: 3 retries, max 5 min delay)
- No persistence: retries lost on partition rebalance or consumer restart
- No deferred retry mechanism for long-term failures
- After max retries, errors are simply logged and offsets are committed (message lost)

### Problem Statement

For pipeline mode, we need:

1. **Persistence**: Survive partition rebalancing and consumer restarts
2. **Adaptive backoff**: Longer delays for persistent failures (hours/days)
3. **Graceful degradation**: Fall back to immediate retries if defer system is unhealthy
4. **Order preservation**: Maintain per-key ordering guarantees
5. **Resource efficiency**: Don't redundantly store message/timer data already in Kafka/timer system
6. **No head-of-line blocking**: Failing keys must not hold up other keys or build up consumer lag

## Goals

### Primary Goals

1. **Persistent deferred retries**: Store failed message offsets in Cassandra for long-term retry scheduling
2. **Per-key isolation**: Track retries independently per key to prevent one failing key from affecting others
3. **Adaptive retry scheduling**: Exponential backoff with increasing delays (seconds → minutes → hours)
4. **Health-based failover**: Disable deferring if failure rate exceeds threshold (default 90% over 5 minutes)
5. **Order preservation**: Process deferred messages in offset order per key
6. **Zero data duplication**: Leverage existing Kafka storage for message payloads

### Non-Goals

- Replace existing `RetryMiddleware` (they work together)
- Provide exactly-once semantics (maintain existing at-least-once)
- Support cross-partition deferred batching
- Guarantee infinite retries (after extended retries, messages may be lost)

## Critical Invariants

The defer system must maintain two essential invariants to prevent message loss:

### Invariant 1: Timer Coverage

**For every key with deferred messages in Cassandra, exactly one defer timer must be scheduled.**

**On first failure (on_message handler):**

- Schedule defer timer using `ctx.schedule_timer()` BEFORE writing to Cassandra
- This creates the initial timer for the key

**On defer timer fires (on_timer handler):**

- Check if more deferred messages exist after processing current one
- **If more messages**: Use `ctx.clear_and_schedule()` to atomically clear current timer and schedule next
- **If no more messages**: Return successfully (timer clears automatically, no new timer scheduled)
- **On processing failure**: Use `ctx.clear_and_schedule()` to atomically clear current timer and schedule retry with
  increased backoff

**Why**: If messages exist without a timer, they will never be retried (abandoned).

**Critical ordering**:

1. First failure: `schedule_timer() → write_to_cassandra()`
2. Subsequent retries: `clear_and_schedule() → update_cassandra()`

The `clear_and_schedule()` method ensures atomic timer transition with no gap in coverage.

### Invariant 2: Cache Consistency

**The cache state must reflect Cassandra state:**

- `NotDeferred` → No messages in Cassandra for this key_id
- `Deferred{retry_count}` → Messages exist in Cassandra with matching retry_count

**Consistency protocol:**

1. Schedule timer (if needed)
2. Write to Cassandra
3. Update cache

If any step fails, error propagates to `RetryMiddleware` which retries the entire operation. This ensures cache and
store remain synchronized.

**Cache miss handling**: Always query Cassandra to determine true state before routing decisions.

## High-Level Design

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   DeferMiddleware                            │
│         (Messages + Defer Timer Handling)                    │
│                                                               │
│  ┌──────────────┐  ┌─────────────┐  ┌────────────────────┐  │
│  │  Cassandra   │  │   Kafka     │  │  EventContext      │  │
│  │   Store      │  │  Loader     │  │  (schedule_timer,  │  │
│  │ (deferred    │  │ (load by    │  │ clear_and_schedule)│  │
│  │  offsets)    │  │  offset)    │  │                    │  │
│  └──────────────┘  └─────────────┘  └────────────────────┘  │
│         │                 │                    │              │
│         └─────────────────┴────────────────────┘              │
│                           │                                   │
│              on_message(): defer on failure                   │
│              on_timer(): retry deferred messages              │
│                           │                                   │
└───────────────────────────┼───────────────────────────────────┘
                            │
                            ▼
               Inner Handler Stack
          (monopolization, shutdown, etc.)
                            │
                            ▼
                    Business Handler

Note: Uses built-in timer system with typed defer timers.
      - on_message(): Catches failures and schedules defer timers
      - on_timer(): Checks timer type; if defer timer, loads & retries message
      - No separate event loop needed - timer system delivers via on_timer()
```

### Middleware Stack

#### Current Pipeline Consumer

From `src/consumer/mod.rs::pipeline_consumer` (lines 823-849):

```rust
// Code
let provider = common_middleware
.layer(monopolization_middleware)
.layer(retry_middleware)
.into_provider(handler);
```

**Execution order (outermost to innermost):**

1. RetryMiddleware (outermost)
2. MonopolizationMiddleware
3. ShutdownMiddleware
4. SchedulerMiddleware
5. TimeoutMiddleware
6. TelemetryMiddleware
7. Handler (innermost - business logic)

#### Proposed Pipeline Consumer with DeferMiddleware

```rust
// Code
let provider = common_middleware
.layer(monopolization_middleware)
.layer(defer_middleware)      // NEW - before (inside) retry
.layer(retry_middleware)       // outermost - retries defer operations
.into_provider(handler);
```

**Execution order (outermost to innermost):**

1. RetryMiddleware (outermost - retries all failures)
2. DeferMiddleware (NEW - defers messages only, not timers)
3. MonopolizationMiddleware (detect monopolies)
4. ShutdownMiddleware (graceful shutdown)
5. SchedulerMiddleware (global concurrency control)
6. TimeoutMiddleware (stall detection)
7. TelemetryMiddleware (observability)
8. Handler (innermost - business logic)

**Execution flow for message failures:**

1. Handler fails processing a message
2. Error propagates through telemetry, timeout, scheduler, shutdown, monopolization
3. DeferMiddleware receives error
    - Check failure tracker: is deferring enabled?
        - If disabled (>90% failure rate): re-raise error to RetryMiddleware
        - If enabled: **schedule retry timer, defer to Cassandra**, update cache, return Ok (maintains Invariant 1)
4. If defer operation fails (Cassandra unavailable): error propagates to RetryMiddleware
5. RetryMiddleware retries the entire on_message call (including defer attempt) with exponential backoff
6. On retry, message may be deferred (if Cassandra recovered) or error may propagate again

**Execution flow for timer events:**

1. Timer fires
2. DeferMiddleware::on_timer() receives timer
3. Check timer type:
    - **If defer timer**: Load deferred message from Cassandra, load from Kafka, call inner handler's on_message()
        - On success: Remove offset from Cassandra, reset retry_count to 0, use `ctx.clear_and_schedule()` if more
          messages exist
        - On failure: Use `ctx.clear_and_schedule()` with increased backoff, increment retry_count in Cassandra, keep
          offset in Cassandra
    - **If regular timer**: Pass through to inner handler's on_timer()
        - On failure: Error propagates to RetryMiddleware for immediate retry

**Deferred message flow:**

1. Message arrives for deferred key
2. DeferMiddleware checks cache/Cassandra: key is deferred
3. Append offset to Cassandra wide row
4. Return Ok (message queued for later retry, offset auto-commits on return)

Note: Defer timer already exists (scheduled during first failure), maintaining Timer Coverage Invariant.

**Defer timer fires (on_timer flow):**

1. DeferMiddleware::on_timer() receives timer event
2. Check timer type - if defer timer:
3. Load next offset from Cassandra for the timer's key
4. Load message from Kafka via KafkaLoader
5. Call inner handler's on_message() (re-process the deferred message)
6. On success with more messages: Use **ctx.clear_and_schedule()** to atomically replace timer, then remove offset from
   Cassandra
7. On success with no more messages: Remove offset from Cassandra, return Ok (timer auto-clears)
8. On failure: Use **ctx.clear_and_schedule()** with increased delay, increment retry_count in Cassandra

Critical: `clear_and_schedule()` atomically replaces the timer with no coverage gap (Invariant 1).

## Detailed Design

### Component Architecture

#### Shared Components (All Partitions)

```rust
/// Cassandra store for deferred messages (connection pool)
pub struct CassandraDeferStore {
    store: CassandraStore,
    queries: Arc<PreparedQueries>,
}

/// Kafka message loader (single instance for all partitions)
pub struct KafkaLoader {
    consumer: Arc<BaseConsumer>,
    semaphore: Arc<Semaphore>,
    cache: Arc<Cache<(Topic, Partition, Offset), DecodedMessage>>,
}

/// Cache of deferred key states with retry counts (shared for efficiency)
type DeferredCache = Cache<Key, DeferState, UnitWeighter, RandomState>;

/// Deferred state for a key
enum DeferState {
    /// Key is not currently deferred
    NotDeferred,
    /// Key is deferred with active retry count
    Deferred { retry_count: u32 },
}
```

#### Per-Partition Components

```rust
/// Defer handler for a specific partition
pub struct DeferHandler<S, T> {
    /// Wrapped inner handler
    handler: T,

    /// Defer store (shared across partitions)
    store: S,

    /// Kafka message loader (shared across partitions)
    kafka_loader: Arc<KafkaLoader>,

    /// Failure rate tracker (per-partition)
    failure_tracker: FailureTracker,

    /// Cache of deferred keys (shared)
    deferred_cache: Arc<DeferredCache>,

    /// Configuration
    config: DeferConfiguration,

    /// Topic, partition, consumer group for this handler
    topic: Topic,
    partition: Partition,
    consumer_group: String,
}
```

### Storage Schema

#### Cassandra Tables

**Table: `deferred_messages`**

```cql
CREATE TABLE deferred_messages (
    key_id uuid,              -- UUIDv5(consumer_group || topic || partition || key)
    offset bigint,            -- Kafka offset (clustering key, sorted ascending)
    retry_count int static,   -- Shared across all offsets for this key

    PRIMARY KEY (key_id, offset)
) WITH CLUSTERING ORDER BY (offset ASC)
  AND comment = 'Deferred Kafka messages awaiting retry';
```

**Schema Design Rationale:**

1. **Wide Row Pattern**: Each row can contain multiple offsets for a key, stored as clustering columns
2. **Static Columns**: `retry_count` is static, shared across all clustering rows for a `key_id`
3. **Sorted Clustering**: `ORDER BY offset ASC` ensures FIFO processing
4. **UUID Partition Key**: Distributes load across Cassandra cluster
5. **Minimal Storage**: Only essential fields - message payloads remain in Kafka
6. **Consumer Group**: Captured during middleware creation, encoded in key_id via UUIDv5
7. **TTL for Automatic Cleanup**: All inserts use TTL (from Prosody's existing TTL configuration) to automatically
   expire old entries and prevent unbounded growth

#### UUID Generation

UUIDs are generated deterministically using UUIDv5:

- **Messages**: `UUIDv5(NAMESPACE_OID, "{consumer_group}:{topic}:{partition}:{key}")`

This ensures the same logical key always maps to the same `key_id` partition in Cassandra.

### Store Trait

The `DeferStore` trait provides storage operations for deferred messages:

**Message operations:**

- `get_deferred_metadata(key_id)` - Get retry_count (returns `Option<DeferredMetadata>`)
- `has_deferred_messages(key_id)` - Check if key has pending retries
- `peek_next_deferred_message(key_id)` - Check if more messages exist without removing (returns `bool`)
- `get_next_deferred_message(key_id)` - Get oldest offset for retry (returns `Option<Offset>`)
- `append_deferred_message(key_id, offset)` - Add offset to queue
- `remove_deferred_message(key_id, offset)` - Remove after successful retry
- `increment_retry_count(key_id)` - Bump shared retry counter (static column)
- `reset_retry_count(key_id)` - Reset retry counter to 0 (after successful retry)

All operations are async and return `Result<T, Self::Error>`.

**Critical for invariant maintenance:**

- `peek_next_deferred_message()` used to check if more messages exist BEFORE scheduling new timer
- This ensures we never schedule a timer when no messages remain

### Message Processing Flow

#### Normal Message Path (First Failure)

```
1. Message arrives
         │
         ▼
2. DeferMiddleware::on_message()
         │
         ├─→ Check cache: get DeferState
         │   ├─→ Cache hit:
         │   │   ├─→ NotDeferred: continue to handler
         │   │   └─→ Deferred { retry_count }: append offset to Cassandra, return Ok
         │   └─→ Cache miss:
         │       ├─→ Query Cassandra: get_deferred_metadata(key_id)
         │       ├─→ If messages found:
         │       │   ├─→ Cache result: Deferred { retry_count }
         │       │   ├─→ Append offset to Cassandra
         │       │   ├─→ Return Ok (timer already exists from previous failure)
         │       ├─→ If no messages:
         │       │   ├─→ Cache result: NotDeferred
         │       │   └─→ Continue to handler
         │
         ▼
3. Call inner handler (monopolization → shutdown → scheduler → ... → business handler)
         │
         ├─→ Handler fails
         ├─→ Error propagates back
         │
         ▼
4. DeferMiddleware receives error
         │
         ├─→ Check failure tracker: is deferring enabled?
         │   ├─→ No: re-raise error (propagates to outer RetryMiddleware)
         │   └─→ Yes: continue
         │
         ▼
5. Defer the message
         │
         ├─→ Generate key_id from (group, topic, partition, key)
         ├─→ Check cache for current state
         │   ├─→ NotDeferred (first failure):
         │   │   ├─→ STEP 1: Schedule defer timer using ctx.schedule_timer(key, type=Defer, delay=backoff(1))
         │   │   ├─→ STEP 2: INSERT offset to Cassandra with retry_count = 1
         │   │   ├─→ STEP 3: Update cache: Deferred { retry_count: 1 }
         │   │   └─→ Invariants maintained: Timer exists BEFORE Cassandra write
         │   └─→ Deferred { retry_count } (subsequent failure):
         │       ├─→ Append offset to Cassandra (timer already exists)
         │       ├─→ Increment retry_count in Cassandra (static column)
         │       └─→ Update cache: Deferred { retry_count: retry_count + 1 }
         ├─→ Record failure in tracker
         ├─→ Return Ok (error handled)
         │
         │   Note: If any step fails, error propagates to RetryMiddleware.
         │         Timer with no messages will cleanup on next fire.
         │
         ▼
6. Return Ok - offset auto-commits
```

#### Subsequent Messages for Deferred Key

```
1. Message arrives for deferred key
         │
         ▼
2. DeferMiddleware::on_message()
         │
         ├─→ Check cache: get DeferState
         │   └─→ Cache hit: Deferred { retry_count }
         │
         ▼
3. Append offset to Cassandra
         │   (adds to existing wide row)
         │   (retry_count unchanged - per-key, not per-message)
         │   (timer already exists - no scheduling needed)
         │
         ▼
4. Return Ok (message queued, offset auto-commits)

Note: Invariant 1 maintained - timer already exists from first failure,
      no new timer scheduling required.
```

#### Defer Timer Fires (on_timer handler)

```
1. Timer fires in consumer system
         │
         ▼
2. DeferMiddleware::on_timer(ctx, timer) called
         │
         ├─→ Check timer type
         │   ├─→ If NOT defer timer: return handler.on_timer(ctx, timer) [pass through]
         │   └─→ If defer timer: continue
         │
         ▼
3. Extract key from timer, generate key_id
         │
         ▼
4. Load next offset from Cassandra: store.get_next_deferred_message(key_id)
         │   (first offset in clustering order)
         │
         ▼
5. Load message from Kafka (via KafkaLoader)
         │
         ▼
6. Call inner handler.on_message(ctx, message, DemandType::Failure)
         │   (monopolization → shutdown → scheduler → ... → business handler)
         │
         ├─→ Success:
         │   ├─→ Check if more offsets exist: store.peek_next_deferred_message(key_id)
         │   ├─→ If more offsets:
         │   │   ├─→ STEP 1: Reset retry_count to 0 in Cassandra
         │   │   ├─→ STEP 2: ctx.clear_and_schedule(key, type=Defer, delay=backoff(0))
         │   │   ├─→ STEP 3: Remove current offset from Cassandra
         │   │   ├─→ STEP 4: Update cache: Deferred { retry_count: 0 }
         │   │   ├─→ STEP 5: Return Ok
         │   │   └─→ Invariant maintained: Timer atomically replaced, next message starts fresh
         │   ├─→ If no more offsets:
         │   │   ├─→ STEP 1: Remove current offset from Cassandra
         │   │   ├─→ STEP 2: Update cache: NotDeferred
         │   │   ├─→ STEP 3: Return Ok (timer auto-clears, no new timer)
         │   │   └─→ Invariant maintained: No messages left, no timer needed
         │   └─→ Record success in tracker
         │
         └─→ Failure:
             ├─→ STEP 1: ctx.clear_and_schedule(key, type=Defer, delay=backoff(retry_count + 1))
             ├─→ STEP 2: Increment retry_count in Cassandra (static column)
             ├─→ STEP 3: Update cache: Deferred { retry_count: retry_count + 1 }
             ├─→ STEP 4: Return Ok
             ├─→ Invariant maintained: Timer atomically replaced with increased delay
             ├─→ Record failure in tracker
             └─→ Message will retry later with new timer
```

### Failure Rate Tracking

The `FailureTracker` tracks success/failure events over a sliding time window to detect when the defer system itself is
unhealthy:

**Structure:**

- `VecDeque<Instant>` - Ring buffer of success timestamps
- `VecDeque<Instant>` - Ring buffer of failure timestamps
- Configurable window (default: 5 minutes)
- Configurable threshold (default: 90%)

**Behavior:**

- `record_success()` - Add timestamp to success deque, cleanup old events
- `record_failure()` - Add timestamp to failure deque, cleanup old events
- `is_deferring_enabled()` - Calculate: `failure_rate = failures.len() / (successes.len() + failures.len())`
- If failure_rate ≥ 90%: disable deferring, errors propagate to outer RetryMiddleware
- Old events automatically removed from both deques when outside window

### Retry Delay Calculation

Uses **Full Jitter** algorithm (same as existing `RetryMiddleware`):

```rust
fn sleep_time(&self, retry_count: u32) -> Duration {
    // Exponential backoff capped at max_delay
    let exp_backoff = min(
        2 ^ retry_count * base_delay,
        max_delay
    );

    // Full jitter: random(0, exp_backoff)
    random(0..exp_backoff)
}
```

This is the "Full Jitter" algorithm
from [AWS's Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
article, which provides optimal distribution of retries and prevents thundering herd.

**Example progression (default: base=1min):**

```
retry_count  max possible delay
───────────────────────────────
0            1 minute
1            2 minutes
2            4 minutes
3            8 minutes
6            ~1 hour
8            ~4 hours
10           ~17 hours
11+          24 hours (capped)
```

Actual delays are uniformly distributed between 0 and the max value.

### Deferred Cache Strategy

The `DeferredCache` stores retry counts for deferred keys, avoiding repeated Cassandra queries and enabling retry-aware
behavior:

**Cache Structure:**

```rust
enum DeferState {
    NotDeferred,
    Deferred { retry_count: u32 },
}

Cache<Key, DeferState>
```

**Cache Operations:**

**On message arrival (cache hit):**

- `NotDeferred` → call inner handler (normal path)
- `Deferred { retry_count }` → append offset to Cassandra, return Ok (queued)

**On message arrival (cache miss):**

```rust
let row = store.get_deferred_metadata(key_id).await?;
let state = match row {
Some(metadata) => DeferState::Deferred { retry_count: metadata.retry_count },
None => DeferState::NotDeferred,
};
cache.insert(key, state);
// Then route based on state
```

**On handler failure (first failure):**

```rust
// INVARIANT MAINTENANCE: Schedule timer BEFORE writing to Cassandra
ctx.schedule_timer(key, timer_type = Defer, delay = backoff(1)).await?;  // STEP 1
store.insert_deferred_message(key_id, offset, retry_count = 1).await?;  // STEP 2
cache.insert(key, DeferState::Deferred { retry_count: 1 });  // STEP 3
// Ensures timer exists before messages exist (Invariant 1)
// Ensures cache reflects Cassandra state (Invariant 2)
```

**On handler failure (subsequent failures):**

```rust
// Timer already exists, just update state
store.increment_retry_count(key_id).await?;
cache.insert(key, DeferState::Deferred { retry_count: retry_count + 1 });
// Invariant 1 maintained: timer already exists
// Invariant 2 maintained: cache updated after Cassandra write
```

**On successful retry (in on_timer handler):**

```rust
// INVARIANT MAINTENANCE: Check for more messages and use clear_and_schedule
let has_more = store.peek_next_deferred_message(key_id).await?;
if has_more {
// Reset retry count for next message (starts fresh)
store.reset_retry_count(key_id).await?;
cache.insert(key, DeferState::Deferred { retry_count: 0 });
// Atomically clear old timer and schedule new one with backoff(0)
ctx.clear_and_schedule(key, timer_type = Defer, delay = backoff(0)).await?;
store.remove_deferred_message(key_id, offset).await?;
// Invariant 1 maintained: atomic timer replacement with no gap
} else {
// No more messages, safe to let timer clear automatically
store.remove_deferred_message(key_id, offset).await?;
cache.insert(key, DeferState::NotDeferred);
// Return Ok - timer auto-clears without replacement
// Invariant 1 maintained: timer removed only after messages removed
}
```

**Cache Correctness Guarantees:**

The cache is **safe from race conditions** due to Kafka's partition-affinity guarantees:

1. **Partition ownership**: Each partition is owned by exactly ONE consumer at a time
2. **Sequential per-key processing**: Messages for a given key within a partition are processed sequentially
3. **key_id includes partition**: `UUIDv5(consumer_group || topic || partition || key)` ensures different partitions map
   to different cache entries
4. **Cache lifetime**: The per-partition cache is destroyed when the partition is rebalanced

Therefore, no concurrent modifications to the same cache entry can occur, making the write-through cache strategy safe.

**Configuration:**

- Size: 10,000 keys (configurable)
- Type: `Cache<Key, DeferState>` using quick_cache
- Eviction: S3-FIFO (automatic)
- Shared across all partitions for memory efficiency

**Benefits:**

- **Observability**: Emit metrics by retry depth (`deferred_keys_by_depth` histogram)
- **Adaptive behavior**: Adjust backoff or alerting based on retry_count
- **Performance**: Avoids Cassandra reads on cache hits (routing decision is immediate)
- **Correctness**: Cache miss performs a single read to populate with accurate state

### Configuration

```rust
#[derive(Builder, Clone, Debug, Validate)]
pub struct DeferConfiguration {
    pub consumer_group: String,
    pub backoff: RetryBackoff,                    // Full Jitter backoff params
    pub failure_window: Duration,                 // Default: 5 min
    pub failure_threshold: f64,                   // Default: 0.9 (90%)
    pub cache_size: usize,                        // Default: 10,000
    pub loader_config: LoaderConfiguration,       // Kafka loader settings
    pub ttl: CompactDuration,                     // TTL for Cassandra entries (uses Prosody's existing TTL config)
}

pub struct RetryBackoff {
    pub base_delay: Duration,      // Default: 1 minute (much longer than RetryMiddleware's 20ms)
    pub max_delay: Duration,       // Default: 24 hours (much longer than RetryMiddleware's 5min)
}
```

**Notes:**

- **Backoff Strategy:** Defer middleware uses the same **Full Jitter** algorithm as `RetryMiddleware` but with much
  longer delays, since it handles persistent failures rather than transient ones. The immediate retry middleware uses
  base=20ms, max=5min for quick recovery from transient issues, while defer uses base=1min, max=24h for long-term
  persistent failures.

- **TTL Usage:** All Cassandra inserts include a TTL to automatically expire old entries and prevent unbounded growth.
  The TTL value is derived from Prosody's existing TTL configuration (typically used for timer storage) to ensure
  consistency across the system. This prevents accumulation of entries for permanently failed keys.

### Middleware Implementation

The defer middleware follows Prosody's standard middleware pattern:

**DeferMiddleware** - Shared state across all partitions:

- `DeferStore` - Cassandra/memory store instance
- `KafkaLoader` - Shared message loader
- `DeferredCache` - Shared cache of deferred keys
- `DeferConfiguration` - Settings

**DeferHandler** - Per-partition handler instance:

- Wraps inner handler
- Maintains global cross-partition `FailureTracker`

**Key methods:**

- `on_message(ctx, message)` - Check if key deferred, defer message on failure. Uses `ctx.schedule_timer()` to create
  initial defer timer.
- `on_timer(ctx, timer)` - Check timer type:
    - If defer timer: Load deferred message from Cassandra, load from Kafka, call `handler.on_message()` with the
      deferred message
    - If regular timer: Pass through to `handler.on_timer()`
    - Uses `ctx.clear_and_schedule()` to atomically replace defer timers. Timer auto-clears on successful return if no
      replacement scheduled.

### Timer Type Handling

The defer middleware uses Prosody's timer system with typed timers to distinguish defer timers from business timers:

**Timer types:**

- **Defer timer**: Special timer type created by DeferMiddleware to trigger deferred message retry
- **Business timer**: Regular timers created by the business logic handler

**on_timer() flow:**

```rust
async fn on_timer(&self, ctx: &EventContext, timer: Timer) -> Result<()> {
    // Check if this is a defer timer
    if timer.timer_type() == TimerType::Defer {
        // Extract key from timer metadata
        let key = timer.key();
        let key_id = generate_key_id(&self.consumer_group, &self.topic, &self.partition, &key);

        // Get current retry count from cache
        let retry_count = self.get_retry_count(&key_id).await?;

        // Load next deferred message
        let offset = self.store.get_next_deferred_message(&key_id).await?;
        let message = self.kafka_loader.load_message(&self.topic, &self.partition, offset).await?;

        // Re-process through handler with DemandType::Failure
        match self.handler.on_message(ctx, message, DemandType::Failure).await {
            Ok(()) => {
                // Success! Check if more messages exist
                let has_more = self.store.peek_next_deferred_message(&key_id).await?;
                if has_more {
                    // Reset retry count for next message
                    self.store.reset_retry_count(&key_id).await?;
                    self.cache.insert(key.clone(), DeferState::Deferred { retry_count: 0 });
                    // Atomically replace timer for next message (starts fresh with backoff(0))
                    ctx.clear_and_schedule(&key, TimerType::Defer, self.backoff(0)).await?;
                } else {
                    // No more messages, clear from cache
                    self.cache.insert(key.clone(), DeferState::NotDeferred);
                }
                // Remove processed message
                self.store.remove_deferred_message(&key_id, offset).await?;
                Ok(()) // Timer auto-clears (or already replaced)
            }
            Err(_) => {
                // Failure - keep retrying with increased backoff
                ctx.clear_and_schedule(&key, TimerType::Defer, self.backoff(retry_count + 1)).await?;
                self.store.increment_retry_count(&key_id).await?;
                self.cache.insert(key.clone(), DeferState::Deferred { retry_count: retry_count + 1 });
                Ok(()) // Timer replaced, will retry later
            }
        }
    } else {
        // Pass through to inner handler for business timers
        self.handler.on_timer(ctx, timer).await
    }
}
```

**Important:** All deferred retries use `DemandType::Failure` since they represent reprocessing of previously failed
events, consistent with how `RetryMiddleware` marks retry attempts.

**Shutdown:** When a partition is revoked, the timer system automatically handles cleanup. No separate event loop
to shut down.

### Error Handling and Invariant Recovery

**When clear_and_schedule fails (in on_timer handler):**

- Error propagates from on_timer() method
- Current timer is NOT cleared (on_timer failed, so timer remains active)
- Timer system will retry the on_timer call later
- No state changes occur in Cassandra
- Invariant maintained: Messages still covered by existing timer

**When Cassandra operations fail after clear_and_schedule:**

- New timer already scheduled
- Current timer will be cleared on successful return
- On next timer fire: query will return updated state, system self-corrects

**When Cassandra write fails:**

- Error propagates to `RetryMiddleware`
- If timer was already scheduled, it exists without messages
- When timer fires, query returns no messages, cleanup occurs
- Invariant temporarily violated but self-healing

**When cache update fails:**

- Cassandra state is correct
- Cache is stale
- Next cache miss will re-query Cassandra and correct
- Invariant maintained in persistent state

**Recovery scenarios:**

1. **Timer exists, no messages (orphaned timer)**:
    - Timer fires → on_timer() called → query returns None → clear cache → return Ok
    - Timer auto-commits on successful return
    - Self-healing: Timer cleans itself up automatically

2. **Messages exist, no timer (abandoned messages)**:
    - CANNOT HAPPEN if ordering is followed (timer scheduled before Cassandra write)
    - If somehow occurs: Messages never retried (lost)
    - **This is why invariant order is critical**

3. **Cache says NotDeferred but messages exist**:
    - New message arrives → cache hit (NotDeferred) → call handler
    - Handler fails → schedule timer → INSERT (conflict/merge)
    - Eventually consistent

## Performance Considerations

### Cassandra Query Patterns

**Reads (per retry timer fire):**

```cql
-- Get next deferred message (O(1) due to clustering)
SELECT offset FROM deferred_messages
WHERE key_id = ? LIMIT 1;

-- Check if more deferred items exist
SELECT key_id FROM deferred_messages
WHERE key_id = ? LIMIT 1;
```

**Writes (per failure):**

```cql
-- Append offset with TTL (uses Prosody's existing TTL configuration)
-- TTL automatically expires old entries to prevent unbounded growth
INSERT INTO deferred_messages (key_id, offset)
VALUES (?, ?)
USING TTL ?;

-- Increment retry count (lightweight transaction for correctness)
UPDATE deferred_messages SET retry_count = retry_count + 1
WHERE key_id = ? IF retry_count = ?;
```

**Deletes (per success):**

```cql
-- Remove processed offset
DELETE FROM deferred_messages
WHERE key_id = ? AND offset = ?;
```

### Kafka Loader Performance

- **Shared instance**: Single `BaseConsumer` across all partitions
- **Backpressure**: Semaphore-limited concurrent loads (default: 256)
- **Caching**: S3-FIFO cache for frequently retried messages (default: 10,000)
- **Batching**: Not needed (retry timers naturally space out requests)

### Memory Footprint

**Per partition:**

- `FailureTracker`: Bounded by 5-minute sliding window × event rate (typically < 100 KB)

**Shared:**

- `DeferredCache`: 10,000 × 64 bytes = 640 KB
- `KafkaLoader cache`: 10,000 × ~1 KB = 10 MB

**Defer timers:** Stored in Cassandra via existing timer system (no per-partition memory overhead)

**Total estimate for 100 partitions:** ~20 MB

## Edge Cases and Error Handling

### Kafka Message Deleted (Retention/Compaction)

When loading a deferred message in on_timer(), if Kafka returns `OffsetDeleted`:

1. Log warning
2. Remove offset from deferred queue (can't retry)
3. Check if more messages exist (`peek_next_deferred_message()`)
4. If more messages exist: Use `ctx.clear_and_schedule()` to replace timer, remove offset, return Ok (maintains
   Invariant 1)
5. If no more messages: Remove offset from Cassandra, clear from cache, return Ok (timer auto-clears, no replacement)

This handles messages that aged out due to Kafka retention or were removed by compaction while maintaining timer
coverage invariant. The `clear_and_schedule()` ensures atomic timer replacement with no coverage gap.

### Partition Rebalancing

**On partition revoke:**

1. Shutdown inner handler
2. Timer system automatically stops processing timers for this partition

**State preservation:**

- All deferred items remain in Cassandra
- All defer timers remain in Cassandra timer storage
- New consumer instance picks up defer timers automatically when partition is reassigned
- Cache is cold on new instance (minor performance impact until warmed)

**No explicit cleanup needed:** The timer system handles all timer lifecycle management automatically.

### Cassandra Unavailability

When Cassandra store operations fail:

1. Log error
2. Re-raise error to outer RetryMiddleware
3. RetryMiddleware retries the entire on_message operation (including defer attempt)
4. On retry: message may be deferred (if Cassandra recovered) or error propagates again to RetryMiddleware

**Future enhancement:** Circuit breaker to temporarily disable deferring during extended Cassandra outages, falling back
to immediate error propagation. This would allow RetryMiddleware to handle the failure directly instead of repeatedly
attempting (and failing) to defer.

### Failure Rate Above Threshold

When `FailureTracker` detects failure rate ≥ threshold (default 90%):

1. Log warning about high failure rate
2. Pass messages directly to inner handler (bypass deferring)
3. Errors propagate to outer RetryMiddleware
4. System automatically re-enables deferring when failure rate drops

This prevents cascading failures when the defer system itself becomes unhealthy.

## Testing Strategy

### Unit Tests

1. **FailureTracker:**
    - Event recording
    - Sliding window cleanup
    - Threshold calculation

2. **RetryBackoff:**
    - Delay calculation
    - Exponential growth
    - Max delay capping
    - Jitter distribution

3. **Store implementations:**
    - CRUD operations
    - Concurrency (multiple partitions)
    - Error handling

### Integration Tests

1. **End-to-end deferral:**
    - Message fails → deferred → retry timer fires → success
    - Multiple failures → increasing backoff
    - Subsequent messages queued while key deferred

2. **Failure rate tracking:**
    - High failure rate disables deferring
    - Recovery re-enables deferring

3. **Partition rebalancing:**
    - Deferred items survive rebalance
    - New instance picks up retry timers

4. **Cassandra failure:**
    - Store unavailable → errors propagate to outer RetryMiddleware, then logged and offset committed
    - Store recovers → deferring resumes

5. **Invariant validation:**
    - **Timer Coverage Invariant**: Verify exactly one timer exists for each key with deferred messages
    - **Cache Consistency Invariant**: Verify cache state matches Cassandra state
    - Test error scenarios: timer scheduling fails, Cassandra write fails, verify recovery
    - Test race conditions: verify partition-affinity prevents concurrent modifications

### Property-Based Tests

1. **Order preservation:**
    - Deferred messages processed in offset order per key

2. **At-least-once delivery:**
    - No message lost during failures
    - Duplicate deliveries acceptable (idempotent handlers)

## Deployment and Operations

### Monitoring Metrics

```rust
// Metrics to expose via telemetry
pub struct DeferMetrics {
    // Counts
    pub deferred_messages_total: Counter,
    pub retry_successes_total: Counter,
    pub retry_failures_total: Counter,

    // Gauges
    pub deferred_keys_active: Gauge,
    pub failure_rate: Gauge,
    pub deferring_enabled: Gauge,
    pub deferred_keys_by_depth: Histogram,  // Buckets: 1-5, 6-10, 11-50, 50+

    // Histograms
    pub retry_count_distribution: Histogram,
    pub retry_delay_seconds: Histogram,
    pub kafka_load_duration_seconds: Histogram,
}
```

### Configuration Tuning

**Lower-latency workloads:**

- `base_delay`: 30s (shorter initial retry)
- `max_delay`: 1h (cap retries sooner)
- `failure_threshold`: 0.95 (more tolerant)
- `cache_size`: 1,000 (fewer unique failing keys expected)

**High-throughput workloads:**

- `base_delay`: 5min (longer initial retry)
- `max_delay`: 24h (extended retry period)
- `failure_threshold`: 0.90 (standard)
- `cache_size`: 100,000 (many unique keys)

### Operational Runbook

**Scenario: High failure rate disables deferring**

*Detection:* `deferring_enabled` metric drops to 0

*Impact:* Errors propagate to outer RetryMiddleware; after retries exhausted, errors are logged and offsets committed (
messages lost)

*Resolution:*

1. Investigate root cause of failures (check handler logs)
2. Fix underlying issue (deploy code fix, restore dependency, etc.)
3. System automatically re-enables deferring when failure rate drops

**Scenario: Cassandra unavailable**

*Detection:* `store_errors_total` metric spikes

*Impact:* Cannot defer new failures; errors propagate to outer RetryMiddleware, then logged and offsets committed

*Resolution:*

1. Restore Cassandra cluster
2. System automatically resumes deferring on next failure
3. Existing deferred items remain and will be retried

**Scenario: Kafka offset deleted**

*Detection:* `OffsetDeleted` errors in logs

*Impact:* Specific deferred messages cannot be retried

*Resolution:*

1. System automatically skips deleted messages
2. Moves to next deferred message in queue
3. No manual intervention needed

## Future Enhancements

1. **Adaptive backoff:** Adjust backoff based on failure patterns
2. **Max retry limit:** After N retries, move failed messages to a separate Cassandra table for manual review
3. **Batch processing:** Group multiple deferred messages for efficiency
4. **Cross-partition coordination:** Deduplicate retries across consumer instances
5. **Metrics dashboard:** Pre-built Grafana dashboard for defer middleware
6. **Circuit breaker:** Temporarily disable deferring if store is unhealthy

## Appendix

### Comparison with Existing Retry Middleware

| Aspect             | RetryMiddleware    | DeferMiddleware          |
|--------------------|--------------------|--------------------------|
| Persistence        | In-memory          | Cassandra                |
| Max retry time     | Minutes            | Days/weeks               |
| Survives rebalance | No                 | Yes                      |
| Backoff type       | Exponential        | Exponential              |
| Typical position   | Outermost          | Inside RetryMiddleware   |
| Use case           | Transient failures | Persistent failures      |
| Overhead           | Minimal            | Moderate (Cassandra I/O) |

### Alternative Designs Considered

**1. Store message data in Cassandra**

*Rejected because:*

- Kafka already stores messages durably
- Would duplicate storage
- Increases Cassandra I/O and storage costs
- KafkaLoader provides efficient offset-based retrieval

**2. Single event loop for all keys**

*Rejected because:*

- Limits parallelism
- Head-of-line blocking
- Per-key loops with scheduler coordination is cleaner

**3. Dynamic dispatch for store trait**

*Rejected because:*

- Performance overhead
- Against Prosody's design principles
- Generics provide zero-cost abstraction

---

**Document Version:** 2.0
**Last Updated:** 2025-01-21
**Author:** Design Team
**Status:** Updated for Timer Types
**Changes:**

- Removed separate TimerManager - uses built-in timer system with defer timer type
- Uses `ctx.schedule_timer()` for initial defer timer creation
- Uses `ctx.clear_and_schedule()` for atomic timer replacement in on_timer handler
- Timer auto-clears on successful on_timer() return if no replacement scheduled
- Simplified architecture by leveraging existing timer infrastructure
