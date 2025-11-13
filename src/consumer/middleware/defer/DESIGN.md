# Defer Middleware Design Document

## Executive Summary

This document proposes a persistent, per-key retry mechanism (Defer Middleware) for Prosody's **pipeline mode** that
provides long-term retry capabilities with Cassandra-backed persistence. The middleware wraps the inner handler stack,
enabling graceful degradation from immediate retries to persistent deferred retries.

The middleware leverages Prosody's built-in timer system with typed timers (DeferRetry vs. Application timers) to
schedule
retries, eliminating the need for a separate timer manager. DeferRetry timers trigger on_timer() calls that reload
deferred
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

- Schedule DeferRetry timer using `context.schedule()` BEFORE writing to Cassandra
- This creates the initial timer for the key

**On DeferRetry timer fires (on_timer handler):**

- Check if more deferred messages exist after processing current one
- **If more messages**: Use `context.clear_and_schedule()` to atomically clear current timer and schedule next
- **If no more messages**: Return successfully (timer clears automatically, no new timer scheduled)
- **On processing failure**: Use `context.clear_and_schedule()` to atomically clear current timer and schedule retry
  with
  increased backoff

**Why**: If messages exist without a timer, they will never be retried (abandoned).

**Critical ordering**:

1. First failure: `schedule() → write_to_cassandra()`
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
│  │   Store      │  │  Loader     │  │  (schedule,        │  │
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

Note: Uses built-in timer system with typed DeferRetry timers.
      - on_message(): Catches failures and schedules DeferRetry timers
      - on_timer(): Checks timer type; if DeferRetry timer, loads & retries message
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
2. DeferMiddleware::on_timer() receives trigger
3. Check timer type:
    - **If DeferRetry timer**: Load deferred message from Cassandra, load from Kafka, call inner handler's on_message()
        - On success: Remove offset from Cassandra, reset retry_count to 0, use `context.clear_and_schedule()` if more
          messages exist
        - On failure: Use `context.clear_and_schedule()` with increased backoff, increment retry_count in Cassandra,
          keep
          offset in Cassandra
    - **If Application timer**: Pass through to inner handler's on_timer()
        - On failure: Error propagates to RetryMiddleware for immediate retry

**Deferred message flow:**

1. Message arrives for deferred key
2. DeferMiddleware checks cache/Cassandra: key is deferred
3. Append offset to Cassandra wide row
4. Return Ok (message queued for later retry, offset auto-commits on return)

Note: Defer timer already exists (scheduled during first failure), maintaining Timer Coverage Invariant.

**DeferRetry timer fires (on_timer flow):**

1. DeferMiddleware::on_timer() receives trigger event
2. Check timer type - if DeferRetry timer:
3. Load next offset from Cassandra for the trigger's key
4. Load message from Kafka via KafkaLoader
5. Call inner handler's on_message() (re-process the deferred message)
6. On success with more messages: Use **context.clear_and_schedule()** to atomically replace timer, then remove offset
   from
   Cassandra
7. On success with no more messages: Remove offset from Cassandra, return Ok (timer auto-clears)
8. On failure: Use **context.clear_and_schedule()** with increased delay, increment retry_count in Cassandra

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

The `DeferStore` trait provides storage operations for deferred messages, following the same pattern as `TriggerStore`:

```rust
/// Trait for persistent storage of deferred message offsets.
///
/// Minimal trait with only essential operations. Follows the same pattern
/// as `TriggerStore` with TTL support and Error associated type.
///
/// All retry_count updates use explicit values (never increments) since the
/// caller always knows the exact value needed.
pub trait DeferStore: Clone + Send + Sync + 'static {
    /// Error type for storage operations.
    type Error: Error + Send + Sync + 'static;

    /// Get the next (oldest) deferred offset and current retry count for a key.
    ///
    /// Returns `None` if no messages are deferred for this key.
    /// Returns `Some((offset, retry_count))` with the oldest offset and the
    /// shared retry counter (from Cassandra static column).
    ///
    /// # Returns
    ///
    /// * `None` - No deferred messages for this key
    /// * `Some((offset, retry_count))` - Oldest offset and current retry counter
    fn get_next_deferred_message(
        &self,
        key_id: &Uuid,
    ) -> impl Future<Output=Result<Option<(Offset, u32)>, Self::Error>> + Send;

    /// Append a new offset to the deferred queue for a key.
    ///
    /// **TTL Calculation:**
    /// The `expected_retry_time` parameter is used to calculate the Cassandra TTL:
    /// ```
    /// TTL = (expected_retry_time - now) + base_ttl
    /// ```
    /// This ensures the entry expires shortly after it would be retried, preventing
    /// unbounded growth. The calculation uses `CassandraStore::calculate_ttl()`.
    ///
    /// **Retry Count Update:**
    /// If `retry_count` is `Some(value)`, also updates the static retry_count column.
    /// This allows combining offset insert with retry_count update in a single query
    /// for the first failure case.
    ///
    /// # Arguments
    ///
    /// * `key_id` - The UUID identifying the deferred key
    /// * `offset` - The Kafka offset to defer
    /// * `expected_retry_time` - When this message is expected to be retried (for TTL calculation)
    /// * `retry_count` - If Some, also sets the retry_count static column (for first failure)
    fn append_deferred_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
        retry_count: Option<u32>,
    ) -> impl Future<Output=Result<(), Self::Error>> + Send;

    /// Remove an offset after successful retry.
    ///
    /// Called after successfully processing a deferred message to remove it
    /// from the queue.
    fn remove_deferred_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
    ) -> impl Future<Output=Result<(), Self::Error>> + Send;

    /// Set the retry counter to an explicit value.
    ///
    /// Called when we need to update retry_count without inserting an offset:
    /// - After a deferred retry fails: set to `retry_count + 1`
    /// - After success with more messages: set to `0` (reset for next message)
    ///
    /// The caller always knows the exact value since they just read it from
    /// `get_next_deferred_message`.
    fn set_retry_count(
        &self,
        key_id: &Uuid,
        retry_count: u32,
    ) -> impl Future<Output=Result<(), Self::Error>> + Send;
}
```

**Design Rationale:**

- **4 methods only** - Minimal surface area matching TriggerStore pattern
- **get_next_deferred_message returns both offset and retry_count** - Single query instead of separate metadata fetch
- **No peek method** - Check for more messages by calling `get_next_deferred_message` again after removal
- **TTL on append** - Matches TriggerStore pattern using `expected_retry_time` for TTL calculation
- **Explicit retry_count values** - Never uses increments; caller always knows exact value to write
- **Optional retry_count on append** - Allows combining offset insert with retry_count update in single query (first
  failure)
- **Static column for retry_count** - Shared across all offsets for a key, updated via explicit `set_retry_count`

**TTL Pattern:**

Implementations use `CassandraStore::calculate_ttl()` to compute TTL from `expected_retry_time`:
`TTL = (expected_retry_time - now) + base_ttl` (from PROSODY_CASSANDRA_RETENTION)

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
         │       ├─→ Query Cassandra: get_next_deferred_message(key_id)
         │       ├─→ If messages found (get_next_deferred_message returns Some):
         │       │   ├─→ Extract retry_count from result
         │       │   ├─→ Cache result: Deferred { retry_count }
         │       │   ├─→ Append offset to Cassandra
         │       │   ├─→ Return Ok (timer already exists from previous failure)
         │       ├─→ If no messages (returns None):
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
         ├─→ Cache MUST be NotDeferred (otherwise we wouldn't have processed the message)
         ├─→ STEP 1: Calculate retry time: CompactDateTime::now()?.add_duration(backoff(1))?
         ├─→ STEP 2: Schedule DeferRetry timer using context.schedule(retry_time, TimerType::DeferRetry)
         ├─→ STEP 3: INSERT offset to Cassandra with retry_count = 1 and TTL based on retry_time
         │   Call: store.append_deferred_message(key_id, offset, retry_time, Some(1))
         │   Single query sets both offset and retry_count static column
         ├─→ STEP 4: Update cache: Deferred { retry_count: 1 }
         ├─→ Invariants maintained: Timer exists BEFORE Cassandra write
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
         │   Call: store.append_deferred_message(key_id, offset, retry_time, None)
         │   Adds to existing wide row without touching retry_count static column
         │   Timer already exists - no scheduling needed
         │
         ▼
4. Return Ok (message queued, offset auto-commits)

Note: Invariant 1 maintained - timer already exists from first failure,
      no new timer scheduling required.
```

#### DeferRetry Timer Fires (on_timer handler)

```
1. Timer fires in consumer system
         │
         ▼
2. DeferMiddleware::on_timer(context, trigger, demand_type) called
         │
         ├─→ Check timer type (trigger.timer_type)
         │   ├─→ If NOT DeferRetry: return handler.on_timer(context, trigger, demand_type) [pass through]
         │   └─→ If DeferRetry: continue
         │
         ▼
3. Extract key from trigger, generate key_id
         │
         ▼
4. Load next offset from Cassandra: store.get_next_deferred_message(key_id)
         │   (first offset in clustering order)
         │
         ▼
5. Load message from Kafka (via KafkaLoader)
         │
         ▼
6. Call inner handler.on_message(context, message, DemandType::Failure)
         │   (monopolization → shutdown → scheduler → ... → business handler)
         │
         ├─→ Success:
         │   ├─→ STEP 1: Remove current offset from Cassandra
         │   ├─→ STEP 2: Check if more offsets exist: store.get_next_deferred_message(key_id)
         │   ├─→ If more offsets (returns Some):
         │   │   ├─→ STEP 3a: Set retry_count to 0: store.set_retry_count(key_id, 0)
         │   │   ├─→ STEP 3b: Calculate next time: CompactDateTime::now()?.add_duration(backoff(0))?
         │   │   ├─→ STEP 3c: context.clear_and_schedule(next_time, TimerType::DeferRetry)
         │   │   ├─→ STEP 3d: Update cache: Deferred { retry_count: 0 }
         │   │   ├─→ STEP 3e: Return Ok
         │   │   └─→ Invariant maintained: Timer atomically replaced, next message starts fresh
         │   ├─→ If no more offsets (returns None):
         │   │   ├─→ STEP 3a: Update cache: NotDeferred
         │   │   ├─→ STEP 3b: Return Ok (timer auto-clears, no new timer)
         │   │   └─→ Invariant maintained: No messages left, no timer needed
         │   └─→ Record success in tracker
         │
         └─→ Failure:
             ├─→ STEP 1: Calculate retry time: CompactDateTime::now()?.add_duration(backoff(retry_count + 1))?
             ├─→ STEP 2: context.clear_and_schedule(retry_time, TimerType::DeferRetry)
             ├─→ STEP 3: Set retry_count to new value: store.set_retry_count(key_id, retry_count + 1)
             ├─→ STEP 4: Update cache: Deferred { retry_count: retry_count + 1 }
             ├─→ STEP 5: Return Ok
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

Uses **Full Jitter** algorithm (same as existing `RetryMiddleware`)
from [AWS's Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).

**Algorithm:**

- Calculate exponential backoff: `min(2^retry_count * base, max_delay)`
- Apply full jitter: `random(0, exponential_backoff)`
- This provides optimal distribution of retries and prevents thundering herd

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

- Query store for next deferred message
- If messages exist, extract retry_count and cache as `Deferred { retry_count }`
- If no messages, cache as `NotDeferred`
- Route based on cached state

**On handler failure (first failure):**

Maintains **Invariant 1** by scheduling timer before Cassandra write:

1. Calculate retry time using backoff(1)
2. Schedule DeferRetry timer with `context.schedule()`
3. Write offset to Cassandra with TTL and retry_count=1 using
   `append_deferred_message(key_id, offset, retry_time, Some(1))`
4. Update cache to `Deferred { retry_count: 1 }`

Single query sets both offset and retry_count. Timer exists before messages exist, ensuring coverage.

**On successful retry (in on_timer handler):**

Maintains **Invariant 1** using atomic timer operations:

1. Remove processed offset from Cassandra
2. Check if more messages exist
3. If more messages:
    - Set retry_count to 0 using `set_retry_count(key_id, 0)` (next message starts fresh)
    - Use `clear_and_schedule()` to atomically replace timer
    - Update cache to `Deferred { retry_count: 0 }`
4. If no more messages:
    - Update cache to `NotDeferred`
    - Return Ok (timer auto-clears without replacement)

**On retry failure (in on_timer handler):**

We just read `retry_count` from `get_next_deferred_message`, so we know the exact next value:

1. Calculate new retry time using backoff(retry_count + 1)
2. Use `clear_and_schedule()` to atomically replace timer with new delay
3. Set retry_count to new value using `set_retry_count(key_id, retry_count + 1)`
4. Update cache to `Deferred { retry_count: retry_count + 1 }`

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

Following the same pattern as `RetryConfiguration`, the defer middleware configuration uses builder pattern with
environment variable support:

```rust
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};

/// Configuration for defer middleware.
///
/// Uses separate environment variables from RetryMiddleware to allow
/// different backoff parameters for persistent vs transient failures.
#[derive(Builder, Clone, Debug, Validate)]
pub struct DeferConfiguration {
    /// Base exponential backoff delay for deferred retries.
    ///
    /// This is much longer than RetryMiddleware's base delay because
    /// deferred retries handle persistent failures that need time to
    /// recover (e.g., downstream service outages).
    ///
    /// Environment variable: `PROSODY_DEFER_BASE`
    /// Default: 1 minute
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_DEFER_BASE\", \
                   Duration::from_secs(60))?",
        setter(into)
    )]
    #[validate(range(min = 1_000))] // Minimum 1 second in millis
    pub base: Duration,

    /// Maximum retry delay for deferred retries.
    ///
    /// Caps the exponential backoff to prevent excessively long delays.
    ///
    /// Environment variable: `PROSODY_DEFER_MAX_DELAY`
    /// Default: 24 hours
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_DEFER_MAX_DELAY\", \
                   Duration::from_secs(24 * 60 * 60))?",
        setter(into)
    )]
    pub max_delay: Duration,

    /// Failure rate threshold for enabling deferral (0.0 to 1.0).
    ///
    /// When failure rate exceeds this threshold within the failure window,
    /// deferral is enabled to prevent cascading failures.
    ///
    /// Environment variable: `PROSODY_DEFER_FAILURE_THRESHOLD`
    /// Default: 0.9 (90%)
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_DEFER_FAILURE_THRESHOLD\", 0.9)?",
        setter(into)
    )]
    #[validate(range(min = 0.0, max = 1.0))]
    pub failure_threshold: f64,

    /// Time window for failure rate tracking.
    ///
    /// Failures are counted within this sliding window to determine
    /// whether to enable deferral.
    ///
    /// Environment variable: `PROSODY_DEFER_FAILURE_WINDOW`
    /// Default: 5 minutes
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_DEFER_FAILURE_WINDOW\", \
                   Duration::from_secs(5 * 60))?",
        setter(into)
    )]
    pub failure_window: Duration,

    /// Cache size for deferred key state.
    ///
    /// Caches the defer state (NotDeferred vs Deferred{retry_count}) for
    /// each key to avoid Cassandra reads on every message.
    ///
    /// Environment variable: `PROSODY_DEFER_CACHE_SIZE`
    /// Default: 10,000 keys
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_DEFER_CACHE_SIZE\", 10_000)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub cache_size: usize,

    /// Storage backend for deferred messages.
    ///
    /// Determines where deferred offsets are persisted. Uses the same
    /// CassandraConfiguration as the trigger store for consistency.
    #[builder(default = "DeferStoreConfiguration::InMemory")]
    pub store: DeferStoreConfiguration,
}

impl DeferConfiguration {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> DeferConfigurationBuilder {
        DeferConfigurationBuilder::default()
    }
}

/// Storage backend configuration for defer middleware.
#[derive(Debug, Clone)]
pub enum DeferStoreConfiguration {
    /// In-memory storage for testing and development.
    ///
    /// Uses HashMap-based storage that is lost on process restart.
    /// Not suitable for production use.
    InMemory,

    /// Cassandra-based persistent storage.
    ///
    /// Uses the same CassandraConfiguration as the trigger store,
    /// ensuring consistent connection pooling and TTL configuration.
    Cassandra(CassandraConfiguration),
}

impl Default for DeferStoreConfiguration {
    fn default() -> Self {
        Self::InMemory
    }
}
```

**Integration with high-level config:**

`DeferConfiguration` is embedded in `ModeConfiguration::Pipeline` alongside other middleware configs. Only Pipeline mode
uses defer middleware; LowLatency and BestEffort modes don't include it.

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

- `on_message(context, message, demand_type)` - Check if key deferred, defer message on failure. Uses
  `context.schedule()` to create
  initial DeferRetry timer.
- `on_timer(context, trigger, demand_type)` - Check timer type:
    - If DeferRetry timer: Load deferred message from Cassandra, load from Kafka, call `handler.on_message()` with the
      deferred message
    - If Application timer: Pass through to `handler.on_timer()`
    - Uses `context.clear_and_schedule()` to atomically replace DeferRetry timers. Timer auto-clears on successful
      return if no
      replacement scheduled.

### Timer Type Handling

The defer middleware uses Prosody's timer system with typed timers to distinguish DeferRetry timers from Application
timers:

**Timer types:**

- **DeferRetry timer**: Special timer type (TimerType::DeferRetry) created by DeferMiddleware to trigger deferred
  message retry
- **Application timer**: Regular timers (TimerType::Application) created by the business logic handler

**on_timer() behavior:**

When a timer fires, the middleware checks the timer type:

**DeferRetry timer:**

1. Extract key from trigger and generate key_id
2. Load next deferred offset and retry_count using `get_next_deferred_message`
3. Load message from Kafka using KafkaLoader
4. Call inner handler's `on_message()` with `DemandType::Failure`
5. On success:
    - Remove offset from store
    - Check if more messages exist
    - If more: Set retry_count=0, use `clear_and_schedule()` for next retry
    - If none: Update cache to NotDeferred, return Ok (timer auto-clears)
6. On failure:
    - Calculate new retry time with backoff(retry_count + 1)
    - Use `clear_and_schedule()` to replace timer with increased delay
    - Set retry_count to retry_count + 1
    - Update cache with new retry_count

**Application timer:**

- Pass through to inner handler's `on_timer()` unchanged

**Important:** All deferred retries use `DemandType::Failure` since they represent reprocessing of previously failed
events, consistent with how `RetryMiddleware` marks retry attempts.

**Shutdown:** When a partition is revoked, the timer system automatically handles cleanup. No separate event loop
to shut down.

### Error Handling and Invariant Recovery

**When clear_and_schedule fails (in on_timer handler):**

- Error propagates from on_timer() method by returning Err
- Current timer is NOT cleared (on_timer failed, so timer remains active)
- RetryMiddleware will retry the on_timer call with backoff
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

**Writes:**

```cql
-- First failure: Append offset with TTL AND set retry_count to 1 (single query)
-- TTL = (expected_retry_time - now) + base_ttl
-- Calculated using CassandraStore::calculate_ttl(expected_retry_time)
-- This uses Prosody's existing PROSODY_CASSANDRA_RETENTION config (default: 30 days)
INSERT INTO deferred_messages (key_id, offset, retry_count)
VALUES (?, ?, 1)
USING TTL ?;

-- Subsequent messages while deferred: Append offset without touching retry_count
INSERT INTO deferred_messages (key_id, offset)
VALUES (?, ?)
USING TTL ?;

-- Update retry_count to explicit value (after timer retry or reset)
-- Used for failures: SET retry_count = (current + 1)
-- Used for success with more messages: SET retry_count = 0
UPDATE deferred_messages SET retry_count = ?
WHERE key_id = ?;
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
4. If more messages exist: Calculate next time, use `context.clear_and_schedule()` to replace timer, remove offset,
   return Ok (maintains
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

### Test Configuration

Property tests use QuickCheck for randomized testing:

- **In-memory tests**: Use default QuickCheck configuration (respects `QUICKCHECK_TESTS` env var, default 100 tests)
    - Example: `prop_defer_cache_store_consistency`, `prop_store_consistency`
    - Fast execution since no external dependencies
    - Default test count is sufficient for coverage

- **Integration tests**: Use environment-driven test counts with fallback defaults
    - Configured via `INTEGRATION_TESTS` env var
    - Lower defaults for CI/local development (faster feedback)
    - Higher counts for exhaustive testing

- **Performance**: All defer tests complete in ~12 seconds with default settings (100 property test iterations)
- Tests hanging for 60+ seconds indicate bugs - investigate immediately

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

## Implementation Plan

This section breaks down the implementation into small, incremental steps that maintain compilation and test coverage
throughout development.

### Phase 1: Foundation (Core Types and Configuration)

**Goal:** Establish data structures, errors, and configuration that everything else will build on.

**Step 1.1: Create module structure**

- Location: `src/consumer/middleware/defer/mod.rs`
- Create module with submodules: `config`, `error`, `store`, `cache`, `failure_tracker`, `handler`
- Ensure project compiles (empty modules)

**Step 1.2: Define error types**

- Location: `src/consumer/middleware/defer/error.rs`
- Define `DeferError` enum using `thiserror`:
    - `StoreError` - wraps store-specific errors
    - `TimerError` - wraps timer system errors
    - `KafkaLoaderError` - wraps message loading errors
    - `ConfigurationError` - invalid configuration
- Implement `ClassifyError` trait (Permanent vs Transient)
- Unit tests for error classification
- Ensure project compiles and tests pass

**Step 1.3: Define DeferState enum**

- Location: `src/consumer/middleware/defer/cache.rs`
- Define `DeferState` enum:
  ```rust
  pub enum DeferState {
      NotDeferred,
      Deferred { retry_count: u32 },
  }
  ```
- Derive `Clone, Debug, PartialEq`
- Basic unit tests
- Ensure project compiles and tests pass

**Step 1.4: Implement configuration**

- Location: `src/consumer/middleware/defer/config.rs`
- Define `DeferConfiguration` with builder pattern
- Define `DeferStoreConfiguration` enum (InMemory, Cassandra)
- Use `from_duration_env_with_fallback` and `from_env_with_fallback` for environment variable support
- Add validation using `validator` crate
- Unit tests for configuration building and validation
- Ensure project compiles and tests pass

**Step 1.5: UUID generation helper** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/mod.rs`
- Implement `generate_key_id()` function using UUIDv5
- Takes: consumer_group, topic, partition, key
- Returns: Uuid
- Unit tests verifying deterministic generation (6 tests)
- Status: Implemented with tests for determinism, consumer group isolation, partition isolation, topic isolation, and
  key isolation
- Ensure project compiles and tests pass ✅

**Phase Completion Checklist:**

- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 2: Store Trait and In-Memory Implementation ✅ **COMPLETE**

**Goal:** Define storage contract and implement testable in-memory version.

**Step 2.1: Define DeferStore trait** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/store/mod.rs`
- Define trait with 4 methods (all using `&Uuid` for key_id):
    - `get_next_deferred_message(key_id: &Uuid) -> Option<(Offset, u32)>`
    - `append_deferred_message(key_id: &Uuid, offset, expected_retry_time, retry_count: Option<u32>)`
    - `remove_deferred_message(key_id: &Uuid, offset)`
    - `set_retry_count(key_id: &Uuid, retry_count)`
- Associated `Error` type
- Document TTL calculation expectations
- Status: Trait defined with full documentation matching TriggerStore pattern
- Ensure project compiles ✅

**Step 2.2: Implement MemoryDeferStore** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/store/memory.rs`
- Use `scc::HashMap<Uuid, (BTreeMap<Offset, Instant>, u32)>` for storage
    - BTreeMap for sorted offsets (oldest first)
    - u32 for shared retry_count
    - Instant tracks expiry (simulates TTL)
- Implement all 4 trait methods
- Thread-safe using `scc::HashMap` (lock-free)
- Status: Fully implemented with lock-free concurrent access
- Ensure project compiles ✅

**Step 2.3: Unit tests for MemoryDeferStore** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/store/memory.rs` (test module)
- Test each method (7 tests):
    - `test_get_nonexistent_key` - returns None for unknown key
    - `test_append_and_get` - append with retry_count and retrieve
    - `test_multiple_offsets_returns_oldest` - BTreeMap ordering works
    - `test_remove_offset` - removal works correctly
    - `test_remove_nonexistent` - idempotent removal
    - `test_set_retry_count` - updates retry counter
    - `test_concurrent_access` - thread safety verified
- All edge cases covered
- Status: 7 tests passing, comprehensive coverage
- Ensure all tests pass ✅

**Phase Completion Checklist:**

- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 3: Failure Tracking

**Goal:** Implement sliding window failure rate tracker.

**Step 3.1: Implement FailureTracker**

- Location: `src/consumer/middleware/defer/failure_tracker.rs`
- Structure:
    - `VecDeque<Instant>` for successes
    - `VecDeque<Instant>` for failures
    - `window: Duration`
    - `threshold: f64`
- Methods:
    - `record_success()`
    - `record_failure()`
    - `is_deferring_enabled() -> bool`
    - `cleanup_old_events()` (private)
- Ensure project compiles

**Step 3.2: Unit tests for FailureTracker**

- Location: `src/consumer/middleware/defer/failure_tracker.rs` (test module)
- Test scenarios:
    - Empty tracker allows deferring
    - High failure rate (>= threshold) disables deferring
    - Old events outside window are ignored
    - Sliding window behavior (time-based)
- Use `tokio::time::pause()` for deterministic time testing
- Ensure all tests pass

**Phase Completion Checklist:**

- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 4: Middleware Structure (Needs UUID Integration)

**Goal:** Define middleware types and integrate with trait system, adding consumer_group for UUID generation.

**Step 4.1: Add consumer_group to DeferMiddleware** ⚠️ **IN PROGRESS**

- Location: `src/consumer/middleware/defer/handler.rs` (DeferMiddleware struct)
- Add field: `consumer_group: Arc<str>` to DeferMiddleware struct
- Update `DeferMiddleware::new()` signature to accept `consumer_group: String`
- Store as `Arc<str>` for efficient cloning
- Update `with_provider()` to pass consumer_group to DeferProvider
- Ensure project compiles

**Step 4.2: Add consumer_group to DeferProvider** ⚠️ **PENDING**

- Location: `src/consumer/middleware/defer/handler.rs` (DeferProvider struct)
- Add field: `consumer_group: Arc<str>` to DeferProvider struct
- Update `with_provider()` in HandlerMiddleware impl to pass consumer_group
- Update `handler_for_partition()` to pass consumer_group to DeferHandler
- Ensure project compiles

**Step 4.3: Add topic/partition/consumer_group to DeferHandler** ⚠️ **PENDING**

- Location: `src/consumer/middleware/defer/handler.rs` (DeferHandler struct)
- Add fields:
    - `topic: Topic`
    - `partition: Partition`
    - `consumer_group: Arc<str>`
- Update both `handler_for_partition()` implementations (FallibleHandlerProvider and HandlerProvider)
  to capture topic, partition, and consumer_group
- These fields are needed for `generate_key_id()` calls in on_message/on_timer
- Ensure project compiles

**Step 4.4: Verify middleware structure compiles** ⚠️ **PENDING**

- Run `cargo build` to verify all changes compile
- Existing tests may fail (expected - will fix in Phase 5/6)
- Goal: Ensure structure is correct before adding logic

**Phase Completion Checklist:**

- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 5: Message Handling (on_message) ✅ **COMPLETE** (integration tests pending)

**Goal:** Implement full on_message logic with defer capability.

**Step 5.1: Implement cache lookup logic** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/handler.rs`
- In `on_message()`:
    - ✅ Check cache/store BEFORE calling handler (line 339-386)
        - Checks if key is deferred, appends offset to queue and returns Ok immediately
    - ✅ On cache miss, query store and update cache (line 348-362)
        - Queries `store.get_next_deferred_message()` on cache miss
        - Updates cache with store state if key has deferred messages
    - ✅ Generate key_id - working correctly
- Ensure project compiles ✅

**Step 5.2: Implement first failure deferral** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/handler.rs`
- In `on_message()` when handler fails:
    - ✅ Check `failure_tracker.should_defer()` (line 369)
    - ✅ If disabled: propagate error (line 370-372)
    - ✅ If enabled: defer message (line 375)
        - ✅ Calculate retry_time using backoff (line 253)
        - ✅ Schedule timer using `context.schedule()` (line 282-285)
        - ✅ Call `store.append_deferred_message(key_id, offset, retry_time, Some(0))` for first failure (line 267-275)
        - ✅ Update cache to `Deferred { retry_count }` (line 278-279)
        - ✅ Record failure in tracker (line 364)
        - ✅ Return Ok (line 376)
- Ensure project compiles ✅

**Step 5.3: Integration tests for on_message** ❌ **NOT STARTED**

- Location: `tests/defer_on_message.rs`
- Test scenarios:
    - First failure defers message and schedules timer
    - Subsequent messages for deferred key are queued
    - High failure rate disables deferring
    - Cache miss populates cache correctly
    - Errors propagate when deferring disabled
- Use MemoryDeferStore and mock timer system
- Ensure all tests pass

**Step 5.4: Implement backoff calculation** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/handler.rs`
- ✅ Implement exponential backoff with cap (line 196-224)
- ✅ Full jitter algorithm implemented (line 211-218)
    - Uses `rand::thread_rng().gen_range(0..=capped_seconds)`
    - Prevents thundering herd when many keys retry simultaneously
- Unit tests for backoff calculation - NOT STARTED (can be added later)
- Ensure tests pass

**Phase Completion Checklist:**

- [x] All functionality implemented
- [x] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete
- **Note:** Mock test infrastructure (`MockHandler`, `MockContext`, `MockError`, `MockBehavior`) has
  `#[allow(dead_code)]` - to be removed when Phase 6+ integration tests are implemented

### Phase 6: Timer Handling (on_timer) ✅ **COMPLETE** (integration tests pending)

**Goal:** Implement DeferRetry timer handling and message retry logic.

**Step 6.1: Implement timer type check** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/handler.rs`
- In `on_timer()`:
    - ✅ Check `trigger.timer_type` (line 399)
    - ✅ If `Application`: pass through to inner handler (line 401-405)
    - ✅ If `DeferRetry`: continue to defer logic (line 408+)
- Ensure project compiles ✅

**Step 6.2: Implement deferred message loading** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/handler.rs`
- In `on_timer()` for DeferRetry:
    - ✅ Extract key from trigger (line 409)
    - ✅ Generate key_id (line 412-417)
    - ✅ Call `store.get_next_deferred_message()` (line 420)
    - ✅ If None: update cache to NotDeferred, return Ok (line 422-427)
    - ✅ Load message from Kafka using `kafka_loader` (line 434-437)
    - ✅ Verify key matches (sanity check) (line 440-449)
- Ensure project compiles ✅

**Step 6.3: Implement success path** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/handler.rs`
- In `on_timer()` when handler succeeds:
    - ✅ Remove offset from store (line 530-533)
    - ✅ Check for more messages (line 536-541)
    - ✅ If more: `set_retry_count(key_id, 0)` (line 545-548)
    - ✅ If more: Calculate next_time with backoff(0) (line 553)
    - ✅ If more: `context.clear_and_schedule(next_time, TimerType::DeferRetry)` (line 560-563)
        - Schedules timer for next deferred message with backoff(0)
    - ✅ If more: Update cache to `Deferred { retry_count: 0 }` (line 549-550)
    - ✅ If none: Update cache to NotDeferred (line 571)
    - ✅ Record success in tracker (line 527)
    - ✅ Return Ok (line 579)
- Ensure project compiles ✅

**Step 6.4: Implement failure path** ✅ **COMPLETE**

- Location: `src/consumer/middleware/defer/handler.rs`
- In `on_timer()` when handler fails:
    - ✅ Calculate retry_time with backoff(retry_count + 1) (line 614-619)
    - ✅ `context.clear_and_schedule(retry_time, TimerType::DeferRetry)` (line 621-624)
        - Uses `clear_and_schedule()` to atomically replace timer
    - ✅ `set_retry_count(key_id, retry_count + 1)` (line 600-603)
    - ✅ Update cache to `Deferred { retry_count: retry_count + 1 }` (line 606-611)
    - ✅ Record failure in tracker (line 582)
    - ✅ Return Ok (line 631)
- Ensure project compiles ✅

**Step 6.5: Integration tests for on_timer** ❌ **NOT STARTED**

- Location: `tests/defer_on_timer.rs`
- Test scenarios:
    - DeferRetry timer loads and retries message
    - Success with no more messages clears state
    - Success with more messages schedules next retry
    - Failure increases backoff and retry_count
    - Application timers pass through unchanged
- Use MemoryDeferStore and test harness
- Ensure all tests pass

**Phase Completion Checklist:**

- [x] All functionality implemented
- [ ] All unit tests passing (integration tests pending)
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete
- **Note:** Mock test infrastructure (`MockHandler`, `MockContext`, `MockError`, `MockBehavior`) has
  `#[allow(dead_code)]` - to be removed when integration tests are implemented

### Phase 7: Property-Based Testing

**Goal:** Verify defer middleware invariants using property-based testing, following the timer store pattern with two
test levels.

**Pattern:** Timer store uses two-level property testing:

1. **Store-level tests**: Prove MemoryDeferStore and CassandraDeferStore are equivalent (model-based)
2. **High-level tests**: Test full middleware with MemoryDeferStore only (equivalence already proven)

This approach provides thorough coverage without requiring complex mocking infrastructure.

**Step 7.1: Store-level property tests**

- Location: `src/consumer/middleware/defer/tests.rs::prop_defer_store_model_equivalence`
- Goal: Prove MemoryDeferStore and CassandraDeferStore produce identical results
- Pattern: Model-based testing (in-memory HashMap as model)
- Operations:
    - `get_deferred_offset(key)` → `Option<DeferredOffset>`
    - `set_deferred_offset(key, offset)` → `()`
    - `delete_deferred_offset(key)` → `()`
- Test structure:
    - Generate random operation sequences (20-50 operations)
    - Apply to both Memory store and Cassandra store
    - Compare results against in-memory HashMap model
    - Verify both stores match model exactly
- Ensures: Store implementations are interchangeable

**Step 7.2: Component-level property tests (existing)**

- Location: `src/consumer/middleware/defer/tests.rs`
- Status: **Already complete** ✅
- Tests:
    - `prop_backoff_monotonic` - Exponential backoff increases monotonically
    - `prop_failure_tracker_threshold` - Threshold logic is correct
    - `prop_store_consistency` - Store operations maintain consistency
- These tests verify individual components in isolation

**Step 7.3: High-level middleware property tests**

- Location: `src/consumer/middleware/defer/tests.rs::prop_defer_middleware_invariants`
- Goal: Verify defer middleware maintains critical invariants
- Pattern: Real implementations (no mocking), operation sequences
- Components used:
    - Real `DeferHandler` with `TestHandler` (configurable behavior)
    - Real `MemoryDeferStore` (equivalence to Cassandra already proven)
    - Real `FailureTracker` (sliding window)
    - Real `Cache<Key, DeferState>` (quick_cache)
    - Mock `EventContext` (minimal - only tracks scheduled timers)
- Operations:
    - `ProcessMessage { key, offset }` - Process a message (may succeed/fail based on handler behavior)
    - `FireTimer { key }` - Fire a defer retry timer
    - `SetBehavior(Success | FailPermanent | FailTransient)` - Change handler behavior
    - `VerifyInvariants` - Check invariants hold
- Test structure:
    - Generate 2-5 test keys
    - Generate 10-30 random operations (70% message, 20% timer, 10% behavior)
    - Execute operation sequence with real components
    - Verify invariants after each operation:
        1. **Cache consistency**: `cache.get(key)` matches `store.get_deferred_offset(key)`
        2. **Timer coverage**: If store has messages for key, timer must be scheduled
        3. **Retry count monotonicity**: `retry_count` never decreases for a key
- Ensures: Full middleware maintains invariants under realistic operation sequences

**Step 7.4: Run all property tests**

- Verify all tests pass with QuickCheck (100 test cases per property)
- Zero clippy warnings
- Tests run in CI

**Phase Completion Checklist:**

- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 8: Cassandra Store Implementation

**Goal:** Implement production-ready Cassandra-backed storage.

**Step 8.1: Define Cassandra schema migration**

- Location: `src/consumer/middleware/defer/store/cassandra/schema.rs`
- Create table definition:
  ```cql
  CREATE TABLE deferred_messages (
      key_id uuid,
      offset bigint,
      retry_count int static,
      PRIMARY KEY (key_id, offset)
  ) WITH CLUSTERING ORDER BY (offset ASC);
  ```
- Schema migration helper
- Ensure compiles

**Step 8.2: Define prepared queries**

- Location: `src/consumer/middleware/defer/store/cassandra/queries.rs`
- Prepare CQL statements:
    - `SELECT offset, retry_count FROM deferred_messages WHERE key_id = ? LIMIT 1`
    - `INSERT INTO deferred_messages (key_id, offset, retry_count) VALUES (?, ?, ?) USING TTL ?`
    - `INSERT INTO deferred_messages (key_id, offset) VALUES (?, ?) USING TTL ?`
    - `UPDATE deferred_messages SET retry_count = ? WHERE key_id = ?`
    - `DELETE FROM deferred_messages WHERE key_id = ? AND offset = ?`
- Ensure compiles

**Step 8.3: Implement CassandraDeferStore**

- Location: `src/consumer/middleware/defer/store/cassandra/mod.rs`
- Structure:
    - `cassandra_store: CassandraStore` (for calculate_ttl)
    - `session: Arc<Session>`
    - `queries: Arc<PreparedQueries>`
- Implement all 4 trait methods
- TTL calculation using `cassandra_store.calculate_ttl(expected_retry_time)`
- Handle static column NULL values properly
- Ensure compiles

**Step 8.4: Unit tests for CassandraDeferStore**

- Location: `src/consumer/middleware/defer/store/cassandra/mod.rs` (test module)
- Use Scylla testcontainer or local Cassandra
- Test each method with actual Cassandra:
    - Insert with retry_count sets static column
    - Insert without retry_count preserves static column
    - get_next returns oldest offset with retry_count
    - set_retry_count updates static column
    - TTL expiry works correctly
- Ensure all tests pass

**Step 8.5: Integration tests with Cassandra store**

- Location: `tests/defer_cassandra.rs`
- Run subset of end-to-end tests with CassandraDeferStore
- Verify persistence across "restarts" (new store instance)
- Ensure all tests pass

**Phase Completion Checklist:**
- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 9: KafkaLoader (If Needed)

**Goal:** Implement or integrate message loading from Kafka by offset.

**Step 9.1: Check if KafkaLoader exists**

- Search codebase for existing implementation
- If exists: verify API matches requirements
- If not: implement KafkaLoader

**Step 9.2: Implement KafkaLoader (if needed)**

- Location: `src/consumer/kafka_loader.rs`
- Structure:
    - `consumer: Arc<BaseConsumer>`
    - `semaphore: Arc<Semaphore>` (concurrency control)
    - `cache: Arc<Cache<(Topic, Partition, Offset), DecodedMessage>>`
- Method: `load_message(topic, partition, offset) -> DecodedMessage`
- Handle `OffsetDeleted` error
- Ensure compiles

**Step 9.3: Unit tests for KafkaLoader**

- Location: `src/consumer/kafka_loader.rs` (test module)
- Test message loading
- Test caching behavior
- Test backpressure (semaphore)
- Ensure tests pass

**Phase Completion Checklist:**
- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 10: Integration with Pipeline Consumer

**Goal:** Wire defer middleware into the consumer pipeline.

**Step 10.1: Add defer configuration to ModeConfiguration**

- Location: `src/config.rs` (or wherever ModeConfiguration lives)
- Add `defer: DeferConfiguration` to `Pipeline` variant
- Update configuration builders
- Ensure compiles

**Step 10.2: Instantiate defer middleware in pipeline_consumer**

- Location: `src/consumer/mod.rs`
- Create DeferMiddleware with configured store
- Layer into middleware stack: `common_middleware.layer(monopolization).layer(defer).layer(retry)`
- Ensure compiles

**Step 10.3: Integration test with full pipeline**

- Location: `tests/pipeline_with_defer.rs`
- Test defer middleware in actual consumer context
- Verify interaction with RetryMiddleware
- Verify timer system integration
- Ensure tests pass

**Phase Completion Checklist:**
- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 11: Metrics and Observability

**Goal:** Add telemetry for monitoring defer middleware behavior.

**Step 11.1: Define metrics**

- Location: `src/consumer/middleware/defer/metrics.rs`
- Define counters, gauges, histograms:
    - `deferred_messages_total`
    - `retry_successes_total`
    - `retry_failures_total`
    - `deferred_keys_active`
    - `failure_rate`
    - `deferring_enabled`
    - `retry_count_distribution`
- Ensure compiles

**Step 11.2: Instrument middleware**

- Location: `src/consumer/middleware/defer/handler.rs`
- Add metric calls at key points:
    - Message deferred
    - Retry success/failure
    - Cache state changes
- Ensure compiles and tests pass

**Step 11.3: Metrics tests**

- Location: `tests/defer_metrics.rs`
- Verify metrics are emitted correctly
- Ensure tests pass

**Phase Completion Checklist:**
- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

### Phase 12: Documentation and Final Touches

**Goal:** Complete documentation and prepare for production use.

**Step 12.1: API documentation**

- Add rustdoc comments to all public types and methods
- Include examples in doc comments
- Ensure `cargo doc` builds without warnings

**Step 12.2: Integration documentation**

- Update main README with defer middleware section
- Document configuration options
- Provide deployment examples

**Step 12.3: Runbook**

- Document operational procedures
- Common failure scenarios and resolutions
- Monitoring and alerting recommendations

**Step 12.4: Final review**

- Run full test suite
- Run Clippy with zero warnings
- Format code with rustfmt
- Verify all lints pass

**Phase Completion Checklist:**
- [ ] All functionality implemented
- [ ] All unit tests passing
- [ ] `cargo clippy` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] `cargo clippy --tests` passes with zero warnings (no `#[allow(...)]` without justification)
- [ ] Documentation complete

---

**Document Version:** 2.2
**Last Updated:** 2025-01-21
**Author:** Design Team
**Status:** Ready for Implementation
**Changes:**

- Removed separate TimerManager - uses built-in timer system with DeferRetry timer type
- Uses `context.schedule()` for initial DeferRetry timer creation
- Uses `context.clear_and_schedule()` for atomic timer replacement in on_timer handler
- Timer auto-clears on successful on_timer() return if no replacement scheduled
- Simplified architecture by leveraging existing timer infrastructure
- All code examples updated to match actual FallibleHandler API signatures
- Replaced increment/reset with explicit `set_retry_count` for efficient Cassandra operations
- Added optional retry_count parameter to `append_deferred_message` for single-query first failure
- Added detailed implementation plan with 12 phases
