# Defer Middleware Design Document

## Executive Summary

This document proposes a persistent, per-key retry mechanism (Defer Middleware) for Prosody's **pipeline mode** that provides long-term retry capabilities with Cassandra-backed persistence. The middleware wraps the inner handler stack, enabling graceful degradation from immediate retries to persistent deferred retries.

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

1. **Persistent deferred retries**: Store failed message offsets and timer times in Cassandra for long-term retry scheduling
2. **Per-key isolation**: Track retries independently per key to prevent one failing key from affecting others
3. **Adaptive retry scheduling**: Exponential backoff with increasing delays (seconds → minutes → hours)
4. **Health-based failover**: Disable deferring if failure rate exceeds threshold (default 90% over 5 minutes)
5. **Order preservation**: Process deferred items in offset/time order per key
6. **Zero data duplication**: Leverage existing Kafka and timer system storage

### Non-Goals

- Replace existing `RetryMiddleware` (they work together)
- Provide exactly-once semantics (maintain existing at-least-once)
- Support cross-partition deferred batching
- Guarantee infinite retries (after extended retries, messages may be lost)

## High-Level Design

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   DeferMiddleware                            │
│                                                               │
│  ┌──────────────┐  ┌─────────────┐  ┌────────────────────┐  │
│  │  Cassandra   │  │   Kafka     │  │  Retry Timer       │  │
│  │   Store      │  │  Loader     │  │   Manager          │  │
│  │ (deferred    │  │ (load by    │  │ (schedule retry)   │  │
│  │  offsets/    │  │  offset)    │  │                    │  │
│  │  times)      │  │             │  │                    │  │
│  └──────────────┘  └─────────────┘  └────────────────────┘  │
│         │                 │                    │              │
│         └─────────────────┴────────────────────┘              │
│                           │                                   │
│                  ┌────────▼────────┐                          │
│                  │ Per-Partition   │                          │
│                  │  Event Loop     │                          │
│                  │ (order + retry) │                          │
│                  └────────┬────────┘                          │
│                           │                                   │
└───────────────────────────┼───────────────────────────────────┘
                            │
                            ▼
               Inner Handler Stack
          (monopolization, shutdown, etc.)
                            │
                            ▼
                    Business Handler
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
1. RetryMiddleware (outermost - retries defer/Cassandra failures)
2. DeferMiddleware (NEW - persistent deferred retries)
3. MonopolizationMiddleware (detect monopolies)
4. ShutdownMiddleware (graceful shutdown)
5. SchedulerMiddleware (global concurrency control)
6. TimeoutMiddleware (stall detection)
7. TelemetryMiddleware (observability)
8. Handler (innermost - business logic)

**Execution flow:**
1. Handler fails
2. Error propagates through telemetry, timeout, scheduler, shutdown, monopolization
3. DeferMiddleware receives error
   - Check failure tracker: is deferring enabled?
     - If disabled (>90% failure rate): re-raise error to RetryMiddleware
     - If enabled: defer to Cassandra, schedule retry timer, return Ok
4. If defer operation fails (Cassandra unavailable): error propagates to RetryMiddleware
5. RetryMiddleware retries the defer operation with exponential backoff
6. If all retries exhausted: error is logged and offset is committed (message lost)

**Deferred message flow:**
1. Message arrives for deferred key
2. DeferMiddleware checks cache/Cassandra: key is deferred
3. Append offset to Cassandra wide row
4. Return Ok (message queued for later retry)
5. Kafka offset committed

**Retry timer flow:**
1. Background retry loop receives timer event
2. Load next offset from Cassandra
3. Load message from Kafka via KafkaLoader
4. Re-inject into handler pipeline
5. On success: remove from deferred queue
6. On failure: increment retry_count, reschedule with longer delay

## Detailed Design

### Component Architecture

#### Shared Components (All Partitions)

```rust
/// Cassandra store for deferred messages and timers (connection pool)
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

/// Cache of which keys have deferred items (shared for efficiency)
type DeferredCache = Cache<Key, bool, UnitWeighter, RandomState>;
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

    /// Timer manager for scheduling retry timers (per-partition)
    retry_timer_manager: TimerManager<CassandraTriggerStore>,

    /// Optional: application timer manager for rescheduling (per-partition)
    app_timer_manager: Option<TimerManager<impl TriggerStore>>,

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

    /// Retry event loop state
    retry_loop_shutdown: Arc<Notify>,
}
```

### Storage Schema

#### Cassandra Tables

**Table 1: `deferred_messages`**

```cql
CREATE TABLE deferred_messages (
    key_id uuid,              -- UUIDv5(consumer_group || topic || partition || key)
    offset bigint,            -- Kafka offset (clustering key, sorted ascending)

    -- Metadata (static per key_id)
    consumer_group text static,
    topic text static,
    partition int static,
    key text static,
    retry_count int static,   -- Shared across all offsets for this key

    -- Per-offset metadata
    added_at timestamp,       -- When this offset was deferred

    PRIMARY KEY (key_id, offset)
) WITH CLUSTERING ORDER BY (offset ASC)
  AND comment = 'Deferred Kafka messages awaiting retry';

-- Index for cleanup queries
CREATE INDEX ON deferred_messages (added_at);
```

**Table 2: `deferred_timers`**

```cql
CREATE TABLE deferred_timers (
    key_id uuid,              -- UUIDv5(segment_id || key)
    time int,                 -- CompactDateTime (clustering key, sorted ascending)

    -- Metadata (static per key_id)
    segment_id uuid static,
    key text static,
    retry_count int static,   -- Shared across all times for this key

    -- Per-time metadata
    added_at timestamp,       -- When this time was deferred

    PRIMARY KEY (key_id, time)
) WITH CLUSTERING ORDER BY (time ASC)
  AND comment = 'Deferred timer events awaiting retry';

-- Index for cleanup queries
CREATE INDEX ON deferred_timers (added_at);
```

**Schema Design Rationale:**

1. **Wide Row Pattern**: Each row can contain multiple offsets/times for a key, stored as clustering columns
2. **Static Columns**: `retry_count` is static, meaning it's shared across all clustering rows (offsets/times) for a `key_id`
3. **Sorted Clustering**: `ORDER BY offset/time ASC` ensures FIFO processing
4. **UUID Partition Key**: Distributes load across Cassandra cluster
5. **No Data Duplication**: Only stores coordinates (offset/time), not actual message/timer data

#### UUID Generation

UUIDs are generated deterministically using UUIDv5:

- **Messages**: `UUIDv5(NAMESPACE_OID, "{consumer_group}:{topic}:{partition}:{key}")`
- **Timers**: `UUIDv5(segment_id, key)`

This ensures the same logical key always maps to the same `key_id` partition in Cassandra.

### Store Trait

The `DeferStore` trait provides storage operations for deferred messages and timers:

**Message operations:**
- `has_deferred_messages(key_id)` - Check if key has pending retries
- `append_deferred_message(key_id, metadata, offset)` - Add offset to queue
- `get_next_deferred_message(key_id)` - Get oldest offset for retry
- `remove_deferred_message(key_id, offset)` - Remove after successful retry
- `increment_retry_count(key_id)` - Bump shared retry counter

**Timer operations:** (similar pattern for timers)
- `append_deferred_timer(key_id, metadata, time)`
- `get_next_deferred_timer(key_id)`
- `remove_deferred_timer(key_id, time)`

All operations are async and return `Result<T, Self::Error>`.

### Message Processing Flow

#### Normal Message Path (First Failure)

```
1. Message arrives
         │
         ▼
2. DeferMiddleware::on_message()
         │
         ├─→ Check cache: is key deferred?
         │   ├─→ Yes: append offset to Cassandra, return Ok
         │   └─→ No: continue
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
         ├─→ Append offset to Cassandra
         ├─→ Schedule retry timer (delay = backoff(retry_count))
         ├─→ Update cache: key is deferred
         ├─→ Record failure in tracker
         ├─→ Return Ok (error handled)
         │
         ▼
6. Kafka offset committed
```

#### Subsequent Messages for Deferred Key

```
1. Message arrives for deferred key
         │
         ▼
2. DeferMiddleware::on_message()
         │
         ├─→ Check cache: is key deferred?
         │   └─→ Yes: hit!
         │
         ▼
3. Append offset to Cassandra
         │   (adds to existing wide row)
         │
         ▼
4. Return Ok (message queued)
         │
         ▼
5. Kafka offset committed
```

#### Retry Timer Fires

```
1. Retry timer fires
         │
         ▼
2. Background retry loop receives timer
         │
         ▼
3. Load next offset from Cassandra
         │   (first offset in clustering order)
         │
         ▼
4. Load message from Kafka (via KafkaLoader)
         │
         ▼
5. Call inner handler (monopolization → shutdown → scheduler → ... → business handler)
         │
         ├─→ Success:
         │   ├─→ Remove offset from Cassandra
         │   ├─→ Check if more offsets exist
         │   │   ├─→ Yes: schedule next retry timer
         │   │   └─→ No: clear from cache
         │   ├─→ Record success in tracker
         │   └─→ Commit retry timer
         │
         └─→ Failure:
             ├─→ Increment retry_count in Cassandra
             ├─→ Schedule new retry timer with increased delay
             ├─→ Record failure in tracker
             └─→ Commit retry timer
```

### Timer Processing Flow

#### Normal Timer Path (First Failure)

```
1. Timer fires
         │
         ▼
2. DeferMiddleware::on_timer()
         │
         ├─→ Check cache: is key deferred?
         │   ├─→ Yes: append time to Cassandra, return Ok
         │   └─→ No: continue
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
5. Defer the timer
         │
         ├─→ Generate key_id from (segment_id, key)
         ├─→ Append time to Cassandra
         ├─→ Schedule retry timer
         ├─→ Update cache: key is deferred
         ├─→ Record failure in tracker
         ├─→ Return Ok (error handled)
         │
         ▼
6. Timer committed in EventHandler
```

#### Retry Timer Fires for Timer

```
1. Retry timer fires
         │
         ▼
2. Background retry loop receives timer
         │
         ▼
3. Load next time from Cassandra
         │
         ▼
4. Reschedule original timer
         │   (requires app_timer_manager)
         │   └─→ TimerManager::schedule(Trigger(key, time, span))
         │
         ▼
5. Original timer fires immediately (past time)
         │
         ▼
6. Handler processes via normal path
         │
         ├─→ Success:
         │   ├─→ Remove time from Cassandra
         │   ├─→ Delete from timer store if needed
         │   ├─→ Check if more times exist
         │   └─→ Record success
         │
         └─→ Failure:
             ├─→ Increment retry_count
             ├─→ Schedule new retry timer
             └─→ Record failure
```

### Failure Rate Tracking

The `FailureTracker` tracks success/failure events over a sliding time window to detect when the defer system itself is unhealthy:

**Structure:**
- `VecDeque<(Instant, bool)>` - Ring buffer of (timestamp, was_success) events
- Configurable window (default: 5 minutes)
- Configurable threshold (default: 90%)
- Max 10,000 events (prevent unbounded growth)

**Behavior:**
- `record_success()` / `record_failure()` - Add events, cleanup old events
- `is_deferring_enabled()` - Calculate: `failure_rate = failures / total`
- If failure_rate ≥ 90%: disable deferring, errors propagate to outer RetryMiddleware
- Old events automatically removed when outside window

### Retry Delay Calculation

Uses **Full Jitter** algorithm (same as existing `RetryMiddleware`):

```rust
fn sleep_time(&self, retry_count: u32) -> Duration {
    // Exponential backoff capped at max_delay
    let exp_backoff = min(
        2^retry_count * base_delay,
        max_delay
    );

    // Full jitter: random(0, exp_backoff)
    random(0..exp_backoff)
}
```

This is the "Full Jitter" algorithm from [AWS's Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) article, which provides optimal distribution of retries and prevents thundering herd.

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

The `DeferredCache` avoids repeated Cassandra queries for keys with pending retries:

**Cache operations:**
- `is_key_deferred(key)` - Check cache first, query Cassandra on miss
- `mark_deferred(key)` - Mark key as having pending retries
- `clear_deferred(key)` - Remove when all retries complete

**Configuration:**
- Size: 10,000 keys (configurable)
- Type: `Cache<Key, bool>` using quick_cache
- Eviction: S3-FIFO (automatic)
- Shared across all partitions for memory efficiency

### Configuration

```rust
#[derive(Builder, Clone, Debug, Validate)]
pub struct DeferConfiguration {
    pub consumer_group: String,
    pub backoff: RetryBackoff,                    // Full Jitter backoff params
    pub failure_window: Duration,                 // Default: 5 min
    pub failure_threshold: f64,                   // Default: 0.9 (90%)
    pub cache_size: usize,                        // Default: 10,000
    pub enable_timer_deferring: bool,             // Default: false
    pub loader_config: LoaderConfiguration,       // Kafka loader settings
    pub retry_timer_segment_id: Uuid,             // Retry timer storage
    pub retry_timer_slab_size: CompactDuration,
}

pub struct RetryBackoff {
    pub base_delay: Duration,      // Default: 1 minute (much longer than RetryMiddleware's 20ms)
    pub max_delay: Duration,       // Default: 24 hours (much longer than RetryMiddleware's 5min)
}
```

**Note:** Defer middleware uses the same **Full Jitter** algorithm as `RetryMiddleware` but with much longer delays, since it handles persistent failures rather than transient ones. The immediate retry middleware uses base=20ms, max=5min for quick recovery from transient issues, while defer uses base=1min, max=24h for long-term persistent failures.

### Middleware Implementation

The defer middleware follows Prosody's standard middleware pattern:

**DeferMiddleware** - Shared state across all partitions:
- `DeferStore` - Cassandra/memory store instance
- `KafkaLoader` - Shared message loader
- `DeferredCache` - Shared cache of deferred keys
- `DeferConfiguration` - Settings

**DeferHandler** - Per-partition handler instance:
- Wraps inner handler
- Creates per-partition `TimerManager` for retry timers
- Spawns background retry event loop
- Maintains per-partition `FailureTracker`

**Key methods:**
- `on_message()` - Check if key deferred, defer on failure
- `on_timer()` - Check if key deferred, defer on failure
- Background retry loop processes fired retry timers

### Retry Event Loop

Each partition runs a background task that processes retry timers:

**Main loop:**
```
loop {
    select! {
        retry_timer fires => {
            key = retry_timer.key()
            deferred_item = store.get_next_deferred_message/timer(key_id)

            if message_retry {
                message = kafka_loader.load_message(offset)
                result = handler.on_message(message, DemandType::Failure)
            } else {
                result = timer_manager.schedule(time)  // Reschedule timer
                // Timer will fire and be processed with DemandType::Failure
            }

            match result {
                Ok => remove from store, schedule next if more items
                Err => increment retry_count, reschedule with longer delay
            }

            retry_timer.commit()
        }
        shutdown => break
    }
}
```

**Flow:**
1. Retry timer fires (scheduled by defer operation)
2. Load next offset/time from Cassandra
3. Load message from Kafka OR reschedule application timer
4. Re-process through handler **with `DemandType::Failure`** (all deferred traffic is failure demand)
5. On success: remove from queue
6. On failure: increment retry count, reschedule with Full Jitter exponential backoff

**Important:** All deferred retries use `DemandType::Failure` since they represent reprocessing of previously failed events, consistent with how `RetryMiddleware` marks retry attempts.

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
-- Append offset (lightweight transaction not needed, append-only)
INSERT INTO deferred_messages (key_id, offset, consumer_group, topic, partition, key, added_at)
VALUES (?, ?, ?, ?, ?, ?, ?);

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
- `FailureTracker`: ≤10,000 × 16 bytes = 160 KB
- `RetryTimerManager`: Minimal (active slabs only)

**Shared:**
- `DeferredCache`: 10,000 × 64 bytes = 640 KB
- `KafkaLoader cache`: 10,000 × ~1 KB = 10 MB

**Total estimate for 100 partitions:** ~26 MB

## Edge Cases and Error Handling

### Kafka Message Deleted (Retention/Compaction)

When loading a deferred message, if Kafka returns `OffsetDeleted`:
1. Log warning
2. Remove offset from deferred queue (can't retry)
3. Continue to next deferred message for the key
4. If no more messages, clear from cache

This handles messages that aged out due to Kafka retention or were removed by compaction.

### Timer Already Fired

When retrying a deferred timer:
- Reschedule with original time (in past)
- Timer fires immediately
- Handler processes normally
- If successful, timer is committed (removed from timer store)

### Partition Rebalancing

**On partition revoke:**
1. Signal retry loop to stop
2. Wait for current retry to complete
3. Shutdown inner handler

**State preservation:**
- All deferred items remain in Cassandra
- New consumer instance picks up retry timers automatically (timer manager loads active slabs)
- Cache is cold on new instance (minor performance impact until warmed)

### Cassandra Unavailability

When Cassandra store operations fail:
1. Log error
2. Re-raise error to outer RetryMiddleware
3. RetryMiddleware will retry the defer operation with exponential backoff
4. If retries exhausted: error logged, offset committed (message lost)

**Future enhancement:** Circuit breaker to temporarily disable deferring during extended Cassandra outages, falling back to immediate error propagation.

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
   - Max events enforcement

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

### Property-Based Tests

1. **Order preservation:**
   - Deferred messages processed in offset order
   - Deferred timers processed in time order

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
    pub deferred_timers_total: Counter,
    pub retry_successes_total: Counter,
    pub retry_failures_total: Counter,

    // Gauges
    pub deferred_keys_active: Gauge,
    pub failure_rate: Gauge,
    pub deferring_enabled: Gauge,

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

*Impact:* Errors propagate to outer RetryMiddleware; after retries exhausted, errors are logged and offsets committed (messages lost)

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

| Aspect | RetryMiddleware | DeferMiddleware |
|--------|----------------|-----------------|
| Persistence | In-memory | Cassandra |
| Max retry time | Minutes | Days/weeks |
| Survives rebalance | No | Yes |
| Backoff type | Exponential | Exponential |
| Typical position | Outermost | Inside RetryMiddleware |
| Use case | Transient failures | Persistent failures |
| Overhead | Minimal | Moderate (Cassandra I/O) |

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

**Document Version:** 1.0
**Last Updated:** 2025-01-21
**Author:** Design Team
**Status:** Draft for Review
