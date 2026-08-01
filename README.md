# Prosody

[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://prosody-events.github.io/prosody/prosody/)
[![Build Status](https://github.com/prosody-events/prosody/actions/workflows/general.yaml/badge.svg?branch=main)](https://github.com/prosody-events/prosody/actions/workflows/general.yaml?query=branch%3Amain)
[![Docs Status](https://github.com/prosody-events/prosody/actions/workflows/documentation.yaml/badge.svg?branch=main)](https://github.com/prosody-events/prosody/actions/workflows/documentation.yaml?query=branch%3Amain)
[![Quality Status](https://github.com/prosody-events/prosody/actions/workflows/quality.yaml/badge.svg?branch=main)](https://github.com/prosody-events/prosody/actions/workflows/quality.yaml?query=branch%3Amain)
[![Coverage Status](https://github.com/prosody-events/prosody/actions/workflows/coverage.yaml/badge.svg?branch=main)](https://github.com/prosody-events/prosody/actions/workflows/coverage.yaml?query=branch%3Amain)
![Test Coverage](https://raw.githubusercontent.com/prosody-events/prosody/badges/main/coverage-badge.svg)

Event sourcing treats the log as the primary source of truth: each change is appended as an immutable record, and
consumers derive their own views by processing the stream. This model decouples producers from consumers — a new
consumer can be added without any producer changes, and a consumer can be rebuilt from scratch by replaying the log.

The pattern comes with operational constraints that the architecture does not answer on its own. The log delivers
messages at least once, so consumers must handle duplicates. Per-key ordering must be preserved, which means a failing
message cannot be skipped — without intervention, it blocks all subsequent messages for that key. Partitions are shared
across many keys, so a burst of activity on one key can delay every other key on the same partition. Under sustained
failure rates, retries generate their own demand and crowd out new work. Consumers that write back to the source need a
way to avoid processing their own events. Async event chains are also difficult to observe: correlating a single user
action across multiple downstream consumers requires explicit trace context propagation.

Prosody is a Kafka client library for Rust that addresses these constraints, backed by Cassandra for persistent state.
Deferred retry stores failing message offsets in Cassandra and schedules a timer, so a key can fail and recover without
blocking other keys or violating ordering. Fair scheduling prevents any single key from monopolizing partition capacity.
Idempotence records and source-system filtering reduce duplicate processing on a best-effort basis; deduplication state
is persisted to Cassandra across restarts and rebalances, but handlers must still be designed to be idempotent. OpenTelemetry integration propagates trace context through the full event
chain. Handler timeouts cancel handlers that exceed their deadline, preventing a single slow or hung handler from blocking concurrency for other keys indefinitely; stall detection identifies partitions that have stopped making progress so the orchestrator can restart the process before the lag becomes irrecoverable.

## Features

- **Kafka Consumer**: Per-key ordering with cross-key concurrency, offset management, consumer groups.
- **Kafka Producer**: Idempotent delivery with configurable retries.
- **Timer System**: Persistent scheduled execution backed by Cassandra or in-memory store.
- **Keyed State**: Durable Value, Map, and Deque collections scoped to each Kafka message key.
- **Quality of Service**: Fair scheduling limits concurrency and prevents failures from starving fresh traffic. Pipeline
  mode adds deferred retry and monopolization detection.
- **Deferred Retry**: Moves transiently-failing keys to timer-based retry, unblocking the partition to continue
  processing other keys (Pipeline mode).
- **Message Deduplication**: Two-tier cache (in-memory + Cassandra) reduces duplicate processing on a best-effort basis. Source-system headers
  reduce producer/consumer loops. Handlers must still be designed to be idempotent.
- **Event Filtering**: Filters messages by event type prefix; unmatched events are committed without processing.
- **Health Probes**: Built-in `/readyz` and `/livez` HTTP endpoints for Kubernetes liveness and readiness checks.
- **Distributed Tracing**: OpenTelemetry integration for tracing message flow across services.
- **Backpressure**: Pauses partitions when handlers fall behind.
- **Mocking**: In-memory Kafka broker for tests (`PROSODY_MOCK=true`).
- **High-Level Client**: Combines producer and consumer with timer support.
- **Failure Handling**: Pipeline (retry forever), Low-Latency (dead letter), Best-Effort (log and skip).

## Usage

### Build prerequisites

Building Prosody compiles its peer wire contract, so the Protocol Buffers
compiler must be on `PATH`:

```bash
apt-get install -y protobuf-compiler   # Debian/Ubuntu
brew install protobuf                  # macOS
```

Add Prosody to your `Cargo.toml`:

```toml
[dependencies]
prosody = "0.1"
```

### High-Level Client Example

```rust,no_run
use prosody::prelude::*;
use serde_json::json;
use std::convert::Infallible;

#[derive(Clone)]
struct MyHandler;

impl FallibleHandler for MyHandler {
    type Payload = serde_json::Value;
    type Error = Infallible;
    type Output = ();

    async fn on_message<C>(
        &self,
        _context: C,
        message: ConsumerMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        println!("Received: {message:?}");
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        println!("Timer fired for key: {}", trigger.key);
        Ok(())
    }

    async fn shutdown(self) {}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_servers = vec!["localhost:9094".to_owned()];

    let mut consumer_config = ConsumerConfiguration::builder();
    consumer_config
        .bootstrap_servers(bootstrap_servers.clone())
        .group_id("my-group")
        .subscribed_topics(["my-topic".to_owned()]);

    let mut producer_config = ProducerConfiguration::builder();
    producer_config
        .bootstrap_servers(bootstrap_servers)
        .source_system("my-source");

    let mut cassandra_config = CassandraConfigurationBuilder::default();
    cassandra_config.nodes(vec!["localhost:9042".to_owned()]);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_config,
        ..ConsumerBuilders::default()
    };

    let client: CassandraHighLevelClient<MyHandler> = CassandraHighLevelClient::new(
        cassandra_config.build()?,
        Mode::Pipeline,
        &mut producer_config,
        &consumer_builders,
    )?;

    client.subscribe(MyHandler).await?;

    client.send("my-topic".into(), "message-key", json!({"value": "Hello, Kafka!"})).await?;

    // Run your application logic here

    client.unsubscribe().await?;
    Ok(())
}
```

## Keyed State

Keyed state gives every Kafka key its own durable working memory. Prosody automatically uses the current message or
timer key, so a handler can relate the current event to earlier events for that key. State survives restarts and
rebalances, and changes become visible only when the event succeeds by default.

Declare each Value, Map, or Deque collection once, register it before subscribing, and give the returned capability to
the handler. Most collections should have a TTL comfortably beyond the longest timer or workflow that uses them.

```rust,ignore
use prosody::state::descriptor::{Registered, StateDescriptor, ValueDescriptor, value_state};
use prosody::timers::duration::CompactDuration;

#[derive(Clone)]
struct CountHandler {
    count: Registered<ValueDescriptor>,
}

let count = value_state("count").ttl(CompactDuration::new(30 * 24 * 60 * 60));
let count = client.register(count).await?;
client.subscribe(CountHandler { count }).await?;

// Inside CountHandler::on_message:
let count = context.state(self.count)?;
let current = count.get().await?.and_then(|value| value.as_u64()).unwrap_or(0);
count.set(json!(current + 1)).await?;
```

Keyed-state cache and recovery settings are listed in [CONFIGURATION.md](CONFIGURATION.md#keyed-state-pipeline-mode).

### Reading another group's state

By default a collection is private to the consumer group that owns it. Mark a
collection `.published(true)` and give the owning consumer a `subsystem` name,
and other services can read that state without owning the partition or running
the write machinery. A published collection with no subsystem is rejected at
registration. The builder field falls back to `PROSODY_SUBSYSTEM` when unset.

```rust,ignore
// One descriptor configures both the owning handle and the published reader.
let current_order = value_state("current-order").published(true);
let config = KeyedStateConfiguration::builder()
    .subsystem(Some(SubsystemName::try_new("checkout")?))
    .build()?;
// After constructing the owning client with `config`:
let registered_order = owner.register(current_order).await?;

// Inside the owner's handler, the current event supplies the state key.
let state = context.state(registered_order)?;
let value = state.get().await?;
state.set(updated).await?;
```

Any other service reads it through the same high-level client, naming the
subsystem and the same collection shape:

```rust,ignore
// Outside a handler, each read supplies the state key explicitly.
let reader = client
    .state(SubsystemName::try_new("checkout")?, current_order)
    .await?;
let value = reader.get("customer-123").await?; // committed value from the owning group
```

A reader observes only **committed** state — never an in-flight value and never
the reader's own writes — read from a single publication source per operation.
Reads are cached for 5 seconds by default. Override the default per process
with `PROSODY_STATE_READ_CACHE_TTL` (`none` disables caching), or per
collection with `.read_cache(...)`, which always wins over the process
default. `PROSODY_STATE_OWNED_CACHE_SIZE` and
`PROSODY_STATE_READ_CACHE_SIZE` accept human-readable values such as `64 MiB`;
the read cache uses `PROSODY_STATE_OWNED_CACHE_SIZE` when set, or 1 MiB when
both size variables are unset. Pass
`ReadCachePolicy::Disabled` to `.read_cache(...)` when one
collection must always read the store despite an enabled process default. A
cached value was the store's committed answer within the last cache TTL. There
is no ordering guarantee across sources. The two TTLs never interact: `.ttl`
bounds how long the owner's written state is **retained** (Cassandra `USING
TTL`, seconds granularity); `.read_cache` bounds how **stale** this read-only
client tolerates a cached value (sub-second `Duration`s are fine). A process
with no consumer of its own can build `StateReaderDependencies::cassandra`,
then pass it to `StateReaderClient::new`. The high-level client shares one
dependency family across its consumer and all readers, so composing a reader
never opens a second Cassandra session, Kafka loader, or cache.

**Retiring a published collection.** Change the collection to
`.published(false)`. Keep its registration and the consumer's `subsystem` for
one complete stop-then-start deployment. Startup reconciliation then removes
the routing row. Deleting the registration or subsystem first strands the
routing row. Routing rows and committed state have no automatic expiry, so
other groups can continue to discover and read the collection.

## Quality of Service

All modes use **fair scheduling** to limit concurrency and distribute execution time. Pipeline mode adds **deferred
retry** and **monopolization detection**.

### Fair Scheduling (All Modes)

The scheduler controls which message runs next and how many run concurrently.

**Virtual Time (VT):** Each key accumulates VT equal to its handler execution time. The scheduler picks the key with the
lowest VT. A key that runs for 500ms accumulates 500ms of VT; a key that hasn't run recently has zero VT and gets
priority.

**Two-Class Split:** Normal messages and failure retries have separate VT pools. The scheduler allocates execution time
between them (default: 70% normal, 30% failure). During a failure spike, retries get at most 30% of execution time—fresh
messages continue processing.

**Starvation Prevention:** Tasks receive a quadratic priority boost based on wait time. A task waiting 2 minutes
(configurable) gets maximum boost, overriding VT disadvantage.

**Decay:** VT decays exponentially (120s half-life). A key that monopolized 10 minutes ago has negligible penalty now.

### Deferred Retry (Pipeline Mode)

Moves failing keys to timer-based retry so the partition can continue processing other keys.

On transient failure: store the message offset in Cassandra, schedule a timer, return success. The partition advances.
When the timer fires, reload the message from Kafka and retry.

**Failure Rate Gating:** When >90% of recent messages fail, deferral disables. The retry middleware blocks the
partition, applying backpressure.

### Monopolization Detection (Pipeline Mode)

Rejects keys that consume too much execution time.

The middleware tracks per-key execution time in 5-minute rolling windows. Keys exceeding 90% of window time are rejected
with a transient error, routing them through defer.

## High-Level Client Modes

### Pipeline Mode

All messages must be processed. Retries indefinitely. Uses defer and monopolization detection.

```text
Kafka → Retry → Defer → Monopolization → Deduplication → Cancellation → Scheduler → Timeout → Telemetry → Handler
```

| Layer          | Purpose                                                  |
|----------------|----------------------------------------------------------|
| Retry          | Retries transient errors indefinitely                    |
| Defer          | Stores failing messages for timer-based retry            |
| Monopolization | Rejects keys exceeding execution time threshold          |
| Deduplication  | Filters duplicate messages via local cache + Cassandra   |
| Cancellation   | Skips work once shutdown or cancellation is signaled     |
| Scheduler      | Enforces concurrency limits and VT-based priority        |
| Timeout        | Cancels handlers exceeding deadline                      |
| Telemetry      | Emits handler lifecycle events                           |

### Low-Latency Mode

Tries a few times, then routes failures to a dead letter topic.

- Retries up to `PROSODY_MAX_RETRIES` times, then writes to failure topic
- Fair scheduling limits how much time retries consume
- Use when you need to keep moving and can reprocess failures later

### Best-Effort Mode

Logs failures and moves on.

- No retries; failed messages are logged and committed
- Fair scheduling still enforces concurrency limits
- Use for development or when message loss is acceptable

## Configuration

Configure via environment variables or the builder pattern. Builders fall back to environment variables for unset
fields, so you can mix both approaches.

For a full reference of all environment variables, see [CONFIGURATION.md](CONFIGURATION.md).

## Mock Mode for Testing

Prosody includes a mock mode that allows you to test your application without requiring a real Kafka cluster. This is
particularly useful for unit tests, integration tests, and local development.

### Enabling Mock Mode

To enable mock mode, set the `PROSODY_MOCK` environment variable to `true` or configure it programmatically. When using
mock mode, Prosody automatically creates topics in the mock cluster based on the `PROSODY_SUBSCRIBED_TOPICS` environment
variable. This ensures that consumers can subscribe to any topics they need without encountering "topic does not exist"
errors.

### Mock Mode Behavior

In mock mode:

- **Kafka Brokers**: Uses an in-memory mock Kafka cluster instead of real brokers
- **Timer Storage**: Uses in-memory storage instead of Cassandra
- **Telemetry**: The telemetry emitter is disabled, opening no real broker connection (regardless of `PROSODY_TELEMETRY_ENABLED`)
- **Topic Creation**: Automatically creates topics listed in `PROSODY_SUBSCRIBED_TOPICS`
- **Message Processing**: Full message processing pipeline works as in production
- **Networking**: No external network dependencies required

## Event Type Filtering

Prosody supports filtering messages based on exact event type prefixes, configured via `PROSODY_ALLOWED_EVENTS` or the
`ConsumerConfiguration` builder.

### Configuration

```sh
# Allow only events starting with exactly 'user.' or 'account.'
export PROSODY_ALLOWED_EVENTS=user.,account.
```

```rust,ignore
let config = ConsumerConfiguration::builder()
    .allowed_events(vec!["user.".to_owned()])
    .build()?;
```

### Matching Behavior

Prefixes must match exactly from the start of the event type:

✓ Matches:

- `{"type": "user.created"}` matches prefix `user.`
- `{"type": "account.deleted"}` matches prefix `account.`

✗ No Match:

- `{"type": "admin.user.created"}` doesn't match `user.`
- `{"type": "my.account.deleted"}` doesn't match `account.`
- `{"type": "notification"}` doesn't match any prefix

If no prefixes are configured, all messages are processed. Messages without a `type` field are always processed.

## Message Deduplication

Prosody reduces duplicate message processing on a best-effort basis using two mechanisms: **source system deduplication**
and **idempotence deduplication**. Deduplication is not guaranteed — handlers must still be designed to be idempotent.

### Source System Deduplication

Prosody introduces the `source-system` header to prevent processing loops caused by messages being reprocessed by the
same system that produced them:

- **Producers** add a `source-system` header to all outgoing messages.
- **Consumers** check incoming messages for the `source-system` header.
- If a message's `source-system` header matches the consumer group, the message is skipped.

This ensures that messages re-emitted by a consumer (e.g., for retry or forwarding purposes) do not create infinite
processing loops. If your application is doing both consumption and production, the source system will default to your
consumer group identifier. If your application is only producing messages and never configures a consumer, you will need
to set the source system. To explicitly set the producer's source system identifier, configure:

```sh
export PROSODY_SOURCE_SYSTEM="my-service"
```

### Idempotence Deduplication (All Modes)

Every consumer mode includes a deduplication middleware that filters duplicate messages using a two-tier cache:

1. **Global cache**: A shared in-memory cache across all partitions for fast lookups. Survives partition reassignments within the same consumer instance.
2. **Persistent store**: A Cassandra-backed store that survives restarts and rebalances.

When a message arrives, the middleware computes a deterministic UUID by hashing the version, consumer group, topic,
partition, key, and either the message's `id` field or its Kafka offset. It checks the local cache first, then
Cassandra. If found in either, the message is skipped. Otherwise, the message is processed and the UUID is recorded in
both tiers.

- **Best-effort persistence**: Cassandra read failures are treated as cache misses; write failures are logged but do not
  fail the message. The global cache still provides deduplication within a single process lifetime.
- **Cache-busting**: Changing `PROSODY_IDEMPOTENCE_VERSION` invalidates all previously recorded entries, causing
  messages to be reprocessed.
- **TTL expiry**: Dedup records in Cassandra expire after `PROSODY_IDEMPOTENCE_TTL` (default: 7 days).
- **Always on**: Deduplication cannot be disabled; `PROSODY_IDEMPOTENCE_CACHE_SIZE` must be at least 1.

The producer also maintains a separate local deduplication cache to avoid sending duplicate messages. It hashes the
`(topic, key, id)` triple into a 128-bit key and stores it in a bounded in-memory set. Messages without an `id` field
bypass the cache entirely. Once a triple is seen, it stays in the cache until evicted by capacity.

## Liveness and Readiness Probes

Prosody includes a built-in probe server that provides health check endpoints for consumer-based applications. The probe
server is tied to the consumer's lifecycle and offers two main endpoints:

1. `/readyz`: A readiness probe that checks if any partitions are assigned to the consumer. It returns a success status
   only when the consumer has at least one partition assigned, indicating it's ready to process messages.
2. `/livez`: A liveness probe that checks if any partitions have stalled.

A partition is considered "stalled" if it has not processed a message within a specified time threshold. This threshold
is determined by the `PROSODY_STALL_THRESHOLD` configuration. By default, this is set to 5 minutes, but it
can be customized to suit your application's needs. If a partition is detected as stalled, the liveness probe will fail,
potentially triggering a restart of the application by the orchestration system.

To configure the probe server:

- Set the `PROSODY_PROBE_PORT` environment variable to a valid port number to enable the server. By default, it uses
  port 8000.
- To disable the probe server, set `PROSODY_PROBE_PORT` to 'none'.
- Adjust the `PROSODY_STALL_THRESHOLD` to change the stall detection threshold. For example, setting it to
  "30s" would consider a partition stalled if it hasn't processed a message in 30 seconds.
- If the probe server is enabled, it will start when the consumer is subscribed and stop when it is unsubscribed.

Note: It's important to set the `PROSODY_STALL_THRESHOLD` to a value that's appropriate for your application's
message processing latency. Setting it too low might result in false positives for stalled partitions, while setting it
too high could delay the detection of actual issues.

These endpoints can be integrated with container orchestration systems like Kubernetes to manage the lifecycle of your
application based on its health and readiness status. They provide valuable information about the consumer's state,
helping to ensure robust and responsive Kafka-based applications.

## Timer System

Prosody includes a distributed timer system that allows you to schedule events for future execution. The timer system
supports:

- **Persistent Storage**: Timers are stored in persistent backends (Cassandra or in-memory for testing)
- **Distributed Processing**: Multiple consumer instances can process timers from the same storage
- **Slab-Based Partitioning**: Timers are organized into time-based slabs for efficient retrieval
- **Automatic Cleanup**: Successfully processed timers are immediately deleted; failed timers expire after configurable
  period

### Timer Configuration

The timer system is automatically configured based on the consumer configuration:

- **Mock Mode**: Uses in-memory storage for testing (`PROSODY_MOCK=true`)
- **Production Mode**: Uses Cassandra for persistent storage
- **Slab Size**: Configure time-based partitioning with `PROSODY_SLAB_SIZE` (default: 1 hour)
- **Retention**: Retention period for timer and failure data via `PROSODY_CASSANDRA_RETENTION` (default: 1 year)

### Usage in Handlers

Your event handlers can receive timer events through the `on_timer` method of the `FallibleHandler` trait, as shown in
the example above.

## Common Project Tasks

Prosody uses a Makefile to simplify common development tasks. Here are some useful commands:

### Setup

- `make bootstrap`: Install Rust and necessary development tools.
- `make up`: Start Kafka and related services using Docker Compose.

### Development

- `make update`: Update project dependencies.
- `make format`: Format Rust code and TOML files.
- `make build`: Build the project.
- `make check`: Check for compilation errors without building.
- `make check-watch`: Watch for changes and check for compilation errors.
- `make lint`: Run Clippy for linting.
- `make lint-watch`: Watch for changes and run Clippy.

### Testing

- `make test`: Run tests (starts Kafka services first).
- `make test-watch`: Watch for changes and run tests.
- `make coverage`: Generate code coverage report.

### Maintenance

- `make dependencies`: Check for unused dependencies.
- `make reset`: Stop and remove Docker containers and volumes.

### Utilities

- `make console`: Open the Kafka console in a web browser.

## Architecture

Prosody processes Kafka messages with partition-level parallelism while guaranteeing ordered delivery for messages with
the same key.

For a detailed breakdown of the consumer architecture, message flow, and component organization, see
[ARCHITECTURE.md](ARCHITECTURE.md).

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
