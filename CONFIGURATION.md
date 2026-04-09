# Configuration

Configure Prosody via environment variables or the builder pattern. Builders fall back to
environment variables for unset fields, so you can mix both approaches.

## Core

| Environment Variable        | Description                                        | Default      | Consumer | Producer |
|-----------------------------|----------------------------------------------------|--------------|----------|----------|
| `PROSODY_BOOTSTRAP_SERVERS` | Kafka servers to connect to                        | -            | ✓        | ✓        |
| `PROSODY_GROUP_ID`          | Consumer group name                                | -            | ✓        |          |
| `PROSODY_SUBSCRIBED_TOPICS` | Topics to read from                                | -            | ✓        |          |
| `PROSODY_ALLOWED_EVENTS`    | Only process events matching these prefixes        | (all)        | ✓        |          |
| `PROSODY_SOURCE_SYSTEM`     | Tag for outgoing messages (prevents reprocessing)  | `<group id>` |          | ✓        |
| `PROSODY_MOCK`              | Use in-memory Kafka for testing                    | false        | ✓        | ✓        |
| `PROSODY_LOG`               | Log level (e.g., `info`, `prosody=debug`)          | info         | ✓        | ✓        |

## Consumer

| Environment Variable             | Description                                          | Default                |
|----------------------------------|------------------------------------------------------|------------------------|
| `PROSODY_MAX_CONCURRENCY`        | Max messages being processed simultaneously          | 32                     |
| `PROSODY_MAX_UNCOMMITTED`        | Max queued messages before pausing consumption       | 64                     |
| `PROSODY_TIMEOUT`                | Cancel handler if it runs longer than this           | 80% of stall threshold |
| `PROSODY_COMMIT_INTERVAL`        | How often to save progress to Kafka                  | 1s                     |
| `PROSODY_POLL_INTERVAL`          | How often to fetch new messages from Kafka           | 100ms                  |
| `PROSODY_SHUTDOWN_TIMEOUT`       | Shutdown budget; handlers complete freely before cancellation fires near the deadline | 30s |
| `PROSODY_STALL_THRESHOLD`        | Report unhealthy if no progress for this long        | 5m                     |
| `PROSODY_PROBE_PORT`             | HTTP port for health checks ('none' to disable)      | 8000                   |
| `PROSODY_FAILURE_TOPIC`          | Send unprocessable messages here (dead letter queue) | -                      |
| `PROSODY_SLAB_SIZE`              | Timer storage granularity (rarely needs changing)    | 1h                     |
| `PROSODY_MESSAGE_SPANS`          | Span linking for message execution: `child` (child-of) or `follows_from`         | `child`       |
| `PROSODY_TIMER_SPANS`            | Span linking for timer execution: `child` (child-of) or `follows_from`           | `follows_from` |

## Producer

| Environment Variable                | Description                                          | Default |
|-------------------------------------|------------------------------------------------------|---------|
| `PROSODY_SEND_TIMEOUT`              | Give up sending after this long                      | 1s      |
| `PROSODY_IDEMPOTENCE_CACHE_SIZE`    | Producer dedup cache capacity (0 to disable)         | 8192    |

## Retry

When a handler fails, retry with exponential backoff:

| Environment Variable      | Description                      | Default |
|---------------------------|----------------------------------|---------|
| `PROSODY_MAX_RETRIES`     | Give up after this many attempts | 3       |
| `PROSODY_RETRY_BASE`      | Wait this long before first retry | 20ms    |
| `PROSODY_RETRY_MAX_DELAY` | Never wait longer than this      | 5m      |

## Deferral (Pipeline Mode)

| Environment Variable              | Description                                       | Default |
|-----------------------------------|---------------------------------------------------|---------|
| `PROSODY_DEFER_ENABLED`           | Enable deferral for new messages                  | true    |
| `PROSODY_DEFER_BASE`              | Wait this long before first deferred retry        | 1s      |
| `PROSODY_DEFER_MAX_DELAY`         | Never wait longer than this                       | 24h     |
| `PROSODY_DEFER_FAILURE_THRESHOLD` | Disable deferral when failure rate exceeds this   | 0.9     |
| `PROSODY_DEFER_FAILURE_WINDOW`    | Measure failure rate over this time window        | 5m      |
| `PROSODY_DEFER_CACHE_SIZE`        | Track this many deferred keys in memory           | 1024    |
| `PROSODY_DEFER_SEEK_TIMEOUT`      | Timeout when loading deferred messages            | 30s     |
| `PROSODY_DEFER_DISCARD_THRESHOLD` | Read optimization (rarely needs changing)         | 100     |

## Deduplication (Pipeline Mode)

| Environment Variable             | Description                                         | Default |
|----------------------------------|-----------------------------------------------------|---------|
| `PROSODY_IDEMPOTENCE_CACHE_SIZE` | Global shared cache capacity (0 to disable)         | 8192    |
| `PROSODY_IDEMPOTENCE_VERSION`    | Version string for cache-busting dedup hashes       | 1       |
| `PROSODY_IDEMPOTENCE_TTL`        | TTL for dedup records in Cassandra                  | 7d      |

## Cassandra

Persistent storage for scheduled retries and deduplication (not needed if `PROSODY_MOCK=true`):

| Environment Variable           | Description                        | Default |
|--------------------------------|------------------------------------|---------|
| `PROSODY_CASSANDRA_NODES`      | Servers to connect to (host:port)  | -       |
| `PROSODY_CASSANDRA_KEYSPACE`   | Keyspace name                      | prosody |
| `PROSODY_CASSANDRA_USER`       | Username                           | -       |
| `PROSODY_CASSANDRA_PASSWORD`   | Password                           | -       |
| `PROSODY_CASSANDRA_DATACENTER` | Prefer this datacenter for queries | -       |
| `PROSODY_CASSANDRA_RACK`       | Prefer this rack for queries       | -       |
| `PROSODY_CASSANDRA_RETENTION`  | Delete data older than this        | 1y      |

## Telemetry Emitter

Publishes message and timer lifecycle events to a Kafka topic:

| Environment Variable        | Description                                | Default                  |
|-----------------------------|--------------------------------------------|--------------------------|
| `PROSODY_TELEMETRY_ENABLED` | Enable the telemetry event emitter         | true                     |
| `PROSODY_TELEMETRY_TOPIC`   | Kafka topic to publish telemetry events to | prosody.telemetry-events |

## Monopolization Detection (Pipeline Mode)

| Environment Variable                | Description                            | Default |
|-------------------------------------|----------------------------------------|---------|
| `PROSODY_MONOPOLIZATION_ENABLED`    | Enable hot key protection              | true    |
| `PROSODY_MONOPOLIZATION_THRESHOLD`  | Max handler time as fraction of window | 0.9     |
| `PROSODY_MONOPOLIZATION_WINDOW`     | Measurement window                     | 5m      |
| `PROSODY_MONOPOLIZATION_CACHE_SIZE` | Max distinct keys to track             | 8192    |

## Fair Scheduling (All Modes)

| Environment Variable               | Description                                                      | Default |
|------------------------------------|------------------------------------------------------------------|---------|
| `PROSODY_SCHEDULER_FAILURE_WEIGHT` | Fraction of processing time reserved for retries                 | 0.3     |
| `PROSODY_SCHEDULER_MAX_WAIT`       | Messages waiting this long get maximum priority                  | 2m      |
| `PROSODY_SCHEDULER_WAIT_WEIGHT`    | Priority boost for waiting messages (higher = more aggressive)   | 200.0   |
| `PROSODY_SCHEDULER_CACHE_SIZE`     | Max distinct keys to track                                       | 8192    |

## Topic Creation

For creating Kafka topics programmatically:

| Environment Variable               | Description                            | Default         |
|------------------------------------|----------------------------------------|-----------------|
| `PROSODY_TOPIC_NAME`               | Topic to create                        | -               |
| `PROSODY_TOPIC_PARTITIONS`         | Number of partitions                   | broker default  |
| `PROSODY_TOPIC_REPLICATION_FACTOR` | Number of replicas per partition       | broker default  |
| `PROSODY_TOPIC_RETENTION`          | Delete messages older than this        | cluster default |
| `PROSODY_TOPIC_CLEANUP_POLICY`     | Cleanup policy (delete, compact, both) | cluster default |
