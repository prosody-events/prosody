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
| `PROSODY_SUBSYSTEM`         | This consumer's request and published-state subsystem | - | ✓ |          |
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
| `PROSODY_STATISTICS_INTERVAL`    | How often librdkafka reports client statistics; must be between 1ms and 24h | 5s |
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

## Peer Requests

Set these values with environment variables or `PeerConfiguration::builder()`.
An explicit builder value replaces its environment value.
Use a different bind address for each client that shares a host.

| Environment variable | Default | Why it is needed | What it controls | Validation |
|---|---:|---|---|---|
| `PROSODY_PEER_BIND_ADDRESS` | `0.0.0.0:9099` | The peer server needs a local listener. | The socket address that the peer server binds. | Must be a socket address. |
| `PROSODY_PEER_ADVERTISED_CONNECT` | unset | Peers on another network need an entry point. | The gRPC connect URI that remote peers use. | Must be a valid gRPC URI. |
| `PROSODY_PEER_NETWORK_NAME` | unset | A shared label lets peers prefer direct routes. | The network group used to choose direct or advertised routes. | 1 through 63 bytes when set. |
| `PROSODY_PEER_CACHE_CAPACITY` | 256 | Peer-keyed caches need a fixed memory bound. | The entry count for address, channel, and route-preference caches. | Must be greater than zero. |
| `PROSODY_PEER_REGISTRATION_TTL` | 30s | A lease removes dead peers without a cleanup task. | The Cassandra TTL and refresh pace for this peer registration. | 5s through 1h. |

Set `PROSODY_SUBSYSTEM` to make the client answer requests for that subsystem.
Without it, the client consumes messages but does not answer requests. A
requestor can target one or more subsystems in either configuration.

## Retry

Retry backoff applies in pipeline and low-latency modes. `PROSODY_MAX_RETRIES` controls how many retries low-latency mode performs before routing the failure to `PROSODY_FAILURE_TOPIC`. Pipeline mode uses deferral and does not use this limit.

| Environment Variable      | Description                      | Default |
|---------------------------|----------------------------------|---------|
| `PROSODY_MAX_RETRIES`     | Low-latency retries before routing to the failure topic | 3       |
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
| `PROSODY_DEFER_STORE_CACHE_SIZE`  | `(key → next_offset/next_timer, retry_count)` entries cached per Cassandra defer store | 8192    |

## Kafka Message Loader (All Modes)

The Kafka message loader is consumer-wide: it serves both deferred-message
reloads and keyed-state message resolution.

| Environment Variable              | Description                                            | Default |
|-----------------------------------|--------------------------------------------------------|---------|
| `PROSODY_LOADER_CACHE_SIZE`       | Decoded Kafka messages cached by the loader            | 1024    |
| `PROSODY_LOADER_SEEK_TIMEOUT`     | Timeout for Kafka seek operations when loading         | 30s     |
| `PROSODY_LOADER_DISCARD_THRESHOLD`| Sequential reads before seeking (rarely needs changing)| 100     |

## Keyed State

| Environment Variable                 | Description                                        | Default                  |
|--------------------------------------|----------------------------------------------------|--------------------------|
| `PROSODY_STATE_CACHE_DIR`            | Disk workspace for the local keyed-state cache. Wiped on restart, so it needs no persistence — but production deployments **must** set it to a mounted path (e.g. a Kubernetes `emptyDir`). | per-process temp dir |
| `PROSODY_STATE_OWNED_CACHE_SIZE`     | Capacity of the owning keyed-state cache. Accepts sizes such as `64 MiB` or `500 MB`. | storage-engine default |
| `PROSODY_STATE_RECOVERY_DELAY` | Grace period before a background sweep reconciles a freshly written value, in case the fast path did not. Rarely needs changing; second-granularity and must be at least `1s`. | 30s |
| `PROSODY_STATE_READ_CACHE_SIZE` | Capacity of the read-only client's shared read-through cache. Accepts sizes such as `1 MiB`. | `PROSODY_STATE_OWNED_CACHE_SIZE` when set; otherwise 1 MiB |
| `PROSODY_STATE_READ_CACHE_TTL` | Default read-cache TTL for composed readers: how long a `StateReader` may serve a collection's reads from its cache before re-reading the store. A humantime duration (`5s`, `750ms`); `none` disables the inherited default. A descriptor can replace it with `.read_cache(duration)` or bypass it with `.read_cache(ReadCachePolicy::Disabled)`. Reader-only — never affects the owning consumer's writes or a collection's durable TTL. | 5s |

`PROSODY_SUBSYSTEM` names the service's published keyed state. Set it whenever
any collection uses `.published(true)`. Prosody trims the name, and refuses it
at startup when it is blank or longer than 64 bytes.

To make a published collection private, change it to `.published(false)` but
keep the collection registered and retain the same subsystem name for one
complete deployment. On startup, Prosody removes the collection from the
routing table. Removing the registration or subsystem at the same time leaves
stale routing information, so readers may continue to discover the collection.

## Deduplication (All Modes)

| Environment Variable             | Description                                         | Default |
|----------------------------------|-----------------------------------------------------|---------|
| `PROSODY_IDEMPOTENCE_CACHE_SIZE` | Global shared cache capacity (must be at least 1)   | 8192    |
| `PROSODY_IDEMPOTENCE_VERSION`    | Version string for cache-busting dedup hashes       | 1       |
| `PROSODY_IDEMPOTENCE_TTL`        | TTL for dedup records in Cassandra                  | 7d      |

## Cassandra

Persistent storage for timers, deferral, deduplication, and keyed state. It is not needed when `PROSODY_MOCK=true`.

| Environment Variable           | Description                        | Default |
|--------------------------------|------------------------------------|---------|
| `PROSODY_CASSANDRA_NODES`      | Servers to connect to (host:port)  | -       |
| `PROSODY_CASSANDRA_KEYSPACE`   | Keyspace name                      | prosody |
| `PROSODY_CASSANDRA_USER`       | Username                           | -       |
| `PROSODY_CASSANDRA_PASSWORD`   | Password                           | -       |
| `PROSODY_CASSANDRA_DATACENTER` | Prefer this datacenter for queries | -       |
| `PROSODY_CASSANDRA_RACK`       | Prefer this rack for queries       | -       |
| `PROSODY_CASSANDRA_RETENTION`  | Delete data older than this        | 1y      |

## Telemetry

Publishes message, timer, and producer lifecycle events to a Kafka topic:

| Environment Variable        | Description                                | Default                  |
|-----------------------------|--------------------------------------------|--------------------------|
| `PROSODY_TELEMETRY_ENABLED` | Enable the telemetry event emitter         | true                     |
| `PROSODY_TELEMETRY_TOPIC`   | Kafka topic to publish telemetry events to | prosody.telemetry-events |

The emitter is disabled automatically when `PROSODY_MOCK=true`, regardless of
`PROSODY_TELEMETRY_ENABLED` — mock mode opens no real broker connection.

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
