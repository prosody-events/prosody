# Architecture

Prosody is designed to provide efficient and parallel processing of Kafka messages while maintaining order for messages
with the same key. Here's an overview of its architecture:

## Consumer Architecture

The consumer in Prosody is built around the concept of partition-level parallelism and key-based ordering.

```mermaid
graph TD
    A[Kafka Topics] --> B[ProsodyConsumer]
    B --> C[Partition Manager: Topic A, Partition 0]
    B --> D[Partition Manager: Topic A, Partition 1]
    B --> E[Partition Manager: Topic B, Partition 0]
    C --> F[Key Queue: User ID 1]
    C --> G[Key Queue: User ID 2]
    D --> H[Key Queue: User ID 3]
    D --> I[Key Queue: User ID 4]
    E --> J[Key Queue: Product ID 1]
    E --> K[Key Queue: Product ID 2]
```

1. **Partition-Level Parallelism**: Each Kafka partition is managed by a separate `PartitionManager`. This allows for
   parallel processing of messages from different partitions. The `PartitionManager` is responsible for buffering
   messages and tracking offsets for its assigned partition.

2. **Key-Based Queuing**: Within each partition, messages are further divided based on their keys. Each unique key
   within a partition has its own queue. This ensures that messages with the same key are processed in order.

3. **Concurrent Processing**: Different keys can be processed concurrently, even within the same partition, allowing for
   high throughput. The `PartitionManager` can process messages from different key queues simultaneously.

4. **Ordered Processing**: Messages with the same key are processed sequentially from their respective queue, ensuring
   ordered processing for each key.

5. **Partition Queues**: Each assigned partition has its own librdkafka queue. The partition's task reads and decodes
   its records, so decoding runs on all runtime threads. One poll loop serves rebalances, statistics, and errors, and
   it stores committed offsets.

6. **Backpressure Management**: Prosody never pauses a partition. It bounds memory at two levels:
    - **Decoded messages**: A record is decoded only when it gets one of `PROSODY_MAX_UNCOMMITTED` in-flight permits,
      shared by all partitions. It holds the permit until its offset commits. A partition without a permit stops
      reading.
    - **Fetched records**: librdkafka stops fetching a partition when its queue holds `PROSODY_PARTITION_PREFETCH`
      bytes. The fetched records stay in librdkafka until the partition reads them. Other partitions continue to make
      progress.
    - **Per-key queuing**: Each key has its own queue to preserve message order. The in-flight permits bound these
      queues.

## Message Flow

```mermaid
sequenceDiagram
    participant Kafka Broker
    participant Poll Loop
    participant Partition Manager
    participant Key Manager
    participant Middleware Stack
    participant User Message Handler
    Kafka Broker ->> Partition Manager: Fetch records into the partition queue
    Partition Manager ->> Key Manager: Enqueue message for specific key
    Key Manager ->> Middleware Stack: Dispatch message in key order
    Middleware Stack ->> User Message Handler: Process message
    User Message Handler -->> Middleware Stack: Return result
    Middleware Stack -->> Key Manager: Complete
    Key Manager -->> Partition Manager: Notify offset tracker
    Partition Manager -->> Poll Loop: Watermark updated
    Poll Loop -->> Kafka Broker: Store committed offsets
```

1. librdkafka fetches the records of each assigned partition into that partition's queue.
2. The `PartitionManager` of the partition reads its queue. It waits for an in-flight permit, then decodes the record.
3. The `PartitionManager` enqueues the message in the correct key-based queue within `KeyManager`, according to the
   message key (e.g., User ID, Product ID).
4. `KeyManager` dispatches messages for each key sequentially through the middleware stack, which applies retry,
   deduplication, telemetry, timeout, cancellation, and other behaviors before invoking the user-provided `FallibleHandler`.
5. After processing, the key's offset is recorded in `OffsetTracker`.
6. The `PartitionManager`'s `OffsetTracker` tracks the partition's high watermark committed offset.
7. The `Poll Loop` reads watermarks from each `PartitionManager` and stores them with librdkafka, which commits them
   back to Kafka asynchronously, ensuring at-least-once message processing semantics.
8. If no in-flight permit is free, the partition stops reading. librdkafka keeps its fetched records and stops fetching
   the partition at `PROSODY_PARTITION_PREFETCH`.

Throughout this flow, OpenTelemetry is used to create and propagate distributed traces, allowing for end-to-end
visibility of message processing across different services.

### Span Linking

By default, message execution spans use **`child`** (child-of relationship — the execution span is part of
the same trace as the producer). Timer execution spans use **`follows_from`** (the execution span starts a
new trace with a span link back to the scheduling span, since timer execution is causally related but not part of
the same operation).

Both strategies are configurable via `PROSODY_MESSAGE_SPANS` and `PROSODY_TIMER_SPANS` (or the builder fields
`message_spans` and `timer_spans`). Accepted values: `child`, `follows_from`.

This architecture allows Prosody to achieve high throughput by processing different partitions and keys concurrently,
while still maintaining strict ordering for messages with the same key. It also provides backpressure management by
limiting the total number of in-flight messages through a global semaphore, and the fetched bytes of each partition
through librdkafka.

## Component Organization

```mermaid
flowchart TD
    classDef subgraphStyle fill:#f5f5f5,stroke:#666

    HLC["<a href='https://github.com/prosody-events/prosody/tree/main/src/high_level/mod.rs'>HighLevelClient</a>"]
    HLC --> Producer["<a href='https://github.com/prosody-events/prosody/tree/main/src/producer/mod.rs'>ProsodyProducer</a>"]
    HLC --> ConsumerMain["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/mod.rs'>ProsodyConsumer</a>"]

    subgraph ProducerComponents["Producer Components"]
        Producer --> KafkaProducer["<a href='https://github.com/prosody-events/prosody/tree/main/src/producer/mod.rs'>Kafka Producer</a>"]
        Producer --> ICache["<a href='https://github.com/prosody-events/prosody/tree/main/src/producer/mod.rs'>Idempotence Cache</a>"]
        Producer --> PropP["<a href='https://github.com/prosody-events/prosody/tree/main/src/propagator.rs'>OpenTelemetry Propagator</a>"]
    end

    subgraph ConsumerComponents["Consumer Components"]
        ConsumerMain --> Context["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/kafka_context.rs'>Kafka Context</a>"]
        ConsumerMain --> PollLoop["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/poll.rs'>Poll Loop</a>"]
        ConsumerMain --> ProbeServer["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/probes.rs'>Probe Server</a>"]
        Context --> PMgr
        PollLoop --> PMgr
    end

    subgraph PartitionComponents["Partition Processing"]
        PMgr["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/partition/mod.rs'>Partition Manager</a>"]
        PMgr --> KeyMgr["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/partition/keyed/mod.rs'>Key Manager</a>"]
        PMgr --> OTracker["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/partition/offsets/mod.rs'>Offset Tracker</a>"]
        KeyMgr --> EHandler["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/mod.rs'>Event Handler</a>"]
    end

    subgraph MiddlewareHandling["Middleware Components"]
        RetryS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/retry.rs'>Retry Middleware</a>"]
        DedupS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/deduplication/mod.rs'>Deduplication Middleware</a>"]
        DeferS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/defer/mod.rs'>Defer Middleware</a>"]
        MonoS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/monopolization.rs'>Monopolization Middleware</a>"]
        SchedS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/scheduler/mod.rs'>Scheduler Middleware</a>"]
        TelS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/telemetry.rs'>Telemetry Middleware</a>"]
        LogS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/log.rs'>Log Middleware</a>"]
        TimeoutS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/timeout.rs'>Timeout Middleware</a>"]
        CancelS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/cancellation.rs'>Cancellation Middleware</a>"]
        TopicS["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/middleware/topic.rs'>Failure Topic Middleware</a>"]
    end

    KeyMgr -.-> MiddlewareHandling
    TopicS --> FTopic["Failure Topic"]
    Producer --> FTopic

    subgraph TracingSystem["OpenTelemetry Integration"]
        OTel["<a href='https://github.com/prosody-events/prosody/tree/main/src/telemetry/mod.rs'>Telemetry</a>"]
        Prop["<a href='https://github.com/prosody-events/prosody/tree/main/src/propagator.rs'>Propagator</a>"]
        MExtract["<a href='https://github.com/prosody-events/prosody/tree/main/src/consumer/extractor.rs'>Message Extractor</a>"]
        RInject["<a href='https://github.com/prosody-events/prosody/tree/main/src/producer/injector.rs'>Record Injector</a>"]
        OTel --> Prop
        Prop --> MExtract
        Prop --> RInject
    end

    ConsumerMain -.-> OTel
    Producer -.-> OTel

    class ProducerComponents,ConsumerComponents,PartitionComponents,MiddlewareHandling,TracingSystem subgraphStyle
```
