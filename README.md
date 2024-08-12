# Prosody

Prosody is a high-level Kafka client library for Rust, featuring robust consumer and producer implementations with
integrated OpenTelemetry support for distributed tracing.

[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://prosody.docs.rg-infra.com/prosody)
[![Build Status](https://github.com/RealGeeks/prosody/actions/workflows/general.yaml/badge.svg?branch=main)](https://github.com/RealGeeks/prosody/actions/workflows/general.yaml?query=branch%3Amain)
[![Audit Status](https://github.com/RealGeeks/prosody/actions/workflows/audit.yaml/badge.svg?branch=main)](https://github.com/RealGeeks/prosody/actions/workflows/audit.yaml?query=branch%3Amain)
[![Docs Status](https://github.com/RealGeeks/prosody/actions/workflows/documentation.yaml/badge.svg?branch=main)](https://github.com/RealGeeks/prosody/actions/workflows/documentation.yaml?query=branch%3Amain)
[![Quality Status](https://github.com/RealGeeks/prosody/actions/workflows/quality.yaml/badge.svg?branch=main)](https://github.com/RealGeeks/prosody/actions/workflows/quality.yaml?query=branch%3Amain)
[![Coverage Status](https://github.com/RealGeeks/prosody/actions/workflows/coverage.yaml/badge.svg?branch=main)](https://github.com/RealGeeks/prosody/actions/workflows/coverage.yaml?query=branch%3Amain)
![Test Coverage](../../raw/badges/main/coverage-badge.svg)

## Features

- **Kafka Consumer**: Efficiently consume messages with support for offset management and consumer groups.
- **Kafka Producer**: Reliably produce messages with idempotent delivery.
- **Distributed Tracing**: Seamless integration with OpenTelemetry for enhanced observability in microservice
  architectures.
- **Configurable**: Flexible configuration through environment variables.
- **Asynchronous**: Built on top of Tokio for high-performance asynchronous operations.
- **Backpressure Management**: Intelligent partition pausing to handle processing backlogs.
- **Mocking Support**: Ability to use mock Kafka brokers for testing purposes.

## Usage

Add Prosody to your `Cargo.toml`:

```toml
[dependencies]
prosody = { git = "https://github.com/RealGeeks/prosody.git" }
```

### Producer Example

```rust
use prosody::Topic;
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
   let config = ProducerConfiguration::builder()
     .bootstrap_servers(["localhost:9092".to_owned()])
     .mock(true) // use mock producer for example
     .build()?;

   let producer = ProsodyProducer::new(&config)?;

   let topic: Topic = "my-topic".into();
   producer.send([], topic, "message-key", json!({"value": "Hello, Kafka!"})).await?;

   Ok(())
}
```

### Consumer Example

```rust
use prosody::consumer::message::{MessageContext, UncommittedMessage};
use prosody::consumer::{ConsumerConfiguration, MessageHandler, ProsodyConsumer};
use prosody::{Partition, Topic};
use std::time::Duration;

#[derive(Clone)]
struct MyMessageHandler;

impl MessageHandler for MyMessageHandler {
   async fn on_message(&self, context: MessageContext, message: UncommittedMessage) {
      println!("Received: {:?}", message);
      message.commit();
   }

   async fn shutdown(self) {}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
   let config = ConsumerConfiguration::builder()
     .bootstrap_servers(["localhost:9092".to_owned()])
     .group_id("my-group")
     .subscribed_topics(["my-topic".to_owned()])
     .mock(true) // use mock consumer for example
     .build()?;

   let consumer = ProsodyConsumer::new(&config, MyMessageHandler)?;

   // Run your application logic here, waiting for shutdown

   consumer.shutdown().await;
   Ok(())
}
```

## High-Level Constructors

Prosody provides two high-level constructors for both consumers and producers to suit different application needs:

### Pipeline Constructor

The pipeline constructors are designed for applications that require all messages to be processed or sent in order.
These constructors ensure that all messages are handled sequentially, retrying failed operations indefinitely based on
the provided retry configuration. They're suitable for applications where message order is critical and processing or
sending must be guaranteed.

Key features:

- Ensures ordered handling of all messages
- Retries failed operations indefinitely based on the retry configuration
- Ideal for pipeline applications where order is crucial

### Low-Latency Constructor

The low-latency constructors are optimized for applications that prioritize quick processing or sending and can tolerate
occasional message failures. These constructors attempt to handle messages rapidly and, if the operation continually
fails after retries, either send the failed messages to a designated failure topic (for consumers) or return an error (
for producers).

Key features:

- Prioritizes low-latency operations
- Implements a retry mechanism for failed operations
- For consumers: Sends persistently failing messages to a failure topic for later analysis or reprocessing
- For producers: Returns an error after a configurable number of retries
- Ideal for applications where speed is crucial and failed messages can be handled separately or errors can be tolerated

## Configuration

Prosody can be configured through environment variables or programmatically using the builder pattern. Both
`ConsumerConfiguration` and `ProducerConfiguration` use this approach. The builder pattern automatically falls back to
environment variables for any unspecified field. This means you can mix and match programmatic configuration with
environment variables, giving you flexibility in how you set up your Kafka clients.

The following table lists the available configuration options and their associated environment variables:

| Environment Variable                 | Description                                     | Default | Consumer | Producer |
|--------------------------------------|-------------------------------------------------|---------|----------|----------|
| `PROSODY_BOOTSTRAP_SERVERS`          | Comma-separated list of Kafka bootstrap servers | -       | ✓        | ✓        |
| `PROSODY_GROUP_ID`                   | Consumer group ID                               | -       | ✓        |          |
| `PROSODY_SUBSCRIBED_TOPICS`          | Comma-separated list of topics to subscribe to  | -       | ✓        |          |
| `PROSODY_MAX_UNCOMMITTED`            | Maximum number of uncommitted messages          | 32      | ✓        |          |
| `PROSODY_MAX_ENQUEUED_PER_KEY`       | Maximum number of enqueued messages per key     | 8       | ✓        |          |
| `PROSODY_PARTITION_SHUTDOWN_TIMEOUT` | Timeout for partition shutdown                  | 5s      | ✓        |          |
| `PROSODY_POLL_INTERVAL`              | Interval between poll operations                | 100ms   | ✓        |          |
| `PROSODY_COMMIT_INTERVAL`            | Interval between commit operations              | 1s      | ✓        |          |
| `PROSODY_SEND_TIMEOUT`               | Timeout for send operations in the producer     | 1s      |          | ✓        |
| `PROSODY_MOCK`                       | Use mock Kafka brokers for testing              | false   | ✓        | ✓        |
| `PROSODY_RETRY_BASE`                 | Base exponential backoff delay                  | 20ms    | ✓        |          |
| `PROSODY_MAX_RETRIES`                | Maximum number of retries                       | 3       | ✓        |          |
| `PROSODY_RETRY_MAX_DELAY`            | Maximum retry delay                             | 1m      | ✓        |          |
| `PROSODY_FAILURE_TOPIC`              | Topic for failed messages                       | -       | ✓        |          |

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

Prosody is designed to provide efficient and parallel processing of Kafka messages while maintaining order for messages
with the same key. Here's an overview of its architecture:

### Consumer Architecture

The consumer in Prosody is built around the concept of partition-level parallelism and key-based ordering.

```mermaid
graph TD
    A[Kafka Topics] --> B[ProsodyConsumer]
    B --> C[Partition Manager: Topic A, Partition 0]
    B --> D[Partition Manager: Topic A, Partition 1]
    B --> E[Partition Manager: Topic B, Partition 0]
    C --> F[Bounded Queue: User ID 1]
    C --> G[Bounded Queue: User ID 2]
    D --> H[Bounded Queue: User ID 3]
    D --> I[Bounded Queue: User ID 4]
    E --> J[Bounded Queue: Product ID 1]
    E --> K[Bounded Queue: Product ID 2]
```

1. **Partition-Level Parallelism**: Each Kafka partition is managed by a separate `PartitionManager`. This allows for
   parallel processing of messages from different partitions. The `PartitionManager` is responsible for buffering
   messages and tracking offsets for its assigned partition.

2. **Key-Based Queuing**: Within each partition, messages are further divided based on their keys. Each unique key
   within a partition has its own bounded queue. This ensures that messages with the same key are processed in order.

3. **Concurrent Processing**: Different keys can be processed concurrently, even within the same partition, allowing for
   high throughput. The `PartitionManager` can process messages from different key queues simultaneously.

4. **Ordered Processing**: Messages with the same key are processed sequentially from their respective queue, ensuring
   ordered processing for each key.

5. **Polling Mechanism**: The `KafkaConsumer` uses a polling mechanism to efficiently fetch messages from Kafka brokers.

6. **Partition Pausing**: If a partition becomes backed up (i.e., its queues are full), Prosody will pause consumption
   from that specific partition. Other partitions continue to make progress, ensuring that a slowdown in one partition
   doesn't affect the entire consumer.

### Message Flow

```mermaid
sequenceDiagram
    participant Kafka Broker
    participant Prosody Consumer
    participant Partition Manager
    participant Key Queue
    participant User Message Handler
    Kafka Broker ->> Prosody Consumer: Send message
    Prosody Consumer ->> Partition Manager: Dispatch message to correct partition
    Partition Manager ->> Key Queue: Enqueue message for specific key
    Key Queue ->> User Message Handler: Process message
    User Message Handler -->> Key Queue: Message processed
    Key Queue -->> Partition Manager: Send commit events
    Partition Manager -->> Prosody Consumer: Update latest processed offset
    Prosody Consumer -->> Kafka Broker: Commit offsets to Kafka
```

1. The `ProsodyConsumer` polls messages from Kafka Brokers.
2. Messages are dispatched to the appropriate `PartitionManager` based on their topic and partition.
3. The `PartitionManager` enqueues the message in the correct key-based queue according to the message key (e.g., User
   ID,
   Product ID).
4. Messages are processed sequentially from each key queue, invoking the user-provided `MessageHandler`.
5. After processing, the latest processed offset for the key is updated.
6. The `PartitionManager` tracks the partition's high watermark committed offset.
7. The Prosody Consumer periodically commits these offsets back to Kafka, ensuring at-least-once message processing
   semantics.
8. If a partition's queues become full, that specific partition is paused until the backlog is processed.

Throughout this flow, OpenTelemetry is used to create and propagate distributed traces, allowing for end-to-end
visibility of message processing across different services.

This architecture allows Prosody to achieve high throughput by processing different partitions and keys concurrently,
while still maintaining strict ordering for messages with the same key. It also provides backpressure management by
limiting the number of in-flight messages per key and partition through bounded queues and selective partition pausing.
