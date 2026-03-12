# Feature Specification: Kafka Telemetry Event Emission

**Feature Branch**: `002-kafka-telemetry`
**Created**: 2026-03-12
**Status**: Draft
**Input**: User description: "We want to implement docs/kafka-telemetry-plan.md"

## Scenarios & Testing *(mandatory)*

### Scenario 1 - Consumer Message Lifecycle Events (Priority: P1)

When the Prosody consumer processes a Kafka message, structured telemetry events are emitted to a dedicated Kafka topic at each lifecycle stage: dispatched (before handler), succeeded (on Ok), and failed (on Err). Each event captures wall-clock time as close to the causal moment as possible, trace context for correlation, and metadata (topic, partition, offset, key, source, hostname).

**Why this priority**: Message processing is the core workload. Observability into message lifecycle is the highest-value signal for external systems (ClickHouse dashboards, alerting, SLA tracking).

**Independent Test**: Can be fully tested by processing a message through the telemetry middleware and verifying that three event types (dispatched, succeeded, failed) appear on the telemetry broadcast channel with correct fields.

**Acceptance Criteria**:

1. **Given** a consumer is processing a message, **When** the message is dispatched to the inner handler, **Then** a `prosody.message.dispatched` event is emitted with `eventTime`, `demandType`, `topic`, `partition`, `offset`, `key`, `source`, `hostname`, and W3C trace context fields
2. **Given** a message handler returns Ok, **When** the handler completes, **Then** a `prosody.message.succeeded` event is emitted immediately with the same correlation fields
3. **Given** a message handler returns Err, **When** the handler fails, **Then** a `prosody.message.failed` event is emitted immediately with `errorCategory` (from ClassifyError) and `exception` (debug-formatted error) in addition to the standard fields
4. **Given** the telemetry emitter is enabled, **When** these events are produced to the broadcast channel, **Then** they are serialized to JSON and delivered to the configured Kafka telemetry topic

---

### Scenario 2 - Timer Lifecycle Events (Priority: P1)

When the Prosody timer system schedules or fires a timer, structured telemetry events are emitted at each stage: scheduled (after store write), dispatched (before handler), succeeded (on Ok), and failed (on Err). Timer events include `timerType` and `scheduledTime` in addition to the standard fields.

**Why this priority**: Timer observability is equally critical — timers are a core Prosody primitive and failures are harder to debug without lifecycle events.

**Independent Test**: Can be fully tested by scheduling a timer and processing a trigger through the telemetry middleware, verifying that scheduled/dispatched/succeeded/failed events appear with correct `timerType`, `scheduledTime`, and `demandType` fields.

**Acceptance Criteria**:

1. **Given** a timer is being scheduled, **When** the store write completes successfully, **Then** a `prosody.timer.scheduled` event is emitted immediately with `eventTime`, `scheduledTime`, `timerType`, `key`, `source`, `hostname`, and trace context
2. **Given** a timer fires, **When** the trigger is dispatched to the handler, **Then** a `prosody.timer.dispatched` event is emitted with `demandType` and `timerType`
3. **Given** a timer handler returns Ok, **When** the handler completes, **Then** a `prosody.timer.succeeded` event is emitted immediately
4. **Given** a timer handler returns Err, **When** the handler fails, **Then** a `prosody.timer.failed` event is emitted immediately with `errorCategory` and `exception`

---

### Scenario 3 - Producer Message Sent Events (Priority: P2)

When the Prosody producer successfully delivers a message to Kafka, a telemetry event is emitted capturing the destination topic, partition, offset, and trace context.

**Why this priority**: Producer telemetry enables end-to-end tracing (produce → consume → timer) but is lower priority since consumer-side events cover the majority of observability needs.

**Independent Test**: Can be tested by sending a message through the producer and verifying a `prosody.message.sent` event appears on the broadcast channel with destination topic/partition/offset.

**Acceptance Criteria**:

1. **Given** a producer sends a message, **When** the delivery acknowledgement is received, **Then** a `prosody.message.sent` event is emitted immediately with `eventTime`, destination `topic`, `partition`, `offset`, `key`, `source` (source_system), `hostname`, and trace context
2. **Given** telemetry is not configured on the producer, **When** a message is sent, **Then** no telemetry event is emitted and the send path is unaffected

---

### Scenario 4 - Kafka Telemetry Emitter (Priority: P1)

A background emitter subscribes to the internal telemetry broadcast channel, serializes telemetry events to JSON, and produces them concurrently to a configurable Kafka topic using the same bootstrap servers as the consumer.

**Why this priority**: Without the emitter, events stay internal — this is the bridge that makes telemetry externally observable.

**Independent Test**: Can be tested by publishing telemetry events to the broadcast channel and verifying they are serialized and produced to the configured Kafka topic.

**Acceptance Criteria**:

1. **Given** the emitter is enabled, **When** telemetry events are broadcast, **Then** they are serialized to JSON and produced to the configured Kafka topic concurrently (not one-at-a-time)
2. **Given** the emitter falls behind, **When** the broadcast channel lags, **Then** the emitter logs a warning with the number of skipped events and continues processing
3. **Given** a produce to the telemetry topic fails, **When** an error occurs, **Then** the error is logged and the emitter continues (telemetry is best-effort, never blocks the main workload)
4. **Given** the emitter is disabled, **When** the system starts, **Then** no emitter is spawned and no telemetry events are produced to Kafka

---

### Scenario 5 - Telemetry Configuration (Priority: P2)

The telemetry emitter is configurable via environment variables with sensible defaults. It reuses the consumer's bootstrap servers — no separate broker configuration.

**Why this priority**: Configuration is essential for deployment but straightforward; sensible defaults mean it works out of the box.

**Independent Test**: Can be tested by verifying default values and override behavior of configuration parameters.

**Acceptance Criteria**:

1. **Given** no telemetry environment variables are set, **When** the system starts, **Then** the emitter uses default topic `prosody.telemetry-events` and is enabled by default
2. **Given** `PROSODY_TELEMETRY_TOPIC` is set, **When** the system starts, **Then** the emitter uses the specified topic
3. **Given** `PROSODY_TELEMETRY_ENABLED=false`, **When** the system starts, **Then** no emitter is spawned
4. **Given** telemetry is enabled, **When** the emitter is created, **Then** it uses the same bootstrap servers as the consumer (no separate `PROSODY_TELEMETRY_BOOTSTRAP_SERVERS`)

---

### Edge Cases

- What happens when the broadcast channel is full and the emitter can't keep up? Events are dropped with a lag warning logged.
- What happens when the telemetry Kafka topic doesn't exist? The produce fails; the emitter logs the error and continues.
- What happens when trace context is not available on a span? `traceParent` and `traceState` are omitted from the JSON (null/absent).
- What happens when existing internal telemetry subscribers receive the new event variants? They ignore unknown `Data` variants — existing `Data::Partition` and `Data::Key` match arms are unaffected.
- What happens when the telemetry emitter is enabled but the Kafka cluster is unreachable? Produce futures time out; errors are logged; the emitter continues draining the broadcast channel.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST emit `prosody.message.dispatched`, `prosody.message.succeeded`, and `prosody.message.failed` events during consumer message processing
- **FR-002**: System MUST emit `prosody.timer.scheduled`, `prosody.timer.dispatched`, `prosody.timer.succeeded`, and `prosody.timer.failed` events during timer lifecycle
- **FR-003**: System MUST emit `prosody.message.sent` events after successful producer delivery
- **FR-004**: All events MUST include `eventTime` (wall-clock), `topic`, `partition`, `key`, `source`, `hostname`, and W3C trace context (`traceParent`, `traceState`) when available
- **FR-005**: Timer events MUST include `timerType` (`application`, `deferred_message`, `deferred_timer`) and `scheduledTime`
- **FR-006**: Events with dispatch context MUST include `demandType` (`normal` or `failure`)
- **FR-007**: Failed events MUST include `errorCategory` (`transient`, `permanent`, `terminal`) and `exception` (debug-formatted error)
- **FR-008**: Message events MUST include `offset` (Kafka offset of the consumed or produced message)
- **FR-009**: The telemetry emitter MUST produce events concurrently (not awaiting acks one at a time)
- **FR-010**: The telemetry emitter MUST reuse the consumer's bootstrap servers — no separate broker configuration
- **FR-011**: The default telemetry topic MUST be `prosody.telemetry-events`
- **FR-012**: The telemetry emitter MUST be opt-out (enabled by default) and configurable via `PROSODY_TELEMETRY_ENABLED`
- **FR-013**: The telemetry topic MUST be configurable via `PROSODY_TELEMETRY_TOPIC`
- **FR-014**: `eventTime` MUST be captured as close to the causal event as possible (immediately after store write, immediately before/after handler call)
- **FR-015**: Telemetry emission MUST NOT block or degrade the main consumer/producer workload
- **FR-016**: Existing internal telemetry subscribers MUST NOT be affected by the new event variants
- **FR-017**: All events MUST be serialized as JSON with camelCase field names
- **FR-018**: `hostname` MUST be resolved once at emitter startup, not per-event

### Key Entities

- **TelemetryEvent**: An internal broadcast event enriched with new data variants (Timer, Message, MessageSent) carrying lifecycle metadata and trace context
- **TelemetryEmitter**: A background task that subscribes to the broadcast channel, serializes events to JSON, and produces them concurrently to a Kafka topic
- **TelemetryInjector**: A zero-allocation trace context extractor that captures W3C traceparent/tracestate without HashMap overhead

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All 8 telemetry event types are emitted at the correct lifecycle points and appear on the configured Kafka topic with valid JSON
- **SC-002**: Events can be correlated end-to-end (produce → consume → timer) via W3C trace context fields in an external system like ClickHouse
- **SC-003**: Telemetry emission adds no observable latency to message or timer processing (best-effort, fire-and-forget via broadcast channel)
- **SC-004**: The system operates correctly with telemetry disabled — no emitter spawned, no events produced, no errors logged
- **SC-005**: Existing telemetry-dependent features (virtual-time fairness, monopolization detection, failure tracking) continue to function without modification
- **SC-006**: All code passes `cargo clippy`, `cargo clippy --tests`, `cargo doc`, and `cargo +nightly fmt` with zero warnings

## Assumptions

- The existing broadcast channel (capacity 8096) is sufficient for telemetry event throughput; no capacity increase is needed
- The `bytes` crate is available or can be added as a dependency for the emitter
- `whoami::hostname()` is available or can be added for hostname resolution
- The existing `ClassifyError` trait is implemented on all handler error types, making `errorCategory` always available on failure events
- `CompactDateTime` has an existing conversion to wall-clock time for `scheduledTime` serialization
