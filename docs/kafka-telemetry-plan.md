# Kafka Telemetry Event Emission

## Context

Prosody has an internal `TelemetryEvent` broadcast system used for runtime scheduling decisions (virtual-time fairness, monopolization detection, failure tracking). We want to augment this system to also emit structured lifecycle events to a Kafka topic so external systems (ClickHouse, etc.) can observe timer and message activity. The trace context on these events enables joining back to the original message in ClickHouse.

## Events

| Event | Emission Point | Span Source | `source` field |
|---|---|---|---|
| `prosody.timer.scheduled` | `TimerManager::schedule()` — immediately after store write | `Span::current()` (ambient) | consumer group_id |
| `prosody.timer.dispatched` | `TelemetryHandler::on_timer()` — before calling inner handler | `trigger.span()` (explicit) | consumer group_id |
| `prosody.timer.succeeded` | `TelemetryHandler::on_timer()` — immediately on Ok return | `trigger.span()` (explicit) | consumer group_id |
| `prosody.timer.failed` | `TelemetryHandler::on_timer()` — immediately on Err return | `trigger.span()` (explicit) | consumer group_id |
| `prosody.message.dispatched` | `TelemetryHandler::on_message()` — before calling inner handler | `message.span()` (explicit) | consumer group_id |
| `prosody.message.succeeded` | `TelemetryHandler::on_message()` — immediately on Ok return | `message.span()` (explicit) | consumer group_id |
| `prosody.message.failed` | `TelemetryHandler::on_message()` — immediately on Err return | `message.span()` (explicit) | consumer group_id |
| `prosody.message.sent` | `ProsodyProducer::send()` — immediately after delivery ack | `Span::current()` (ambient) | producer source_system |

## JSON Schemas

### Timer scheduled
```json
{
    "type": "prosody.timer.scheduled",
    "eventTime": "2026-03-11T18:08:04Z",
    "scheduledTime": "2026-03-11T18:08:34Z",
    "timerType": "application",
    "topic": "witco.lead-events",
    "partition": 21,
    "traceParent": "00-...-...-01",
    "traceState": "...",
    "key": "some-key",
    "source": "rookery",
    "hostname": "host-1"
}
```

### Timer dispatched/succeeded
```json
{
    "type": "prosody.timer.dispatched",
    "eventTime": "2026-03-11T18:08:34Z",
    "scheduledTime": "2026-03-11T18:08:34Z",
    "timerType": "application",
    "demandType": "normal",
    "topic": "witco.lead-events",
    "partition": 21,
    "traceParent": "00-...-...-01",
    "traceState": "...",
    "key": "some-key",
    "source": "rookery",
    "hostname": "host-1"
}
```

### Timer failed
```json
{
    "type": "prosody.timer.failed",
    "eventTime": "...",
    "scheduledTime": "...",
    "timerType": "deferred_message",
    "demandType": "failure",
    "topic": "...",
    "partition": 21,
    "traceParent": "...",
    "traceState": "...",
    "key": "some-key",
    "source": "rookery",
    "hostname": "host-1",
    "errorCategory": "transient",
    "exception": "dump of stack trace"
}
```

### Message dispatched/succeeded
```json
{
    "type": "prosody.message.dispatched",
    "eventTime": "2026-03-11T18:08:04Z",
    "demandType": "normal",
    "topic": "witco.lead-events",
    "partition": 21,
    "offset": 12345,
    "traceParent": "00-...-...-01",
    "traceState": "...",
    "key": "some-key",
    "source": "rookery",
    "hostname": "host-1"
}
```

### Message failed
```json
{
    "type": "prosody.message.failed",
    "eventTime": "...",
    "demandType": "normal",
    "topic": "...",
    "partition": 21,
    "offset": 12345,
    "traceParent": "...",
    "traceState": "...",
    "key": "some-key",
    "source": "rookery",
    "hostname": "host-1",
    "errorCategory": "permanent",
    "exception": "dump of stack trace"
}
```

### Message sent (producer)
```json
{
    "type": "prosody.message.sent",
    "eventTime": "...",
    "topic": "witco.lead-events",
    "partition": 21,
    "offset": 4,
    "traceParent": "00-...-...-01",
    "traceState": "...",
    "key": "",
    "source": "rookery",
    "hostname": "host-1"
}
```

## Field Reference

| Field | Type | Present On | Description |
|---|---|---|---|
| `type` | string | all | Event name (`prosody.timer.*`, `prosody.message.*`) |
| `eventTime` | ISO 8601 | all | Wall-clock time captured as close to the causal event as possible (`Utc::now()` at emission point) |
| `scheduledTime` | ISO 8601 | timer.* | When the timer is/was scheduled to fire |
| `timerType` | string | timer.* | `"application"`, `"deferred_message"`, or `"deferred_timer"` |
| `demandType` | string | timer.dispatched/succeeded/failed, message.dispatched/succeeded/failed | `"normal"` or `"failure"` |
| `topic` | string | all | Consumer source topic (timer/message) or producer destination topic (sent) |
| `partition` | i32 | all | Consumer source partition or producer destination partition |
| `offset` | i64 | message.* | Kafka offset of the consumed message (dispatched/succeeded/failed) or produced message (sent) |
| `traceParent` | string? | all | W3C traceparent header |
| `traceState` | string? | all | W3C tracestate header |
| `key` | string | all | Message or timer entity key |
| `source` | string | all | Consumer `group_id` or producer `source_system` |
| `hostname` | string | all | Machine hostname, resolved once at emitter startup |
| `errorCategory` | string | *.failed | `"transient"`, `"permanent"`, or `"terminal"` (from `ClassifyError`) |
| `exception` | string | *.failed | `format!("{e:?}")` of the error |

## Approach

Enrich the existing `Data` enum with new variants (`Timer`, `Message`, `MessageSent`). Add new emission methods to the senders. Spawn a new Kafka subscriber event loop that filters for the new variants, serializes to JSON via simd-json with a thread-local buffer, and produces concurrently to a configurable topic (default `prosody.telemetry-events`) using `buffer_unordered`. The emitter reuses the consumer's bootstrap servers — no separate configuration. Enum fields use standard `#[derive(Serialize)]` with `#[serde(rename_all = "...")]`. A custom `TelemetryInjector` avoids the `HashMap<String, String>` allocation that the standard propagator path requires. Existing internal subscribers are unaffected — they match `Data::Key(..)` and ignore unknown variants.

---

## Design Decisions

### simd-json for serialization

The emitter serializes every event to JSON before producing to Kafka. We use `simd_json::to_writer()` writing into a thread-local `Vec<u8>` buffer (`.clear()` between events, capacity retained) to avoid per-event serialization allocation. After serialization, the buffer contents are copied into a `Bytes` for the produce future (cheap — just a memcpy into a ref-counted buffer). This follows the existing codebase pattern where the producer already aliases `simd_json as json` on non-ARM and the consumer poll loop reuses `simd_json::Buffers` across iterations.

The cfg gate pattern matches existing usage:
```rust
#[cfg(not(target_arch = "arm"))]
use simd_json as json;
#[cfg(target_arch = "arm")]
use serde_json as json;
```

### Custom `TelemetryInjector` for zero-alloc trace context extraction

The existing `inject_span_context` pattern (in `cassandra/mod.rs:87-93`) allocates a `HashMap<String, String>` per call. For the telemetry hot path, we instead implement `opentelemetry::propagation::Injector` directly on a struct that writes into fixed `Option<Box<str>>` fields:

```rust
struct TelemetryInjector {
    trace_parent: Option<Box<str>>,
    trace_state: Option<Box<str>>,
}

impl Injector for TelemetryInjector {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" => self.trace_parent = Some(value.into_boxed_str()),
            "tracestate" => self.trace_state = Some(value.into_boxed_str()),
            _ => {} // baggage headers ignored for telemetry
        }
    }
}
```

This avoids: HashMap allocation, HashMap hashing, extra String copies. The propagator's `inject_context` calls `set` directly into our fields. We construct a single `TextMapCompositePropagator` once in the emitter and reuse it for every event (or pass the existing one where available).

For the `TelemetryHandler` path (message/timer dispatched/succeeded/failed), the propagator lives on `TelemetryHandler` as a field — constructed once in `handler_for_partition`.

---

## Implementation Steps

### Step 1: New event types in `src/telemetry/event.rs`

Add to the `Data` enum:
```rust
pub enum Data {
    Partition(PartitionEvent),
    Key(KeyEvent),
    Timer(TimerTelemetryEvent),        // NEW
    Message(MessageTelemetryEvent),    // NEW
    MessageSent(MessageSentEvent),     // NEW
}
```

New structs:
```rust
#[derive(Clone, Debug)]
pub struct TimerTelemetryEvent {
    pub event_type: TimerEventType,
    pub event_time: DateTime<Utc>,
    pub scheduled_time: CompactDateTime,
    pub timer_type: TimerType,
    pub key: Key,
    pub source: Arc<str>,
    pub trace_parent: Option<Box<str>>,
    pub trace_state: Option<Box<str>>,
}

#[derive(Clone, Debug)]
pub enum TimerEventType {
    Scheduled,
    Dispatched { demand_type: DemandType },
    Succeeded { demand_type: DemandType },
    Failed {
        demand_type: DemandType,
        error_category: ErrorCategory,
        exception: Box<str>,
    },
}

#[derive(Clone, Debug)]
pub struct MessageTelemetryEvent {
    pub event_type: MessageEventType,
    pub event_time: DateTime<Utc>,
    pub offset: Offset,
    pub key: Key,
    pub source: Arc<str>,
    pub trace_parent: Option<Box<str>>,
    pub trace_state: Option<Box<str>>,
}

#[derive(Clone, Debug)]
pub enum MessageEventType {
    Dispatched { demand_type: DemandType },
    Succeeded { demand_type: DemandType },
    Failed {
        demand_type: DemandType,
        error_category: ErrorCategory,
        exception: Box<str>,
    },
}

#[derive(Clone, Debug)]
pub struct MessageSentEvent {
    pub event_time: DateTime<Utc>,
    pub topic: Topic,
    pub partition: Partition,
    pub offset: i64,
    pub key: Key,
    pub source: Arc<str>,
    pub trace_parent: Option<Box<str>>,
    pub trace_state: Option<Box<str>>,
}
```

Notes:
- `event_time` is `DateTime<Utc>` (wall clock via `Utc::now()` captured as close to the causal event as possible — e.g., immediately after store write for `scheduled`, immediately before/after inner handler call for dispatched/succeeded/failed). NOT the existing `quanta::Instant` timestamp on `TelemetryEvent` (which is monotonic and used by internal subscribers).
- `demand_type` lives inside the event type enum variants (not on the outer struct) because `Scheduled` has no demand type — it occurs before dispatch.
- `timer_type` is on the outer struct because it's always present for timer events. `TimerType` has three variants: `Application`, `DeferredMessage`, `DeferredTimer`.
- `error_category` is extracted via `ClassifyError::classify_error()` on the error. The `FallibleHandler` trait bound requires `Error: ClassifyError`, so this is always available.
- Timer and message topic/partition come from the outer `TelemetryEvent.topic`/`.partition` (already set by the partition sender). `MessageSentEvent` carries its own topic/partition since they're the *destination*, not the consumer partition.
- `MessageTelemetryEvent` carries `offset` from `ConsumerMessage.offset()` — the Kafka offset of the consumed message being processed.
- `source` is `Arc<str>` — cheap to clone from the group_id that's already `Arc<str>` at the call site.

### Step 2: Custom `TelemetryInjector` in `src/telemetry/injector.rs`

New file. Implements `opentelemetry::propagation::Injector` to extract trace context without HashMap allocation:

```rust
use opentelemetry::propagation::{Injector, TextMapPropagator};
use opentelemetry::Context;

pub(crate) struct TelemetryInjector {
    pub trace_parent: Option<Box<str>>,
    pub trace_state: Option<Box<str>>,
}

impl TelemetryInjector {
    pub fn new() -> Self {
        Self {
            trace_parent: None,
            trace_state: None,
        }
    }

    /// Extracts trace context from the given OTel context using the provided propagator.
    pub fn extract(
        propagator: &impl TextMapPropagator,
        context: &Context,
    ) -> Self {
        let mut injector = Self::new();
        propagator.inject_context(context, &mut injector);
        injector
    }
}

impl Injector for TelemetryInjector {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" => self.trace_parent = Some(value.into_boxed_str()),
            "tracestate" => self.trace_state = Some(value.into_boxed_str()),
            _ => {}
        }
    }
}
```

### Step 3: New emission methods on `TelemetryPartitionSender` (`src/telemetry/partition.rs`)

Add a `propagator` field (constructed once in `TelemetryPartitionSender::new()`):

```rust
propagator: TextMapCompositePropagator,
```

Add seven methods for consumer-side events. These build the appropriate `Data` variant and send through the existing broadcast channel:

**Timer methods:**
- `timer_scheduled(&self, key, scheduled_time, timer_type, source)` — captures `Span::current()` internally
- `timer_dispatched(&self, key, scheduled_time, timer_type, demand_type, source, span: &Span)` — explicit span
- `timer_succeeded(&self, key, scheduled_time, timer_type, demand_type, source, span: &Span)` — explicit span
- `timer_failed(&self, key, scheduled_time, timer_type, demand_type, error_category, source, span: &Span, exception)` — explicit span

**Message methods:**
- `message_dispatched(&self, key, offset, demand_type, source, span: &Span)` — explicit span
- `message_succeeded(&self, key, offset, demand_type, source, span: &Span)` — explicit span
- `message_failed(&self, key, offset, demand_type, error_category, source, span: &Span, exception)` — explicit span

Each calls `TelemetryInjector::extract(&self.propagator, &span.context())` and `Utc::now()` for `event_time`.

### Step 4: New emission method on `TelemetrySender` (`src/telemetry/sender.rs`)

Add a `propagator` field (constructed once in `TelemetrySender::new()`).

Add `message_sent(&self, topic, partition, offset, key, source)`. Captures `Span::current()` internally. Uses `TelemetryInjector::extract()`. Builds `Data::MessageSent(MessageSentEvent { .. })`. The outer `TelemetryEvent.topic`/`.partition` are set to the *destination* topic/partition.

### Step 5: Thread `TelemetrySender` into `PartitionConfiguration` and `TimerManager`

**`src/consumer/partition/mod.rs`** — add field to `PartitionConfiguration<T>`:
```rust
pub telemetry_sender: TelemetrySender,
```

**`src/consumer/kafka_context.rs`** — `Context::new()` already receives `telemetry: TelemetrySender` (line 90). Pass it into `PartitionConfiguration` at line 99.

**`src/timers/manager.rs`** — add to `TimerManagerInner<T>`:
```rust
telemetry: Option<TelemetryPartitionSender>,
source: Arc<str>,
```

Update `TimerManager::new()` signature with two new params. In `schedule()`, after successful store write, emit `timer_scheduled` if telemetry is `Some`. Pass `trigger.timer_type` through.

**`src/consumer/partition/mod.rs` `handle_messages()`** — create `TelemetryPartitionSender` from `config.telemetry_sender.for_partition(topic, partition)` and pass into `TimerManager::new()` along with `config.group_id.clone()`.

**Test call sites** of `TimerManager::new()` — pass `None` for telemetry and `Arc::from("")` for source.

### Step 6: Wire message and timer events into `TelemetryMiddleware` (`src/consumer/middleware/telemetry.rs`)

Add `source: Arc<str>` field to `TelemetryMiddleware`, `TelemetryProvider`, `TelemetryHandler`.

Update `TelemetryMiddleware::new()`:
```rust
pub fn new(telemetry: Telemetry, source: Arc<str>) -> Self
```

Update `on_message()` in `TelemetryHandler`:
1. Capture `message.span()`, `message.key().clone()`, `message.offset()` before passing to inner handler
2. Before inner call: emit `message_dispatched` via `self.sender` with the captured span and `demand_type`
3. On Ok: emit `message_succeeded` with `demand_type`
4. On Err: emit `message_failed` with `demand_type`, `e.classify_error()`, and `format!("{e:?}")` as exception

Update `on_timer()` in `TelemetryHandler`:
1. Capture `trigger.time`, `trigger.key.clone()`, `trigger.timer_type`, `trigger.span()` before passing trigger to inner handler
2. Before inner call: emit `timer_dispatched` via `self.sender` with `demand_type` and `timer_type`
3. On Ok: emit `timer_succeeded` with `demand_type` and `timer_type`
4. On Err: emit `timer_failed` with `demand_type`, `timer_type`, `e.classify_error()`, and `format!("{e:?}")` as exception

Note: `T::Error: ClassifyError` is guaranteed by the `FallibleHandler` trait bound, so `classify_error()` is always available.

Update `build_common_middleware()` in `src/consumer/mod.rs` — pass `Arc::from(...)` of the group_id as source. The function already has access to the consumer config.

### Step 7: Wire `message.sent` into `ProsodyProducer` (`src/producer/mod.rs`)

Add `telemetry: Option<TelemetrySender>` field. Add `with_telemetry(mut self, sender: TelemetrySender) -> Self` builder method.

In `send()`, after successful delivery (after the span recording at line 402):
```rust
if let Some(ref sender) = self.telemetry {
    sender.message_sent(topic, delivery.partition, delivery.offset, key.clone(), ...);
}
```

`source` = `Arc::from(self.source_system.as_ref())`.

### Step 8: New `src/telemetry/emitter.rs` — Kafka subscriber event loop

New file. Key components:

**Configuration:**
```rust
#[derive(Builder, Clone, Debug, Validate)]
pub struct TelemetryEmitterConfiguration {
    pub topic: String,   // PROSODY_TELEMETRY_TOPIC (default: "prosody.telemetry-events")
    pub enabled: bool,   // PROSODY_TELEMETRY_ENABLED (default: true)
}
```

Bootstrap servers are inherited from the consumer's existing Kafka configuration — no separate `PROSODY_TELEMETRY_BOOTSTRAP_SERVERS` env var. The emitter receives the bootstrap servers from the consumer config at construction time.

**Entry point:**
```rust
pub fn spawn_telemetry_emitter(
    config: &TelemetryEmitterConfiguration,
    bootstrap_servers: &[String],
    telemetry: &Telemetry,
) -> Result<JoinHandle<()>, EmitterError>
```

Resolves `hostname` once via `whoami::hostname()`. Creates a dedicated `FutureProducer` using the provided `bootstrap_servers` (same as consumer). Subscribes to broadcast channel. Spawns background task.

**Event loop — thread-local buffer reuse + concurrent produce via `buffer_unordered`:**

The emitter receives events from the broadcast channel and produces to Kafka concurrently using `StreamExt::buffer_unordered`. This avoids waiting for acks one at a time — many produce futures run in flight simultaneously. Serialization uses a thread-local `Vec<u8>` buffer that retains capacity across events, then copies into `Bytes` for the produce future.

```rust
use bytes::Bytes;
use futures::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

thread_local! {
    static SERIALIZE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(512));
}

BroadcastStream::new(rx)
    .filter_map(|result| async {
        match result {
            Ok(event) => Some(event),
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                warn!(skipped = n, "telemetry emitter lagged");
                None
            }
        }
    })
    .filter_map(|event| {
        // Serialize into thread-local buffer, then copy to Bytes for the produce future
        let bytes = SERIALIZE_BUF.with_borrow_mut(|buf| {
            buf.clear();
            serialize_event(buf, &event, &hostname)?;
            Some(Bytes::copy_from_slice(buf))
        })?;
        Some(bytes)
    })
    .map(|bytes| {
        let record = OwnedRecord::to(&topic)
            .payload(&bytes)
            .key(&[] as &[u8]);
        producer.send(record, Timeout::After(Duration::from_secs(1)))
    })
    .buffer_unordered(64) // up to 64 produce futures in flight
    .for_each(|result| async {
        if let Err((e, _)) = result {
            warn!(error = %e, "telemetry produce failed");
        }
    })
    .await;
```

The thread-local buffer avoids allocation churn — `clear()` retains capacity across events. The `Bytes::copy_from_slice` is a single memcpy into a ref-counted buffer that the produce future owns independently.

**Serialization structs** use `#[derive(Serialize)]` with `#[serde(rename_all = "camelCase")]`. Enum fields use standard serde derive serialization — no hand-written string conversion functions:

```rust
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TimerEventPayload<'a> {
    #[serde(rename = "type")]
    event_type: TelemetryEventName,
    event_time: &'a str,
    scheduled_time: &'a str,
    timer_type: TimerType,
    #[serde(skip_serializing_if = "Option::is_none")]
    demand_type: Option<DemandType>,
    topic: &'a str,
    partition: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_parent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_state: Option<&'a str>,
    key: &'a str,
    source: &'a str,
    hostname: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_category: Option<ErrorCategory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exception: Option<&'a str>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageEventPayload<'a> {
    #[serde(rename = "type")]
    event_type: TelemetryEventName,
    event_time: &'a str,
    demand_type: DemandType,
    topic: &'a str,
    partition: i32,
    offset: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_parent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_state: Option<&'a str>,
    key: &'a str,
    source: &'a str,
    hostname: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_category: Option<ErrorCategory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exception: Option<&'a str>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageSentPayload<'a> {
    #[serde(rename = "type")]
    event_type: TelemetryEventName,
    event_time: &'a str,
    topic: &'a str,
    partition: i32,
    offset: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_parent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_state: Option<&'a str>,
    key: &'a str,
    source: &'a str,
    hostname: &'a str,
}
```

**Enum serialization via standard serde derive** — all enums use `#[derive(Serialize)]` with `#[serde(rename_all = "...")]`. No hand-written `*_str()` conversion functions:

```rust
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerType {
    Application,
    DeferredMessage,
    DeferredTimer,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DemandType {
    Normal,
    Failure,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorCategory {
    Transient,
    Permanent,
    Terminal,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum TelemetryEventName {
    #[serde(rename = "prosody.timer.scheduled")]
    TimerScheduled,
    #[serde(rename = "prosody.timer.dispatched")]
    TimerDispatched,
    #[serde(rename = "prosody.timer.succeeded")]
    TimerSucceeded,
    #[serde(rename = "prosody.timer.failed")]
    TimerFailed,
    #[serde(rename = "prosody.message.dispatched")]
    MessageDispatched,
    #[serde(rename = "prosody.message.succeeded")]
    MessageSucceeded,
    #[serde(rename = "prosody.message.failed")]
    MessageFailed,
    #[serde(rename = "prosody.message.sent")]
    MessageSent,
}
```

**`eventTime` handling:** New event structs carry `event_time: DateTime<Utc>` captured at emission time. Formatted via `.to_rfc3339_opts(SecondsFormat::Millis, true)` into a local `String`.

**`scheduledTime` serialization:** `CompactDateTime` -> `DateTime<Utc>` via existing `Into` impl, then `.to_rfc3339_opts(SecondsFormat::Millis, true)`.

**Hostname:** Resolved once in `spawn_telemetry_emitter()`, passed by value into the event loop. Not per-event.

### Step 9: Wire emitter into `HighLevelClient` (`src/high_level/mod.rs`)

Add `telemetry: Telemetry` field to `HighLevelClient<T>`.

In `HighLevelClient::new()`:
1. Create `Telemetry::new()` once
2. Wire into producer: `producer = producer.with_telemetry(telemetry.sender())`
3. Store on struct

Add optional `TelemetryEmitterConfiguration` to `ConsumerBuilders` (or as a separate param). When present and enabled, call `spawn_telemetry_emitter()` in `new()`.

In `subscribe()`: pass `telemetry.clone()` to consumer constructors so they use the shared broadcast channel instead of creating their own `Telemetry::new()`.

Expose `pub fn telemetry(&self) -> &Telemetry` for direct access.

### Step 10: Exports in `src/telemetry/mod.rs` and `src/lib.rs`

Add `pub mod emitter;` and `pub(crate) mod injector;` to `src/telemetry/mod.rs`. Ensure `TelemetryEmitterConfiguration`, `spawn_telemetry_emitter`, and `EmitterError` are accessible.

---

## Files Changed (ordered by dependency)

| File | Change |
|---|---|
| `src/telemetry/event.rs` | Add `TimerTelemetryEvent`, `TimerEventType`, `MessageTelemetryEvent`, `MessageEventType`, `MessageSentEvent`, new `Data` variants |
| `src/telemetry/injector.rs` | **New file** — `TelemetryInjector` implementing `Injector` for zero-alloc trace context extraction |
| `src/telemetry/mod.rs` | Add `pub(crate) mod injector`, `pub mod emitter` |
| `src/telemetry/partition.rs` | Add `propagator` field, 4 timer + 3 message emission methods using `TelemetryInjector` |
| `src/telemetry/sender.rs` | Add `propagator` field, `message_sent()` method using `TelemetryInjector` |
| `src/telemetry/emitter.rs` | **New file** — config, `buffer_unordered` event loop, thread-local simd-json serialization, serde-derived enum serialization |
| `src/timers/manager.rs` | Add `telemetry`/`source` fields, emit in `schedule()` with `timer_type` |
| `src/consumer/partition/mod.rs` | Add `telemetry_sender` to `PartitionConfiguration`, thread into `TimerManager::new()` |
| `src/consumer/kafka_context.rs` | Pass `telemetry` into `PartitionConfiguration` |
| `src/consumer/middleware/telemetry.rs` | Add `source` field, emit message + timer events with `demand_type`, `timer_type`, `error_category` in `on_message()`/`on_timer()` |
| `src/consumer/mod.rs` | Update `build_common_middleware` and `TelemetryMiddleware::new()` call, thread telemetry into partition config |
| `src/producer/mod.rs` | Add `telemetry` field + `with_telemetry()`, emit in `send()` |
| `src/high_level/mod.rs` | Lift `Telemetry` to struct, wire to producer + consumer, spawn emitter |

---

## Performance Characteristics

### Allocation profile (steady-state, per event)

| Component | Allocations | Notes |
|---|---|---|
| `TelemetryInjector` | 2 `Box<str>` | `traceparent` + `tracestate` from propagator — unavoidable since propagator owns the `String` |
| Serialization buffer | 0 | Thread-local `Vec<u8>` cleared and reused across events (capacity retained) |
| Produce payload | 1 `Bytes` | `Bytes::copy_from_slice` — single memcpy from thread-local buffer into ref-counted buffer for the in-flight produce future |
| `DateTime::to_rfc3339()` | 1 `String` | Stack-local, dropped after serialization |
| `CompactDateTime` conversion | 1 `String` | Same as above (timer events only) |
| Enum serialization | 0 | serde `#[derive(Serialize)]` with `rename_all` — inline `&'static str` |
| Kafka record | 0 extra | Payload is `&[u8]` reference to `Bytes` |

### What we avoid vs naive approach

| Naive approach | Our approach | Savings |
|---|---|---|
| `HashMap<String, String>` per trace extraction | `TelemetryInjector` with fixed fields | No HashMap alloc, no hashing |
| `serde_json::to_vec()` new Vec per event | Thread-local `Vec<u8>` + `json::to_writer` | No Vec alloc after warmup |
| `serde_json::Value` intermediate | `#[derive(Serialize)]` structs with borrowed refs | No Value tree alloc |
| `serde_json` on non-ARM | `simd_json` on non-ARM | SIMD-accelerated serialization |
| Custom `*_str()` match functions | `#[derive(Serialize)]` + `#[serde(rename_all)]` on enums | Idiomatic, zero-alloc via serde codegen |
| Await produce one at a time | `buffer_unordered(64)` stream combinator | Up to 64 produces in flight concurrently |

---

## Verification

1. **Compile:** `cargo clippy` and `cargo clippy --tests` — zero warnings
2. **Unit tests:** Existing telemetry middleware tests pass (new `Data` variants don't break pattern matches)
3. **New unit tests in `src/telemetry/injector.rs`:** Verify `TelemetryInjector::extract()` captures traceparent/tracestate correctly
4. **New unit tests in `src/telemetry/emitter.rs`:** Verify serialization produces correct JSON for each event type including `timerType`, `demandType`, `errorCategory` fields via serde derive; verify thread-local buffer reuse
5. **New unit tests in `src/consumer/middleware/telemetry.rs`:** Verify `on_message` emits `Message(Dispatched)`, `Message(Succeeded)`, `Message(Failed)` events with correct `demand_type` and `error_category`; verify `on_timer` emits `Timer(Dispatched)`, `Timer(Succeeded)`, `Timer(Failed)` events with `timer_type`, `demand_type`, and `error_category` via broadcast subscriber
6. **Integration:** Run with `PROSODY_TELEMETRY_ENABLED=true` (uses consumer bootstrap servers, default topic `prosody.telemetry-events`) and verify events appear on the topic
7. **Format:** `cargo +nightly fmt`
8. **Docs:** `cargo doc`
