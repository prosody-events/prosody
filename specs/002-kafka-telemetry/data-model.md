# Data Model: Kafka Telemetry Event Emission

## Entities

### TelemetryEvent (existing, enriched)

The existing internal broadcast event. Enriched with three new `Data` variants.

| Field | Type | Description |
|-------|------|-------------|
| timestamp | quanta::Instant | Monotonic timestamp (existing, used by internal subscribers) |
| topic | Topic | Consumer source topic or producer destination topic |
| partition | Partition | Consumer source partition or producer destination partition |
| data | Data | Event payload — now includes Timer, Message, MessageSent variants |

### Data enum (existing, enriched)

```
Data
├── Partition(PartitionEvent)     # existing
├── Key(KeyEvent)                 # existing
├── Timer(TimerTelemetryEvent)    # NEW
├── Message(MessageTelemetryEvent) # NEW
└── MessageSent(MessageSentEvent) # NEW
```

### TimerTelemetryEvent (new)

| Field | Type | Description |
|-------|------|-------------|
| event_type | TimerEventType | Lifecycle stage (Scheduled, Dispatched, Succeeded, Failed) |
| event_time | DateTime\<Utc\> | Wall-clock time captured at the causal moment |
| scheduled_time | CompactDateTime | When the timer is/was scheduled to fire |
| timer_type | TimerType | Application, DeferredMessage, or DeferredTimer |
| key | Key | Timer entity key |
| source | Arc\<str\> | Consumer group_id |
| trace_parent | Option\<Box\<str\>\> | W3C traceparent header |
| trace_state | Option\<Box\<str\>\> | W3C tracestate header |

### TimerEventType (new)

Discriminated union — lifecycle stage with stage-specific fields.

| Variant | Additional Fields | Description |
|---------|------------------|-------------|
| Scheduled | — | Timer was written to store |
| Dispatched | demand_type: DemandType | Timer fired, handler about to be called |
| Succeeded | demand_type: DemandType | Handler returned Ok |
| Failed | demand_type: DemandType, error_category: ErrorCategory, exception: Box\<str\> | Handler returned Err |

### MessageTelemetryEvent (new)

| Field | Type | Description |
|-------|------|-------------|
| event_type | MessageEventType | Lifecycle stage (Dispatched, Succeeded, Failed) |
| event_time | DateTime\<Utc\> | Wall-clock time captured at the causal moment |
| offset | Offset | Kafka offset of the consumed message |
| key | Key | Message entity key |
| source | Arc\<str\> | Consumer group_id |
| trace_parent | Option\<Box\<str\>\> | W3C traceparent header |
| trace_state | Option\<Box\<str\>\> | W3C tracestate header |

### MessageEventType (new)

| Variant | Additional Fields | Description |
|---------|------------------|-------------|
| Dispatched | demand_type: DemandType | Message dispatched to handler |
| Succeeded | demand_type: DemandType | Handler returned Ok |
| Failed | demand_type: DemandType, error_category: ErrorCategory, exception: Box\<str\> | Handler returned Err |

### MessageSentEvent (new)

| Field | Type | Description |
|-------|------|-------------|
| event_time | DateTime\<Utc\> | Wall-clock time immediately after delivery ack |
| topic | Topic | Destination topic |
| partition | Partition | Destination partition |
| offset | i64 | Destination offset |
| key | Key | Message key |
| source | Arc\<str\> | Producer source_system |
| trace_parent | Option\<Box\<str\>\> | W3C traceparent header |
| trace_state | Option\<Box\<str\>\> | W3C tracestate header |

### TelemetryInjector (new)

Zero-allocation trace context extractor. Implements `opentelemetry::propagation::Injector`.

| Field | Type | Description |
|-------|------|-------------|
| trace_parent | Option\<Box\<str\>\> | Captured from propagator |
| trace_state | Option\<Box\<str\>\> | Captured from propagator |

### TelemetryEmitterConfiguration (new)

| Field | Type | Default | Env Var |
|-------|------|---------|---------|
| topic | String | `prosody.telemetry-events` | PROSODY_TELEMETRY_TOPIC |
| enabled | bool | true | PROSODY_TELEMETRY_ENABLED |

Bootstrap servers are passed separately from the consumer config — not part of this struct.

## Shared Enums (existing, enriched with Serialize)

### TimerType (existing — add `#[derive(Serialize)]` + `#[serde(rename_all = "camelCase")]`)

| Variant | JSON value |
|---------|-----------|
| Application | `"application"` |
| DeferredMessage | `"deferredMessage"` |
| DeferredTimer | `"deferredTimer"` |

### DemandType (existing — add `#[derive(Serialize)]` + `#[serde(rename_all = "camelCase")]`)

| Variant | JSON value |
|---------|-----------|
| Normal | `"normal"` |
| Failure | `"failure"` |

### ErrorCategory (existing — add `#[derive(Serialize)]` + `#[serde(rename_all = "camelCase")]`)

| Variant | JSON value |
|---------|-----------|
| Transient | `"transient"` |
| Permanent | `"permanent"` |
| Terminal | `"terminal"` |

### TelemetryEventName (new)

| Variant | JSON value |
|---------|-----------|
| TimerScheduled | `"prosody.timer.scheduled"` |
| TimerDispatched | `"prosody.timer.dispatched"` |
| TimerSucceeded | `"prosody.timer.succeeded"` |
| TimerFailed | `"prosody.timer.failed"` |
| MessageDispatched | `"prosody.message.dispatched"` |
| MessageSucceeded | `"prosody.message.succeeded"` |
| MessageFailed | `"prosody.message.failed"` |
| MessageSent | `"prosody.message.sent"` |

## Relationships

```
TelemetryEvent 1──* Data (Timer | Message | MessageSent)
TimerTelemetryEvent 1──1 TimerEventType (Scheduled | Dispatched | Succeeded | Failed)
MessageTelemetryEvent 1──1 MessageEventType (Dispatched | Succeeded | Failed)
TimerEventType.Failed ──> ErrorCategory
MessageEventType.Failed ──> ErrorCategory
TelemetryEmitter ──subscribes──> broadcast::Receiver<TelemetryEvent>
TelemetryEmitter ──produces──> FutureProducer (Kafka topic)
```
