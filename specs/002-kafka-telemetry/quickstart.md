# Quickstart: Kafka Telemetry Event Emission

## Overview

This feature adds external telemetry event emission to Prosody. Structured lifecycle events (message dispatched/succeeded/failed, timer scheduled/dispatched/succeeded/failed, message sent) are emitted to a Kafka topic for consumption by external systems like ClickHouse.

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PROSODY_TELEMETRY_ENABLED` | `true` | Enable/disable the telemetry emitter |
| `PROSODY_TELEMETRY_TOPIC` | `prosody.telemetry-events` | Kafka topic for telemetry events |

The emitter reuses the consumer's bootstrap servers (`PROSODY_BOOTSTRAP_SERVERS`). No separate broker configuration is needed.

## What Gets Emitted

### Consumer-side events (via telemetry middleware)

| Event | When | Key Fields |
|-------|------|------------|
| `prosody.message.dispatched` | Before handler call | demandType, offset |
| `prosody.message.succeeded` | Handler returns Ok | demandType, offset |
| `prosody.message.failed` | Handler returns Err | demandType, offset, errorCategory, exception |
| `prosody.timer.scheduled` | After store write | timerType, scheduledTime |
| `prosody.timer.dispatched` | Before handler call | timerType, scheduledTime, demandType |
| `prosody.timer.succeeded` | Handler returns Ok | timerType, scheduledTime, demandType |
| `prosody.timer.failed` | Handler returns Err | timerType, scheduledTime, demandType, errorCategory, exception |

### Producer-side events

| Event | When | Key Fields |
|-------|------|------------|
| `prosody.message.sent` | After delivery ack | destination topic, partition, offset |

### Common fields on all events

`eventTime`, `topic`, `partition`, `key`, `source`, `hostname`, `traceParent`, `traceState`

## JSON Format

All events are serialized as JSON with camelCase field names. Example:

```json
{
  "type": "prosody.message.dispatched",
  "eventTime": "2026-03-11T18:08:04.123Z",
  "demandType": "normal",
  "topic": "witco.lead-events",
  "partition": 21,
  "offset": 12345,
  "traceParent": "00-abc123-def456-01",
  "traceState": "...",
  "key": "lead-123",
  "source": "rookery",
  "hostname": "worker-1"
}
```

## Disabling Telemetry

Set `PROSODY_TELEMETRY_ENABLED=false`. No emitter is spawned, no events are produced, no overhead.

## Architecture Notes

- **Best-effort**: Telemetry never blocks the main consumer/producer workload. Events are broadcast via an internal channel; the emitter drains and produces concurrently.
- **Concurrent produce**: Up to 64 produce futures in flight simultaneously via `buffer_unordered`.
- **Trace correlation**: W3C `traceParent`/`traceState` fields enable joining telemetry events back to the original message trace in ClickHouse.
