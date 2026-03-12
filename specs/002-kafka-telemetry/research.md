# Research: Kafka Telemetry Event Emission

## R-001: `bytes` crate availability

**Decision**: Add `bytes` as a dependency for `Bytes::copy_from_slice` in the emitter.

**Rationale**: The emitter uses `buffer_unordered(64)` to have multiple produce futures in flight concurrently. Each needs its own payload buffer. A thread-local `Vec<u8>` handles serialization (zero-alloc steady state), then `Bytes::copy_from_slice` creates a cheap ref-counted copy for the in-flight future. `bytes` is already a transitive dependency via `tokio` and `rdkafka`, so adding it directly has zero impact on compile time or binary size.

**Alternatives considered**: `Vec<u8>::clone()` — functionally equivalent but `Bytes` is more idiomatic for owned byte buffers in async Kafka producers and avoids unnecessary capacity slack.

## R-002: simd-json cfg gate pattern

**Decision**: Follow the existing pattern: `#[cfg(not(target_arch = "arm"))] use simd_json as json;` / `#[cfg(target_arch = "arm")] use serde_json as json;`. Use `json::to_writer(&mut buf, &payload)` for serialization.

**Rationale**: This is the established pattern in `src/producer/mod.rs`, `src/consumer/decode.rs`, and `src/consumer/middleware/defer/message/loader/kafka/mod.rs`. Consistency avoids confusion.

**Alternatives considered**: Always using `serde_json` for simplicity — rejected because telemetry is on the hot path and simd-json is already available.

## R-003: Concurrent produce strategy

**Decision**: Use `StreamExt::buffer_unordered(64)` on a `BroadcastStream` to produce up to 64 telemetry events concurrently.

**Rationale**: This matches the existing codebase pattern — `buffer_unordered` is already used in `src/consumer/event_context.rs`, `src/timers/loader.rs`, and `src/timers/store/cassandra/migration.rs` for concurrent I/O operations. The `BroadcastStream` adapter (from `tokio-stream`) wraps the broadcast receiver into a `Stream`.

**Alternatives considered**:
- `FuturesUnordered` manually — more boilerplate for the same result; `buffer_unordered` is cleaner for a linear pipeline.
- Sequential `await` per produce — too slow; telemetry events arrive in bursts.

## R-004: Enum serialization approach

**Decision**: Use standard `#[derive(Serialize)]` with `#[serde(rename_all = "snake_case")]` / `#[serde(rename_all = "camelCase")]` on enums. Add `Serialize` derive to `TimerType`, `DemandType`, `ErrorCategory`. Create a `TelemetryEventName` enum with per-variant `#[serde(rename = "prosody.timer.*")]`.

**Rationale**: No enum in the codebase currently derives `Serialize`, but this is the idiomatic Rust/serde approach. No custom `*_str()` match functions needed — serde codegen handles it at compile time with zero runtime allocation.

**Alternatives considered**: Hand-written `fn timer_type_str(t: TimerType) -> &'static str` match functions — rejected as unnecessary boilerplate that serde derive handles automatically.

## R-005: `tokio-stream` dependency

**Decision**: Add `tokio-stream` (with `sync` feature) as a dependency for `BroadcastStream`.

**Rationale**: `BroadcastStream` wraps `tokio::sync::broadcast::Receiver` into a `Stream`, which is required for the `filter_map` → `map` → `buffer_unordered` pipeline. `tokio-stream` is a lightweight crate from the tokio project and is likely already a transitive dependency.

**Alternatives considered**: Manual `poll_recv` loop — more complex, harder to compose with stream combinators.

## R-006: Thread-local serialization buffer

**Decision**: Use `thread_local! { static SERIALIZE_BUF: RefCell<Vec<u8>> }` for the serialization buffer. Clear between events (capacity retained). Copy to `Bytes` for the produce future.

**Rationale**: The emitter runs on a single tokio task, so the thread-local is effectively task-local. This avoids per-event `Vec` allocation while keeping each in-flight produce future's payload independent.

**Alternatives considered**: Per-event `Vec::with_capacity(512)` — simpler but allocates/deallocates on every event. The thread-local pattern is a well-known optimization in serialization loops.

## R-007: Bootstrap server reuse

**Decision**: The telemetry emitter receives `bootstrap_servers: &[String]` as a parameter from the consumer config. No separate `PROSODY_TELEMETRY_BOOTSTRAP_SERVERS` env var.

**Rationale**: All Kafka components (consumer, producer, loader, admin) already share `PROSODY_BOOTSTRAP_SERVERS`. A separate telemetry broker config adds unnecessary complexity — telemetry events go to the same cluster.

**Alternatives considered**: Separate `PROSODY_TELEMETRY_BOOTSTRAP_SERVERS` — rejected for simplicity; can be added later if a separate cluster is needed.

## R-008: `eventTime` capture timing

**Decision**: Capture `Utc::now()` as close to the causal event as possible:
- `timer.scheduled`: immediately after successful store write in `TimerManager::schedule()`
- `timer.dispatched` / `message.dispatched`: immediately before calling the inner handler in `TelemetryHandler`
- `timer.succeeded` / `message.succeeded`: immediately on `Ok` return from inner handler
- `timer.failed` / `message.failed`: immediately on `Err` return from inner handler
- `message.sent`: immediately after delivery ack in `ProsodyProducer::send()`

**Rationale**: Capturing at the point of cause gives the most accurate timeline for external dashboards. The cost of `Utc::now()` is negligible (single syscall).

**Alternatives considered**: Capturing once at the start of the middleware and reusing — rejected because dispatched/succeeded/failed would all have the same timestamp, losing the duration signal.
