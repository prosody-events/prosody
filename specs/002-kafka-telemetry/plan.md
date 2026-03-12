# Implementation Plan: Kafka Telemetry Event Emission

**Branch**: `002-kafka-telemetry` | **Date**: 2026-03-12 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/002-kafka-telemetry/spec.md`

## Summary

Emit structured lifecycle telemetry events (timer scheduled/dispatched/succeeded/failed, message dispatched/succeeded/failed, message sent) to a dedicated Kafka topic so external systems (ClickHouse) can observe Prosody activity. Events carry W3C trace context for end-to-end correlation. The implementation enriches the existing internal broadcast system with new `Data` variants, adds a background emitter that serializes to JSON via simd-json and produces concurrently using `buffer_unordered`, and wires emission points into the telemetry middleware, timer manager, and producer.

## Technical Context

**Language/Version**: Rust Edition 2024 (stable)
**Primary Dependencies**: rdkafka 0.39, tokio 1.49, futures 0.3, serde 1.0, simd-json 0.17 (non-ARM), serde_json 1.0 (ARM fallback), opentelemetry 0.31, tracing 0.1, tracing-opentelemetry 0.32, whoami 2.1
**Storage**: N/A (telemetry is fire-and-forget to Kafka; no persistent storage)
**Testing**: cargo test + quickcheck 1.0, quickcheck_macros 1.1, color-eyre 0.6
**Target Platform**: Linux server (ARM/non-ARM; simd-json cfg-gated)
**Project Type**: Single Rust library crate
**Performance Goals**: Zero observable latency impact on message/timer processing; telemetry is best-effort via broadcast channel
**Constraints**: No new allocations in the serialization hot path (thread-local buffer reuse); no blocking the consumer/producer event loops
**Scale/Scope**: 8 new event types, 2 new files (emitter.rs, injector.rs), ~12 modified files

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Error Handling | PASS | Emitter logs errors and continues; no `unwrap`/`expect`. Producer/middleware emission uses `let _ =` on broadcast send (existing pattern). |
| II. Code Quality | PASS | All code must pass `cargo clippy`, `cargo clippy --tests`, `cargo doc`, `cargo +nightly fmt` |
| III. Debugging Discipline | PASS | No debugging claims — this is new feature work |
| IV. Testing Standards | PASS | Tests will use channel-based waiting; no `sleep`; `assert`/`color_eyre::Result` |
| V. Simplicity | PASS | No unnecessary abstractions. The emitter is a single background task. Enum serialization uses standard serde derive. |
| VI. Trait-Based Interfaces | PASS | The emitter produces to Kafka via `FutureProducer` — no new external state interface that needs mocking. Telemetry emission is tested via the existing broadcast channel subscriber pattern. |
| VII. Documentation Independence | PASS | No references to spec/story/ticket IDs in code |
| Cassandra Anti-Patterns | N/A | No Cassandra queries in this feature |

**Result: ALL GATES PASS** — no violations to justify.

## Project Structure

### Documentation (this feature)

```text
specs/002-kafka-telemetry/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (JSON schema)
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
src/
├── telemetry/
│   ├── mod.rs           # MODIFY — add pub mod emitter, pub(crate) mod injector
│   ├── event.rs         # MODIFY — add Timer/Message/MessageSent Data variants + structs
│   ├── partition.rs     # MODIFY — add propagator field, 7 emission methods
│   ├── sender.rs        # MODIFY — add propagator field, message_sent method
│   ├── emitter.rs       # NEW — config, buffer_unordered event loop, serialization
│   └── injector.rs      # NEW — TelemetryInjector (zero-alloc trace context)
├── timers/
│   └── manager.rs       # MODIFY — add telemetry/source fields, emit in schedule()
├── consumer/
│   ├── mod.rs           # MODIFY — thread telemetry into partition config + middleware
│   ├── kafka_context.rs # MODIFY — pass telemetry into PartitionConfiguration
│   ├── partition/
│   │   └── mod.rs       # MODIFY — add telemetry_sender to PartitionConfiguration
│   └── middleware/
│       └── telemetry.rs # MODIFY — add source field, emit message/timer events
├── producer/
│   └── mod.rs           # MODIFY — add telemetry field, emit in send()
└── high_level/
    └── mod.rs           # MODIFY — lift Telemetry to struct, wire emitter

tests/                   # Existing integration tests — no new test files needed
```

**Structure Decision**: This feature adds 2 new files to the existing `src/telemetry/` module and modifies ~12 existing files. No structural changes to the project layout.

## Complexity Tracking

> No constitution violations — this section is intentionally empty.
