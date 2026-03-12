# Tasks: Kafka Telemetry Event Emission

**Input**: Design documents from `/specs/002-kafka-telemetry/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/

**Organization**: Tasks grouped by scenario for independent implementation and testing.

## Format: `[ID] [P?] [S#] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[S#]**: Which scenario this task belongs to (S1–S5)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Add new dependencies and create new files

- [x] T001 Add `bytes`, `tokio-stream` (with `sync` feature), and `chrono` (if not present) to `[dependencies]` in `Cargo.toml`
- [x] T002 Add `#[derive(Serialize)]` with `#[serde(rename_all = "camelCase")]` to `TimerType` in `src/timers/mod.rs` (add `serde::Serialize` import; preserve existing derives and `#[repr(i8)]`)
- [x] T003 [P] Add `#[derive(Serialize)]` with `#[serde(rename_all = "camelCase")]` to `DemandType` in `src/consumer/mod.rs` (add `serde::Serialize` import; preserve existing derives)
- [x] T004 [P] Add `#[derive(Serialize)]` with `#[serde(rename_all = "camelCase")]` to `ErrorCategory` in `src/error/mod.rs` (add `serde::Serialize` import; preserve existing derives)

**Checkpoint**: Dependencies added, existing enums enriched with Serialize — ready for foundational types.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: New event types and injector that ALL scenarios depend on

**⚠️ CRITICAL**: No scenario work can begin until this phase is complete

- [x] T005 Create `TelemetryEventName` enum with per-variant `#[serde(rename = "prosody.timer.*")]` / `#[serde(rename = "prosody.message.*")]` in `src/telemetry/event.rs` (add at bottom before errors; derive `Clone, Copy, Debug, Serialize`)
- [x] T006 Add `TimerTelemetryEvent` struct and `TimerEventType` enum to `src/telemetry/event.rs` per data-model.md (derive `Clone, Debug`; fields: event_type, event_time, scheduled_time, timer_type, key, source, trace_parent, trace_state)
- [x] T007 Add `MessageTelemetryEvent` struct and `MessageEventType` enum to `src/telemetry/event.rs` per data-model.md (derive `Clone, Debug`; fields: event_type, event_time, offset, key, source, trace_parent, trace_state)
- [x] T008 Add `MessageSentEvent` struct to `src/telemetry/event.rs` per data-model.md (derive `Clone, Debug`; fields: event_time, topic, partition, offset, key, source, trace_parent, trace_state)
- [x] T009 Add `Timer(TimerTelemetryEvent)`, `Message(MessageTelemetryEvent)`, `MessageSent(MessageSentEvent)` variants to the existing `Data` enum in `src/telemetry/event.rs`
- [x] T010 Create `src/telemetry/injector.rs` — implement `TelemetryInjector` struct with `trace_parent: Option<Box<str>>`, `trace_state: Option<Box<str>>`, `new()`, `extract()` method, and `impl Injector for TelemetryInjector` per plan Step 2
- [x] T011 Add `pub(crate) mod injector;` to `src/telemetry/mod.rs`

**Checkpoint**: Foundation ready — all new types exist, injector available. Scenario implementation can begin.

---

## Phase 3: Scenario 1 — Consumer Message Lifecycle Events (Priority: P1) 🎯 MVP

**Goal**: Emit `prosody.message.dispatched`, `prosody.message.succeeded`, `prosody.message.failed` events during consumer message processing.

**Independent Test**: Process a message through telemetry middleware → verify dispatched/succeeded/failed events on broadcast channel with correct fields.

### Implementation for Scenario 1

- [x] T012 [S1] Add `propagator: TextMapCompositePropagator` field to `TelemetryPartitionSender` in `src/telemetry/partition.rs` — construct once in `new()` / `for_partition()` factory
- [x] T013 [S1] Add `message_dispatched()`, `message_succeeded()`, `message_failed()` methods to `TelemetryPartitionSender` in `src/telemetry/partition.rs` — each builds `Data::Message(MessageTelemetryEvent { .. })` using `TelemetryInjector::extract()` and `Utc::now()`, sends via existing broadcast channel
- [x] T014 [S1] Add `source: Arc<str>` field to `TelemetryMiddleware`, `TelemetryProvider`, `TelemetryHandler` in `src/consumer/middleware/telemetry.rs` — thread through constructors
- [x] T015 [S1] Update `TelemetryMiddleware::new()` signature to accept `source: Arc<str>` in `src/consumer/middleware/telemetry.rs`
- [x] T016 [S1] Update `on_message()` in `TelemetryHandler` in `src/consumer/middleware/telemetry.rs` — capture span/key/offset before inner call; emit `message_dispatched` before, `message_succeeded` on Ok, `message_failed` on Err (with `e.classify_error()` and `format!("{e:?}")`)
- [x] T017 [S1] Update `build_common_middleware()` in `src/consumer/mod.rs` — pass `Arc::from(group_id)` as `source` to `TelemetryMiddleware::new()`

**Checkpoint**: Message lifecycle telemetry events emitted on broadcast channel. Independently testable via broadcast subscriber.

---

## Phase 4: Scenario 2 — Timer Lifecycle Events (Priority: P1)

**Goal**: Emit `prosody.timer.scheduled`, `prosody.timer.dispatched`, `prosody.timer.succeeded`, `prosody.timer.failed` events during timer lifecycle.

**Independent Test**: Schedule a timer and process a trigger through telemetry middleware → verify scheduled/dispatched/succeeded/failed events with timerType, scheduledTime, demandType.

### Implementation for Scenario 2

- [x] T018 [S2] Add `timer_scheduled()`, `timer_dispatched()`, `timer_succeeded()`, `timer_failed()` methods to `TelemetryPartitionSender` in `src/telemetry/partition.rs` — each builds `Data::Timer(TimerTelemetryEvent { .. })` using `TelemetryInjector::extract()` and `Utc::now()`
- [x] T019 [S2] Add `telemetry: Option<TelemetryPartitionSender>` and `source: Arc<str>` fields to `TimerManagerInner<T>` in `src/timers/manager.rs` — update `TimerManager::new()` signature
- [x] T020 [S2] Emit `timer_scheduled` in `TimerManager::schedule()` in `src/timers/manager.rs` — after successful store write, if `telemetry.is_some()`, call `timer_scheduled()` with key, scheduled_time, timer_type, source
- [x] T021 [S2] Add `telemetry_sender: TelemetrySender` field to `PartitionConfiguration<T>` in `src/consumer/partition/mod.rs`
- [x] T022 [S2] Pass `config.telemetry_sender` into `PartitionConfiguration` in `src/consumer/kafka_context.rs` (Context::new() already receives telemetry)
- [x] T023 [S2] In `handle_messages()` in `src/consumer/partition/mod.rs` — create `TelemetryPartitionSender` from `config.telemetry_sender.for_partition(topic, partition)` and pass into `TimerManager::new()` along with `config.group_id.clone()`
- [x] T024 [S2] Update test call sites of `TimerManager::new()` — pass `None` for telemetry and `Arc::from("")` for source (search for all `TimerManager::new` calls in `src/` and `tests/`)
- [x] T025 [S2] Update `on_timer()` in `TelemetryHandler` in `src/consumer/middleware/telemetry.rs` — capture trigger.time/key/timer_type/span before inner call; emit `timer_dispatched` before, `timer_succeeded` on Ok, `timer_failed` on Err (with `e.classify_error()` and `format!("{e:?}")`)

**Checkpoint**: Timer lifecycle telemetry events emitted on broadcast channel. Independently testable via broadcast subscriber.

---

## Phase 5: Scenario 4 — Kafka Telemetry Emitter (Priority: P1)

**Goal**: Background emitter that subscribes to broadcast channel, serializes events to JSON, and produces concurrently to Kafka.

**Independent Test**: Publish telemetry events to broadcast channel → verify JSON-serialized events appear on configured Kafka topic.

### Implementation for Scenario 4

- [ ] T026 [S4] Create `src/telemetry/emitter.rs` — add `TelemetryEmitterConfiguration` struct with `#[derive(Builder, Clone, Debug, Validate)]`, fields: `topic: String` (default `prosody.telemetry-events`), `enabled: bool` (default `true`)
- [ ] T027 [S4] Add serialization payload structs to `src/telemetry/emitter.rs` — `TimerEventPayload<'a>`, `MessageEventPayload<'a>`, `MessageSentPayload<'a>` with `#[derive(Serialize)]` `#[serde(rename_all = "camelCase")]` and borrowed `&'a str` fields per plan Step 8
- [ ] T028 [S4] Add `serialize_event()` helper function in `src/telemetry/emitter.rs` — matches on `Data::Timer`/`Data::Message`/`Data::MessageSent`, constructs the appropriate payload struct, serializes with `json::to_writer()` into provided buffer, returns `Option<()>` (None for non-telemetry variants); add simd-json/serde_json cfg gate
- [ ] T029 [S4] Add `spawn_telemetry_emitter()` in `src/telemetry/emitter.rs` — takes `config`, `bootstrap_servers: &[String]`, `telemetry: &Telemetry`; resolves hostname via `whoami::hostname()`; creates `FutureProducer`; subscribes to broadcast; spawns tokio task with `BroadcastStream` → `filter_map` → thread-local serialize → `map` produce → `buffer_unordered(64)` → `for_each` log errors
- [ ] T030 [S4] Add `EmitterError` error enum to `src/telemetry/emitter.rs` (derive `Debug, Error` via thiserror)
- [ ] T031 [S4] Add `pub mod emitter;` to `src/telemetry/mod.rs`
- [ ] T032 [S4] Re-export `TelemetryEmitterConfiguration`, `spawn_telemetry_emitter`, `EmitterError` from `src/telemetry/mod.rs` or `src/lib.rs` as needed

**Checkpoint**: Telemetry events from broadcast channel are serialized and produced to Kafka concurrently.

---

## Phase 6: Scenario 3 — Producer Message Sent Events (Priority: P2)

**Goal**: Emit `prosody.message.sent` events after successful producer delivery.

**Independent Test**: Send a message through ProsodyProducer → verify `prosody.message.sent` event on broadcast channel with destination topic/partition/offset.

### Implementation for Scenario 3

- [ ] T033 [S3] Add `propagator: TextMapCompositePropagator` field to `TelemetrySender` in `src/telemetry/sender.rs` — construct once in `new()`
- [ ] T034 [S3] Add `message_sent()` method to `TelemetrySender` in `src/telemetry/sender.rs` — captures `Span::current()`, uses `TelemetryInjector::extract()`, builds `Data::MessageSent(MessageSentEvent { .. })`, sends via broadcast channel
- [ ] T035 [S3] Add `telemetry: Option<TelemetrySender>` field and `with_telemetry(mut self, sender: TelemetrySender) -> Self` builder method to `ProsodyProducer` in `src/producer/mod.rs`
- [ ] T036 [S3] Emit `message_sent` in `ProsodyProducer::send()` in `src/producer/mod.rs` — after successful delivery (after span recording), if `self.telemetry.is_some()`, call `sender.message_sent()` with destination topic, partition, offset, key, `Arc::from(self.source_system.as_ref())`

**Checkpoint**: Producer telemetry events emitted. End-to-end trace correlation now possible (produce → consume → timer).

---

## Phase 7: Scenario 5 — Telemetry Configuration & Wiring (Priority: P2)

**Goal**: Wire telemetry emitter into HighLevelClient with configuration via environment variables.

**Independent Test**: Start system with default config → emitter uses `prosody.telemetry-events`; set `PROSODY_TELEMETRY_ENABLED=false` → no emitter spawned.

### Implementation for Scenario 5

- [ ] T037 [S5] Add `telemetry: Telemetry` field to `HighLevelClient<T>` in `src/high_level/mod.rs` — create `Telemetry::new()` once in `HighLevelClient::new()`
- [ ] T038 [S5] Wire telemetry into producer in `HighLevelClient::new()` in `src/high_level/mod.rs` — call `producer.with_telemetry(telemetry.sender())`
- [ ] T039 [S5] In `subscribe()` in `src/high_level/mod.rs` — pass `telemetry.clone()` to consumer constructors so they use the shared broadcast channel
- [ ] T040 [S5] Add `TelemetryEmitterConfiguration` to consumer builder or `HighLevelClient` params in `src/high_level/mod.rs` — when enabled, call `spawn_telemetry_emitter()` with consumer's bootstrap_servers
- [ ] T041 [S5] Expose `pub fn telemetry(&self) -> &Telemetry` on `HighLevelClient` in `src/high_level/mod.rs`

**Checkpoint**: Telemetry fully wired — emitter running, all events flowing from middleware → broadcast → Kafka topic.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Verify quality across all scenarios

- [ ] T042 Run `cargo clippy` and `cargo clippy --tests` — fix any warnings to zero
- [ ] T043 [P] Run `cargo doc` — fix any documentation warnings
- [ ] T044 [P] Run `cargo +nightly fmt` — format all modified files
- [ ] T045 Run `cargo test` — verify all existing tests pass with the new telemetry plumbing (pipe output to temp file per constitution)
- [ ] T046 Verify existing internal telemetry subscribers (virtual-time fairness, monopolization detection) still work — they should ignore the new `Data` variants via wildcard/default match arms

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all scenarios
- **Phase 3 (S1: Message Events)**: Depends on Phase 2
- **Phase 4 (S2: Timer Events)**: Depends on Phase 2; can run in parallel with Phase 3
- **Phase 5 (S4: Emitter)**: Depends on Phase 2; can run in parallel with Phases 3–4
- **Phase 6 (S3: Producer Events)**: Depends on Phase 2; can run in parallel with Phases 3–5
- **Phase 7 (S5: Configuration)**: Depends on Phases 3, 4, 5, 6 (wires everything together)
- **Phase 8 (Polish)**: Depends on all previous phases

### Scenario Dependencies

- **S1 (Message Events)**: Independent after Phase 2
- **S2 (Timer Events)**: Independent after Phase 2; shares `TelemetryPartitionSender` methods with S1 (same file, different methods)
- **S4 (Emitter)**: Independent after Phase 2; consumes events from S1/S2/S3 but can be built against the types alone
- **S3 (Producer Events)**: Independent after Phase 2
- **S5 (Configuration)**: Depends on S1, S2, S3, S4 — final wiring phase

### Parallel Opportunities

Within Phase 1:
- T002, T003, T004 can run in parallel (different files)

Within Phase 2:
- T005, T006, T007, T008 are sequential (same file: event.rs)
- T010 can run in parallel with T005–T009 (different file: injector.rs)

After Phase 2 completes:
- Phase 3 (S1), Phase 4 (S2), Phase 5 (S4), Phase 6 (S3) can all start in parallel
- Note: S1 and S2 both modify `src/telemetry/partition.rs` and `src/consumer/middleware/telemetry.rs` — if parallelized, coordinate on these shared files

---

## Implementation Strategy

### MVP First (Scenarios 1 + 4)

1. Complete Phase 1: Setup (dependencies, enum derives)
2. Complete Phase 2: Foundational (new types, injector)
3. Complete Phase 3: Scenario 1 — message lifecycle events on broadcast channel
4. Complete Phase 5: Scenario 4 — emitter produces to Kafka
5. **STOP and VALIDATE**: Message events flow from middleware → broadcast → Kafka topic
6. This is a deployable MVP — external systems can observe message processing

### Incremental Delivery

1. Setup + Foundational → types ready
2. Add S1 (messages) + S4 (emitter) → message telemetry live (MVP)
3. Add S2 (timers) → timer telemetry live
4. Add S3 (producer) → full end-to-end tracing
5. Add S5 (wiring) → configuration + HighLevelClient integration
6. Polish → clippy, fmt, docs, test verification

---

## Notes

- [P] tasks = different files, no dependencies
- [S#] label maps task to specific scenario for traceability
- All emission methods use `let _ =` on broadcast send (existing pattern — fire-and-forget)
- The emitter is best-effort — produce failures are logged, never block the main workload
- Commit after each phase or logical group
- Stop at any checkpoint to validate independently
