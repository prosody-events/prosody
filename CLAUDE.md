# CLAUDE.md

Development patterns and practices for Prosody: distributed Kafka consumer with timer system and storage abstractions.

## Critical Rules

**Error Handling:**

- Never use `expect`, `unwrap`, `panic`, or `ok()` - forbidden by lints
- Propagate errors with `?` unless explicitly authorized to swallow
- Use `thiserror` for structured errors; box only when Clippy warns

**Code Quality:**

- Clippy must pass for code and tests - zero warnings tolerated
- Never suppress warnings with `#[allow(...)]` without permission
- Run: `cargo clippy`, `cargo clippy --tests`, `cargo doc`, `cargo +nightly fmt`

**JSON codec isolation:**

- `serde_json`, `simd_json`, and the `json!` macro are **banned** in all production code outside `src/codec.rs`
- Tests may use `serde_json::Value` as a concrete payload type — that is fine
- Any `use serde_json` or `use simd_json` import in non-test, non-codec production code is a bug

**Debugging Discipline:**

- Never claim "found the issue" without rigorous proof
- Evidence first (logs, tests, reproducible behavior) → hypothesis → test → verify

**Style:**

- Prefer `use` statements over fully qualified prefixes
- Methods without `self` should be functions (except `new` and similar)
- Ask before large structural changes

**Git:**

- Never add self-attribution to commits, PR descriptions, or code comments
- Use conventional commits (e.g., `fix:`, `feat:`, `docs:`, `refactor:`)

## Code Organization

**Order within files (topological by dependencies):**

1. Constants → Statics → Types → Implementations → Functions → Errors (bottom)

```rust
const MAX_RETRIES: usize = 3;
static CONFIG: LazyLock<Config> = LazyLock::new(Config::default);

pub struct Manager {
    /* ... */
}
impl Manager { /* ... */ }
pub fn helper_fn() { /* ... */ }

#[derive(Debug, Error)]
pub enum ManagerError { /* ... */ }
```

## Error Classification

Distinguish permanent from transient errors for retry logic:

```rust
#[derive(Debug, Clone, Copy)]
pub enum ErrorType {
    Permanent,  // Business logic - don't retry
    Transient,  // Network/timeout - retry with backoff
}

trait ClassifyError {
    fn classify_error(&self) -> ErrorType;
}
```

## Testing

**Organization:** Integration (`tests/`), Unit (`#[cfg(test)]`), Property (`src/timers/store/tests/`)

**Synchronization - never use `sleep` except for backpressure simulation:**

```rust
// Channel-based waiting (preferred)
let timer_event = env.expect_timer(5).await?;

// Notification with timeout
tokio::select! {
    () = notify.notified() => {},
    () = tokio::time::sleep_until(deadline) => return Err("Timeout".into()),
}
```

**Use `assert` or `color_eyre::Result` in tests - never `expect`/`unwrap`**

**Integration tests:** When running slow integration tests, write output to a temp file rather than piping to `grep`,
`head`, or `tail`. Re-running tests is expensive; keep output files around for exploration:

```bash
# Good: preserve output for exploration
cargo test 2>&1 | tee /tmp/test_output.log
grep FAILED /tmp/test_output.log

# Bad: loses output, forces expensive re-runs
cargo test 2>&1 | grep FAILED
```

## API Design

**Traits:** Keep generic with associated types; use type erasure only for FFI (JS/Python/Ruby)

**Configuration:** Use `#[derive(Builder, Validate)]`, mark builders with `#[must_use]`

```rust
#[derive(Builder, Clone, Debug, Validate)]
pub struct Configuration {
    #[validate(length(min = 1_u64))]
    bootstrap_servers: Vec<String>,

    #[validate(range(min = 1, max = 10000))]
    max_concurrency: usize,
}
```

## Architecture

**Consumer:** Hierarchical (Consumer → PartitionManager → KeyedManager)

- Partition-level parallelism with per-key ordering
- Cross-key concurrency, capacity-based backpressure

**Timer System:** Slab-based time partitioning (TimerManager → Store + Scheduler + SlabLoader)

- Persistent storage via `TriggerStore` trait (Cassandra/Memory)
- In-memory scheduler with background preloading

## Cassandra

**CRITICAL Anti-Patterns - NEVER USE:**

1. **ALLOW FILTERING** - Full table scans destroy cluster
2. **Secondary Indices** - Coordinator bottlenecks
3. **Materialized Views** - Breaks under write load

**Instead:** Proper partition keys, clustering columns for ranges, `Option<T>` for NULLs (filter in code)

**Handling NULLs from static columns:**

```rust
// Static columns return NULL for non-first clustering rows
let stream = session
.execute_iter("SELECT slab_id FROM segments WHERE id = ?", (segment_id,))
.await?
.rows_stream::<(Option<i32>, ) > () ?;

while let Some((slab_id_opt,)) = stream.try_next().await? {
if let Some(slab_id) = slab_id_opt {
yield slab_id;
}
}
```

**TTL overflow protection (Cassandra max: 630,720,000 seconds):**

```rust
fn calculate_ttl(&self, time: CompactDateTime) -> Option<i32> {
    const MAX_TTL: i32 = 630_720_000;
    Some(
        time.compact_duration_from_now()
            .unwrap_or(CompactDuration::MIN)
            .checked_add(self.base_ttl())
            .unwrap_or(CompactDuration::MAX)
            .seconds()
            .try_into()
            .unwrap_or(MAX_TTL),
    )
        .filter(|&ttl| ttl < MAX_TTL)
}
```

**Secrets:** Use `#[educe(Debug(ignore))]` for password fields

## Common Patterns

- Use `parking_lot` over `std::sync`
- Use `tokio::sync` primitives (`Notify`, channels, `select!`) for async
- Mark builders with `#[must_use]`
- Use `LazyLock` for expensive static initialization
- Implement `Arbitrary` for QuickCheck property tests
- Efficient strings: `Flexstr` (stack), `Intern` (interning)
- Dependencies: `ahash`, `parking_lot`, `simd-json` (non-ARM)

## Tracing / OpenTelemetry

**Never cache `Span` - cache `Context` instead:**

Spans have a lifecycle - they must be finished to flush to the collector. Caching spans causes problems:
- Spans get replaced with `Span::none()` after processing completes
- Cloning a span creates another reference to the same underlying span - finishing one finishes all
- Holding spans in cache prevents proper flushing

Instead, cache `opentelemetry::Context` and recreate spans on read:

```rust
use opentelemetry::Context;
use tracing::{Span, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

// On cache write: extract context from span
fn extract_context(span: &Span) -> Context {
    span.context()
}

// On cache read: create fresh span linked to cached context
fn create_span_from_context(context: &Context) -> Span {
    let span = info_span!("operation.cached_load", cached = true);
    span.set_parent(context.clone());
    span
}
```

See `CachedTimerDeferStore` for the reference implementation.

## Research

- Automatically use context7 for code generation and library documentation.

## Active Technologies
- Rust Edition 2024 (stable) + scylla 1.5 (Cassandra driver), tokio 1.50, parking_lot, scc, quick_cache 0.6, tracing, tracing-opentelemetry 0.32, opentelemetry 0.31 (001-reduce-timer-tombstones)
- Apache Cassandra (via scylla-rust-driver) - `timer_typed_keys` and `timer_typed_slabs` tables (001-reduce-timer-tombstones)
- Rust Edition 2024 (stable) + scylla 1.5 (Cassandra driver), tokio 1.50, parking_lot 0.12, quick_cache 0.6, scc 3.6, tracing 0.1, tracing-opentelemetry 0.32, opentelemetry 0.31, thiserror 2.0, async-stream 0.3, smallvec 1.15, strum 0.28 (001-reduce-timer-tombstones)
- Apache Cassandra via scylla-rust-driver — `timer_typed_keys` and `timer_typed_slabs` tables (001-reduce-timer-tombstones)

- Rust Edition 2024 (stable) + rdkafka 0.39, tokio 1.49, futures 0.3, serde 1.0, simd-json 0.17 (non-ARM), serde_json
  1.0 (ARM fallback), opentelemetry 0.31, tracing 0.1, tracing-opentelemetry 0.32, whoami 2.1 (002-kafka-telemetry)

## Recent Changes

- 001-simplified-prop-tests: Added Rust Edition 2024 (stable) + quickcheck 1.0, quickcheck_macros 1.1, color-eyre 0.6 (
  dev-dependencies), existing defer middleware
