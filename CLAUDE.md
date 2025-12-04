# CLAUDE.md

Development patterns and practices for Prosody: distributed Kafka consumer with timer system and storage abstractions.

## Critical Rules

**Error Handling:**
- Never use `expect`, `unwrap`, `panic`, `unwrap_or`, or `ok()` - forbidden by lints
- Propagate errors with `?` unless explicitly authorized to swallow
- Use `thiserror` for structured errors; box only when Clippy warns

**Code Quality:**
- Clippy must pass for code and tests - zero warnings tolerated
- Never suppress warnings with `#[allow(...)]` without permission
- Run: `cargo clippy`, `cargo clippy --tests`, `cargo doc`, `cargo +nightly fmt`

**Debugging Discipline:**
- Never claim "found the issue" without rigorous proof
- Evidence first (logs, tests, reproducible behavior) → hypothesis → test → verify

**Style:**
- Prefer `use` statements over fully qualified prefixes
- Methods without `self` should be functions (except `new` and similar)
- Ask before large structural changes

## Code Organization

**Order within files (topological by dependencies):**
1. Constants → Statics → Types → Implementations → Functions → Errors (bottom)

```rust
const MAX_RETRIES: usize = 3;
static CONFIG: LazyLock<Config> = LazyLock::new(Config::default);

pub struct Manager { /* ... */ }
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

**Integration tests:** When running slow integration tests, write output to a temp file rather than piping to `grep`, `head`, or `tail`. Re-running tests is expensive; keep output files around for exploration:

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
    .rows_stream::<(Option<i32>,)>()?;

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

## Research

- Automatically use context7 for code generation and library documentation.

## Active Technologies
- Rust Edition 2024 (stable) + quickcheck 1.0, quickcheck_macros 1.1, color-eyre 0.6 (dev-dependencies), existing defer middleware (001-simplified-prop-tests)
- N/A (tests use `MemoryDeferStore`) (001-simplified-prop-tests)
- Rust Edition 2024 (stable) + quickcheck 1.0, quickcheck_macros 1.1, color-eyre 0.6, tokio, parking_lo (001-simplified-prop-tests)
- Rust Edition 2024 (stable) + scylla-rust-driver, tokio, scc, parking_lot, uuid, thiserror (002-segment-based-defer-schema)
- Apache Cassandra (via scylla driver) (002-segment-based-defer-schema)

## Recent Changes
- 001-simplified-prop-tests: Added Rust Edition 2024 (stable) + quickcheck 1.0, quickcheck_macros 1.1, color-eyre 0.6 (dev-dependencies), existing defer middleware
