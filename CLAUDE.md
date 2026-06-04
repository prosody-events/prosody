# Prosody

Distributed Kafka consumer framework with a timer system and pluggable storage backends. This file documents the patterns and practices coding agents should follow when working in this repository.

## Design Principles

These come before everything else. Every change is judged against them.

**Write code that is simple, clear, well-factored, elegant, beautiful, easy to understand, correct, and idiomatic.** A reader should grasp the intent without effort. If a change makes the code harder to read, the change is wrong, even if it's faster or shorter. If two designs are correct, pick the one that's easier to delete.

**Make invalid states unrepresentable in the type system.** When the compiler can prove a contract, no test, comment, or convention has to. Prefer:
- Distinct types for distinct concepts (`TimerRequest` vs `Trigger`) over flag fields and "set this when X" rules.
- Restricted constructors (`pub(in crate::foo)`) over public ones with documented preconditions.
- Sum types (`enum`) over `Option<T>` plus a separate boolean.
- Newtypes over raw primitives at API boundaries when the primitive carries semantic meaning the type system can capture.

If a bug class can be made uncompilable, do that instead of writing a runtime check.

**Delete more than you add.** Every change should leave the codebase smaller, simpler, or both — measured by lines, types, indirections, or cognitive load. If you must add code, look first for duplication you can fold, abstractions that no longer pay rent, dead branches, and stale comments. The end-state diff should net negative whenever the task allows. Bloat compounds; aggressively prune.

**Identify, document, and enforce invariants.** For every load-bearing piece of state:
1. Name the invariant.
2. Write it down — preferably as a doc comment near the type or function that owns it.
3. Enforce it in the type system if you can; otherwise enforce it with an assertion at the boundary that establishes it.
4. Cover it with a property test. Example tests catch the path you thought of; property tests catch the corners.

If you can't name the invariant, you don't yet understand the code well enough to change it.

**Leave the codebase better than you found it.** Drive-by simplifications are encouraged when they're scoped to the area you're already touching. Don't sprawl — but don't walk past obvious cleanup either.

## Critical Rules

**Error Handling:**

- Never use `expect`, `unwrap`, `panic`, or `ok()` - forbidden by lints
- Propagate errors with `?` unless explicitly authorized to swallow
- Use `thiserror` for structured errors; box only when Clippy warns

**Memory:**

- **Never leak memory.** `std::mem::forget`, `Box::leak`, and `ManuallyDrop`
  without an explicit reclamation path are forbidden. If a test or
  production path needs to simulate "Drop never ran" (e.g. crash
  simulation), reproduce the on-disk / on-wire state directly — open the
  underlying store and seed it without going through the type whose Drop
  would clean up. Forgetting is never the shortcut.

**Code Quality:**

- Clippy must pass for code and tests - zero warnings tolerated
- Never suppress warnings with `#[allow(...)]` without permission
- Never introduce `dyn` (trait objects, `Box<dyn ...>`, `&dyn ...`) without permission - prefer generics and associated types
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
- Keep trait constraints as local as possible. If a constraint can sit on a
  function instead of a struct, put it on the function. Include only the
  constraints that function actually needs — not a superset for the whole
  type. The struct should compile and be usable without the bound unless
  every reachable method requires it.

**Git:**

- Never add self-attribution to branch names, commits, PR titles, PR descriptions, or code comments.
- Use conventional commits for commit titles and PR titles (e.g., `fix:`, `feat:`, `docs:`, `refactor:`).
- PR titles and descriptions are written for a reader who is **not** intimately familiar with the project. Be readable, well written, and well styled. Lead with what changed and why; assume nothing about the reader's session context.
- **PR descriptions never include a test plan or a list of verification steps.** Reviewers don't need a checklist of what you ran — they need to understand what changed and why. Test coverage belongs in the tests themselves.
- **Never run `git reset` or `git checkout` that would destroy uncommitted or committed changes without explicit human permission.** This includes `git reset --hard`, `git checkout -- <path>`, and switching branches over a dirty working tree. Prefer `git stash`, an explicit commit, or `git restore --staged <path>` when the goal is just to unstage. Read-only git commands (`status`, `diff`, `log`) are always fine.

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

**Tests live in their own modules.** Default to a sibling `tests.rs` (or
`foo/tests.rs` if `foo.rs` becomes a directory), declared as
`#[cfg(test)] mod tests;`. Inline `#[cfg(test)] mod tests { ... }` blocks
are acceptable only for a handful of tiny tests on a small file; promote
to a sibling file as soon as the block has fixtures, helpers, or starts
to dominate the read of the production code. Production code reads better
without the scaffolding inline; tests get their own headers and structure.

**Drive tests by invariants, not by paths.** For a piece of code, ask
"what must remain true here?" and write that down. Then write the test
that proves it across random inputs. Example tests catch obvious paths;
property tests catch the corners — silent bugs (state that drifts,
watermarks that leap, invariants that break only on specific interleavings)
do not get caught any other way. If you can identify the invariant
(round-trip, parity between two structures, monotonicity, idempotence,
crash-recovery equivalence, oracle correctness), use a property test.
Reach for an example test only when the invariant is too narrow to
generalize, or as a fast smoke alongside the prop test.

**Property-test iteration count must come from `QUICKCHECK_TESTS`** (or the
equivalent env var for your generator). Never hardcode a count in the test
body — `QuickCheck::new().quickcheck(...)` reads `QUICKCHECK_TESTS`
automatically, with a sensible default when unset. CI can crank this up; dev
loops stay fast.

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

**Concurrency invariants — load-bearing for correctness:**

- **One handler per key, system-wide.** `KeyManager` ensures at most one message or timer handler for a given key is executing anywhere in the cluster at any moment. This is enforced by Kafka partition ownership (one consumer group member owns each partition) plus in-process per-key serialization. Never design for concurrent writers on the same key — that scenario cannot occur.
- **Zero or one `PartitionManager` per Kafka partition, system-wide.** Kafka's partition assignment guarantees at most one consumer group member holds a partition at a time; the `PartitionManager` is the single owner of both the message stream and the timers that hang off that partition. Timer storage is scoped to the partition's segment; no two `PartitionManager`s for the same partition can be live simultaneously.

These invariants are why LWTs, distributed locks, and optimistic concurrency are never needed for per-key or per-partition state. The framework provides the exclusivity; code inside it can assume it.

## Cassandra

**CRITICAL Anti-Patterns - NEVER USE:**

1. **ALLOW FILTERING** - Full table scans destroy cluster
2. **Secondary Indices** - Coordinator bottlenecks
3. **Materialized Views** - Breaks under write load
4. **LWTs (Lightweight Transactions / `IF [NOT] EXISTS` / `IF <cond>`)** - Paxos round-trips serialize all writes to a partition; latency and contention scale catastrophically

**Instead:** Proper partition keys, clustering columns for ranges, `Option<T>` for NULLs (filter in code). For "insert-if-new" semantics, prefer idempotent writes or app-level coordination over LWTs.

**Batching:** When multiple statements target the **same partition (same row key)**, group them into an `UNLOGGED BATCH` whenever possible. Same-partition unlogged batches are atomic on the replica and execute as a single mutation, eliminating extra coordinator round-trips. Never use `LOGGED BATCH` for performance reasons, and never batch across partitions to "reduce round-trips" — that's an anti-pattern that overloads the coordinator.

**Bind persisted types directly via their scylla serdes.** Pass persisted types to the driver through their `SerializeValue`/`DeserializeValue` impls; never hand-convert to a driver primitive (`i8`/`i16`/etc.) at the call site. When you persist a type, define its scylla serde (delegating to the type's own discriminator method, e.g. `self.as_i8().serialize(...)` — see `TimerType`, `StateType`, `CollectionKindId`, `PayloadEncoding`, `WalFormat`). Reads may keep deserializing the raw primitive and validating it in a fallible post-step **only** when a bad value must classify `Permanent` (or be skipped for forward-compat) rather than become scylla's `Terminal` `DeserializationError` — as the `EventRef` UDT and the discriminators above do. In that case the serde is serialize-only by design; document the read-side validator it pairs with.

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
- Rust Edition 2024 (stable) + scylla 1.5 (Cassandra driver), tokio 1.50, parking_lot 0.12, quick_cache 0.6, scc 3.6, tracing 0.1, tracing-opentelemetry 0.32, opentelemetry 0.31, thiserror 2.0, async-stream 0.3, smallvec 1.15, strum 0.28 (001-reduce-timer-tombstones)
- Apache Cassandra via scylla-rust-driver — `timer_typed_keys` and `timer_typed_slabs` tables (001-reduce-timer-tombstones)
- Rust Edition 2024 (stable) + rdkafka 0.39, tokio 1.50, futures 0.3, serde 1.0, simd-json 0.17 (non-ARM), serde_json 1.0 (ARM fallback), opentelemetry 0.31, tracing 0.1, tracing-opentelemetry 0.32, whoami 2.1 (002-kafka-telemetry)

## Recent Changes

- 001-simplified-prop-tests: Added Rust Edition 2024 (stable) + quickcheck 1.0, quickcheck_macros 1.1, color-eyre 0.6 (
  dev-dependencies), existing defer middleware
