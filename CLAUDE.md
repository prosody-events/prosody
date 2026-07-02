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

**Allocation (tiger style / data-oriented — https://tigerstyle.dev/):**

- **No hot-path allocation that isn't upfront and bounded.** A steady-state
  path (per message, per timer fire, per event, per cell) must not allocate a
  buffer whose size is discovered at runtime and grown to "whatever's needed."
  Bound it, size it once to its known cardinality (`Vec::with_capacity`,
  `smallvec`), and never let it reallocate.
- **Never add a *gratuitous* allocation to satisfy the borrow checker or the
  compiler.** When a `.map(|x| ...)` closure trips a higher-ranked-lifetime
  error, reach for a **function item** (`.map(Type::method)`), an index, or a
  borrow before you reach for a scratch `Vec`. (See `Weighed::weight`, mapped as
  a fn item so the batch-packing boundary iterator needs **no** `Vec<u64>`
  scratch.)
- **No amortized / cached resize buffers** ("allowed to grow to the max size
  ever seen") on the hot path. If a reusable scratch buffer is truly
  unavoidable, allocate it once at construction with a fixed bound and reuse it
  — never amortize-grow it per call.
- **Simplicity is not sacrificed for this.** The design principles above still
  win: prefer the reading that's clearest. Zero-alloc and simple are usually
  *not* in conflict — the fn-item fix above removed an allocation *and* a line.
  When they genuinely do conflict, keep it simple and leave a comment naming the
  allocation; do not contort the code to shave a bounded, upfront `Vec`.

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

**Documentation:**

- Write doc comments for a reader unfamiliar with the codebase: help them
  navigate the concept. Lead with what the reader needs — what the thing is,
  how to use it, what guarantee it gives — not the internal mechanism.
- Capture the key concepts and, whenever applicable, **state the invariant** —
  but at the type or function that owns it, **once**. Don't restate the same
  invariant across related items; reference the owning type instead.
- Be concise. No walls of text, no verbosity, and no examples that don't earn
  their place (a stub that only shows syntax, or a comment that restates the
  prose above it, adds nothing). Bad or needless docs **hurt** readability —
  prefer fewer, sharper words over more.
- **Never cite a plan's or spec's section number, phase number, or ordinal
  (`§2.7`, "Phase 4", "(O8)", "finding #3") in durable docs** — code doc
  comments, CLAUDE.md, TESTING.md, PR/commit text. Plans live in gitignored
  scratch (`docs/`, `specs/`); the number is meaningless to a reader without
  that file and goes stale when the plan is renumbered or deleted. Name the
  concept instead, or link a durable symbol (`[`CollectionDef`]`). Stable
  cross-references to invariants/findings **documented in CLAUDE.md itself**
  (e.g. "invariant 8", "finding F2") are fine — those live in a durable doc.

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

The full testing guide — the property-test idiom catalog, invariant
shapes, exemplar files, and how to run the suites — lives in
[TESTING.md](TESTING.md). Read it before writing tests. The rules below
are the non-negotiables.

**Organization:** Integration (`tests/`), unit and property tests in
sibling modules next to the code they check. Default to a sibling
`tests.rs` (or `foo/tests.rs`), declared as `#[cfg(test)] mod tests;`.
Inline `#[cfg(test)] mod tests { ... }` blocks are acceptable only for a
handful of tiny tests on a small file.

**Drive tests by invariants, not by paths — and prefer few broad property
tests over many narrow example tests.** Name the invariant (round-trip,
parity, monotonicity, idempotence, crash-recovery equivalence, oracle
correctness) and write the property test that proves it over a realistic
generator: random operation sequences, interleavings, and boundary values,
not happy-path toys. One such property subsumes dozens of example tests
and keeps finding bugs as the code evolves; a property over toy inputs is
just a slow example test. About to write a third example test for the same
function? Write the generalizing property instead, and fold existing
example clusters into a property when you're already touching their
module. Example tests are for invariants too narrow to generalize, or as
fast smokes alongside the property. Copy the idioms cataloged in
TESTING.md (trace + model oracle, backend-generic suite runners, crash
simulation, explicit shrinking) rather than inventing new harnesses.

**Iteration counts come from the environment — never hardcoded:**
`QUICKCHECK_TESTS` for in-memory property tests (quickcheck reads it
automatically), `INTEGRATION_TESTS` for property tests against live
backends. CI cranks these up; dev loops stay fast.

**Never use `sleep` except for backpressure simulation.** Wait on
channels, `Notify`, or `select!` with a deadline — the deadline is a
hang-guard, never the assertion. Patterns in TESTING.md.

**Use `assert` or `color_eyre::Result` with `?` in tests — never
`expect`/`unwrap`, never swallow errors.**

**Run tests with `cargo nextest run` and tee output to a file**
(`cargo nextest run 2>&1 | tee /tmp/test_output.log`) — re-running slow
suites is expensive; grep the file, not the pipe.

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

**Middleware composition — memorize this, it is easy to invert:**

`some_mw.layer(x)` adds `x` as the new **OUTERMOST** layer (it builds
`ComposedMiddleware(outer=x, inner=self)`; `with_provider` nests as
`outer.with_provider(inner.with_provider(base))`). `into_provider(handler)`
terminates the chain with the handler as the **INNERMOST** component.

- **Handler is INNERMOST. Retry is OUTERMOST.** Request phase runs
  OUTER→INNER (retry first, handler last); response phase unwinds INNER→OUTER.
- The block built by `build_common_middleware`
  (`telemetry.layer(timeout).layer(scheduler).layer(cancellation).layer(dedup)`)
  is the **innermost** block, directly outside the handler. It carries every
  cross-mode concern — including the mandatory `dedup` commit oracle — so modes
  layer only their mode-specific middleware OUTSIDE it. Within the block
  OUTER→INNER is
  `dedup → cancellation → scheduler → timeout → telemetry → handler`.
- Pipeline stack OUTERMOST→INNERMOST:
  `retry → message_defer → timer_defer → monopolization →
  dedup → (cancellation → scheduler → timeout → telemetry) → handler`.

**The keyed-state durability sequence is NOT in the stack.** It runs once
after the stack returns, owned by the `settle` boundary (the blanket
`FallibleEventHandler → EventHandler` impl in `consumer::middleware`; `retry`
routes its final outcome through the same `settle`/`abandon`). State is one
**provisional cell** per value (`data | prev_data | event`); there is no WAL.
Handler writes buffer in a single **in-memory** `DirtyStore` the session
owns and rebuilds (clears in place) per event — it is **never** a durability or
recovery source (recovery is Cassandra provisional cells + the commit oracle),
so do not re-add a disk-backed dirty store. Fjall is retained **only** as the
committed-value write-through cache (`FjallCellCache`, which owns its
workspace).
`settle` does, in straight-line code: stage provisional cells / write resolved
(`finalize`) → arm `StateRecovery` if anything staged (a per-key singleton via
`clear_and_schedule`, **arm-if-sooner**: re-armed only when the newly-staged fire
is strictly earlier than the standing one, else skipped) →
**flush the registered dedup marker, strictly after the stage** → commit the
offset/trigger → promote the staged cells (best-effort, O(1) per cell) →
`after_commit`.
**Retry forever; abort only on shutdown; never emit Terminal.** Every internal
durability step (`retry_step`) retries **transient _and_ terminal** store
failures forever — a broken store self-heals when it recovers, and a genuinely
stuck store stalls the offset until the liveness probe restarts the process (a
visible last resort, strictly better than silently abandoning); only a
**permanent** data-rejection is skipped, and only **shutdown** abandons (aborts
the marker → redelivery). Arming the backstop is **must-succeed** (invariant 8):
`arm_backstop` retries *every* non-shutdown failure — including a permanent
timer-store error and a fire-time-computation error — and returns
`ArmOutcome::ShuttingDown` only on shutdown, so it never gates the marker except
by a shutdown abandon. This makes "abort in normal operation" structurally
unwritable at the boundary.
The per-key fire is `min(recovery_delay, tightest touched collection's
`recovery_within`)`: `recovery_delay` is the always-on durability floor and
per-collection `recovery_within` is a tightening-only reader-convergence bound
(it never loosens the floor). `ArmedKeys` maps each key to its standing fire time
so arm-if-sooner can compare.
The boundary **never** unschedules the backstop: the per-key `StateRecovery`
timer is only ever pulled sooner (never pushed out) by each stateful commit, and
the fired trigger (a per-`(key, TimerType)` singleton) is committed by the sweep
arm itself — there is **no** `unschedule_all`, so one event can never
point-clear — nor loosen — another event's still-needed backstop (finding F2).
The sweep (`StateManager::recover`) mirrors the boundary's posture: it **never
aborts the trigger except on shutdown**, committing it on progress (a resolved
sweep, or a per-cell permanent skip that first-touch/the next commit recover)
and, on a transient sweep failure, rescheduling a fresh backstop
(`clear_and_schedule(now + recovery_delay)`, retried until it lands or shutdown)
before committing. The stage→arm→commit order also closes the
crash-before-arm window without any acquisition-time sweep: the offset is
uncommitted until after the arm, so a crash there redelivers and re-arms. Because the
marker flush is textually after the stage in one
function, "marker before durable state" is **unwritable**. The dedup middleware
in the stack only *filters* duplicates and *registers* the marker (on `Ok` /
`Permanent`); it never writes the dedup store. Three residual order facts remain:
  - `retry` stays OUTERMOST so each attempt is a fresh dispatch that resets the
    session **and the registered marker** between attempts.
  - The defer middlewares sit OUTSIDE the common block so a defer-swallow
    `reset()`s the session *before* swallowing a transient error into
    `Ok(Deferred)` — discarding the registered marker, so `settle` flushes no
    marker and the deferred reload is **not** deduped (no flag, no outcome
    inspection). The reload re-dispatches `on_message` and `settle` stages under
    the reloading timer's `EventRef`.
  - `dedup` stays in the stack as a duplicate **filter** so the deferred-message
    reload dispatch still filters producer duplicates. (`monopolization` is
    state-agnostic; its position is immaterial.)

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
5. **`USING TIMESTAMP` / manually-set write timestamps** - The session installs a `MonotonicTimestampGenerator` (`cassandra/mod.rs`), so the **driver** stamps every write client-side. Because one handler per key and one `PartitionManager` per partition mean all writes to a partition flow through that single session, those timestamps increase monotonically in issue order — which is exactly what makes last-write-wins lost-write-free. Setting the timestamp by hand (`USING TIMESTAMP`, a per-statement timestamp override, any client-supplied value) bypasses the generator and lets a later write silently lose to an earlier one — a lost write with no error. Never set it; let the generator stamp every write.

**Instead:** Proper partition keys, clustering columns for ranges, `Option<T>` for NULLs (filter in code). For "insert-if-new" semantics, prefer idempotent writes or app-level coordination over LWTs.

**Batching:** When multiple statements target the **same partition (same row key)**, group them into an `UNLOGGED BATCH` whenever possible. Same-partition unlogged batches are atomic on the replica and execute as a single mutation, eliminating extra coordinator round-trips. Never use `LOGGED BATCH` for performance reasons, and never batch across partitions to "reduce round-trips" — that's an anti-pattern that overloads the coordinator.

**Bind persisted types directly via their scylla serdes.** Pass persisted types to the driver through their `SerializeValue`/`DeserializeValue` impls; never hand-convert to a driver primitive (`i8`/`i16`/etc.) at the call site. When you persist a type, give it idiomatic `From<Self> for iN` / `TryFrom<iN> for Self` discriminator conversions (a dedicated error for the fallible direction) and let the scylla serde delegate through them — e.g. `i8::from(*self).serialize(...)`; see `TimerType`, `SegmentVersion`, `StateType`, `CollectionKindId`, `Encoding`, `CellKind`. Do not add bespoke `as_iN`/`from_iN` inherent methods; the trait impls are the single conversion surface. Reads may keep deserializing the raw primitive and validating it through `TryFrom` in a fallible post-step **only** when a bad value must classify `Permanent` (or be skipped for forward-compat) rather than become scylla's `Terminal` `DeserializationError` — as the `EventRef` UDT and the discriminators above do. In that case the serde is serialize-only by design (a `SerializeValue` impl with no `DeserializeValue`, since the latter cannot express "skip this row"); document the read-side validator it pairs with.

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
- For concurrent hash sets/maps, use `scc` (`scc::HashSet` / `scc::HashMap`),
  never a `Mutex<HashSet>` / `Mutex<HashMap>` — `scc` is lock-free and sharded,
  so a single mutex would serialize unrelated keys. In async code prefer its
  async interface (`insert_async` / `contains_async` / `remove_async`); pair it
  with `ahash::RandomState` as the hasher.
- Use `tokio::sync` primitives (`Notify`, channels, `select!`) for async
- **Drive streams/futures over non-tokio primitives through the cooperative
  budget.** Tokio auto-decrements its per-task coop budget (and forces a yield
  when it hits zero) only at its *own* leaf awaits — network/channel/timer I/O.
  A future that completes without touching one of those leaves — an in-memory or
  fjall store op, a `futures` channel, a `buffer_unordered` of CPU-bound work —
  never decrements the budget, so a fan-out or a `while let Some(x) =
  stream.next()` loop over ready items can drain the whole batch in one poll and
  starve the worker. Wrap each such future with
  [`tokio::task::coop::cooperative`] — it adds a per-poll budget checkpoint so
  the work yields every ~128 items. There is **no** `.cooperative()` method and
  no coop *stream* adapter: `cooperative(fut)` is a free function returning a
  `Coop<F>` future. The idiomatic combinator form keeps full concurrency —
  `stream.map(|x| cooperative(async move { … })).buffer_unordered(N)` (a
  `FuturesUnordered<Coop<…>>`, as `KeyManager` does). Wrap the **per-item**
  future inside the `map`, not the outer `fold`/`try_collect` — wrapping the
  combinator only checkpoints once for the entire drain. Pass `cooperative`
  inline in the producing closure; `.map(cooperative)` / `.map(|f|
  cooperative(f))` as a separate stage trips a higher-ranked-lifetime error on
  non-`'static` futures.
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
