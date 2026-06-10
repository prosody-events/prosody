# Testing

How prosody is tested, and the idioms to copy when adding tests. The
non-negotiable rules live in [CLAUDE.md](CLAUDE.md) under `## Testing`;
this document explains the property-testing patterns in depth and points
at the exemplar files that embody them. When you need a pattern, read its
exemplar — the live test code is the authoritative version of every idiom
here.

## Layout

| Kind | Where | Notes |
| --- | --- | --- |
| Integration | `tests/` | Run against real Kafka and Cassandra (see `docker-compose.yaml`) |
| Unit + property | Sibling `tests.rs` modules, declared `#[cfg(test)] mod tests;` | Property tests live next to the code they check |
| Shared suites | `src/state/tests/`, `src/timers/store/tests/` | Generic trace runners instantiated by each backend's test module |

Cassandra-backed tests expect a live local cluster (`localhost:9042`,
keyspace `prosody_test`) and are **not** skip-gated — a down cluster fails
them rather than silently passing.

## Philosophy: invariants, not paths

Ask "what must remain true here?", write that down, then write the test
that proves it across random inputs. Example tests pin single paths and
miss interactions; one property over a realistic generator subsumes dozens
of them and keeps finding bugs as the code evolves. Common invariant
shapes and where to see them proven:

| Invariant shape | Property | Exemplar |
| --- | --- | --- |
| Round-trip | `decode(encode(x)) == x` | `src/state/tests/encoding.rs` |
| Oracle correctness | Real impl tracks a simple model op-for-op | `src/state/tests/value_suite.rs` |
| Parity | Two implementations answer identically | `src/consumer/middleware/deduplication/tests/prop_dedup_store.rs` |
| Idempotence | Re-applying a resolution is a `NoOp` | `run_idempotence_trace` in `src/state/tests/value_suite.rs` |
| Crash-recovery equivalence | Recovery converges to committed-or-rolled-back, never half-applied | `TraceOp::Crash` + `OraclePolicy` in `src/state/tests/value_suite.rs` |
| Monotonicity | Watermarks never move backwards | `src/consumer/partition/offsets/test.rs` |

A property over toy inputs is just a slow example test. Generators must
cover what production actually sends: empty/min/max sizes, duplicate keys,
out-of-order delivery, interleaved operations, error outcomes.

## Idiom catalog

### Trace + model in lockstep

The flagship pattern. Generate a `Trace` (a `Vec` of operations plus
events), drive the real store and a deliberately simple model through it
together, and assert equivalence **after every operation**, not just at
the end. The model uses plain `HashMap`/`BTreeSet`/`Option` — it must be
obviously correct, never a re-implementation of the production code.

Exemplars: `Trace`/`TraceOp` and the `Model` in
`src/state/tests/value_suite.rs`; `DeferModel` in
`src/consumer/middleware/defer/message/store/tests/prop_defer_store.rs`;
`HighLevelOperation` in `src/timers/store/tests/prop_high_level.rs`.

### Operation generators with stateful preconditions

Generators embed domain knowledge so traces stay valid and realistic: a
`DeferAdditional` is only generated for a key that already saw
`DeferFirst`; keys come from a small pre-generated pool so collisions and
re-use actually happen; operation counts are bounded; outcome variants
(success / permanent error / transient error) are weighted so error paths
get real coverage.

Exemplars: `DeferTestInput` in
`src/consumer/middleware/defer/message/store/tests/prop_defer_store.rs`;
the trace generator in
`src/consumer/middleware/defer/timer/tests/properties.rs`.

### Custom `Arbitrary` with explicit shrinking

Domain types get hand-rolled `Arbitrary` impls, and the ones driving long
traces implement `shrink()` (returning `Box<dyn Iterator<Item = Self>>`)
so failures reduce to minimal reproductions. Without shrinking, a failing
50-op trace is nearly undebuggable. Avoid wall-clock or RNG-seeded values
inside generators — deterministic ranges keep failures reproducible.

Exemplars: `src/state/tests/value_suite.rs` (`Trace`),
`src/state/tests/encoding.rs` (`Arb*` wrappers around enums),
`DispatchTrace` in `src/consumer/middleware/state_lifecycle/tests.rs`.

### Backend-generic suite runners

Write the property once as a generic runner, then instantiate it from each
backend's test module. `run_trace` / `run_trace_with_policy` /
`run_idempotence_trace` in `src/state/tests/value_suite.rs` are generic
over store bundles and run unchanged against memory, Cassandra, and Fjall
— every backend must satisfy the same invariants. New backends get the
whole suite for the cost of one instantiation.

Instantiations: `src/state/fjall/tests.rs`, `src/state/cassandra/tests.rs`.

### Differential (parity) testing

When a simple reference implementation exists, run the subject and the
reference through identical operations and assert every query answers
identically. This is oracle testing where the model is itself a real
implementation — e.g. any dedup store vs. `MemoryDeduplicationStore` in
`src/consumer/middleware/deduplication/tests/prop_dedup_store.rs`, or
`LayeredValueStore` vs. its bare backing store.

### Crash-recovery simulation

Traces include a `Crash` operation that captures the sealed-but-unresolved
state; the next iteration drives recovery under an explicit
`OraclePolicy` (`AlwaysCommitted` / `AlwaysNotCommitted`) so both the
apply and rollback arms are exercised deterministically. **Never simulate
a crash by leaking or forgetting a value** — reproduce the on-disk /
on-wire state directly (see the Memory rule in CLAUDE.md).

Exemplar: `TraceOp::Crash` handling in `src/state/tests/value_suite.rs`.

### Seeding stale state directly

To test recovery paths that normal execution cannot produce (a pending
index row with no WAL, a pre-existing identity row), write the rows
through the store's low-level API — bypassing the type whose lifecycle
would normally prevent the state — then assert the sweep/recovery path
cleans it up.

Exemplar: `run_stale_pending_index` in `src/state/tests/value_suite.rs`,
run from both the memory and Cassandra suites.

### Runtime errors are errors, not property failures

A store error during a property run is a broken test environment or a
bug to surface — never a reason for quickcheck to shrink. Map operation
errors to `TestResult::error(...)` (or propagate `color_eyre::Result`
with `?` and convert at the boundary) with enough context to identify the
failing operation index. Never swallow them into a `false` property
result.

Exemplar: `finish_trace` in `src/state/tests/value_suite.rs`.

### Deterministic time: per-iteration runtimes

Time-sensitive properties build a fresh
`Builder::new_current_thread()` runtime **inside** the quickcheck closure
— one per iteration — with `.start_paused(true)` when the test advances
time manually. A paused-time runtime cannot be shared across iterations
(state leaks between cases). Ordinary suites share the multi-threaded
`TEST_RUNTIME` in `tests/common.rs`.

Exemplars: `src/commit_manager/tests.rs`,
`src/consumer/partition/offsets/test.rs`.

### Iteration counts from the environment

Never hardcode an iteration count in a test body.

- In-memory property tests: quickcheck reads `QUICKCHECK_TESTS` itself;
  `QuickCheck::new().quickcheck(...)` needs no extra configuration.
- Property tests against live backends: read `INTEGRATION_TESTS` via the
  local `get_test_count()` helper (default 25) and pass it to
  `.tests(...)` — see `src/state/cassandra/tests.rs`.

CI cranks these up; dev loops stay fast.

## Synchronization: never sleep

`sleep` is allowed only to simulate backpressure. Everything else waits on
a real signal, with a deadline as a hang-guard — the deadline is never the
assertion:

```rust
// Channel-based waiting (preferred)
let timer_event = env.expect_timer(5).await?;

// Notification with timeout
tokio::select! {
    () = notify.notified() => {},
    () = tokio::time::sleep_until(deadline) => return Err("Timeout".into()),
}
```

If a test only passes under low load, it is asserting on timing, not on
behavior — fix the test.

## Running the suites

Use `cargo nextest run`, and always tee output to a file — slow suites are
expensive to re-run, and a piped `grep` throws the rest of the output
away:

```bash
cargo nextest run 2>&1 | tee /tmp/test_output.log
grep FAILED /tmp/test_output.log
```

Integration tests that drive a real consumer must shut the consumer down
before propagating a failure, or rdkafka's background threads hang the
test binary.
