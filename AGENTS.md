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

**Delete more than you add.** Every change should leave the codebase smaller, simpler, or both — measured by lines, types, indirections, or cognitive load. If you must add code, look first for duplication you can fold, abstractions that no longer pay rent, dead branches, and stale comments. The end-state diff should net negative whenever the task allows. Bloat compounds; aggressively prune. The bar applies per change, and line count is not the only axis: a fold that buys line savings with generic machinery (GATs, trait plumbing, flag parameters) is not a simplification — plain duplicated arms are often the better reading.

**Identify, document, and enforce invariants.** For every load-bearing piece of state:
1. Name the invariant.
2. Write it down — preferably as a doc comment near the type or function that owns it.
3. Enforce it in the type system if you can; otherwise enforce it with an assertion at the boundary that establishes it.
4. Cover it with a property test. Example tests catch the path you thought of; property tests catch the corners.

If you can't name the invariant, you don't yet understand the code well enough to change it.

**Leave the codebase better than you found it.** Drive-by simplifications are encouraged when they're scoped to the area you're already touching. Don't sprawl — but don't walk past obvious cleanup either.

## Definition of Done

No change is complete until every line below holds. These are acts, not
aspirations — perform each one; do not merely agree with it:

1. `cargo clippy` and `cargo clippy --tests` — zero warnings. `cargo doc` —
   zero warnings. `cargo +nightly fmt`; stable `cargo fmt --check` must also
   pass.
2. `cargo nextest run <filter> 2>&1 | tee /tmp/test_output.log` — re-running
   slow suites is expensive; grep the file, not the pipe.
3. Every new or converted test was proved falsifiable once: inject the
   failure, watch it go red, revert.
4. Every deleted test names its surviving stronger test in the commit message.
5. Everything the change replaces is gone — code, tests, config fields, doc
   vocabulary (see Redesign hygiene). "The new thing works" is half done.
6. Any defect fixed or convention applied was swept repo-wide by grep and
   applied to structural twins (message/timer, memory/Cassandra) together —
   a partial sweep is drift; finish it or record where and why it stopped.
7. Every claim written this session — doc cross-reference, "covered by" note,
   exemplar path, metric motivating the change — was verified to resolve, not
   recalled from memory. Re-measure headline numbers before acting on them;
   measurement artifacts (file moves, reclassified lines) masquerade as signal.
8. The diff is net-negative, or each addition is individually justified.

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
- **No unbounded keyed RAM.** Operating assumptions: partitions owned for
  weeks, ~6 collections per key, 100M keys per instance, total in-memory
  budget ≈ 1 GiB (about the fjall block cache). Any in-memory structure keyed
  by user key or collection must have a fixed capacity bound — at that scale
  even ~100 bytes per key×collection is 10–60 GiB. Acceptable homes for keyed
  state: fjall (RAM = block cache + memtables; data spills to the
  assignment-scoped disk workspace) and a capacity-bounded `quick_cache`. An
  insert-only `scc` map/set keyed by key or collection is a defect regardless
  of entry size (the `MarkerMemo.checked` bug class). Every in-memory map
  names its removal path; self-draining maps (removed on settle/fire) are
  fine but still need the drain named.

**Allocation and layout (tiger style / data-oriented — https://tigerstyle.dev/):**

Tiger style and data-oriented design agree on two things: minimize allocation,
and lay data out for the way the machine reads it. Never *pessimize* a path
whose size you know.

- **No hot-path allocation that isn't upfront and bounded.** A steady-state
  path (per message, per timer fire, per event, per cell) must not allocate a
  buffer whose size is discovered at runtime and grown to "whatever's needed."
  Bound it, size it once to its known cardinality (`Vec::with_capacity`,
  `smallvec`), and never let it reallocate.
- **`with_capacity` excuses the sizing, never the allocation.** A per-call heap
  allocation on a steady-state path is the defect itself, however well it is
  bounded. Pick the buffer by what is known about the size:
  - **Compile-time constant** → stack array (`[u8; N]`). A heap `Vec` for a
    fixed-size key or frame is never acceptable (the fjall index-key builders
    once did exactly this by copying a sibling).
  - **Runtime-varying, but almost always ≤ some small N** → `SmallVec<[T; N]>`
    with N sized to the common case: the steady state stays on the stack and
    only the rare outlier pays for a heap spill. This is what `smallvec` is
    *for* — using it as a resizable `Vec` with extra steps misses the point.
  - **Genuinely unbounded or large runtime cardinality** → `Vec::with_capacity`
    sized once; heap is unavoidable there anyway.
- **Never add a *gratuitous* allocation to satisfy the borrow checker or the
  compiler.** When a `.map(|x| ...)` closure trips a higher-ranked-lifetime
  error, reach for a **function item** (`.map(Type::method)`), an index, or a
  borrow before you reach for a scratch `Vec`. (See `BatchUnit::weight` mapped
  as a fn item into `chunk_boundaries` in `src/cassandra/mod.rs` — the
  batch-packing boundary iterator needs **no** `Vec<u64>` scratch.)
- **No amortized / cached resize buffers** ("allowed to grow to the max size
  ever seen") on the hot path. If a reusable scratch buffer is truly
  unavoidable, allocate it once at construction with a fixed bound and reuse it
  — never amortize-grow it per call.
- **Lay data out for the access pattern.** A hot path that scans one or two
  fields across many entries must find those fields contiguously. Reach the
  full record only for the entry the scan selects. An array of `Option<Arc<T>>`
  turns a two-word decision into one heap dereference per entry, and thrashes
  the CPU cache. Memory bandwidth is the bottleneck today, so the scan decides
  the layout, not the record. Don't thrash the cache. False sharing counts:
  keep atomics that different threads write off one line.
- **Simplicity is not sacrificed for this.** The design principles above still
  win: prefer the reading that's clearest. Zero-alloc and simple are usually
  *not* in conflict — the fn-item fix above removed an allocation *and* a line.
  When they genuinely do conflict, keep it simple and leave a comment naming the
  allocation; do not contort the code — manual stack buffers, `unsafe`, lifetime
  gymnastics — to shave a bounded, upfront `Vec`. A `Vec → SmallVec` swap is
  **not** such a contortion: it is the idiomatic tool for a known-small size, so
  this clause never shields a `Vec` where a `SmallVec` fits.

**Code Quality:**

- Lint/doc/fmt gates live in Definition of Done — zero warnings tolerated
- Never suppress warnings with `#[allow(...)]` without permission
- Never introduce `dyn` (trait objects, `Box<dyn ...>`, `&dyn ...`) without permission - prefer generics and associated types

**Redesign hygiene:**

When a design is replaced, remove *all* of it in the same change — half-deleted
designs are where bloat and bug re-introduction live:

- Sweep the old design's vocabulary from every doc comment. A stale doc is
  worse than noise: one can instruct a reader to re-introduce a fixed bug class
  (a leftover doc describing the abandoned `unschedule_all` did exactly that).
- Code whose only caller is its own test is dead — delete both together. Watch
  for conversion/serde bridges (`TryFrom<iN>`, serde derives) kept alive by a
  self-referential round-trip test; move any wire-discriminant pins that test
  provided into a frozen-bytes test first, then delete.
- Struct fields threaded through configs/contexts but only read at construction
  are residue from a superseded design — remove them end-to-end.
- Don't build surface ahead of a caller. A zero-caller, zero-test durable-write
  path is the worst of both worlds: delete it, or make it an owner-confirmed,
  tested feature — never leave it as-is.

**JSON codec isolation:**

- `serde_json`, `simd_json`, and the `json!` macro are **banned** in all production code outside `src/codec/`
- Tests may use `serde_json::Value` as a concrete payload type — that is fine
- Any `use serde_json` or `use simd_json` import in non-test, non-codec production code is a bug

**Debugging Discipline:**

- Never claim "found the issue" without rigorous proof
- Evidence first (logs, tests, reproducible behavior) → hypothesis → test → verify

**Documentation:**

- **All written text for this project must conform to ASD-STE100
  (Simplified Technical English). No written text is exempt.** This rule
  applies to documentation, comments, READMEs, plans, issues, reviews, chat
  responses, commit messages, PR text, and user-facing text. Apply these
  primary STE rules:
  - Use the active voice. Write instructions in the imperative.
  - Write short sentences. Use 20 words or fewer for instructions. Use 25
    words or fewer for descriptions.
  - Write one instruction per sentence. Keep one topic per paragraph. Use a
    maximum of six sentences in each paragraph.
  - Use a word with only one meaning. Use the same word for the same thing.
  - Use simple verb tenses. Do not use an "-ing" form as a verb when a simple
    tense is correct.
  - Do not use a noun cluster of more than three nouns.
  - Use approved technical names and technical verbs consistently.
- Write doc comments for a reader unfamiliar with the codebase: help them
  navigate the concept. Lead with what the reader needs — what the thing is,
  how to use it, what guarantee it gives — not the internal mechanism.
- Docs address the future reader, never the current conversation: no
  review-response prose, no "the reviewer/advisor said", no phrasing copied
  from scratch plans or design docs. Restate the invariant in the code's own
  words — a doc whose referent is an ephemeral plan rots the day the plan does.
- Capture the key concepts and, whenever applicable, **state the invariant** —
  but at the type or function that owns it, **once**. Don't restate the same
  invariant across related items; reference the owning type instead.
- Be concise. No walls of text, no verbosity, and no examples that don't earn
  their place (a stub that only shows syntax, or a comment that restates the
  prose above it, adds nothing). Bad or needless docs **hurt** readability —
  prefer fewer, sharper words over more.
- **Never cite a plan's or spec's section number, phase number, or ordinal
  (`§2.7`, "Phase 4", "(O8)", "finding #3") in durable docs** — code doc
  comments, AGENTS.md, TESTING.md, PR/commit text. Plans live in gitignored
  scratch (`docs/`, `specs/`); the number is meaningless to a reader without
  that file and goes stale when the plan is renumbered or deleted. Name the
  concept instead, or link a durable symbol (`[`CollectionDef`]`). Stable
  cross-references to invariants/findings **documented in AGENTS.md itself**
  (e.g. "invariant 8", "finding F2") are fine — those live in a durable doc.

**Style:**

- Prefer `use` statements over fully qualified prefixes
- Methods without `self` should be functions (except `new` and similar)
- Ask before large structural changes
- Default to `pub(crate)`/`pub(super)`; make something `pub` only as a
  deliberate downstream-API decision. Blanket `pub` freezes internals into the
  supported surface and silences rustc's dead-code lint.
- When a proposed simplification is examined and rejected, record the ruling in
  one sentence at the site (e.g. "exists for type-parameter compression") so
  the next pass doesn't re-litigate it. The converse also holds: a trait layer
  with a single impl is not thereby dead — confirm no planned work depends on
  it before collapsing.
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

Classify errors through the `ClassifyError` trait and its `ErrorCategory` in
`src/error/mod.rs`:

- `Transient` — retry with backoff.
- `Permanent` — a message-level rejection; do not retry.
- `Terminal` — the client is unusable and must shut down.

Implement the trait; never invent a parallel classification. The settle
boundary's posture toward each category is documented in Architecture.

## Testing

**Before writing or modifying any test, read TESTING.md** — the
property-test idiom catalog, invariant shapes, exemplar files, shared
scaffolding homes, and how to run the suites live there, and tests written
without it reinvent harnesses the catalog already provides. The rules
below are the non-negotiables.

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

**A test must be able to fail.** The false-pass idioms are cataloged in
TESTING.md — `else { return; }` setup guards, discarded `Option<()>` bodies,
tautological asserts, detectors unreachable by construction, properties that
never call their subject. When writing or converting a test, prove it can go
red once: inject the failure, watch it fail, revert.

**Never delete a test without naming, in the commit, the surviving test that
covers the same invariant at least as strongly** — and when a new property
subsumes an example cluster, prune the examples in the same change.

**Root-cause every property-test failure — no exceptions.** A failing
property or quickcheck run is evidence of a bug (in the code or in the
test's design), never noise to re-run away: a passing re-run proves
nothing, because the failing input or schedule may simply not recur.
Extract the reproducer — the shrunk input when the property has one; the
failure message plus its mechanism when it doesn't (a `fn(())` repetition
harness has nothing to shrink) — and turn it into a deterministic test at
the lowest layer that can express it, preferring paused time
(`start_paused(true)`) or manually driven dispatch over wall-clock waits.
Only then decide whether the fix belongs in the code or the test, and
land the reproducer as the regression pin.

**Iteration counts come from the environment — never hardcoded:**
`QUICKCHECK_TESTS` for in-memory property tests (quickcheck reads it
automatically), `INTEGRATION_TESTS` for property tests against live
backends. CI cranks these up; dev loops stay fast.

**Never use `sleep` except for backpressure simulation.** Wait on
channels, `Notify`, or `select!` with a deadline — the deadline is a
hang-guard, never the assertion. Patterns in TESTING.md.

**Use `assert` or `color_eyre::Result` with `?` in tests — never
`expect`/`unwrap`, never swallow errors.**

## API Design

**Traits:** Keep generic with associated types; use type erasure only for FFI (JS/Python/Ruby/C#)

**Monomorphize dispatch — never close it over an enum.** When a component
varies by backend, bind it through a generic parameter or an associated
type. Each instantiation then compiles against its concrete stores. The
anti-pattern is the closed dispatch enum: one variant per backend, a `match`
in every operation. Every new operation must repeat the match, and the arms
duplicate what monomorphization writes for free. One rewrite of such an enum
family into generics deleted ~500 lines. Select the backend once, at the
construction boundary. From there the choice travels as a type. The erased
FFI layer is the only sanctioned home for runtime dispatch (see the Traits
line above).

**Compress type parameters behind one bundle trait.** When several
cooperating generics thread through every signature, package them as one
trait with associated types. Structs and functions then name a single `B`
instead of an `<O, I, C>` family. `StateBackend` (`src/state/backend.rs`) is
the exemplar and documents the ruling. Apply this wherever a signature
accumulates generic parameters.

**Every public API must stay FFI-exposable.** Prosody's public surface is
**cross-language**: the sibling clients `prosody-{js,py,rb,cs}` wrap a
*published* `prosody` through type erasure (napi / pyo3 / magnus / C-ABI). Treat
every `pub` type or method a client could call — for *any* feature, not just
keyed state — as a potential binding target. When you add or change public API,
confirm the shape can be exposed to all four clients before landing it:
- Return types must be expressible across the boundary — a simple owned value, a
  `Result` with a structured `thiserror` error, or a plain C-like enum
  (`StoreOutcome`), never an exotic generic or borrow in return position that
  can't be materialized at a concrete instantiation.
- `async` and sync are **both** fine: every client bridges the tokio runtime
  (napi async, `pyo3-async-runtimes`, tokio rt in cs/rb). A sync-infallible
  method is the easiest shape; an `async fn -> Result<_, E>` is the established
  one — mirror an existing exposed sibling.
- The clients consume a released crate version, so nothing reaches them
  automatically. The bar is **not foreclosing** exposure: grep `~/code/prosody-*`
  for the existing binding pattern and check the new shape survives
  napi/pyo3/magnus/C-ABI.

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

A `Validate` derive with zero rules is a false promise. Every field that can
express a degenerate value (zero, a sub-unit duration truncating to zero) either
gets a validation rule or the consuming code must provably tolerate it — the
retry-jitter panic (`random_range(0..0)` on a sub-ms base delay) shipped exactly
this way.

## Architecture

**Consumer:** Hierarchical (Consumer → PartitionManager → KeyManager)

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
after the stack returns, owned by the `settle` boundary in
`src/consumer/middleware/settle.rs` (called from the blanket
`FallibleEventHandler → EventHandler` impl; `retry` routes its final outcome
through the same `settle`/`abandon`). Whether a dispatch settles the event at
all is a pure function of the stack's final result: the crate-internal
`Settlement` classification (`SettlementHandler::settlement`, one explicit
impl per framework wrapper, the leaf adapter minted at `into_provider`
hardcoding `Final`) decides `Final` vs `Bypassed` before the error category is
consulted; the message commit marker is read from the session's event identity
(`message_marker()` — the message `EventRef`'s dedup id, or the
deferred-reload's last-wins identity override), never deposited by middleware.
The full stage → arm-backstop → marker-record → commit → promote order, its
crash-window argument, and the sweep's mirrored posture are documented once on
their owning items — `settle`/`settle_committed`, `arm_backstop`/`ArmOutcome`,
and `StateManager::recover` — read those doc comments before touching any of
it. The anchors code comments cite by name:

- **Invariant 8:** arming the backstop is must-succeed. `arm_backstop` retries
  every non-shutdown failure and can only report `ShuttingDown`, so "abort in
  normal operation" is structurally unwritable at the boundary.
- **Finding F2:** neither the boundary nor the sweep ever unschedules a
  backstop — per-key `StateRecovery` timers are only ever pulled sooner
  (arm-if-sooner), so one event can never clear or loosen another event's
  still-needed backstop. There is no `unschedule_all`; do not reintroduce one.
- **Posture:** retry transient AND terminal store failures forever; skip only
  permanent data-rejections; abort only on shutdown; never emit Terminal.
- **No WAL, ever.** State is one provisional cell per value. The in-memory
  `DirtyStore` (one shared per-partition workspace; race-free per-event
  key-range clears) is never a durability or recovery source — recovery is
  Cassandra provisional cells + the commit oracle. Do not re-add a disk-backed
  dirty store; fjall remains only the committed-value cache (`FjallCellCache`).
  The marker record sits textually after the stage inside one function, so
  "marker before durable state" is unwritable.

Two residual order facts govern middleware placement:
  - `retry` stays OUTERMOST so each attempt is a fresh dispatch, isolated by
    the `next_attempt` verb between attempts: its `reset` transition discards
    the failed attempt's dirty overlay and bumps the session's attempt epoch
    under one gate hold, so a handle leaked past its attempt is fenced
    (`Terminated`) instead of joining the next attempt's transaction.
  - The dedup filter sits INSIDE message-defer so a deferred reload's
    duplicate check sees the reload identity override. `dedup` is a stateless
    duplicate **filter** over the boundary-readable message marker; the settle
    boundary records it — the `Settlement` classification decides whether a
    dispatch settles the event at all. (`monopolization` is state-agnostic;
    its position is immaterial.)

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
4. **LWTs (Lightweight Transactions / `IF [NOT] EXISTS` / `IF <cond>`)** - Paxos round-trips serialize all writes to a partition; latency and contention scale catastrophically. Sole authorized exception: the descriptor-identity first-use registration (`INSERT … IF NOT EXISTS` in `src/state/cassandra/identity.rs`) — once per collection per group, never on the hot path. Do not "fix" it, and do not cite it to justify a new one.
5. **`USING TIMESTAMP` / manually-set write timestamps** - The session installs a `MonotonicTimestampGenerator` (`cassandra/mod.rs`), so the **driver** stamps every write client-side. Because one handler per key and one `PartitionManager` per partition mean all writes to a partition flow through that single session, those timestamps increase monotonically in issue order — which is exactly what makes last-write-wins lost-write-free. Setting the timestamp by hand (`USING TIMESTAMP`, a per-statement timestamp override, any client-supplied value) bypasses the generator and lets a later write silently lose to an earlier one — a lost write with no error. Never set it; let the generator stamp every write.

**Instead:** Proper partition keys, clustering columns for ranges, `Option<T>` for NULLs (filter in code). For "insert-if-new" semantics, prefer idempotent writes or app-level coordination over LWTs.

**Batching:** When multiple statements target the **same partition (same row key)**, group them into an `UNLOGGED BATCH` whenever possible. Same-partition unlogged batches are atomic on the replica and execute as a single mutation, eliminating extra coordinator round-trips. Never use `LOGGED BATCH` for performance reasons, and never batch across partitions to "reduce round-trips" — that's an anti-pattern that overloads the coordinator.

**Bind persisted types directly via their scylla serdes.** Pass persisted types to the driver through their `SerializeValue`/`DeserializeValue` impls; never hand-convert to a driver primitive (`i8`/`i16`/etc.) at the call site. When you persist a type, give it idiomatic `From<Self> for iN` / `TryFrom<iN> for Self` discriminator conversions (a dedicated error for the fallible direction) and let the scylla serde delegate through them — e.g. `i8::from(*self).serialize(...)`; see `TimerType`, `SegmentVersion`, `StateType`, `CollectionKindId`, `Encoding`, `CellKind`. Do not add bespoke `as_iN`/`from_iN` inherent methods; the trait impls are the single conversion surface. Reads may keep deserializing the raw primitive and validating it through `TryFrom` in a fallible post-step **only** when a bad value must classify `Permanent` (or be skipped for forward-compat) rather than become scylla's `Terminal` `DeserializationError` — as the `EventRef` UDT and the discriminators above do. In that case the serde is serialize-only by design (a `SerializeValue` impl with no `DeserializeValue`, since the latter cannot express "skip this row"); document the read-side validator it pairs with.

**Static columns:** a static column returns NULL for every clustering row
except the first in the partition. Read it as `Option<T>` and filter in code.

**TTL overflow protection:** Cassandra's maximum TTL is 630,720,000 seconds
(20 years). Check every computed TTL against that maximum before binding.
Reuse `calculate_ttl` (`src/cassandra/mod.rs`); do not hand-roll TTL
arithmetic.

**Secrets:** Use `#[educe(Debug(ignore))]` for password fields

## Common Patterns

- Use `parking_lot` over `std::sync`
- For concurrent hash sets/maps, use `scc` (`scc::HashSet` / `scc::HashMap`),
  never a `Mutex<HashSet>` / `Mutex<HashMap>` — `scc` is lock-free and sharded,
  so a single mutex would serialize unrelated keys. In async code prefer its
  async interface (`insert_async` / `contains_async` / `remove_async`); pair it
  with `ahash::RandomState` as the hasher.
- Use `tokio::sync` primitives (`Notify`, channels, `select!`) for async
- **Independent I/O runs concurrently, never serially.** A path that issues N
  independent reads — point gets, cell fetches, per-item durable loads — must not
  `await` them one at a time. Serial round trips multiply latency exactly when it
  hurts most: a cold cache after a rebalance turns one logical read of an N-item
  window into N sequential coordinator round trips. Drive them through a bounded
  `buffered(N)` (order-preserving) or `buffer_unordered(N)` (unordered), each
  future wrapped in `cooperative` per the bullet below. "Independent" means the
  ops don't race on shared mutable state — concurrent reads of distinct keys
  qualify (per-key cache fills are already safe); writes whose correctness
  depends on serialization do not. When the reads form a contiguous range, a
  single bulk/range query beats N concurrent point reads — one round trip.
  Reserve serial `await` for genuinely *dependent* reads, where each result
  determines the next.
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

## Tracing / OpenTelemetry

**Instrument with `#[instrument]`, never a hand-built `info_span!` +
`.instrument(...)`.** The repo pattern: `#[instrument(name = "map.set",
skip_all, fields(collection = ..., map.key = ?key), err)]` for user-facing
operations (span name = the operation, low-cardinality; `skip_all` plus
explicit `fields` control the attributes; `err` records failures on the span),
and `#[instrument(level = "debug", skip(self), err)]` for internal plumbing.
Hand-built spans are reserved for the cases the attribute cannot express: a
function returning `impl Stream` (instrument each inner await with a clone of
one span), a relation against a carried context (`related_span!`), a value
known only mid-body (declare the field `Empty`, then
`Span::current().record(...)`), and a level known only at runtime (a tracing
callsite's level is static — branch between two invocations). Record unsigned
integers as `i64` — the OTel layer exports signed ints as typed Int attributes
but stringifies `u64`/`usize` through their Debug form. Timer instants on span
attributes are recorded as `timer.fire_time = %time.to_rfc3339()` (paired with
`timer.type`); this RFC 3339 convention governs **span attributes only** — log
events keep the plain `Display` form (`fire_time = %fire_time`).

**Span level is audience.** Application-facing spans — message lifecycle,
keyed-state collection ops, application-timer ops — export at info;
framework-internal spans use `level = "debug"`, so a trace filtered at info
contains only spans the user's own code caused. Spans whose subject is a
runtime `TimerType` decide via `TimerType::is_application`: the crate-internal
`timer_span!` macro and `related_span!`'s `level:` form own the branch.
Mid-body records on such spans must go through the **owned span handle**,
never `Span::current()`: a level-disabled span never becomes current, so
"current" silently falls back to the ambient event span and defaces it with a
duplicate attribute (`run_spanned` in `event_context` is the exemplar).
Record attribute values with `%` (Display) whenever the type allows — Debug
quotes strings, which breaks joining the same key across spans.

**Import tracing macros from `tracing` directly — never `use tracing::log::…`.**
`tracing::log` re-exports the bare `log` crate and no `log`→`tracing` bridge is
installed, so events logged through it silently vanish (a producer
duplicate-suppression log never emitted for this reason).

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

See the write-through cache in `CassandraTimerDeferStore`
(`src/consumer/middleware/defer/timer/store/cassandra/mod.rs`) for the
reference implementation.

## Workflows

When launching multi-agent workflows:

- **Select model and effort per task by complexity — don't let every agent
  inherit the session model.** Reserve the top tier for the hardest
  creative/correctness-critical work; use mid tiers for review lenses and
  judgment-guided-but-narrow tasks; use the smallest tier and low effort for
  mechanical work (grep enumeration, running gate commands). But never
  downgrade a stage whose output gates a commit or ship decision.
- **Disable the advisor in every agent prompt.** The advisor tool stalls and
  kills workflow agents; each prompt must explicitly forbid consulting it.
- **Keep structured-output schemata trivially simple.** One of the most common
  workflow failure modes is an agent unable to satisfy its output schema:
  complex schemas, long strings, angle-bracket content, and tight constraints
  make the StructuredOutput call fail and the agent die. Use flat objects with
  a few short bounded fields (status, one-line summary, report path) and put
  all detail in report files the agent writes to the scratchpad.

## Research

- Automatically use context7 for code generation and library documentation.

## CI planning

- Check Cargo Rail after each CI path or repository layout change.
- Confirm that README-only changes select documentation jobs only.
- Confirm that source changes select all required build and test jobs.
- Add `rail.toml` only when the default rules classify a path incorrectly.
