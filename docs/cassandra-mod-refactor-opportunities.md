# Refactoring Opportunities: `src/timers/store/cassandra/mod.rs`

> Reviewed 2026-02-24. File is 3 170 lines; ~1 220 lines are tests (lines 1948–3170).

---

## Summary

The file mixes five distinct concerns in one place:

1. **Domain types** (`TimerState`, `InlineTimer`, `RawTimerState` + serde impls)
2. **Store struct + constructor helpers**
3. **Internal state-column operations** (fetch, set, batch writes)
4. **`TriggerOperations` impl** (the public trait surface)
5. **Provider + free function** (`cassandra_store`, `CassandraTriggerStoreProvider`)
6. **Tests** (integration + property, 1 220 lines)

Each of those can live in its own submodule/file.

---

## 1. Extract tests into `tests/mod.rs` (no-brainer)

**Lines 1948–3170** (`mod test { … }`) are pure integration/property tests.
They have nothing structurally in common with the production code above them.

**Proposed layout:**

```
cassandra/
  mod.rs           (~1 950 lines → ~730 lines after extraction)
  queries.rs        (unchanged)
  migration.rs      (unchanged)
  v1/               (unchanged)
  tests/
    mod.rs          (re-exports / shared helpers)
    helpers.rs      (setup_test_store, test_cassandra_config, get_test_count,
                     assert_key_reads, assert_state_and_reads,
                     collect_key_times, collect_trigger_times,
                     collect_all_types_times)
    state_transitions.rs   (test_state_transitions_*)
    slab_range.rs          (test_slab_range_wrap_around_edge_cases, test_simple_wrap_around)
    migration_tests.rs     (test_pre_migration_reads_and_migration,
                            test_pre_migration_mutations)
    inline_round_trip.rs   (test_inline_state_round_trip)
    provider_tests.rs      (test_provider_creates_independent_stores)
    clear_all_types.rs     (test_clear_all_types_clears_inline_and_overflow)
    prop_invariant.rs      (test_prop_timer_state_invariant,
                            prop_timer_state_invariant helper)
```

---

## 2. Duplicate store-creation boilerplate in tests

Every integration test independently constructs `(CassandraStore, CassandraTriggerStore)`
with the same three-line pattern:

```rust
let config = test_cassandra_config("prosody_test");
let cassandra_store = CassandraStore::new(&config).await?;
let store = CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;
```

This appears in at least **6 tests** (lines 2023–2025, 2122–2124, 2719–2722,
2813–2816, 2954–2959, etc.), plus the `setup_test_store` helper exists *only* for
some of them. The helper (line 2547–2563) is good — it should be the *only*
construction path in tests. The tests that bypass it (e.g., `test_clear_all_types_clears_inline_and_overflow`,
`test_inline_state_round_trip`, `test_prop_timer_state_invariant`) should use it
or a small variant of it instead.

**Fix:** Extend `setup_test_store` to accept an optional `SegmentVersion` parameter
(it currently hard-codes `V3`) so all tests can go through a single factory.

---

## 3. Duplicate span-injection pattern (3 callers)

The pattern of extracting a span context and injecting it into a `HashMap` is
copy-pasted in three `TriggerOperations` methods:

```rust
// insert_key_trigger  (line ~1446)
let mut new_span_map: HashMap<String, String> = HashMap::with_capacity(2);
let context = trigger.span.load().context();
self.propagator().inject_context(&context, &mut new_span_map);

// insert_key_trigger, Absent branch  (line ~1467)
let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
let context = trigger.span.load().context();
self.propagator().inject_context(&context, &mut span_map);

// clear_and_schedule_key  (line ~1629)
let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
let context = trigger.span.load().context();
self.propagator().inject_context(&context, &mut span_map);
```

`add_key_trigger_clustering` (line ~819) has the same pattern too.

**Fix:** Extract a private helper:

```rust
fn extract_span_map(&self, trigger: &Trigger) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(2);
    let context = trigger.span.load().context();
    self.propagator().inject_context(&context, &mut map);
    map
}
```

---

## 4. Duplicate span-reconstruction pattern (`set_parent` + error log)

The pattern of extracting a propagator context into a span appears five times,
always with the same `if let Err(error) = span.set_parent(…)` guard:

```rust
let context = self.propagator().extract(&span_map);
let span = info_span!("fetch_key_trigger_inline");
if let Err(error) = span.set_parent(context) {
    debug!("failed to set parent span: {error:#}");
}
```

Occurrences: `get_key_triggers` (line ~1272), `get_key_triggers_all_types`
(line ~1408), `advance_inline` (line ~1836), `advance_clustering` (line ~1870),
`get_slab_triggers` / `get_slab_triggers_all_types` (lines ~1085, ~1123).

**Fix:** A small free function (already partially addressed by `advance_inline`
and `advance_clustering` being extracted — but those still duplicate the pattern
internally). A helper like:

```rust
fn make_span(
    propagator: &TextMapCompositePropagator,
    span_map: &HashMap<String, String>,
    name: &'static str,
) -> Span {
    let context = propagator.extract(span_map);
    let span = info_span!(name);  // static name still needed per callsite
    if let Err(error) = span.set_parent(context) {
        debug!("failed to set parent span: {error:#}");
    }
    span
}
```

…won't work perfectly because `info_span!` needs a static name. But the
`set_parent` + error-log half can at least be pulled into a utility.

---

## 5. `get_slab_triggers` and `get_slab_triggers_all_types` — near-identical bodies

These two methods (lines 1062–1132) are almost identical. The only differences are:
- the query (`get_slab_triggers` vs `get_slab_triggers_all_types`)
- the `timer_type` argument (present in single-type, absent in all-types)
- the span name string

The stream body — `execute_iter`, `rows_stream`, `pin_mut!`, the `while let` loop,
propagator extraction, `Trigger::new` — is copy-pasted verbatim.

**Fix:** A private `stream_slab_triggers` method that takes the prepared statement
and the decoded row directly, or factor the stream-to-trigger conversion into a
shared closure/helper.

---

## 6. `get_slab_range` — very long inline comment block

Lines 912–929 are a 17-line comment block explaining the u32/i32 wrap-around
problem. This is not wrong, but the comment repeats information already captured
by the inline comments within the function body (lines 952–1026). There's no need
for both the top-level doc comment and the per-branch inline comments to each give
the full explanation.

**Fix:** Keep the doc comment at the method level; trim the per-branch comments to
a single line each. This cuts the method body by ~15 lines.

---

## 7. `batch_promote_and_set_overflow` — verbose TTL parameter repetition

The two closures in `execute_with_optional_ttl` for `batch_promote_and_set_overflow`
(lines 764–800) each repeat the 14-element parameter tuple nearly identically.
The only difference is the presence/absence of `ttl`. This is hard to read and
easy to get wrong when the query signature changes.

This pattern appears across the codebase wherever `execute_with_optional_ttl` is
called — it's a consequence of the TTL helper design, not just this one site. A
potential improvement is a macro or a builder pattern for the batch parameters.
This is a lower-priority cleanup but worth noting.

---

## 8. `get_timer_state` is a trivial wrapper around `fetch_state`

```rust
pub async fn get_timer_state(…) -> Result<TimerState, …> {
    self.fetch_state(segment_id, key, timer_type).await
}
```

(Lines 408–415.) This bypasses the cache (`resolve_state`) every time it is
called. In production code this isn't called on the hot path, but in tests it is
called frequently for assertions — and those test calls cause DB round-trips even
when the cache is warm. This asymmetry is confusing.

Two options:
- **Make it cache-aware** by routing through `resolve_state`.
- **Delete it** and require callers to choose between `fetch_state` (DB-direct)
  or `resolve_state` (cache-first) explicitly.

The current split between `fetch_state`, `get_timer_state`, and `resolve_state` is
not well-differentiated. `get_timer_state` adds no value over `fetch_state`.

---

## 9. `TimerState` serde lives alongside the struct

`RawTimerState`, `SerializeValue for TimerState`, and `DeserializeValue for
TimerState` (lines 101–160) are the Cassandra wire-format concern for `TimerState`.
They would fit more naturally in a `serde.rs` or `types.rs` submodule, keeping
`mod.rs` focused on the store logic.

---

## 10. `advance_inline` / `advance_clustering` — free functions at module level

Lines 1822–1879 are two free functions that are only called from one place:
`get_key_triggers_all_types`. They were correctly extracted to simplify the main
method, but they are at module level when they could be private helpers within an
`impl` block (as `fn`, not `&self` methods) or in the future `impl` block of a
helper struct. This is minor but violates the "Methods without self should be
functions" rule from CLAUDE.md only partially — they are already functions, but
they are at the wrong scope level given they're single-callers.

---

## Recommended Extraction Order

| Priority | Change | Impact |
|----------|--------|--------|
| 1 | Extract `#[cfg(test)]` into `tests/` submodule | ~40% line reduction in `mod.rs` |
| 2 | Unify test store construction via `setup_test_store` | Removes 6+ duplicate blocks |
| 3 | Extract `extract_span_map` helper | Removes 3–4 duplicate blocks |
| 4 | Extract `TimerState` serde into `types.rs` or `serde.rs` | Improves navigability |
| 5 | Factor shared body of `get_slab_triggers*` | Removes ~30 lines of duplication |
| 6 | Remove `get_timer_state` / merge with `resolve_state` | Clarifies cache semantics |
| 7 | Trim verbose TTL closure repetition (longer-term) | Harder, requires API change |
