//! Memory collections satisfy the shared model properties.

use super::*;

/// Deque collection soundness over the real session lifecycle: random
/// push/pop/clear/mid-handler-commit traces with commit/abort/crash
/// outcomes keep the handle's `len`/`values`/`get` and every `pop` return
/// value in step with a `VecDeque` oracle — the window invariant (incl. the
/// index-space reset on clear), bounds+entries crash atomicity, and the
/// at-least-once `commit()` contract (`commit()`-landed ops survive
/// abort/crash-rollback; post-commit ops roll back — so a
/// commit-then-clear-then-abort trace restores the `commit()`-landed state).
#[test]
pub(super) fn prop_deque_collection_lifecycle() {
    fn property(trace: DequeTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_trace(trace, CommitMode::ReadCommitted, None))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The deque lifecycle property in `ReadUncommitted` mode: `finalize` commits
/// everything, so every outcome that reaches it — including crash-abort —
/// converges to the full scratch model.
#[test]
pub(super) fn prop_deque_collection_lifecycle_read_uncommitted() {
    fn property(trace: DequeTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_trace(trace, CommitMode::ReadUncommitted, None))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The deque lifecycle property on a **bounded** deque (capacity 2, under the
/// push burst so eviction fires on nearly every push-to-full): the handle keeps
/// step with a `VecDeque` model that applies the identical capped-trim rule, in
/// both commit modes — so lazy push-only eviction, its rollback under
/// abort/crash, and the at-least-once `commit()` floor all hold with a cap in
/// play. The unbounded lifecycle properties above test the `capacity = None`
/// path.
///
/// Falsification: Make `evictions` always return zero.
/// Then the handle exceeds the model and the `assert_deque` check fails.
#[test]
pub(super) fn prop_deque_bounded_lifecycle() {
    fn property(trace: DequeTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_trace(
            trace,
            CommitMode::ReadCommitted,
            Some(BOUNDED_TEST_CAP),
        ))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// The bounded deque lifecycle property in `ReadUncommitted` mode.
#[test]
pub(super) fn prop_deque_bounded_lifecycle_read_uncommitted() {
    fn property(trace: DequeTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_trace(
            trace,
            CommitMode::ReadUncommitted,
            Some(BOUNDED_TEST_CAP),
        ))
    }
    QuickCheck::new().quickcheck(property as fn(DequeTrace) -> Result<bool>);
}

/// Deque runtime-capacity convergence: over a directly-seeded over-wide (and
/// possibly holed) window, lazy push-only eviction converges to `len <= cap`
/// within the computed catch-up pushes, evicting at most `TRIM_MAX` slots per
/// push (read from the buffered dirty overlay). See
/// [`run_deque_capacity_convergence`] for the full disposition.
///
/// Falsification: Remove `.min(TRIM_MAX)` from `evictions`.
/// Then one push exceeds the delete limit and the per-push cap assert fails.
#[test]
pub(super) fn prop_deque_capacity_convergence() {
    fn property(shape: DequeCapacityShape) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_capacity_convergence(shape))
    }
    QuickCheck::new().quickcheck(property as fn(DequeCapacityShape) -> Result<bool>);
}

/// Map reads and bounded queries match the model through the full lifecycle.
/// Both commit modes cover tracked keysets, range scans, aborts, and recovery.
#[test]
pub(super) fn prop_map_query_matches_model() {
    fn property(trace: MapTrace, constraints: StreamConstraints) -> Result<bool> {
        TEST_RUNTIME.block_on(run_map_query_trace(trace, constraints))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace, StreamConstraints) -> Result<bool>);
}

/// Both deque sources match the model across holes and position bounds.
#[test]
pub(super) fn prop_deque_constraint_parity() {
    fn property(shape: DequeConstraints) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_constraint_parity(shape))
    }
    QuickCheck::new().quickcheck(property as fn(DequeConstraints) -> Result<bool>);
}

/// Keyset exactness: over an arbitrary committed trace on a non-overflowing
/// map, the stored keyset decodes to exactly the live key set after every
/// settled event — `set` adds, `remove` subtracts, `clear` erases. A loose
/// superset (the pre-keyset design, or a `remove` that failed to subtract)
/// would fail here.
#[test]
pub(super) fn prop_map_keyset_exact() {
    fn property(trace: MapTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_map_keyset_exact_trace(trace))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// Map batch-read parity: values and presence answer each position exactly as
/// their point twins over random populations and query lists. The inputs cover
/// duplicates, absent keys, and lengths above `CELL_BATCH` in dirty and
/// committed arms.
#[test]
pub(super) fn prop_map_get_many_parity() {
    fn property(input: MapGetManyInput) -> Result<bool> {
        TEST_RUNTIME.block_on(run_map_get_many_parity_trace(input))
    }
    QuickCheck::new().quickcheck(property as fn(MapGetManyInput) -> Result<bool>);
}

/// Map TTL keyset-refresh: on a TTL'd map every `set` — including a re-set of
/// an already-tracked key, and once overflowed — buffers the keyset cell, so
/// its TTL is refreshed and it outlives every entry. Staged-set composition, so
/// no clock is needed.
#[test]
pub(super) fn prop_map_ttl_keyset_refresh() {
    fn property(trace: MapTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_map_ttl_keyset_refresh_trace(trace))
    }
    QuickCheck::new().quickcheck(property as fn(MapTrace) -> Result<bool>);
}

/// Map key-scan presence: over a directly-seeded map whose keyset frame
/// over-reports a TTL-expired coordinate, `keys()` yields exactly the present
/// keys in order across both arms (tracked point-get and degrade scan), and
/// agrees with `stream()` on the live key set — the presence-only key scan
/// skips a coordinate the keyset lists but the store no longer holds.
#[test]
pub(super) fn prop_map_key_scan_holes() {
    fn property(shape: MapKeyHoles) -> Result<bool> {
        TEST_RUNTIME.block_on(run_map_key_scan_holes(shape))
    }
    QuickCheck::new().quickcheck(property as fn(MapKeyHoles) -> Result<bool>);
}

/// Deque TTL holes: over a directly-seeded sparse window, `len` is the full
/// span (an upper bound on live elements) and `get`/`values` skip expired
/// indices without error — the TTL'd-deque hole read contract.
#[test]
pub(super) fn prop_deque_ttl_holes() {
    fn property(shape: DequeHoles) -> Result<bool> {
        TEST_RUNTIME.block_on(run_deque_holes(shape))
    }
    QuickCheck::new().quickcheck(property as fn(DequeHoles) -> Result<bool>);
}

/// Apply idempotence over the memory cell store: any generated interleaving of
/// marker resolution, verdict-matching settle re-applies, and per-cell
/// reads over one staged set with durable section clears converges to
/// the verdict state — no marker, no provisional residue, exact row shape.
#[test]
pub(super) fn prop_memory_apply_idempotence() {
    fn property(input: ApplyTrace) -> Result<bool> {
        let cells = MemoryCells::new();
        let store = MemoryCellStore::new(cells.clone());
        let probe = MemoryShapeProbe(cells);
        TEST_RUNTIME.block_on(run_apply_idempotence(store, input, &probe))
    }
    QuickCheck::new().quickcheck(property as fn(ApplyTrace) -> Result<bool>);
}

/// The backend-generic publication-store contract over the memory backend.
/// The Cassandra instantiation in `state::cassandra::tests` runs the same
/// runner.
#[test]
pub(super) fn prop_memory_publication_trace() {
    fn property(trace: PublicationTrace) -> Result<bool> {
        let store = MemoryPublicationStore::new();
        let token = Uuid::new_v4().to_string();
        TEST_RUNTIME.block_on(run_publication_trace(&store, &token, trace))
    }
    QuickCheck::new().quickcheck(property as fn(PublicationTrace) -> Result<bool>);
}
