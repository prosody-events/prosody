use super::SerializeBufGuard;
use color_eyre::eyre::{Result, ensure};
use quickcheck_macros::quickcheck;

/// Pool monotonicity: across an arbitrary sequence of serializes, every
/// acquired buffer is handed out empty, and the pool's retained capacity
/// never shrinks — each acquire returns capacity ≥ the largest write seen
/// so far, so a steady stream of serializes stops allocating.
///
/// This drives both arms of the capacity-aware return in `Drop`: a write
/// larger than the pooled buffer replaces it, a smaller one keeps the
/// existing (larger) allocation. `sizes` are `u16`, so each write is at
/// most 64 KiB — random inputs cannot trigger a runaway allocation.
/// Assertions are relative to the running maximum, never an absolute
/// capacity, so the property holds regardless of buffer state left over
/// from earlier tests on the shared thread-local.
#[quickcheck]
fn prop_pool_capacity_is_monotonic(sizes: Vec<u16>) -> Result<()> {
    let mut high_water = 0_usize;
    for size in sizes {
        let size = usize::from(size);
        let mut guard = SerializeBufGuard::acquire();
        ensure!(guard.is_empty(), "acquired buffer was not cleared");
        ensure!(
            guard.capacity() >= high_water,
            "pool capacity {} regressed below high-water mark {high_water}",
            guard.capacity(),
        );
        guard.resize(size, 0);
        high_water = high_water.max(size);
    }
    Ok(())
}

/// Nested-acquire safety: a second `acquire` while the first guard is still
/// live must not panic (the documented "release the `RefCell` borrow
/// immediately" invariant), must hand out a fresh empty buffer rather than
/// aliasing the outer guard, and must still feed the larger of the two
/// capacities back into the pool on drop.
///
/// The monotonicity property above never holds two guards at once, so this
/// is the only test of the simultaneous-borrow path that a refactor
/// holding a `RefMut` (instead of `take`-ing the `Vec`) would silently
/// break with a double-borrow panic.
#[quickcheck]
fn prop_nested_acquire_preserves_max(outer: u16, inner: u16) -> Result<()> {
    let outer = usize::from(outer);
    let inner = usize::from(inner);

    let mut g_outer = SerializeBufGuard::acquire();
    g_outer.resize(outer, 0);

    let mut g_inner = SerializeBufGuard::acquire();
    ensure!(
        g_inner.is_empty(),
        "nested acquire aliased the outer buffer instead of taking a fresh one",
    );
    g_inner.resize(inner, 0);

    drop(g_inner);
    drop(g_outer);

    let restored = SerializeBufGuard::acquire();
    ensure!(
        restored.capacity() >= outer.max(inner),
        "pool capacity {} dropped below the larger nested write {}",
        restored.capacity(),
        outer.max(inner),
    );
    Ok(())
}
