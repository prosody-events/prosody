//! Test-only fault seams for the cell cache.

use super::FjallCellCacheError;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Faults a test injects into one cache and its clones.
#[derive(Clone, Default)]
pub(crate) struct Faults {
    puts: Arc<AtomicBool>,
    deletes: Arc<AtomicU64>,
    reads: Arc<AtomicBool>,
    probes: Arc<AtomicU64>,
}

impl Faults {
    /// The flag that fails every publish while set: `put`, `put_batch`, and
    /// `commit_batch` return an engine error without a fjall write.
    #[must_use]
    pub(crate) fn fail_puts(&self) -> Arc<AtomicBool> {
        self.puts.clone()
    }

    /// The countdown of delete calls to fail. `delete_batch` and
    /// `delete_section` each consume one charge, then deletes heal.
    #[must_use]
    pub(crate) fn fail_deletes(&self) -> Arc<AtomicU64> {
        self.deletes.clone()
    }

    /// The flag that fails point and batch probes while set.
    #[must_use]
    pub(crate) fn fail_reads(&self) -> Arc<AtomicBool> {
        self.reads.clone()
    }

    /// The number of blocking hops that batch probes launched. One batch
    /// probe launches one hop, however many keys it carries.
    #[must_use]
    pub(crate) fn probe_hops(&self) -> u64 {
        self.probes.load(Ordering::Relaxed)
    }

    /// Fails a publish while the put fault is set.
    pub(super) fn put(&self) -> Result<(), FjallCellCacheError> {
        injected(self.puts.load(Ordering::Relaxed))
    }

    /// Fails a probe while the read fault is set.
    pub(super) fn read(&self) -> Result<(), FjallCellCacheError> {
        injected(self.reads.load(Ordering::Relaxed))
    }

    /// Counts one blocking probe hop and fails it while the read fault is set.
    pub(super) fn probe(&self) -> Result<(), FjallCellCacheError> {
        self.probes.fetch_add(1, Ordering::Relaxed);
        self.read()
    }

    /// Consumes one delete charge and fails while charges remain.
    pub(super) fn delete(&self) -> Result<(), FjallCellCacheError> {
        injected(
            self.deletes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok(),
        )
    }
}

/// Returns the injected error when `fail` is set.
fn injected(fail: bool) -> Result<(), FjallCellCacheError> {
    if fail {
        Err(FjallCellCacheError::Injected)
    } else {
        Ok(())
    }
}
