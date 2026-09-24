//! Admission proofs stored in the assignment's disk workspace.

use super::FjallCellCacheError;
use crate::state::backend::AdmissionChecks;
use educe::Educe;
use fjall::Keyspace;
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::task::spawn_blocking;
use tracing::warn;

/// Assignments that disabled their cell cache after a repair failure.
///
/// [`FjallCellCache::disable`](super::FjallCellCache::disable) increments this
/// counter once per assignment.
static CACHE_DISABLED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.state.cell.cache.disabled_assignments")
        .with_description("Keyed-state assignments that disabled their cell cache")
        .with_unit("{assignment}")
        .build()
});

/// Stores admission proofs in the assignment's disk workspace.
/// Workspace deletion and the startup orphan sweep reclaim the rows.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub(crate) struct MarkerCheckSet {
    #[educe(Debug(ignore))]
    pub(super) index: Keyspace,
    #[educe(Debug(ignore))]
    pub(super) disabled: Arc<AtomicBool>,
}

impl MarkerCheckSet {
    /// Disables this assignment once, with its log and counter.
    pub(super) fn disable(&self) {
        if !self.disabled.swap(true, Ordering::Relaxed) {
            warn!("keyed-state cell cache disabled for this assignment; using durable reads");
            CACHE_DISABLED.add(1, &[]);
        }
    }
}

impl AdmissionChecks for MarkerCheckSet {
    type Error = FjallCellCacheError;

    async fn contains(&self, key: &crate::Key) -> Result<bool, Self::Error> {
        if self.disabled.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let index = self.index.clone();
        let key = key.clone();
        Ok(spawn_blocking(move || index.contains_key(key.as_bytes())).await??)
    }

    async fn mark(&self, key: &crate::Key) -> Result<(), Self::Error> {
        if self.disabled.load(Ordering::Relaxed) {
            return Ok(());
        }
        let index = self.index.clone();
        let key = key.clone();
        spawn_blocking(move || index.insert(key.as_bytes(), [])).await??;
        Ok(())
    }

    async fn unmark(&self, key: &crate::Key) -> Result<(), Self::Error> {
        let index = self.index.clone();
        let key = key.clone();
        let result = async {
            spawn_blocking(move || index.remove(key.as_bytes())).await??;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.disable();
        }
        result
    }
}
