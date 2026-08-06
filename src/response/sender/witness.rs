//! Test delivery accounting with no production cost.

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(super) struct DeliveryWitness(Arc<SendCounters>);

#[cfg(not(test))]
#[derive(Clone, Debug, Default)]
pub(super) struct DeliveryWitness;

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct SendCounters {
    sent: AtomicU64,
    dropped: AtomicU64,
}

#[cfg(test)]
impl DeliveryWitness {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn sent(&self) {
        self.0.sent.fetch_add(1, Relaxed);
    }

    pub(super) fn dropped(&self) {
        self.0.dropped.fetch_add(1, Relaxed);
    }

    pub(super) fn counters(&self) -> Arc<SendCounters> {
        Arc::clone(&self.0)
    }
}

#[cfg(not(test))]
impl DeliveryWitness {
    pub(super) const fn new() -> Self {
        Self
    }

    pub(super) const fn sent(&self) {
        let _ = self;
    }

    pub(super) const fn dropped(&self) {
        let _ = self;
    }
}

#[cfg(test)]
impl SendCounters {
    pub(crate) fn sent(&self) -> u64 {
        self.sent.load(Relaxed)
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Relaxed)
    }
}
