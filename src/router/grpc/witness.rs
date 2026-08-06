//! Test transport accounting with no production cost.

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[cfg(test)]
pub(crate) struct TransportCounters {
    served: AtomicU64,
    refused_connections: AtomicU64,
    rejected_frames: AtomicU64,
    misrouted: AtomicU64,
    forwarded: AtomicU64,
}

#[cfg(not(test))]
pub(crate) struct TransportCounters;

#[cfg(test)]
impl TransportCounters {
    pub(super) const fn new() -> Self {
        Self {
            served: AtomicU64::new(0),
            refused_connections: AtomicU64::new(0),
            rejected_frames: AtomicU64::new(0),
            misrouted: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
        }
    }

    pub(crate) fn served(&self) -> u64 {
        self.served.load(Relaxed)
    }

    pub(crate) fn refused_connections(&self) -> u64 {
        self.refused_connections.load(Relaxed)
    }

    pub(crate) fn rejected_frames(&self) -> u64 {
        self.rejected_frames.load(Relaxed)
    }

    pub(crate) fn misrouted(&self) -> u64 {
        self.misrouted.load(Relaxed)
    }

    pub(crate) fn forwarded(&self) -> u64 {
        self.forwarded.load(Relaxed)
    }

    pub(super) fn record_served(&self) {
        self.served.fetch_add(1, Relaxed);
    }

    pub(super) fn record_refused_connection(&self) {
        self.refused_connections.fetch_add(1, Relaxed);
    }

    pub(super) fn record_rejected_frame(&self) {
        self.rejected_frames.fetch_add(1, Relaxed);
    }

    pub(super) fn record_misrouted(&self) {
        self.misrouted.fetch_add(1, Relaxed);
    }

    pub(super) fn record_forwarded(&self) {
        self.forwarded.fetch_add(1, Relaxed);
    }
}

#[cfg(not(test))]
impl TransportCounters {
    pub(super) const fn new() -> Self {
        Self
    }

    pub(super) const fn record_served(&self) {
        let _ = self;
    }

    pub(super) const fn record_refused_connection(&self) {
        let _ = self;
    }

    pub(super) const fn record_rejected_frame(&self) {
        let _ = self;
    }

    pub(super) const fn record_misrouted(&self) {
        let _ = self;
    }

    pub(super) const fn record_forwarded(&self) {
        let _ = self;
    }
}

/// Test witness for transport events that process metrics cannot isolate.
pub(crate) static TRANSPORT: TransportCounters = TransportCounters::new();
