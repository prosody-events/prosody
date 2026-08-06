//! Test delivery accounting with no production cost.

#[cfg(test)]
mod imp {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    #[derive(Clone, Debug, Default)]
    pub(crate) struct DeliveryWitness(Arc<SendCounters>);

    #[derive(Debug, Default)]
    pub(crate) struct SendCounters {
        sent: AtomicU64,
        dropped: AtomicU64,
    }

    impl DeliveryWitness {
        pub(in crate::response::sender) fn sent(&self) {
            self.0.sent.fetch_add(1, Relaxed);
        }

        pub(in crate::response::sender) fn dropped(&self) {
            self.0.dropped.fetch_add(1, Relaxed);
        }

        pub(in crate::response::sender) fn counters(&self) -> Arc<SendCounters> {
            Arc::clone(&self.0)
        }
    }

    impl SendCounters {
        pub(crate) fn sent(&self) -> u64 {
            self.sent.load(Relaxed)
        }

        pub(crate) fn dropped(&self) -> u64 {
            self.dropped.load(Relaxed)
        }
    }
}

#[cfg(not(test))]
mod imp {
    #[derive(Clone, Debug, Default)]
    pub(crate) struct DeliveryWitness;

    impl DeliveryWitness {
        pub(in crate::response::sender) const fn sent(&self) {}

        pub(in crate::response::sender) const fn dropped(&self) {}
    }
}

pub(super) use imp::DeliveryWitness;
#[cfg(test)]
pub(crate) use imp::SendCounters;
