//! In-memory deduplication store for testing.
//!
//! Provider drop releases the records after all store handles drop.

use super::store::{DeduplicationStore, DeduplicationStoreProvider, Presence};
use crate::{Partition, Topic};
use ahash::RandomState;
use scc::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Message markers shared by the stores from one provider.
#[derive(Clone, Debug)]
pub struct MemoryDeduplicationStore {
    records: Arc<HashMap<Uuid, Instant, RandomState>>,
    acquired: Instant,
}

impl MemoryDeduplicationStore {
    /// Creates a new empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records: Arc::new(HashMap::with_hasher(RandomState::new())),
            acquired: Instant::now(),
        }
    }
}

impl Default for MemoryDeduplicationStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DeduplicationStore for MemoryDeduplicationStore {
    type Error = Infallible;

    async fn lookup(&self, id: Uuid) -> Result<Presence, Self::Error> {
        let Some(mut entry) = self.records.get_async(&id).await else {
            return Ok(Presence::Absent);
        };
        if *entry.get() >= self.acquired {
            Ok(Presence::Settled)
        } else {
            *entry.get_mut() = Instant::now();
            Ok(Presence::Inherited)
        }
    }

    async fn insert(&self, id: Uuid) -> Result<(), Self::Error> {
        self.records.upsert_async(id, Instant::now()).await;
        Ok(())
    }
}

/// Share marker records across assignments. Each new store starts an
/// assignment.
#[derive(Clone, Debug, Default)]
pub struct MemoryDeduplicationStoreProvider {
    records: Arc<HashMap<Uuid, Instant, RandomState>>,
}

impl MemoryDeduplicationStoreProvider {
    /// Returns the number of stored message markers.
    #[cfg(test)]
    pub(crate) fn marker_count(&self) -> usize {
        self.records.len()
    }

    /// Creates a new provider backed by one fresh shared store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DeduplicationStoreProvider for MemoryDeduplicationStoreProvider {
    type Store = MemoryDeduplicationStore;

    fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
    ) -> Self::Store {
        MemoryDeduplicationStore {
            records: self.records.clone(),
            acquired: Instant::now(),
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use std::convert::Infallible;

    use super::MemoryDeduplicationStoreProvider;

    crate::dedup_store_tests!(async {
        Ok::<_, Infallible>(MemoryDeduplicationStoreProvider::new())
    });
}

#[cfg(test)]
mod tests;
