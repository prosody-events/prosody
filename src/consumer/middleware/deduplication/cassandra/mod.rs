//! Cassandra-backed deduplication store.

use super::queries::DeduplicationQueries;
use super::store::{DeduplicationStore, DeduplicationStoreProvider, Marker, Presence};
use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::{Partition, Topic};
use quick_cache::sync::Cache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Read durable markers through a shared cache.
/// Each read or write adds the marker to the cache.
#[derive(Clone, Debug)]
pub struct CassandraDeduplicationStore {
    store: CassandraStore,
    queries: Arc<DeduplicationQueries>,
    ttl: i32,
    cache: Arc<Cache<Uuid, Marker>>,
    acquired: Instant,
}

impl DeduplicationStore for CassandraDeduplicationStore {
    type Error = CassandraStoreError;

    async fn recorded(&self, id: Uuid) -> Result<bool, Self::Error> {
        if self.cache.get(&id).is_some() {
            return Ok(true);
        }
        let recorded = self.read_marker(id).await?;
        if recorded {
            self.cache.insert(id, Marker::Recorded);
        }
        Ok(recorded)
    }

    async fn lookup(&self, id: Uuid) -> Result<Presence, Self::Error> {
        match self.cache.get(&id) {
            Some(Marker::Observed(stamp)) if stamp >= self.acquired => Ok(Presence::Settled),
            Some(Marker::Recorded | Marker::Observed(_)) => {
                self.cache.insert(id, Marker::Observed(Instant::now()));
                Ok(Presence::Inherited)
            }
            None => {
                if self.read_marker(id).await? {
                    self.cache.insert(id, Marker::Observed(Instant::now()));
                    Ok(Presence::Inherited)
                } else {
                    Ok(Presence::Absent)
                }
            }
        }
    }

    async fn insert(&self, id: Uuid) -> Result<(), Self::Error> {
        self.store
            .session()
            .execute_unpaged(&self.queries.insert_with_ttl, (id, self.ttl))
            .await?;
        self.cache.insert(id, Marker::Observed(Instant::now()));
        Ok(())
    }
}

impl CassandraDeduplicationStore {
    async fn read_marker(&self, id: Uuid) -> Result<bool, CassandraStoreError> {
        Ok(self
            .store
            .session()
            .execute_unpaged(&self.queries.check_exists, (id,))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(Uuid,)>()?
            .is_some())
    }
}

/// Share the session, queries, TTL, and cache across partitions.
#[derive(Clone, Debug)]
pub struct CassandraDeduplicationStoreProvider {
    store: CassandraStore,
    queries: Arc<DeduplicationQueries>,
    ttl: i32,
    cache: Arc<Cache<Uuid, Marker>>,
}

impl CassandraDeduplicationStoreProvider {
    /// Creates a new provider. `cache_capacity` is the number of UUIDs the
    /// shared write-through cache can hold.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<DeduplicationQueries>,
        ttl: i32,
        cache_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            store,
            queries,
            ttl,
            cache: Arc::new(Cache::new(cache_capacity.get())),
        }
    }
}

impl DeduplicationStoreProvider for CassandraDeduplicationStoreProvider {
    type Store = CassandraDeduplicationStore;

    fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
    ) -> Self::Store {
        CassandraDeduplicationStore {
            store: self.store.clone(),
            queries: self.queries.clone(),
            ttl: self.ttl,
            cache: self.cache.clone(),
            acquired: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests;
