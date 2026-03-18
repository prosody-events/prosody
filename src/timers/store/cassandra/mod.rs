//! Cassandra-backed [`TriggerStore`] implementation.
//!
//! The V3 schema eliminates range tombstones by maintaining a `state` static
//! map column alongside the clustering rows. Every `(key, timer_type)` pair
//! carries a [`TimerState`] — [`Absent`], [`Inline`], or [`Overflow`] — that
//! lets the common single-timer case skip clustering row I/O entirely and
//! allows `clear_and_schedule_key` to issue a plain `UPDATE` instead of a
//! `DELETE`-bearing `BATCH`.
//!
//! This file is the public hub: it defines [`CassandraTriggerStore`], its
//! constructors, and the shared low-level helpers (`execute_with_optional_ttl`,
//! etc.) that all submodules call. The bulk of the logic lives in focused
//! submodules to keep individual files navigable.
//!
//! [`TriggerStore`]: crate::timers::store::TriggerStore
//! [`Absent`]: crate::timers::store::cassandra::TimerState::Absent
//! [`Inline`]: crate::timers::store::cassandra::TimerState::Inline
//! [`Overflow`]: crate::timers::store::cassandra::TimerState::Overflow
//! [`CassandraTriggerStore`]: crate::timers::store::cassandra::CassandraTriggerStore
//! [`TimerState`]: crate::timers::store::cassandra::TimerState

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::cassandra::queries::Queries;
use crate::timers::store::{Segment, SegmentId, SegmentVersion};
use educe::Educe;
use opentelemetry::propagation::TextMapCompositePropagator;
use quick_cache::sync::Cache;
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use state::{CachedState, StateCacheKey};
use std::sync::Arc;

mod error;
mod mutators;
mod provider;
mod state;
mod trigger_store;

mod queries;

#[cfg(test)]
mod tests;

/// V1 schema operations (internal, Cassandra-only).
pub(crate) mod v1;

/// Migration utilities for V1→V2 and slab size changes (internal,
/// Cassandra-only).
pub(crate) mod migration;

pub use crate::cassandra::CassandraConfiguration;
pub use error::CassandraTriggerStoreError;
pub use provider::{CassandraTriggerStoreProvider, cassandra_store};
pub use state::{InlineTimer, TimerState};

/// Cassandra-based implementation of [`TriggerStore`](super::TriggerStore).
///
/// Each instance is scoped to a single partition and has its own state cache.
/// Created by [`CassandraTriggerStoreProvider`].
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct CassandraTriggerStore {
    pub(super) store: CassandraStore,
    pub(super) queries: Arc<Queries>,
    pub(super) segment: Segment,
    /// Per-partition cache of `(Key, TimerType) → TimerState`.
    ///
    /// Tracks the current state of each key/type pair:
    /// - `Inline(timer)` — exactly 1 timer, stored in the state column
    /// - `Overflow` — >1 timers, stored in clustering rows
    /// - `Absent` — 0 timers (post-V3: all states including Absent are cached)
    ///
    /// Cache miss → `resolve_state` reads only the requested type via
    /// `fetch_state` (single map-entry query) and caches the result.
    #[educe(Debug(ignore))]
    pub(super) state_cache: Arc<Cache<StateCacheKey, CachedState>>,
}

#[expect(
    clippy::multiple_inherent_impl,
    reason = "impl blocks are split across submodules (mutators.rs, trigger_store.rs) for \
              file-size management; each file implements a distinct concern"
)]
impl CassandraTriggerStore {
    /// Creates a new Cassandra trigger store using an existing
    /// `CassandraStore`.
    ///
    /// This allows sharing a single Cassandra session across multiple stores
    /// (e.g., trigger store and defer store), avoiding the creation of multiple
    /// sessions which is not allowed.
    ///
    /// # Arguments
    ///
    /// * `store` - Existing `CassandraStore` to share
    /// * `keyspace` - Cassandra keyspace name for query preparation
    /// * `segment` - Segment this store is scoped to
    ///
    /// # Errors
    ///
    /// Returns error if query preparation fails.
    pub async fn with_store(
        store: CassandraStore,
        keyspace: &str,
        segment: Segment,
    ) -> Result<Self, CassandraTriggerStoreError> {
        let queries = Arc::new(Queries::new(store.session(), keyspace).await?);

        Ok(Self {
            store,
            queries,
            segment,
            state_cache: state::new_state_cache(),
        })
    }

    /// Creates a store using pre-existing shared resources and a fresh cache.
    ///
    /// Used by `CassandraTriggerStoreProvider` to create per-partition stores
    /// that share the session and queries but have independent caches.
    pub(super) fn with_shared(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment: Segment,
    ) -> Self {
        Self {
            store,
            queries,
            segment,
            state_cache: state::new_state_cache(),
        }
    }

    pub(super) fn session(&self) -> &Session {
        self.store.session()
    }

    pub(super) fn queries(&self) -> &Queries {
        &self.queries
    }

    pub(super) fn propagator(&self) -> &TextMapCompositePropagator {
        self.store.propagator()
    }

    pub(super) fn calculate_ttl(&self, time: CompactDateTime) -> Option<i32> {
        self.store.calculate_ttl(time)
    }

    /// Helper to execute a query conditionally based on TTL.
    ///
    /// Executes `query_with_ttl` if TTL is available, otherwise executes
    /// `query_no_ttl`. The `params_with_ttl` builder receives the TTL value.
    pub(super) async fn execute_with_optional_ttl<P1, P2>(
        &self,
        time: CompactDateTime,
        query_with_ttl: &PreparedStatement,
        query_no_ttl: &PreparedStatement,
        params_with_ttl: impl FnOnce(i32) -> P1,
        params_no_ttl: impl FnOnce() -> P2,
    ) -> Result<(), CassandraTriggerStoreError>
    where
        P1: SerializeRow,
        P2: SerializeRow,
    {
        match self.calculate_ttl(time) {
            Some(ttl) => {
                self.session()
                    .execute_unpaged(query_with_ttl, params_with_ttl(ttl))
                    .await
                    .map_err(CassandraStoreError::from)?;
            }
            None => {
                self.session()
                    .execute_unpaged(query_no_ttl, params_no_ttl())
                    .await
                    .map_err(CassandraStoreError::from)?;
            }
        }
        Ok(())
    }

    /// Executes an unpaged query and discards the result.
    ///
    /// Convenience wrapper for fire-and-forget mutations that only need
    /// error propagation.
    pub(super) async fn execute_unpaged_discard(
        &self,
        query: &PreparedStatement,
        params: impl SerializeRow,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.session()
            .execute_unpaged(query, params)
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }

    /// Creates V1 operations on-demand for migration support.
    ///
    /// This method constructs a `V1Operations` instance that provides access
    /// to V1 schema operations (reading/writing data without `timer_type`).
    /// Only used during V1→V2 migration.
    pub(crate) fn v1(&self) -> v1::V1Operations {
        v1::V1Operations::new(self.store.clone(), Arc::clone(&self.queries))
    }

    /// Reads a segment from the database without applying any migrations.
    ///
    /// This is an internal helper used during migration to reload segments
    /// after version or slab size updates, avoiding infinite recursion.
    pub(super) async fn get_segment_unchecked(
        &self,
        segment_id: &SegmentId,
    ) -> Result<Option<Segment>, CassandraTriggerStoreError> {
        let row = self
            .session()
            .execute_unpaged(&self.queries().get_segment, (segment_id,))
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(String, CompactDuration, Option<SegmentVersion>)>()
            .map_err(CassandraStoreError::from)?;

        let Some((name, slab_size, version)) = row else {
            return Ok(None);
        };

        let version = version.unwrap_or(SegmentVersion::V1);

        Ok(Some(Segment {
            id: *segment_id,
            name,
            slab_size,
            version,
        }))
    }
}
