//! Cassandra-backed message defer store with internal write-through cache.
//!
//! Eliminates tombstone reads by maintaining a `next_offset` static column that
//! always equals the minimum live offset. `get_next` becomes a single
//! static-column read that resolves on the live tail of the partition
//! (`ORDER BY offset DESC LIMIT 1`), skipping the FIFO tombstone graveyard at
//! low `offset`.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::message::store::cassandra::queries::Queries;
use crate::consumer::middleware::defer::message::store::provider::MessageDeferStoreProvider;
use crate::consumer::middleware::defer::message::store::{
    MessageDeferStore, MessageRetryCompletionResult,
};
use crate::consumer::middleware::defer::segment::{CassandraSegmentStore, LazySegment};
use crate::{Key, Offset, Partition, Topic};
use quick_cache::sync::Cache;
use scylla::client::session::Session;
use std::fmt;
use std::sync::Arc;
use tracing::{debug, instrument};

pub mod queries;

pub use queries::Queries as MessageQueries;

/// Cassandra-backed message defer store with internal write-through cache.
///
/// # Storage Model
///
/// - **Partition key**: `(segment_id, key)` where segment = `UUIDv5` of
///   `{topic}/{partition}:{consumer_group}`
/// - **Clustering**: `offset ASC` for FIFO ordering
/// - **Static columns**: `next_offset bigint`, `retry_count int` (shared per
///   key)
/// - **TTL**: Fixed duration from [`CassandraStore::base_ttl()`]
///
/// `next_offset` strictly equals the minimum live offset (I1). Every mutating
/// path either keeps it valid or repairs it in the same UNLOGGED BATCH.
#[derive(Clone)]
pub struct CassandraMessageDeferStore {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment: LazySegment<CassandraSegmentStore>,
    /// Write-through cache: `key → Option<(next_offset, retry_count)>`.
    /// `Some(None)` = known-empty; `Some(Some(_))` = live; `None` = unknown.
    cache: Arc<Cache<Key, Option<(Offset, u32)>>>,
}

impl CassandraMessageDeferStore {
    /// Creates a store; segment persisted lazily on first access.
    ///
    /// `cache_size` sizes the internal write-through
    /// `quick_cache::sync::Cache`.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment: LazySegment<CassandraSegmentStore>,
        cache_size: usize,
    ) -> Self {
        Self {
            store,
            queries,
            segment,
            cache: Arc::new(Cache::new(cache_size)),
        }
    }

    fn session(&self) -> &Session {
        self.store.session()
    }

    async fn segment_id(&self) -> Result<uuid::Uuid, CassandraDeferStoreError> {
        let segment = self.segment.get().await?;
        Ok(segment.id())
    }

    /// Reads `(next_offset, retry_count)` static columns from the DB.
    /// Returns `None` if the partition is empty.
    ///
    /// Handles legacy partitions (written before the `next_offset` static
    /// column existed): when `next_offset = NULL` but `retry_count != NULL`,
    /// the partition is ambiguous — it could be post-migration orphan
    /// `retry_count` (empty partition) or pre-migration clustering rows with
    /// no cached hint. A `probe_min` disambiguates, and on a hit we fire a
    /// `repair_next_offset` UPDATE so subsequent reads take the fast path.
    async fn read_next_static(
        &self,
        segment_id: &uuid::Uuid,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, CassandraDeferStoreError> {
        let result = self
            .session()
            .execute_unpaged(&self.queries.get_next_static, (segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = result
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<Offset>, Option<i32>)>()
            .map_err(CassandraStoreError::from)?;

        match row_opt {
            None | Some((None, None)) => Ok(None),
            Some((Some(offset), retry_opt)) => {
                let retry_count = retry_opt.and_then(|c| c.try_into().ok()).unwrap_or(0);
                Ok(Some((offset, retry_count)))
            }
            Some((None, Some(rc_raw))) => {
                self.repair_legacy_partition(segment_id, key, rc_raw).await
            }
        }
    }

    /// Lazy on-read repair of a legacy partition whose `next_offset` was never
    /// populated. Probes for the minimum live clustering row; on a hit, writes
    /// it back to `next_offset` via an LWW UPDATE and returns the synthesized
    /// entry. On a miss the partition is truly empty (orphan `retry_count`),
    /// and no repair is issued.
    async fn repair_legacy_partition(
        &self,
        segment_id: &uuid::Uuid,
        key: &Key,
        raw_retry_count: i32,
    ) -> Result<Option<(Offset, u32)>, CassandraDeferStoreError> {
        let probe = self
            .session()
            .execute_unpaged(&self.queries.probe_min, (segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = probe
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<Offset>,)>()
            .map_err(CassandraStoreError::from)?;

        let Some((Some(min_offset),)) = row_opt else {
            // No clustering row exists — orphan `retry_count`. `next_offset =
            // NULL` is already the correct state; do not issue a repair.
            return Ok(None);
        };

        let ttl = self.store.base_ttl();
        self.session()
            .execute_unpaged(
                &self.queries.repair_next_offset,
                (ttl, min_offset, segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        let retry_count: u32 = raw_retry_count.try_into().unwrap_or(0);
        Ok(Some((min_offset, retry_count)))
    }

    /// Probes for the first clustering row strictly after `after_offset`.
    /// Returns `None` if no such row exists.
    async fn probe_next(
        &self,
        segment_id: &uuid::Uuid,
        key: &Key,
        after_offset: Offset,
    ) -> Result<Option<Offset>, CassandraDeferStoreError> {
        let result = self
            .session()
            .execute_unpaged(
                &self.queries.probe_next,
                (segment_id, key.as_ref(), after_offset),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = result
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<Offset>,)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row_opt.and_then(|(offset_opt,)| offset_opt))
    }

    /// Resolves `cur_next` from cache; falls back to a static-column DB read on
    /// miss. Returns `(segment_id, cur_next_opt, cur_rc)`.
    async fn resolve_cache_or_read(
        &self,
        key: &Key,
    ) -> Result<(uuid::Uuid, Option<(Offset, u32)>), CassandraDeferStoreError> {
        let segment_id = self.segment_id().await?;
        if let Some(cached) = self.cache.get(key.as_ref()) {
            return Ok((segment_id, cached));
        }
        let db_val = self.read_next_static(&segment_id, key).await?;
        Ok((segment_id, db_val))
    }

    /// First-deferral INSERT given pre-resolved partition state. Writes the
    /// clustering row, `next_offset = min(offset, cur_next)`, and
    /// `retry_count = 0` in a single statement, atomically wiping any orphan
    /// static left by a prior `set_retry_count`. Shared by
    /// `defer_first_message` and the empty-partition recovery branch of
    /// `append_deferred_message`.
    async fn defer_first_message_resolved(
        &self,
        segment_id: &uuid::Uuid,
        key: &Key,
        offset: Offset,
        cached: Option<(Offset, u32)>,
    ) -> Result<(), CassandraDeferStoreError> {
        let ttl = self.store.base_ttl();
        let new_next = cached.map_or(offset, |(cur_next, _)| cur_next.min(offset));

        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_message_with_retry_count,
                (segment_id, key.as_ref(), offset, 0_i32, new_next, ttl),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        self.cache.insert(Arc::clone(key), Some((new_next, 0)));

        Ok(())
    }
}

impl fmt::Debug for CassandraMessageDeferStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraMessageDeferStore")
            .field("segment", &self.segment)
            .finish_non_exhaustive()
    }
}

impl MessageDeferStore for CassandraMessageDeferStore {
    type Error = CassandraDeferStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        // Consult current state so we don't violate I1 when the caller's
        // "fresh key" precondition is violated: the new row must not raise
        // `next_offset` above a lower live offset that already exists.
        let (segment_id, cached) = self.resolve_cache_or_read(key).await?;
        self.defer_first_message_resolved(&segment_id, key, offset, cached)
            .await
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        // Cache hit
        if let Some(cached) = self.cache.get(key.as_ref()) {
            return Ok(cached);
        }

        // Cache miss: single static-column read, reverse-scan skips the
        // FIFO tombstone graveyard at low `offset`.
        let segment_id = self.segment_id().await?;
        let result = self.read_next_static(&segment_id, key).await?;
        self.cache.insert(Arc::clone(key), result);
        Ok(result)
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn complete_retry_success(
        &self,
        key: &Key,
        offset: Offset,
    ) -> Result<MessageRetryCompletionResult, Self::Error> {
        let (segment_id, cached) = self.resolve_cache_or_read(key).await?;
        let ttl = self.store.base_ttl();

        // cached: Option<(Offset, u32)> — None = no live offsets. Still need
        // to clean up the partition because `retry_count` may hold an orphan
        // value from a prior `set_retry_count`; leaving it leaks state that
        // differs from the memory store / model.
        let Some((cur_next, _cur_rc)) = cached else {
            self.delete_key(key).await?;
            return Ok(MessageRetryCompletionResult::Completed);
        };

        if cur_next == offset {
            // FIFO hot path: probe for the successor before deleting
            let found_next = self.probe_next(&segment_id, key, offset).await?;
            if let Some(next_offset) = found_next {
                self.session()
                    .execute_unpaged(
                        &self.queries.batch_complete_retry,
                        (
                            &segment_id,
                            key.as_ref(),
                            offset,
                            ttl,
                            next_offset,
                            &segment_id,
                            key.as_ref(),
                        ),
                    )
                    .await
                    .map_err(CassandraStoreError::from)?;

                self.cache.insert(Arc::clone(key), Some((next_offset, 0)));
                Ok(MessageRetryCompletionResult::MoreMessages { next_offset })
            } else {
                self.delete_key(key).await?;
                Ok(MessageRetryCompletionResult::Completed)
            }
        } else {
            // Non-FIFO path: offset is not the minimum; leave next_offset alone
            self.session()
                .execute_unpaged(
                    &self.queries.batch_complete_retry_no_advance,
                    (
                        &segment_id,
                        key.as_ref(),
                        offset,
                        ttl,
                        &segment_id,
                        key.as_ref(),
                    ),
                )
                .await
                .map_err(CassandraStoreError::from)?;

            self.cache.insert(Arc::clone(key), Some((cur_next, 0)));
            Ok(MessageRetryCompletionResult::MoreMessages {
                next_offset: cur_next,
            })
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn append_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        let (segment_id, cached) = self.resolve_cache_or_read(key).await?;
        let ttl = self.store.base_ttl();

        // cached: Option<(Offset, u32)>
        match cached {
            None => {
                // Empty partition: precondition violation (caller should have
                // used defer_first_message). Recover via the shared
                // first-deferral INSERT, which writes retry_count = 0 and so
                // wipes any orphan static left by a prior set_retry_count.
                self.defer_first_message_resolved(&segment_id, key, offset, None)
                    .await?;
            }
            Some((cur_next, cur_rc)) if offset < cur_next => {
                // Out-of-order: lower next_offset in the same BATCH as the INSERT
                self.session()
                    .execute_unpaged(
                        &self.queries.batch_append_with_next,
                        (
                            &segment_id,
                            key.as_ref(),
                            offset,
                            ttl,
                            ttl,
                            offset,
                            &segment_id,
                            key.as_ref(),
                        ),
                    )
                    .await
                    .map_err(CassandraStoreError::from)?;

                self.cache.insert(Arc::clone(key), Some((offset, cur_rc)));
            }
            Some(_) => {
                // Monotonic append: cur_next remains the minimum, just INSERT
                self.session()
                    .execute_unpaged(
                        &self.queries.insert_deferred_message_without_retry_count,
                        (&segment_id, key.as_ref(), offset, ttl),
                    )
                    .await
                    .map_err(CassandraStoreError::from)?;
            }
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        let (segment_id, cached) = self.resolve_cache_or_read(key).await?;
        let ttl = self.store.base_ttl();

        // cached: Option<(Offset, u32)>
        let cur_next = cached.map(|(o, _)| o);

        if cur_next == Some(offset) {
            let found_next = self.probe_next(&segment_id, key, offset).await?;
            match found_next {
                Some(next_offset) => {
                    self.session()
                        .execute_unpaged(
                            &self.queries.batch_remove_and_repair_next,
                            (
                                &segment_id,
                                key.as_ref(),
                                offset,
                                ttl,
                                next_offset,
                                &segment_id,
                                key.as_ref(),
                            ),
                        )
                        .await
                        .map_err(CassandraStoreError::from)?;

                    let cur_rc = cached.map_or(0, |(_, rc)| rc);
                    self.cache
                        .insert(Arc::clone(key), Some((next_offset, cur_rc)));
                }
                None => {
                    self.delete_key(key).await?;
                }
            }
        } else {
            self.session()
                .execute_unpaged(
                    &self.queries.remove_deferred_message,
                    (&segment_id, key.as_ref(), offset),
                )
                .await
                .map_err(CassandraStoreError::from)?;
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        // No-op on a partition with no live messages. Cassandra's blind UPDATE
        // upserts a static-only row and leaves an orphan retry_count after
        // all messages have been processed. resolve_cache_or_read also fires
        // legacy repair, so a partition with clustering rows but a NULL
        // next_offset still resolves as Some(_) here.
        let (segment_id, cached) = self.resolve_cache_or_read(key).await?;
        let Some((cur_next, _)) = cached else {
            debug!(key = ?key, "set_retry_count on empty partition: no-op");
            return Ok(());
        };

        let ttl = self.store.base_ttl();
        let retry_count_i32: i32 = retry_count.try_into().unwrap_or(i32::MAX);

        self.session()
            .execute_unpaged(
                &self.queries.update_retry_count,
                (ttl, retry_count_i32, &segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        self.cache
            .insert(Arc::clone(key), Some((cur_next, retry_count)));

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;

        self.session()
            .execute_unpaged(&self.queries.delete_key, (&segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        self.cache.insert(Arc::clone(key), None);

        Ok(())
    }
}

#[cfg(test)]
impl CassandraMessageDeferStore {
    /// Reads `next_offset` directly from Cassandra for I1 invariant assertions.
    async fn read_next_offset_for_invariant_check(
        &self,
        key: &Key,
    ) -> color_eyre::Result<Option<Offset>> {
        let segment_id = self
            .segment_id()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        let result = self
            .session()
            .execute_unpaged(&self.queries.get_next_static, (&segment_id, key.as_ref()))
            .await?;
        let row = result
            .into_rows_result()?
            .maybe_first_row::<(Option<Offset>, Option<i32>)>()?;
        Ok(row.and_then(|(off_opt, _)| off_opt))
    }

    /// Reads the raw partition state for the no-orphan invariant: any
    /// clustering row, the static `next_offset`, and the static
    /// `retry_count`. Returns `(has_clustering, next_offset_set,
    /// retry_count_set)`.
    async fn read_partition_liveness_for_invariant_check(
        &self,
        key: &Key,
    ) -> color_eyre::Result<(bool, bool, bool)> {
        let segment_id = self
            .segment_id()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;

        let static_row = self
            .session()
            .execute_unpaged(&self.queries.get_next_static, (&segment_id, key.as_ref()))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(Option<Offset>, Option<i32>)>()?;
        let (next_set, rc_set) = match static_row {
            None => (false, false),
            Some((off_opt, rc_opt)) => (off_opt.is_some(), rc_opt.is_some()),
        };

        let probe = self
            .session()
            .execute_unpaged(&self.queries.probe_min, (&segment_id, key.as_ref()))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(Option<Offset>,)>()?;
        let has_clustering = matches!(probe, Some((Some(_),)));

        Ok((has_clustering, next_set, rc_set))
    }

    /// Seeds a pre-migration legacy partition for tests: inserts clustering
    /// rows without touching `next_offset`, and optionally sets a static
    /// `retry_count`. Invalidates the cache so subsequent reads hit the DB
    /// and exercise the lazy on-read repair path.
    pub(crate) async fn seed_legacy_for_test(
        &self,
        key: &Key,
        clustering_offsets: &[Offset],
        retry_count: Option<u32>,
    ) -> color_eyre::Result<()> {
        let segment_id = self
            .segment_id()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        let ttl = self.store.base_ttl();

        for &offset in clustering_offsets {
            self.session()
                .execute_unpaged(
                    &self.queries.insert_deferred_message_without_retry_count,
                    (&segment_id, key.as_ref(), offset, ttl),
                )
                .await?;
        }

        if let Some(rc) = retry_count {
            let rc_i32: i32 = rc.try_into().unwrap_or(i32::MAX);
            self.session()
                .execute_unpaged(
                    &self.queries.update_retry_count,
                    (ttl, rc_i32, &segment_id, key.as_ref()),
                )
                .await?;
        }

        let _ = self.cache.remove(key.as_ref());
        Ok(())
    }
}

/// Factory for partition-scoped Cassandra message defer stores.
///
/// Each call to [`create_store`](MessageDeferStoreProvider::create_store)
/// produces a [`CassandraMessageDeferStore`] with its **own independent cache**
/// scoped to that partition's lifetime. The Cassandra session and prepared
/// statements are shared across partitions (cheap, read-only), but the
/// write-through cache is **never** shared across partitions — sharing would
/// cause data corruption (stale offsets for wrong partitions).
#[derive(Clone, Debug)]
pub struct CassandraMessageDeferStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment_store: CassandraSegmentStore,
}

impl CassandraMessageDeferStoreProvider {
    /// Creates a provider with shared Cassandra resources.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment_store: CassandraSegmentStore,
    ) -> Self {
        Self {
            store,
            queries,
            segment_store,
        }
    }
}

impl MessageDeferStoreProvider for CassandraMessageDeferStoreProvider {
    type Store = CassandraMessageDeferStore;

    fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
        cache_size: usize,
    ) -> Self::Store {
        // Each call creates a new store with its own fresh cache.
        // The cache must never outlive or be shared across partition assignments.
        let segment = LazySegment::new(
            self.segment_store.clone(),
            topic,
            partition,
            Arc::from(consumer_group),
        );
        CassandraMessageDeferStore::new(
            self.store.clone(),
            self.queries.clone(),
            segment,
            cache_size,
        )
    }
}

/// Shared cross-store conformance suite (`defer_store_tests!`) run against a
/// live Cassandra-backed store.
#[cfg(test)]
mod tests;

/// Invariant tests: directly assert I1 (`next_offset` == model minimum) after
/// every operation in a property-generated sequence.
#[cfg(test)]
mod invariant_tests;

/// Deterministic unit tests for the lazy on-read repair path that fires when
/// a pre-migration partition (clustering rows, `next_offset = NULL`) is read.
#[cfg(test)]
mod legacy_repair_tests;

/// Regression test for `tombstone_warn_threshold` warnings emitted by
/// `read_next_static` on FIFO-completed partitions.
///
/// The query selects only static columns from `deferred_offsets` with
/// `LIMIT 1`. With no clustering predicate, a forward scan walks the
/// clustering iterator from the bottom up to materialise the static
/// row — straight through the tombstone graveyard FIFO completion
/// leaves at low `offset`. Appending `ORDER BY offset DESC` resolves
/// on the live tail and skips the graveyard entirely.
#[cfg(test)]
mod tombstone_reverse_scan_tests;
