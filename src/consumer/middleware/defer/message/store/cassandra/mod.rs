//! Cassandra-backed message defer store with internal write-through cache.
//!
//! Eliminates tombstone reads by maintaining a `next_offset` static column that
//! always equals the minimum live offset. `get_next` becomes a single
//! static-column read with zero clustering scan.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::message::store::cassandra::queries::Queries;
use crate::consumer::middleware::defer::message::store::provider::MessageDeferStoreProvider;
use crate::consumer::middleware::defer::message::store::{
    MessageDeferStore, MessageRetryCompletionResult,
};
use crate::consumer::middleware::defer::segment::{CassandraSegmentStore, LazySegment};
use crate::{ConsumerGroup, Key, Offset, Partition, Topic};
use quick_cache::sync::Cache;
use scylla::client::session::Session;
use std::fmt;
use std::sync::Arc;
use tracing::instrument;

pub mod queries;

pub use queries::Queries as MessageQueries;

const DEFER_CACHE_CAPACITY: usize = 8_192;

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
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment_store: CassandraSegmentStore,
        topic: Topic,
        partition: Partition,
        consumer_group: ConsumerGroup,
    ) -> Self {
        let segment = LazySegment::new(segment_store, topic, partition, consumer_group);
        Self {
            store,
            queries,
            segment,
            cache: Arc::new(Cache::new(DEFER_CACHE_CAPACITY)),
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
            .maybe_first_row::<(Option<Offset>, Option<i32>)>()
            .map_err(CassandraStoreError::from)?;

        let Some((Some(min_offset), _)) = row_opt else {
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
            .maybe_first_row::<(Option<Offset>, Option<i32>)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row_opt.and_then(|(offset_opt, _)| offset_opt))
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
        let ttl = self.store.base_ttl();
        let new_next = cached.map_or(offset, |(cur_next, _)| cur_next.min(offset));

        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_message_with_retry_count,
                (&segment_id, key.as_ref(), offset, 0_i32, new_next, ttl),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        self.cache.insert(Arc::clone(key), Some((new_next, 0)));

        Ok(())
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

        // Cache miss: single static-column read, zero clustering scan
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
                // Empty partition: INSERT + initialize next_offset in one BATCH so
                // I1 holds from the first row. `retry_count` is untouched — it may
                // already hold an orphan value from a prior `set_retry_count` on
                // this key, which we must preserve. Invalidate the cache so the
                // next read picks up the real static-column values from the DB.
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

                let _ = self.cache.remove(key.as_ref());
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
        let segment_id = self.segment_id().await?;
        let ttl = self.store.base_ttl();
        let retry_count_i32: i32 = retry_count.try_into().unwrap_or(i32::MAX);

        self.session()
            .execute_unpaged(
                &self.queries.update_retry_count,
                (ttl, retry_count_i32, &segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        // Update retry_count in cache in-place
        if let Some(Some((cur_next, _))) = self.cache.get(key.as_ref()) {
            self.cache
                .insert(Arc::clone(key), Some((cur_next, retry_count)));
        } else {
            self.cache.remove(key.as_ref());
        }

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
    ) -> Self::Store {
        // Each call creates a new store with its own fresh cache.
        // The cache must never outlive or be shared across partition assignments.
        CassandraMessageDeferStore::new(
            self.store.clone(),
            self.queries.clone(),
            self.segment_store.clone(),
            topic,
            partition,
            Arc::from(consumer_group),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cassandra::{CassandraConfiguration, CassandraStore};
    use crate::defer_store_tests;
    use crate::{ConsumerGroup, Partition, Topic};

    defer_store_tests!(async {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

        let cassandra_store = CassandraStore::new(&config).await?;
        let segment_store =
            CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
        let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
        let defer_store = CassandraMessageDeferStore::new(
            cassandra_store,
            queries,
            segment_store,
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
        );
        Ok::<_, color_eyre::Report>(defer_store)
    });
}

/// Invariant tests: directly assert I1 (`next_offset` == model minimum) after
/// every operation in a property-generated sequence.
#[cfg(test)]
mod invariant_tests {
    use super::*;
    use crate::cassandra::{CassandraConfiguration, CassandraStore};
    use crate::consumer::middleware::defer::message::store::tests::prop_defer_store::{
        DeferModel, DeferOperation, DeferTestInput, TestKeyComponents,
    };
    use crate::tracing::init_test_logging;
    use crate::{ConsumerGroup, Partition, Topic};
    use quickcheck::{QuickCheck, TestResult};
    use tokio::runtime::Builder;
    use tracing::Instrument;

    async fn build_store() -> color_eyre::Result<CassandraMessageDeferStore> {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;
        let cassandra_store = CassandraStore::new(&config).await?;
        let segment_store =
            CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
        let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
        Ok(CassandraMessageDeferStore::new(
            cassandra_store,
            queries,
            segment_store,
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
        ))
    }

    /// Asserts I1: DB `next_offset` == model minimum live offset after each op.
    #[test]
    fn test_defer_store_i1_invariant() {
        init_test_logging();
        let _span = tracing::info_span!("test_i1").entered();
        QuickCheck::new().quickcheck(prop_i1 as fn(DeferTestInput) -> TestResult);
    }

    fn prop_i1(input: DeferTestInput) -> TestResult {
        let span = tracing::Span::current();
        let runtime = match Builder::new_multi_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => return TestResult::error(format!("Runtime: {e}")),
        };
        let store = match runtime.block_on(build_store().instrument(span.clone())) {
            Ok(s) => s,
            Err(e) => return TestResult::error(format!("Store: {e}")),
        };
        let input_dbg = format!("{input:#?}");
        match runtime.block_on(
            async move {
                let mut model = DeferModel::new();
                // Keys that were SeedLegacy-written but have not yet had a
                // read trigger the repair UPDATE. I1 is intentionally
                // violated for these keys until the next op that reaches
                // `resolve_cache_or_read` (reads + most mutators). Ops that
                // bypass the read path (SetRetryCount, IncrementRetryCount)
                // leave the partition legacy until a future op touches it.
                let mut pending_legacy: ahash::HashSet<Key> = ahash::HashSet::default();
                for (op_idx, op) in input.operations.iter().enumerate() {
                    let key_index = key_index_of(op);
                    let key = input.key_components[key_index].key.clone();

                    model.apply(op, &input.key_components);
                    apply_op(&store, op, &input.key_components).await;

                    if matches!(op, DeferOperation::SeedLegacy { .. }) {
                        pending_legacy.insert(key);
                        continue;
                    }

                    if pending_legacy.contains(&key) {
                        if matches!(
                            op,
                            DeferOperation::SetRetryCount { .. }
                                | DeferOperation::IncrementRetryCount { .. }
                        ) {
                            // No read path touched; legacy state persists.
                            continue;
                        }
                        // All other ops either trigger `read_next_static`
                        // (repair) or wipe the partition (DeleteKey).
                        pending_legacy.remove(&key);
                    }

                    let db_next = store
                        .read_next_offset_for_invariant_check(&key)
                        .await
                        .map_err(|e| color_eyre::eyre::eyre!("op #{op_idx}: {e}"))?;
                    let model_min = model.get_next(&key).map(|(o, _)| o);

                    if db_next != model_min {
                        return Err(color_eyre::eyre::eyre!(
                            "I1 after op #{op_idx} key={key}: db={db_next:?} model={model_min:?}"
                        ));
                    }
                }
                Ok::<_, color_eyre::Report>(())
            }
            .instrument(span),
        ) {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(format!("{e}\nFailing input:\n{input_dbg}")),
        }
    }

    fn key_index_of(op: &DeferOperation) -> usize {
        match op {
            DeferOperation::GetNext(i)
            | DeferOperation::IsDeferred(i)
            | DeferOperation::DeleteKey(i) => *i,
            DeferOperation::DeferFirst { key_index, .. }
            | DeferOperation::DeferAdditional { key_index, .. }
            | DeferOperation::CompleteRetrySuccess { key_index, .. }
            | DeferOperation::IncrementRetryCount { key_index, .. }
            | DeferOperation::Append { key_index, .. }
            | DeferOperation::Remove { key_index, .. }
            | DeferOperation::SetRetryCount { key_index, .. }
            | DeferOperation::SeedLegacy { key_index, .. } => *key_index,
        }
    }

    async fn apply_op(
        store: &CassandraMessageDeferStore,
        op: &DeferOperation,
        kcs: &[TestKeyComponents],
    ) {
        match op {
            DeferOperation::GetNext(i) => {
                let _ = store.get_next_deferred_message(&kcs[*i].key).await;
            }
            DeferOperation::IsDeferred(i) => {
                let _ = store.is_deferred(&kcs[*i].key).await;
            }
            DeferOperation::DeferFirst { key_index, offset } => {
                let _ = store
                    .defer_first_message(&kcs[*key_index].key, *offset)
                    .await;
            }
            DeferOperation::DeferAdditional { key_index, offset } => {
                let _ = store
                    .defer_additional_message(&kcs[*key_index].key, *offset)
                    .await;
            }
            DeferOperation::CompleteRetrySuccess { key_index, offset } => {
                let _ = store
                    .complete_retry_success(&kcs[*key_index].key, *offset)
                    .await;
            }
            DeferOperation::IncrementRetryCount {
                key_index,
                current_retry_count,
            } => {
                let _ = store
                    .increment_retry_count(&kcs[*key_index].key, *current_retry_count)
                    .await;
            }
            DeferOperation::Append { key_index, offset } => {
                let _ = store
                    .append_deferred_message(&kcs[*key_index].key, *offset)
                    .await;
            }
            DeferOperation::Remove { key_index, offset } => {
                let _ = store
                    .remove_deferred_message(&kcs[*key_index].key, *offset)
                    .await;
            }
            DeferOperation::SetRetryCount {
                key_index,
                retry_count,
            } => {
                let _ = store
                    .set_retry_count(&kcs[*key_index].key, *retry_count)
                    .await;
            }
            DeferOperation::DeleteKey(i) => {
                let _ = store.delete_key(&kcs[*i].key).await;
            }
            DeferOperation::SeedLegacy {
                key_index,
                clustering_offsets,
                retry_count,
            } => {
                let _ = store
                    .seed_legacy_for_test(&kcs[*key_index].key, clustering_offsets, *retry_count)
                    .await;
            }
        }
    }
}

/// Deterministic unit tests for the lazy on-read repair path that fires when
/// a pre-migration partition (clustering rows, `next_offset = NULL`) is read.
#[cfg(test)]
mod legacy_repair_tests {
    use super::*;
    use crate::cassandra::{CassandraConfiguration, CassandraStore};
    use crate::{ConsumerGroup, Partition, Topic};

    async fn build_store() -> color_eyre::Result<CassandraMessageDeferStore> {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;
        let cassandra_store = CassandraStore::new(&config).await?;
        let segment_store =
            CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
        let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
        Ok(CassandraMessageDeferStore::new(
            cassandra_store,
            queries,
            segment_store,
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
        ))
    }

    fn key() -> Key {
        Arc::from(format!("legacy-key-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn test_legacy_get_next_repairs() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(10_i64)], Some(2))
            .await?;

        let got = store.get_next_deferred_message(&k).await?;
        assert_eq!(got, Some((Offset::from(10_i64), 2)));

        // Next-offset static column is populated after the repair UPDATE.
        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, Some(Offset::from(10_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_is_deferred_repairs() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(7_i64)], Some(1))
            .await?;

        let rc = store.is_deferred(&k).await?;
        assert_eq!(rc, Some(1));

        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, Some(Offset::from(7_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_defer_first_on_legacy_partition() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(5_i64)], Some(3))
            .await?;

        // A fresh-key precondition is already violated on legacy partitions;
        // the store must still converge `next_offset` to the true minimum.
        store.defer_first_message(&k, Offset::from(20_i64)).await?;

        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, Some(Offset::from(5_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_append_on_legacy_partition() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(9_i64)], Some(0))
            .await?;

        store
            .append_deferred_message(&k, Offset::from(3_i64))
            .await?;

        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, Some(Offset::from(3_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_complete_retry_at_min() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(5_i64), Offset::from(10_i64)], Some(2))
            .await?;

        let result = store
            .complete_retry_success(&k, Offset::from(5_i64))
            .await?;
        assert!(matches!(
            result,
            MessageRetryCompletionResult::MoreMessages { next_offset } if next_offset == Offset::from(10_i64)
        ));

        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, Some(Offset::from(10_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_complete_retry_above_min() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(5_i64), Offset::from(10_i64)], Some(2))
            .await?;

        let result = store
            .complete_retry_success(&k, Offset::from(10_i64))
            .await?;
        assert!(matches!(
            result,
            MessageRetryCompletionResult::MoreMessages { next_offset } if next_offset == Offset::from(5_i64)
        ));

        // Repair advanced next_offset to 5 on the initial read; completing a
        // non-min offset leaves next_offset anchored at the true minimum.
        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, Some(Offset::from(5_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_orphan_retry_count_only() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        // No clustering rows — only the static retry_count. This matches a
        // post-migration orphan (`set_retry_count` on a fresh key).
        store.seed_legacy_for_test(&k, &[], Some(4)).await?;

        let got = store.get_next_deferred_message(&k).await?;
        assert_eq!(got, None);

        // No bogus repair UPDATE was issued; next_offset remains NULL.
        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_repair_idempotent() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(11_i64)], Some(5))
            .await?;

        let first = store.get_next_deferred_message(&k).await?;
        assert_eq!(first, Some((Offset::from(11_i64), 5)));

        // Cache is populated after the first call; second call must not
        // re-probe or re-repair. We verify by asserting the cache now holds
        // the synthesized entry — a subsequent read served from cache.
        assert_eq!(
            store.cache.get(k.as_ref()),
            Some(Some((Offset::from(11_i64), 5)))
        );

        let second = store.get_next_deferred_message(&k).await?;
        assert_eq!(second, Some((Offset::from(11_i64), 5)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_remove_at_min() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(5_i64), Offset::from(10_i64)], Some(1))
            .await?;

        store
            .remove_deferred_message(&k, Offset::from(5_i64))
            .await?;

        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, Some(Offset::from(10_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_remove_above_min() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(5_i64), Offset::from(10_i64)], Some(1))
            .await?;

        store
            .remove_deferred_message(&k, Offset::from(10_i64))
            .await?;

        // Repair anchored next_offset at the true minimum; removing a
        // non-min clustering row leaves that anchor unchanged.
        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, Some(Offset::from(5_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_delete_key_wipes_partition() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(5_i64), Offset::from(10_i64)], Some(3))
            .await?;

        // delete_key bypasses resolve_cache_or_read, so repair never fires;
        // the partition must still be wiped cleanly.
        store.delete_key(&k).await?;

        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, None);
        // A subsequent read must agree: no partition row, not an ambiguous
        // legacy state the next reader would probe.
        let got = store.get_next_deferred_message(&k).await?;
        assert_eq!(got, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_set_retry_count_preserves_legacy_then_repairs() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        store
            .seed_legacy_for_test(&k, &[Offset::from(7_i64)], Some(2))
            .await?;

        // set_retry_count does not read next_offset, so the legacy state
        // survives this op: DB still has next_offset = NULL.
        store.set_retry_count(&k, 9).await?;
        let db_next_after_set = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next_after_set, None);

        // The first read path *after* set_retry_count must still trigger
        // repair, observing the updated retry_count.
        let got = store.get_next_deferred_message(&k).await?;
        assert_eq!(got, Some((Offset::from(7_i64), 9)));

        let db_next_after_read = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next_after_read, Some(Offset::from(7_i64)));
        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_none_none_returns_none_without_repair() -> color_eyre::Result<()> {
        let store = build_store().await?;
        let k = key();
        // Seed clustering rows but no static retry_count — produces the
        // (next_offset=NULL, retry_count=NULL) state the plan deliberately
        // treats as "truly empty" to avoid probing every empty partition.
        store
            .seed_legacy_for_test(&k, &[Offset::from(4_i64)], None)
            .await?;

        let got = store.get_next_deferred_message(&k).await?;
        assert_eq!(got, None);

        // No repair UPDATE was issued; next_offset stays NULL. Cache is
        // populated with Some(None) so subsequent reads stay on the fast path.
        let db_next = store.read_next_offset_for_invariant_check(&k).await?;
        assert_eq!(db_next, None);
        assert_eq!(store.cache.get(k.as_ref()), Some(None));
        Ok(())
    }
}
