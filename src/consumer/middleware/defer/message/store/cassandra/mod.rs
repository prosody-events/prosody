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

        Ok(row_opt.and_then(|(offset_opt, retry_opt)| {
            offset_opt.map(|offset| {
                let retry_count = retry_opt.and_then(|c| c.try_into().ok()).unwrap_or(0);
                (offset, retry_count)
            })
        }))
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
                for (op_idx, op) in input.operations.iter().enumerate() {
                    let key_index = key_index_of(op);
                    let key = input.key_components[key_index].key.clone();

                    model.apply(op, &input.key_components);
                    apply_op(&store, op, &input.key_components).await;

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
            | DeferOperation::SetRetryCount { key_index, .. } => *key_index,
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
        }
    }
}
