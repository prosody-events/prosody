//! Property tests asserting the two `next_offset` storage invariants after
//! every operation against a live Cassandra cluster, with a reference model
//! as the oracle:
//!
//! - **I1** — `next_offset` equals the minimum live offset for the key
//!   (`next_offset` strictly encodes the minimum live row).
//! - **no-orphan** — `next_offset` is present iff live rows are present. When
//!   the model says a key has no logical state, the Cassandra partition must be
//!   empty: no clustering rows, no static `next_offset`, no static
//!   `retry_count`.

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
    let segment_store = CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
    let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
    let segment = LazySegment::new(
        segment_store,
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
    );
    Ok(CassandraMessageDeferStore::new(
        cassandra_store,
        queries,
        segment,
        1_024,
    ))
}

/// Drives a random operation sequence through the store and the reference
/// model, asserting I1 and the no-orphan invariant after each operation. See
/// the module docs for the invariant definitions.
#[test]
fn test_defer_store_i1_and_no_orphan_invariants() {
    init_test_logging();
    let _span = tracing::info_span!("test_i1_no_orphan").entered();
    QuickCheck::new().quickcheck(prop_i1_no_orphan as fn(DeferTestInput) -> TestResult);
}

fn prop_i1_no_orphan(input: DeferTestInput) -> TestResult {
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
            let mut pending_legacy: ahash::HashSet<Key> = ahash::HashSet::default();
            for (op_idx, op) in input.operations.iter().enumerate() {
                let key_index = key_index_of(op);
                let key = input.key_components[key_index].key.clone();

                model.apply(op, &input.key_components);
                apply_op(&store, op, &input.key_components).await?;

                // Legacy partitions seeded with clustering rows but no
                // `next_offset` only get repaired lazily, the first time an op
                // reaches the read path. Until then I1 cannot hold (`next_offset`
                // is still NULL), so we hold the key pending and skip its checks.
                if matches!(op, DeferOperation::SeedLegacy { .. }) {
                    pending_legacy.insert(key);
                    continue;
                }

                // Any op after `SeedLegacy` reaches the store's read path and
                // triggers the lazy repair that synthesizes `next_offset` —
                // the two retry-only ops included, since setting `retry_count`
                // first resolves the static columns. So the key's legacy NULL
                // state is cleared and its invariants can be checked from here.
                pending_legacy.remove(&key);

                // I1: the static `next_offset` equals the model minimum.
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

                // no-orphan: if the model says the key has no logical state,
                // the Cassandra partition must also be empty — no clustering
                // rows, no static `next_offset`, no static `retry_count`.
                if model.has_no_state(&key) {
                    let (has_clustering, next_set, rc_set) = store
                        .read_partition_liveness_for_invariant_check(&key)
                        .await
                        .map_err(|e| color_eyre::eyre::eyre!("op #{op_idx}: {e}"))?;
                    if has_clustering || next_set || rc_set {
                        return Err(color_eyre::eyre::eyre!(
                            "no-orphan after op #{op_idx} key={key}: model empty but DB has \
                             clustering={has_clustering} next_offset={next_set} \
                             retry_count={rc_set}"
                        ));
                    }
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
) -> color_eyre::Result<()> {
    match op {
        DeferOperation::GetNext(i) => {
            store.get_next_deferred_message(&kcs[*i].key).await?;
        }
        DeferOperation::IsDeferred(i) => {
            store.is_deferred(&kcs[*i].key).await?;
        }
        DeferOperation::DeferFirst { key_index, offset } => {
            store
                .defer_first_message(&kcs[*key_index].key, *offset)
                .await?;
        }
        DeferOperation::DeferAdditional { key_index, offset } => {
            store
                .defer_additional_message(&kcs[*key_index].key, *offset)
                .await?;
        }
        DeferOperation::CompleteRetrySuccess { key_index, offset } => {
            store
                .complete_retry_success(&kcs[*key_index].key, *offset)
                .await?;
        }
        DeferOperation::IncrementRetryCount {
            key_index,
            current_retry_count,
        } => {
            store
                .increment_retry_count(&kcs[*key_index].key, *current_retry_count)
                .await?;
        }
        DeferOperation::Append { key_index, offset } => {
            store
                .append_deferred_message(&kcs[*key_index].key, *offset)
                .await?;
        }
        DeferOperation::Remove { key_index, offset } => {
            store
                .remove_deferred_message(&kcs[*key_index].key, *offset)
                .await?;
        }
        DeferOperation::SetRetryCount {
            key_index,
            retry_count,
        } => {
            store
                .set_retry_count(&kcs[*key_index].key, *retry_count)
                .await?;
        }
        DeferOperation::DeleteKey(i) => {
            store.delete_key(&kcs[*i].key).await?;
        }
        DeferOperation::SeedLegacy {
            key_index,
            clustering_offsets,
            retry_count,
        } => {
            store
                .seed_legacy_for_test(&kcs[*key_index].key, clustering_offsets, *retry_count)
                .await?;
        }
    }
    Ok(())
}
