//! Property tests asserting the two `next_timer` storage invariants after
//! every operation against a live Cassandra cluster, with a reference model
//! as the oracle:
//!
//! - **I1** — `next_timer.time` equals the minimum live timer for the key
//!   (`next_timer` strictly encodes the minimum live row).
//! - **I4 (no-orphan)** — `next_timer` is present iff live rows are present.
//!   When the model says a key has no logical state, the Cassandra partition
//!   must be empty: no clustering rows, no static `next_timer`, no static
//!   `retry_count`.

use super::*;
use crate::consumer::middleware::defer::timer::store::tests::prop_timer_defer_store::{
    TestKeyComponents, TimerDeferModel, TimerDeferOperation, TimerDeferTestInput,
};
use crate::tracing::init_test_logging;
use quickcheck::{QuickCheck, TestResult};
use tokio::runtime::Builder;
use tracing::Instrument;

/// Drives a random operation sequence through the store and the reference
/// model, asserting I1 and I4 after each operation. See the module docs for
/// the invariant definitions.
#[test]
fn test_timer_defer_store_i1_i4_invariant() {
    init_test_logging();
    let _span = tracing::info_span!("test_i1_i4").entered();
    QuickCheck::new().quickcheck(prop_i1_i4 as fn(TimerDeferTestInput) -> TestResult);
}

fn prop_i1_i4(input: TimerDeferTestInput) -> TestResult {
    let span = tracing::Span::current();
    let runtime = match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => return TestResult::error(format!("Runtime: {e}")),
    };
    let store = match runtime.block_on(build_test_store().instrument(span.clone())) {
        Ok(s) => s,
        Err(e) => return TestResult::error(format!("Store: {e}")),
    };
    let input_dbg = format!("{input:#?}");
    match runtime.block_on(
        async move {
            let mut model = TimerDeferModel::new();
            let mut pending_legacy: ahash::HashSet<Key> = ahash::HashSet::default();
            for (op_idx, op) in input.operations.iter().enumerate() {
                let key_index = key_index_of(op);
                let key = input.key_components[key_index].key.clone();

                model.apply(op, &input.key_components);
                apply_op(&store, op, &input.key_components).await?;

                // Legacy partitions seeded with clustering rows but no
                // `next_timer` only get repaired lazily, the first time an op
                // touches them. Until then I1 cannot hold (`next_timer` is
                // still NULL), so we hold the key pending and skip its checks.
                if matches!(op, TimerDeferOperation::SeedLegacy { .. }) {
                    pending_legacy.insert(key);
                    continue;
                }

                if pending_legacy.contains(&key) {
                    // The two retry-only ops mutate the static `retry_count`
                    // alone; they never recompute `next_timer`, so they do not
                    // trigger the lazy repair and the key stays pending. Every
                    // other op recomputes or repairs `next_timer`, clearing the
                    // pending mark so the invariants can be checked from here on.
                    if matches!(
                        op,
                        TimerDeferOperation::SetRetryCount { .. }
                            | TimerDeferOperation::IncrementRetryCount { .. }
                    ) {
                        continue;
                    }
                    pending_legacy.remove(&key);
                }

                // I1: the static `next_timer.time` equals the model minimum.
                let db_next = store
                    .read_next_timer_for_invariant_check(&key)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("op #{op_idx}: {e}"))?;
                let model_min = model.get_next(&key).map(|(t, _)| t);

                let db_time = db_next.as_ref().map(|(t, _)| *t);
                if db_time != model_min {
                    return Err(color_eyre::eyre::eyre!(
                        "I1 after op #{op_idx} key={key}: db={db_time:?} model={model_min:?}"
                    ));
                }

                // I4 (no-orphan): if the model says the key has no logical
                // state, the Cassandra partition must also be empty — no
                // clustering rows, no static `next_timer`, no static
                // `retry_count`.
                if model.has_no_state(&key) {
                    let (has_clustering, next_set, rc_set) = store
                        .read_partition_liveness_for_invariant_check(&key)
                        .await
                        .map_err(|e| color_eyre::eyre::eyre!("op #{op_idx}: {e}"))?;
                    if has_clustering || next_set || rc_set {
                        return Err(color_eyre::eyre::eyre!(
                            "I4 (no-orphan) after op #{op_idx} key={key}: model empty but DB has \
                             clustering={has_clustering} next_timer={next_set} \
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

fn key_index_of(op: &TimerDeferOperation) -> usize {
    match op {
        TimerDeferOperation::GetNext(i)
        | TimerDeferOperation::IsDeferred(i)
        | TimerDeferOperation::DeleteKey(i) => *i,
        TimerDeferOperation::DeferFirst { key_index, .. }
        | TimerDeferOperation::DeferAdditional { key_index, .. }
        | TimerDeferOperation::CompleteRetrySuccess { key_index, .. }
        | TimerDeferOperation::IncrementRetryCount { key_index, .. }
        | TimerDeferOperation::Append { key_index, .. }
        | TimerDeferOperation::Remove { key_index, .. }
        | TimerDeferOperation::SetRetryCount { key_index, .. }
        | TimerDeferOperation::SeedLegacy { key_index, .. } => *key_index,
    }
}

async fn apply_op(
    store: &CassandraTimerDeferStore,
    op: &TimerDeferOperation,
    kcs: &[TestKeyComponents],
) -> color_eyre::Result<()> {
    match op {
        TimerDeferOperation::GetNext(i) => {
            store.get_next_deferred_timer(&kcs[*i].key).await?;
        }
        TimerDeferOperation::IsDeferred(i) => {
            store.is_deferred(&kcs[*i].key).await?;
        }
        TimerDeferOperation::DeferFirst { key_index, time } => {
            store
                .defer_first_timer(&trigger_for(kcs, *key_index, *time))
                .await?;
        }
        TimerDeferOperation::DeferAdditional { key_index, time } => {
            store
                .defer_additional_timer(&trigger_for(kcs, *key_index, *time))
                .await?;
        }
        TimerDeferOperation::CompleteRetrySuccess { key_index, time } => {
            store
                .complete_retry_success(&kcs[*key_index].key, *time)
                .await?;
        }
        TimerDeferOperation::IncrementRetryCount {
            key_index,
            current_retry_count,
        } => {
            store
                .increment_retry_count(&kcs[*key_index].key, *current_retry_count)
                .await?;
        }
        TimerDeferOperation::Append { key_index, time } => {
            store
                .append_deferred_timer(&trigger_for(kcs, *key_index, *time))
                .await?;
        }
        TimerDeferOperation::Remove { key_index, time } => {
            store
                .remove_deferred_timer(&kcs[*key_index].key, *time)
                .await?;
        }
        TimerDeferOperation::SetRetryCount {
            key_index,
            retry_count,
        } => {
            store
                .set_retry_count(&kcs[*key_index].key, *retry_count)
                .await?;
        }
        TimerDeferOperation::DeleteKey(i) => {
            store.delete_key(&kcs[*i].key).await?;
        }
        TimerDeferOperation::SeedLegacy {
            key_index,
            clustering_times,
            retry_count,
        } => {
            store
                .seed_legacy_for_test(&kcs[*key_index].key, clustering_times, *retry_count)
                .await?;
        }
    }
    Ok(())
}

/// Builds an application `Trigger` for the key at `key_index`, firing at
/// `time`, parented to the current span.
fn trigger_for(kcs: &[TestKeyComponents], key_index: usize, time: CompactDateTime) -> Trigger {
    Trigger::new(
        kcs[key_index].key.clone(),
        time,
        TimerType::Application,
        tracing::Span::current(),
    )
}
