//! Snapshot acquisition and the three-outcome refresh rule: withdrawals apply
//! unconditionally, an already-admitted source is never re-validated, a failed
//! read keeps the previous snapshot, and an emptied routing table fails
//! `UnknownPublication` until re-admission.
//!
//! Every reader here refreshes on every operation (`new_eager`), so each `get`
//! exercises a fresh refresh.

use super::support::{
    CountingIdentityStore, GROUP_A, GROUP_B, ScriptedCellSource, fixed_clock_cache, mock_count,
    owner_commit, publish_scripted, registry_of, scripted_deps, source_state_key, state_name,
    subsystem, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::error::ErrorCategory;
use crate::state::descriptor::{DescriptorIdentity, value_state};
use crate::state::descriptor_identity::DurableDescriptorIdentity;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::CollectionDef;
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::eyre::{Result, bail, eyre};
use serde_json::Value;
use std::sync::Arc;

/// A withdrawal drops the source without consulting its identity, and an
/// already-admitted source is never re-validated on a later refresh (asserted
/// by the identity-store read count).
///
/// Falsify: keep the previous snapshot on a successful read (apply additions
/// only) — the withdrawn source stays readable; or re-read identity for
/// already-admitted sources — the read count climbs.
#[tokio::test]
async fn withdrawal_applies_and_admitted_not_revalidated() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-withdraw");
    let name = state_name("v-withdraw")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    let sk_b = source_state_key(tp_b, GROUP_B, &key, count)?;

    owner_commit(
        &cells.cells(),
        &registry,
        &sk_a,
        descriptor,
        1,
        |h| async move { h.set(Value::from("A")).await.map_err(|e| eyre!("set: {e}")) },
    )
    .await?;
    owner_commit(
        &cells.cells(),
        &registry,
        &sk_b,
        descriptor,
        2,
        |h| async move { h.set(Value::from("B")).await.map_err(|e| eyre!("set: {e}")) },
    )
    .await?;
    publish_scripted(
        (&publications, &identities),
        &sub,
        &name,
        GROUP_A,
        tp_a,
        count,
        &descriptor,
    )
    .await;
    publish_scripted(
        (&publications, &identities),
        &sub,
        &name,
        GROUP_B,
        tp_b,
        count,
        &descriptor,
    )
    .await;

    let publications_edit = publications.clone();
    let identities_probe = identities.clone();
    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;

    // Op 1: admits A and B — one identity read each.
    assert_eq!(reader.get(key.clone()).await?, Some(Value::from("A")));
    let reads_after_admit = identities_probe.reads();
    assert_eq!(reads_after_admit, 2, "both new groups validated once");

    // Withdraw A; B stays advertised.
    publications_edit
        .remove(&subsystem()?, &name, GROUP_A, tp_a)
        .await
        .map_err(|e| eyre!("remove: {e}"))?;

    // Op 2: A withdrawn (no identity read), B already admitted (no identity
    // read) — the lowest surviving source B now answers.
    assert_eq!(reader.get(key).await?, Some(Value::from("B")));
    assert_eq!(
        identities_probe.reads(),
        reads_after_admit,
        "withdrawal reads no identity; the admitted survivor is not re-validated"
    );
    Ok(())
}

/// A failed routing-table read keeps the previous snapshot: reads still answer
/// from it.
///
/// Falsify: clear the snapshot on a failed read — the next read errors where it
/// should still answer.
#[tokio::test]
async fn failed_read_keeps_snapshot() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-failread");
    let name = state_name("v-failread")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let tp_a = topic("topic-a");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    owner_commit(
        &cells.cells(),
        &registry,
        &sk_a,
        descriptor,
        1,
        |h| async move { h.set(Value::from("A")).await.map_err(|e| eyre!("set: {e}")) },
    )
    .await?;
    publish_scripted(
        (&publications, &identities),
        &sub,
        &name,
        GROUP_A,
        tp_a,
        count,
        &descriptor,
    )
    .await;

    let publications_edit = publications.clone();
    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;

    // Op 1 admits A.
    assert_eq!(reader.get(key.clone()).await?, Some(Value::from("A")));

    // Every subsequent routing read errors.
    publications_edit.fail_reads_with(ErrorCategory::Transient);

    // Op 2: the refresh read fails, the prior snapshot is retained, the read
    // still answers.
    assert_eq!(
        reader.get(key).await?,
        Some(Value::from("A")),
        "failed refresh keeps the previous snapshot"
    );
    Ok(())
}

/// An emptied routing table stores the absence and fails `UnknownPublication`
/// (Transient) until a source is re-admitted.
#[tokio::test]
async fn emptied_snapshot_is_unknown_publication() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-empty");
    let name = state_name("v-empty")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let tp_a = topic("topic-a");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    owner_commit(
        &cells.cells(),
        &registry,
        &sk_a,
        descriptor,
        1,
        |h| async move { h.set(Value::from("A")).await.map_err(|e| eyre!("set: {e}")) },
    )
    .await?;
    publish_scripted(
        (&publications, &identities),
        &sub,
        &name,
        GROUP_A,
        tp_a,
        count,
        &descriptor,
    )
    .await;

    let publications_edit = publications.clone();
    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;

    assert_eq!(reader.get(key.clone()).await?, Some(Value::from("A")));

    // Withdraw the only source.
    publications_edit
        .remove(&subsystem()?, &name, GROUP_A, tp_a)
        .await
        .map_err(|e| eyre!("remove: {e}"))?;

    match reader.get(key).await {
        Err(StateReaderError::UnknownPublication { .. }) => Ok(()),
        other => bail!("expected UnknownPublication, got {other:?}"),
    }
}

/// A present-but-unequal identity is sticky across the refresh interval: once a
/// source's frozen identity disagrees with the descriptor, EVERY read within
/// the interval surfaces the Permanent `IdentityMismatch` — it is never masked
/// by the admitted (valid) subset served from the cached snapshot.
///
/// `GROUP_A` carries a matching identity (admitted); `GROUP_B` a perturbed one
/// (present-but-unequal). A non-zero interval plus a fixed clock keeps the
/// second read on the cached-snapshot fast path, where the mask would occur.
///
/// Falsify: drop the sticky-mismatch check in `StateReader::snapshot` — the
/// second read serves A's admitted subset (`Ok(None)`) and the arm goes red.
#[tokio::test]
async fn identity_mismatch_is_sticky_within_interval() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-sticky");
    let name = state_name("v-sticky")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // A: matching identity → admitted.
    publish_scripted(
        (&publications, &identities),
        &sub,
        &name,
        GROUP_A,
        tp_a,
        count,
        &descriptor,
    )
    .await;
    // B: advertised, but its frozen identity is perturbed → present-but-unequal.
    publications
        .seed(
            &sub,
            &name,
            &StatePublication {
                group_id: Arc::from(GROUP_B),
                topic: tp_b,
                partition_count: count,
            },
        )
        .await;
    let mut perturbed = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        name.as_str(),
        &descriptor.structural_identity(),
    );
    perturbed.kind = perturbed.kind.wrapping_add(1);
    identities.seed(GROUP_B, &perturbed).await;

    let (cache, _clock) = fixed_clock_cache(1 << 20);
    let deps = scripted_deps(cells, publications, identities, cache);
    // A non-zero interval; the fixed clock never advances past it, so the second
    // read takes the cached-snapshot fast path.
    let reader = StateReader::new_with_interval(&deps, sub, descriptor, 60_000)?;

    // Op 1: the initial refresh detects B's mismatch (A is still admitted).
    match reader.get(key.clone()).await {
        Err(StateReaderError::IdentityMismatch { .. }) => {}
        other => bail!("op 1 expected IdentityMismatch, got {other:?}"),
    }
    // Op 2: within the interval — the mismatch must still surface, not A's
    // admitted subset.
    match reader.get(key).await {
        Err(StateReaderError::IdentityMismatch { .. }) => Ok(()),
        other => bail!("op 2 (within interval) expected a sticky IdentityMismatch, got {other:?}"),
    }
}

/// A sticky mismatch survives a later routing-table read failure: a transient
/// outage is no evidence the Permanent misconfiguration was repaired, so the
/// failed-read path keeps surfacing `IdentityMismatch` rather than demoting it
/// to the admitted (valid) subset.
///
/// Falsify: return `Ok(prior_snapshot)` on a failed read without consulting the
/// sticky mismatch — the read serves A's subset (`Ok(None)`) and the arm reds.
#[tokio::test]
async fn sticky_mismatch_survives_failed_refresh() -> Result<()> {
    use std::sync::atomic::Ordering;

    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-sticky-fail");
    let name = state_name("v-sticky-fail")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // A admitted; B present with a perturbed frozen identity → mismatch.
    publish_scripted(
        (&publications, &identities),
        &sub,
        &name,
        GROUP_A,
        tp_a,
        count,
        &descriptor,
    )
    .await;
    publications
        .seed(
            &sub,
            &name,
            &StatePublication {
                group_id: Arc::from(GROUP_B),
                topic: tp_b,
                partition_count: count,
            },
        )
        .await;
    let mut perturbed = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        name.as_str(),
        &descriptor.structural_identity(),
    );
    perturbed.kind = perturbed.kind.wrapping_add(1);
    identities.seed(GROUP_B, &perturbed).await;

    let (cache, clock) = fixed_clock_cache(1 << 20);
    let publications_edit = publications.clone();
    let deps = scripted_deps(cells, publications, identities, cache);
    let reader = StateReader::new_with_interval(&deps, sub, descriptor, 1_000)?;

    // Op 1 (t=0): the initial refresh detects and records the sticky mismatch.
    match reader.get(key.clone()).await {
        Err(StateReaderError::IdentityMismatch { .. }) => {}
        other => bail!("op 1 expected IdentityMismatch, got {other:?}"),
    }

    // Elapse the interval and make every routing read fail.
    clock.store(5_000, Ordering::Relaxed);
    publications_edit.fail_reads_with(ErrorCategory::Transient);

    // Op 2 (t=5000, stale → refresh, read fails): the sticky mismatch outranks
    // the admitted subset even though the routing read failed.
    match reader.get(key).await {
        Err(StateReaderError::IdentityMismatch { .. }) => Ok(()),
        other => bail!("op 2 (failed refresh) expected a sticky IdentityMismatch, got {other:?}"),
    }
}
