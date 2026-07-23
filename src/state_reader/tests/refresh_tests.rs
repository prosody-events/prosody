//! Snapshot acquisition and the three-outcome refresh rule: withdrawals apply
//! unconditionally, an already-admitted source is never re-validated, a failed
//! read keeps the previous snapshot, and an emptied routing table fails
//! `UnknownPublication` until re-admission.
//!
//! Every reader here refreshes on every operation (`new_eager`), so each `get`
//! exercises a fresh refresh.

use super::support::{
    CountingIdentityStore, GROUP_A, GROUP_B, ScriptedCellSource, mock_count, owner_commit,
    publish_scripted, registry_of, scripted_deps, source_state_key, state_name, subsystem, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::error::ErrorCategory;
use crate::state::descriptor::value_state;
use crate::state::publication::PublicationStore;
use crate::state::registry::CollectionDef;
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::eyre::{Result, bail, eyre};
use serde_json::Value;

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
