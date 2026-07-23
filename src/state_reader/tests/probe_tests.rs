//! Probe-and-pin over multiple sources, driven through the scripted fault
//! source: source-order preference, data-beats-error, scan fault handling, and
//! single-source coherence (the pin addresses one source after the first
//! data-bearing read).

use super::support::{
    CountingIdentityStore, FaultPoint, GROUP_A, GROUP_B, ScriptedCellSource, mock_count,
    owner_commit, publish_scripted, registry_of, scripted_deps, source_state_key, state_name,
    subsystem, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::Direction;
use crate::state::descriptor::{deque_state, map_state, value_state};
use crate::state::order_codec::I64KeyCodec;
use crate::state::registry::CollectionDef;
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use serde_json::Value;

/// A deque length forcing the range-scan stream arm (> the point-get ceiling),
/// so the scan probe path is exercised.
const SCAN_ARM_LEN: usize = 130;

/// The lowest-ordered source with data answers a point read and pins.
///
/// Falsify: pin/return the first future to COMPLETE rather than the lowest in
/// source order — the higher-ordered source's value could win.
#[tokio::test]
async fn lowest_source_with_data_wins() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-lowest");
    let name = state_name("v-lowest")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    let sk_b = source_state_key(tp_b, GROUP_B, &key, count)?;

    // Both sources hold divergent values; A (group-aaa) is the lowest.
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

    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;
    assert_eq!(reader.get(key).await?, Some(Value::from("A")));
    Ok(())
}

/// A source that errors is skipped: data at a higher-ordered source beats a
/// lower-ordered source's error.
///
/// Falsify: short-circuit `Err` at the first source instead of skipping — the
/// read returns `Err` where data exists downstream.
#[tokio::test]
async fn data_beats_skipped_error() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-skip");
    let name = state_name("v-skip")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    let sk_b = source_state_key(tp_b, GROUP_B, &key, count)?;

    // A (lowest) faults; B holds data.
    cells.fault_at(sk_a.segment_id, FaultPoint::AtOpen);
    owner_commit(
        &cells.cells(),
        &registry,
        &sk_b,
        descriptor,
        1,
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

    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;
    assert_eq!(reader.get(key).await?, Some(Value::from("B")));
    Ok(())
}

/// No source has data → `None` (no pin, no error).
#[tokio::test]
async fn all_none_is_none() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-none");
    let name = state_name("v-none")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");
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

    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;
    assert_eq!(reader.get(key).await?, None);
    Ok(())
}

/// No data plus at least one error → `Err`.
#[tokio::test]
async fn no_data_with_error_is_err() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = value_state::<JsonCodec>("v-err");
    let name = state_name("v-err")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    cells.fault_at(sk_a.segment_id, FaultPoint::AtOpen);
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

    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;
    // No data anywhere plus a skipped error → an error, classified Transient
    // (the scripted fault's posture is preserved through the erasure).
    match reader.get(key).await {
        Err(error) if error.classify_error() == ErrorCategory::Transient => Ok(()),
        other => bail!("expected a Transient store error, got {other:?}"),
    }
}

/// Scan probes sources sequentially: a pre-yield error skips the source and the
/// next one answers. Driven over a deque wide enough to take the range-scan
/// arm, so the sequential scan probe (not the point fan-out) runs.
///
/// Falsify: propagate a pre-yield error instead of skipping — the reader errors
/// where a later source has data.
#[tokio::test]
async fn scan_preyield_error_skips_source() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = deque_state::<JsonCodec>("d-skip");
    let name = state_name("d-skip")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    let sk_b = source_state_key(tp_b, GROUP_B, &key, count)?;

    cells.fault_at(sk_a.segment_id, FaultPoint::AtOpen);
    owner_commit(
        &cells.cells(),
        &registry,
        &sk_b,
        descriptor,
        1,
        |h| async move {
            for i in 0..SCAN_ARM_LEN {
                h.push_back(Value::from(i as i64))
                    .await
                    .map_err(|e| eyre!("push: {e}"))?;
            }
            Ok(())
        },
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

    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;
    let scanned: Vec<Value> = reader
        .stream(key, Direction::Forward)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    let expected: Vec<Value> = (0..SCAN_ARM_LEN).map(|i| Value::from(i as i64)).collect();
    assert_eq!(
        scanned, expected,
        "source A's scan errored at open; B answered"
    );
    Ok(())
}

/// A mid-stream error after the scan has pinned a source terminates with `Err`
/// (no silent restart that would double-yield or skip).
///
/// Falsify: restart on a post-pin error — the reader yields a duplicated prefix
/// or swallows the error.
#[tokio::test]
async fn scan_midstream_error_propagates() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = deque_state::<JsonCodec>("d-mid");
    let name = state_name("d-mid")?;
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
        |h| async move {
            for i in 0..SCAN_ARM_LEN {
                h.push_back(Value::from(i as i64))
                    .await
                    .map_err(|e| eyre!("push: {e}"))?;
            }
            Ok(())
        },
    )
    .await?;
    // Yield exactly five present cells, then fault mid-stream.
    cells.fault_at(sk_a.segment_id, FaultPoint::AfterYields(5));
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

    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;
    let items: Vec<Result<Value, StateReaderError>> = reader
        .stream(key, Direction::Forward)
        .collect::<Vec<_>>()
        .await;
    // Five yielded prefix elements, then an error terminates the stream — no
    // restart, no duplicated prefix.
    assert_eq!(items.len(), 6, "five-element prefix + terminating error");
    assert!(items[..5].iter().all(Result::is_ok), "prefix yielded");
    assert!(items[5].is_err(), "mid-stream error terminates");
    Ok(())
}

/// Single-source coherence: the lowest-ordered source with any `Some` answers
/// the ENTIRE `get_many` batch — never a per-cell splice from a different
/// source. Source A (lowest) holds only key 0; source B holds only key 1. The
/// batch resolves entirely from A, so key 1 reads `None` (A's answer), never
/// B's tagged value.
///
/// Falsify: splice per cell (fill each absent slot from the next source) — key
/// 1 then carries B's value and the `None` assert goes red.
#[tokio::test]
async fn get_many_answers_from_one_source() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("m-coherent");
    let name = state_name("m-coherent")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    let sk_b = source_state_key(tp_b, GROUP_B, &key, count)?;

    // A (lowest) has only key 0; B (decoy) has only key 1, tagged distinctly.
    owner_commit(
        &cells.cells(),
        &registry,
        &sk_a,
        descriptor,
        1,
        |h| async move {
            h.set(0, Value::from("A0"))
                .await
                .map_err(|e| eyre!("set: {e}"))
        },
    )
    .await?;
    owner_commit(
        &cells.cells(),
        &registry,
        &sk_b,
        descriptor,
        2,
        |h| async move {
            h.set(1, Value::from("B1"))
                .await
                .map_err(|e| eyre!("set: {e}"))
        },
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

    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;

    let got = reader.get_many(key, &[0, 1]).await?;
    assert_eq!(
        got,
        vec![Some(Value::from("A0")), None],
        "the whole batch resolves from the lowest source A; B's key 1 is never spliced in"
    );
    Ok(())
}
