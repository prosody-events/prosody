//! Probe-and-pin over multiple sources, driven through the scripted fault
//! source.
//!
//! The source-selection semantics — the lowest-`SourceId` source with data
//! wins, an errored source is skipped, data beats a skipped error, all-empty is
//! `None`/empty, and no-data-plus-an-error is `Err` — are proven together by
//! [`prop_probe_and_pin`] over random fault scripts, for both the point fan-out
//! (`get`/`len`) and the pinned scan (`stream`). The focused examples pin
//! invariants the script model does not express: the batch (`get_many`) error
//! precedence and single-source splice, the post-pin mid-stream scan error, and
//! the source-call trace proving a pinned scan never opens the decoy.

use super::support::{
    CountingIdentityStore, FaultPoint, GROUP_A, GROUP_B, ScriptedCellSource, mock_count,
    owner_commit, publish_scripted, registry_of, scripted_deps, source_state_key, state_name,
    subsystem, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::Direction;
use crate::state::descriptor::{DequeDescriptor, deque_state, map_state};
use crate::state::order_codec::I64KeyCodec;
use crate::state::registry::CollectionDef;
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use futures::executor::block_on;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::iter::{empty, once};

/// A deque length forcing the range-scan stream arm (> the point-get ceiling),
/// so the scan probe path is exercised.
const SCAN_ARM_LEN: usize = 130;

// --- P2: probe-and-pin property ---------------------------------------------

/// The ordered group pool the fault script assigns sources to. Lexicographic
/// order (`g0 < g1 < …`) makes `SourceId` order equal index order, so the
/// model's "lowest source" is `sources[0]`.
const GROUP_POOL: [&str; 4] = ["probe-g0", "probe-g1", "probe-g2", "probe-g3"];

/// The per-source deque lengths the script draws from: small (the chunked
/// point-get stream arm) through one past the range-scan ceiling
/// ([`SCAN_ARM_LEN`]), so both stream arms are exercised.
const LEN_POOL: [usize; 4] = [1, 2, 3, SCAN_ARM_LEN];

/// One source's disposition.
#[derive(Clone, Copy, Debug)]
enum SourceDisposition {
    /// No committed data (bounds absent) — skipped in selection.
    Empty,
    /// A dense deque of `LEN_POOL[idx]` elements, tagged by source index so a
    /// wrong pin serves visibly wrong values.
    Data(u8),
    /// A committed read that errors before any row (`FaultPoint::AtOpen`) — the
    /// bounds point read fails, so selection skips it (remembering the error).
    FaultOpen,
}

impl Arbitrary for SourceDisposition {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Self::Empty,
            1 => Self::Data(u8::arbitrary(g) % LEN_POOL.len() as u8),
            _ => Self::FaultOpen,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            // Shrink toward the simplest disposition (Empty), and a data
            // length toward its smallest.
            Self::Empty => Box::new(empty()),
            Self::Data(idx) => Box::new(once(Self::Empty).chain(idx.shrink().map(Self::Data))),
            Self::FaultOpen => Box::new(once(Self::Empty)),
        }
    }
}

/// A fault script: one disposition per source, over a bounded source pool.
#[derive(Clone, Debug)]
struct FaultScript {
    sources: Vec<SourceDisposition>,
}

impl Arbitrary for FaultScript {
    fn arbitrary(g: &mut Gen) -> Self {
        // At least one source; at most the group pool.
        let n = 1 + usize::arbitrary(g) % GROUP_POOL.len();
        Self {
            sources: (0..n).map(|_| SourceDisposition::arbitrary(g)).collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Keep at least one source (an empty snapshot is a different, structural
        // case), otherwise shrink each disposition and drop trailing sources.
        let sources = self.sources.clone();
        Box::new(
            sources
                .shrink()
                .filter(|s| !s.is_empty())
                .map(|sources| Self { sources }),
        )
    }
}

/// The tagged element value at index `j` of source `idx` — distinct per source
/// so a mis-pin is visible.
fn element(idx: usize, j: usize) -> Value {
    Value::from((idx as i64) * 1000 + j as i64)
}

/// The selection the point fan-out resolves to under a script: the first
/// data-bearing source in `SourceId` (index) order, or the absence of one plus
/// whether any earlier source errored.
enum Selection {
    /// Source `idx` with a dense `len`-element deque pins.
    Pinned { idx: usize, len: usize },
    /// No data, but at least one source errored → the read is `Err`.
    ErrOnly,
    /// No data and no error → `None`/empty.
    EmptyOnly,
}

fn selection(script: &FaultScript) -> Selection {
    let mut saw_err = false;
    for (idx, disposition) in script.sources.iter().enumerate() {
        match disposition {
            SourceDisposition::FaultOpen => saw_err = true,
            SourceDisposition::Empty => {}
            SourceDisposition::Data(len_idx) => {
                return Selection::Pinned {
                    idx,
                    len: LEN_POOL[*len_idx as usize],
                };
            }
        }
    }
    if saw_err {
        Selection::ErrOnly
    } else {
        Selection::EmptyOnly
    }
}

/// The lowest-`SourceId` source with committed data answers every read; an
/// errored source is skipped, data beats a skipped error, all-empty is
/// `None`/empty, and no-data-plus-an-error is `Err` — for both the point
/// fan-out (`get`/`len`) and the pinned scan (`stream`, forward and backward).
///
/// FALSIFICATION: short-circuit `Err` at the first source in `ReadSession::get`
/// (session.rs) instead of skipping → a `FaultOpen`-then-`Data` script errors
/// where data exists → mismatch. Reverse the snapshot source order → a higher
/// source pins → the tagged value diverges.
#[test]
fn prop_probe_and_pin() {
    fn property(script: FaultScript) -> Result<bool> {
        block_on(run_probe_and_pin(script))
    }
    QuickCheck::new().quickcheck(property as fn(FaultScript) -> Result<bool>);
}

async fn run_probe_and_pin(script: FaultScript) -> Result<bool> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = deque_state::<JsonCodec>("probe-dq");
    let name = state_name("probe-dq")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    for (idx, disposition) in script.sources.iter().enumerate() {
        let group = GROUP_POOL[idx];
        let tp = topic(GROUP_POOL[idx]);
        let state_key = source_state_key(tp, group, &key, count)?;
        match disposition {
            SourceDisposition::Empty => {}
            SourceDisposition::FaultOpen => {
                cells.fault_at(state_key.segment_id, FaultPoint::AtOpen);
            }
            SourceDisposition::Data(len_idx) => {
                let len = LEN_POOL[*len_idx as usize];
                owner_commit(
                    &cells.cells(),
                    &registry,
                    &state_key,
                    descriptor,
                    idx as u128 + 1,
                    move |handle| async move {
                        for j in 0..len {
                            handle
                                .push_back(element(idx, j))
                                .await
                                .map_err(|e| eyre!("push: {e}"))?;
                        }
                        Ok(())
                    },
                )
                .await?;
            }
        }
        publish_scripted(
            (&publications, &identities),
            &sub,
            &name,
            group,
            tp,
            count,
            &descriptor,
        )
        .await;
    }

    let deps = scripted_deps(
        cells,
        publications,
        identities,
        ReaderCache::with_budget(1 << 20),
    );
    let reader = StateReader::new_eager(&deps, sub, descriptor)?;
    assert_probe(&reader, &key, selection(&script)).await
}

/// The concrete deque reader the probe property drives.
type DequeReader = StateReader<DequeDescriptor<JsonCodec>, JsonCodec>;

/// Collects a reader deque stream, surfacing any error item.
async fn forward_deque(reader: &DequeReader, key: &Key) -> Result<Vec<Value>> {
    Ok(reader
        .stream(key.clone(), Direction::Forward)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?)
}

/// Asserts the reader's point (`len`/`get`) and scan (`stream`) reads match the
/// point-fan-out selection the script resolves to.
async fn assert_probe(reader: &DequeReader, key: &Key, selection: Selection) -> Result<bool> {
    match selection {
        Selection::Pinned { idx, len } => {
            let expected: Vec<Value> = (0..len).map(|j| element(idx, j)).collect();
            let backward: Vec<Value> = reader
                .stream(key.clone(), Direction::Backward)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<_, _>>()?;
            Ok(reader.len(key.clone()).await? == len
                && reader.get(key.clone(), 0).await? == Some(element(idx, 0))
                && reader.get(key.clone(), len).await?.is_none()
                && forward_deque(reader, key).await? == expected
                && backward == expected.into_iter().rev().collect::<Vec<_>>())
        }
        Selection::ErrOnly => {
            // No data through a failed source: absence is not provable, so
            // every read errors.
            let streamed: Vec<Result<Value, StateReaderError>> = reader
                .stream(key.clone(), Direction::Forward)
                .collect::<Vec<_>>()
                .await;
            Ok(reader.len(key.clone()).await.is_err()
                && reader.get(key.clone(), 0).await.is_err()
                && streamed.iter().any(Result::is_err))
        }
        Selection::EmptyOnly => Ok(reader.len(key.clone()).await? == 0
            && reader.get(key.clone(), 0).await?.is_none()
            && forward_deque(reader, key).await?.is_empty()),
    }
}

// --- Focused survivors ------------------------------------------------------

/// A mid-stream error after the scan has pinned a source terminates with `Err`
/// (no silent restart that would double-yield or skip). Needs the range-scan
/// arm and a precise fault position, which the arm-agnostic
/// [`prop_probe_and_pin`] does not model.
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

/// `get_many`: when the lowest source errors and the next answers with an
/// all-`None` buffer (it holds none of the batch cells), the read is an `Err` —
/// absence is not provable through a failed source, exactly as the point read
/// treats no-data-plus-an-error. The batch (`get_many`) error precedence is a
/// narrow invariant the deque script does not exercise. Source A (lowest)
/// faults at open; source B is admitted but empty.
///
/// Falsify: return the remembered all-`None` buffer instead of the error — the
/// Transient store failure is masked as a false batch-absence.
#[tokio::test]
async fn get_many_error_beats_all_none() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("m-err-none");
    let name = state_name("m-err-none")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;

    // A (lowest) faults at open; B is published but holds none of the batch
    // cells, so it answers an all-`None` buffer.
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

    match reader.get_many(key, &[0, 1]).await {
        Err(error) if error.classify_error() == ErrorCategory::Transient => Ok(()),
        other => bail!("expected a Transient store error, got {other:?}"),
    }
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

/// Single-source coherence for a scan, proven by the source-call trace: the
/// lowest-ordered source with data pins and answers the whole scan; the decoy
/// source B is never read (its recorded read count stays zero) — the deque's
/// bounds read pins A, and every later read addresses that one pinned source.
///
/// Falsify: reverse the snapshot's source-preference order (pin the *highest*
/// source) — B pins instead, its values answer the scan, and both its read
/// count and the value assert go red.
#[tokio::test]
async fn scan_reads_only_pinned_source() -> Result<()> {
    let cells = ScriptedCellSource::new();
    let publications = ScriptedPublicationStore::new();
    let identities = CountingIdentityStore::new();
    let descriptor = deque_state::<JsonCodec>("d-coherent");
    let name = state_name("d-coherent")?;
    let sub = subsystem()?;
    let count = mock_count();
    let key = Key::from("user-1");
    let registry = registry_of(&descriptor, CollectionDef::new(None))?;

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");
    let sk_a = source_state_key(tp_a, GROUP_A, &key, count)?;
    let sk_b = source_state_key(tp_b, GROUP_B, &key, count)?;

    // Both sources hold a full deque (wide enough for the range-scan arm); A
    // (group-aaa) is the lowest and must answer alone. B's values are tagged
    // distinctly so a splice would be visible.
    for (sk, base, event) in [(&sk_a, 0i64, 1u128), (&sk_b, 1000i64, 2u128)] {
        owner_commit(
            &cells.cells(),
            &registry,
            sk,
            descriptor,
            event,
            |h| async move {
                for i in 0..SCAN_ARM_LEN {
                    h.push_back(Value::from(base + i as i64))
                        .await
                        .map_err(|e| eyre!("push: {e}"))?;
                }
                Ok(())
            },
        )
        .await?;
    }
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

    let cells_probe = cells.clone();
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
        "the whole scan resolves from the lowest source A"
    );
    assert!(
        cells_probe.reads(sk_a.segment_id) >= 1,
        "source A was scanned"
    );
    assert_eq!(
        cells_probe.reads(sk_b.segment_id),
        0,
        "source B (decoy) was never opened"
    );
    Ok(())
}
