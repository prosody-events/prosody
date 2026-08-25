//! Probe-and-pin over multiple sources, driven through the scripted fault
//! source.
//!
//! Selection picks the lowest-`SourceId` source that has data, and skips a
//! source that errored on open. Data always beats a skipped error. An
//! all-empty source set reads `None` or empty. No data plus at least one
//! source error reads `Err`. [`prop_probe_and_pin`] proves all of this
//! together over random fault scripts, for both the point fan-out (`get` and
//! `len`) and the pinned scan (`stream`).
//!
//! The focused tests below cover invariants the script model does not express:
//!
//! * `get_many` batch error precedence;
//! * the batch alignment check on the uncached read path;
//! * `get_many` single-source splicing;
//! * a mid-stream scan error after a source has pinned;
//! * a source-call trace that proves a pinned scan never opens the decoy
//!   source;
//! * the overlap of two concurrent reads on one reader;
//! * the session-wide reuse of one selection across scoped operations.

use super::support::{
    FaultPoint, GROUP_A, GROUP_B, ScriptedEnv, collect_stream, source_state_key, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::ReadCachePolicy;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{
    DequeDescriptor, StateDescriptor, deque_state, map_state, value_state,
};
use crate::state::order_codec::I64KeyCodec;
use crate::state_reader::backend::ScriptedReaderBackend;
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use futures::executor::block_on;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::iter::{empty, once};
use std::time::Duration;
use tokio::time::timeout;

/// A deque length above the point-get ceiling, forcing the range-scan stream
/// arm to be exercised.
const SCAN_ARM_LEN: usize = 130;

// --- Probe-and-pin property -------------------------------------------------

/// The ordered group pool the fault script assigns sources to. Lexicographic
/// order (`g0 < g1 < …`) makes `SourceId` order match index order. The
/// model's "lowest source" is therefore `sources[0]`.
const GROUP_POOL: [&str; 4] = ["probe-g0", "probe-g1", "probe-g2", "probe-g3"];

/// The per-source deque lengths the script draws from. The small lengths
/// exercise the chunked point-get stream arm; [`SCAN_ARM_LEN`] is one past
/// the range-scan ceiling, exercising that arm too.
const LEN_POOL: [usize; 4] = [1, 2, 3, SCAN_ARM_LEN];

/// One source's disposition.
#[derive(Clone, Copy, Debug)]
enum SourceDisposition {
    /// No committed data: bounds are absent, so selection skips this source.
    Empty,
    /// A dense deque of `LEN_POOL[idx]` elements, tagged by source index so a
    /// wrong pin serves visibly wrong values.
    Data(u8),
    /// A committed read that errors before any row (`FaultPoint::AtOpen`). The
    /// bounds point read fails, so selection skips this source but remembers
    /// the error.
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
            // Shrink toward the simplest disposition (Empty), and shrink a
            // data length toward its smallest.
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
        // Keep at least one source; an empty snapshot is a different,
        // structural case. Otherwise shrink each disposition and drop
        // trailing sources.
        let sources = self.sources.clone();
        Box::new(
            sources
                .shrink()
                .filter(|s| !s.is_empty())
                .map(|sources| Self { sources }),
        )
    }
}

/// The tagged element value at index `j` of source `idx`. Values are distinct
/// per source, so a mis-pin is visible.
fn element(idx: usize, j: usize) -> Value {
    Value::from((idx as i64) * 1000 + j as i64)
}

/// The selection the point fan-out resolves to under a script. If any source
/// has data, it is the first one in `SourceId` (index) order. Otherwise the
/// selection depends on whether an earlier source errored.
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

/// The lowest-`SourceId` source with committed data answers every read. An
/// errored source is skipped; data beats a skipped error. All sources empty
/// gives `None`/empty, and no data plus an earlier error gives `Err`. This
/// holds for the point fan-out (`get`/`len`) and for the pinned scan,
/// `stream` in both directions.
///
/// FALSIFICATION: short-circuit `Err` at the first source in
/// `ReadSession::probe_point` instead of skipping. A `FaultOpen`-then-`Data`
/// script then errors where data exists. Reverse the snapshot source order and
/// a higher source pins instead, so the tagged value diverges.
#[test]
fn prop_probe_and_pin() {
    fn property(script: FaultScript) -> Result<bool> {
        block_on(run_probe_and_pin(script))
    }
    QuickCheck::new().quickcheck(property as fn(FaultScript) -> Result<bool>);
}

async fn run_probe_and_pin(script: FaultScript) -> Result<bool> {
    let env = ScriptedEnv::new(deque_state::<JsonCodec>("probe-dq"))?;
    let key = Key::from("user-1");

    for (idx, disposition) in script.sources.iter().enumerate() {
        let group = GROUP_POOL[idx];
        let tp = topic(GROUP_POOL[idx]);
        match disposition {
            SourceDisposition::Empty => {}
            SourceDisposition::FaultOpen => {
                env.fault(group, tp, &key, FaultPoint::AtOpen)?;
            }
            SourceDisposition::Data(len_idx) => {
                let len = LEN_POOL[*len_idx as usize];
                env.commit(group, tp, &key, idx as u128 + 1, move |handle| async move {
                    for j in 0..len {
                        handle
                            .push_back(element(idx, j))
                            .await
                            .map_err(|e| eyre!("push: {e}"))?;
                    }
                    Ok(())
                })
                .await?;
            }
        }
        env.publish(group, tp).await;
    }

    let reader = env.reader_eager()?;
    Box::pin(assert_probe(&reader, &key, selection(&script))).await
}

/// The concrete deque reader the probe property drives.
type DequeReader = StateReader<DequeDescriptor<JsonCodec>, JsonCodec, ScriptedReaderBackend>;

/// Asserts the reader's point reads and scan match the point-fan-out
/// selection the script resolves to. Point reads are `len` and `get`; the
/// scan is `stream`.
async fn assert_probe(reader: &DequeReader, key: &Key, selection: Selection) -> Result<bool> {
    match selection {
        Selection::Pinned { idx, len } => {
            let expected: Vec<Value> = (0..len).map(|j| element(idx, j)).collect();
            let forward = Box::pin(collect_stream(
                reader.stream(key.clone(), Direction::Forward).await?,
            ))
            .await?;
            let backward = Box::pin(collect_stream(
                reader.stream(key.clone(), Direction::Backward).await?,
            ))
            .await?;
            Ok(reader.len(key.clone()).await? == len
                && reader.get(key.clone(), 0).await? == Some(element(idx, 0))
                && reader.get(key.clone(), len).await?.is_none()
                && forward == expected
                && backward == expected.into_iter().rev().collect::<Vec<_>>())
        }
        Selection::ErrOnly => {
            // No data through a failed source: absence is not provable, so
            // every read errors.
            let streamed: Vec<Result<Value, StateReaderError>> = reader
                .stream(key.clone(), Direction::Forward)
                .await?
                .collect::<Vec<_>>()
                .await;
            Ok(reader.len(key.clone()).await.is_err()
                && reader.get(key.clone(), 0).await.is_err()
                && streamed.iter().any(Result::is_err))
        }
        Selection::EmptyOnly => Ok(reader.len(key.clone()).await? == 0
            && reader.get(key.clone(), 0).await?.is_none()
            && Box::pin(collect_stream(
                reader.stream(key.clone(), Direction::Forward).await?,
            ))
            .await?
            .is_empty()),
    }
}

// --- Focused survivors ------------------------------------------------------

/// A mid-stream error after the scan has pinned a source terminates with
/// `Err`. There is no silent restart that would double-yield or skip data.
/// This needs the range-scan arm and a precise fault position, and the fault
/// script [`prop_probe_and_pin`] draws from has no mid-stream fault point to
/// express it.
///
/// Falsify: restart on a post-pin error. The reader would then yield a
/// duplicated prefix or swallow the error.
#[tokio::test]
async fn scan_midstream_error_propagates() -> Result<()> {
    let env = ScriptedEnv::new(deque_state::<JsonCodec>("d-mid"))?;
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    env.commit(GROUP_A, tp_a, &key, 1, |h| async move {
        for i in 0..SCAN_ARM_LEN {
            h.push_back(Value::from(i as i64))
                .await
                .map_err(|e| eyre!("push: {e}"))?;
        }
        Ok(())
    })
    .await?;
    // Yield exactly five present cells, then fault mid-stream.
    env.fault(GROUP_A, tp_a, &key, FaultPoint::AfterYields(5))?;
    env.publish(GROUP_A, tp_a).await;

    let reader = env.reader_eager()?;
    let items: Vec<Result<Value, StateReaderError>> = reader
        .stream(key, Direction::Forward)
        .await?
        .collect::<Vec<_>>()
        .await;
    // Five yielded prefix elements, then an error terminates the stream.
    assert_eq!(items.len(), 6, "five-element prefix + terminating error");
    assert!(items[..5].iter().all(Result::is_ok), "prefix yielded");
    assert!(items[5].is_err(), "mid-stream error terminates");
    Ok(())
}

/// When the lowest source errors and the next source answers with an
/// all-`None` buffer, `get_many` returns `Err`. The all-`None` buffer means B
/// holds none of the batch's cells, but absence is not provable through a
/// failed source. This mirrors how a point read treats no data plus an error.
/// The deque fault script never exercises this batch case, so it gets its
/// own test: source A (lowest) faults at open, and source B is admitted but
/// empty.
///
/// Falsify: return the remembered all-`None` buffer instead of the error.
/// That would mask a transient store failure as a false absence for the
/// batch.
#[tokio::test]
async fn get_many_error_beats_all_none() -> Result<()> {
    let env = ScriptedEnv::new(map_state::<I64KeyCodec, JsonCodec>("m-err-none"))?;
    let key = Key::from("user-1");
    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // A (lowest) faults at open; B is published but holds none of the batch
    // cells, so it answers an all-`None` buffer.
    env.fault(GROUP_A, tp_a, &key, FaultPoint::AtOpen)?;
    env.publish(GROUP_A, tp_a).await;
    env.publish(GROUP_B, tp_b).await;
    let reader = env.reader_eager()?;

    match reader.get_many(key, &[0, 1]).await {
        Err(error) if error.classify_error() == ErrorCategory::Transient => Ok(()),
        other => bail!("expected a Transient store error, got {other:?}"),
    }
}

/// A contract-violating source answers a batch read with fewer values than
/// the batch requested. The uncached batch path checks that alignment in
/// every build, so the read fails instead of zipping the short buffer into a
/// truncated, misaligned answer. `CommittedCellSource` is a downstream trait,
/// so a debug assertion cannot hold this line in a release build.
///
/// Falsify: remove the length check from the uncached arm of `cached_batch`.
/// `get_many` then answers a two-cell batch with one value.
#[tokio::test]
async fn short_batch_buffer_fails_the_uncached_read() -> Result<()> {
    let env = ScriptedEnv::new(map_state::<I64KeyCodec, JsonCodec>("m-short-batch"))?;
    let key = Key::from("user-1");
    let tp_a = topic("topic-a");

    env.commit(GROUP_A, tp_a, &key, 1, |h| async move {
        h.set(&0, Value::from("A0"))
            .await
            .map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.publish(GROUP_A, tp_a).await;
    // Arm the fault after seeding: `commit` writes through the same source.
    env.fault(GROUP_A, tp_a, &key, FaultPoint::ShortBatch)?;
    let reader = env.reader_eager()?;

    match reader.get_many(key, &[0, 1]).await {
        Err(error) if error.classify_error() == ErrorCategory::Permanent => {
            assert!(
                error.to_string().contains("batch read returned 1 values"),
                "expected the alignment error, got {error}"
            );
            Ok(())
        }
        other => bail!("expected a Permanent alignment error, got {other:?}"),
    }
}

/// The lowest-ordered source with any `Some` answers the entire `get_many`
/// batch. There is no per-cell splice from a different source. Source A
/// (lowest) holds only key 0; source B holds only key 1. The batch resolves
/// entirely from A, so key 1 reads `None`, A's answer, never B's tagged
/// value.
///
/// Falsify: splice per cell, filling each absent slot from the next source.
/// Key 1 would then carry B's value and the `None` assert goes red.
#[tokio::test]
async fn get_many_answers_from_one_source() -> Result<()> {
    let env = ScriptedEnv::new(map_state::<I64KeyCodec, JsonCodec>("m-coherent"))?;
    let key = Key::from("user-1");
    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // A is the lowest source and holds only key 0; B is the decoy and holds
    // only key 1, tagged distinctly.
    env.commit(GROUP_A, tp_a, &key, 1, |h| async move {
        h.set(&0, Value::from("A0"))
            .await
            .map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.commit(GROUP_B, tp_b, &key, 2, |h| async move {
        h.set(&1, Value::from("B1"))
            .await
            .map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.publish(GROUP_A, tp_a).await;
    env.publish(GROUP_B, tp_b).await;
    let reader = env.reader_eager()?;

    let got = reader.get_many(key, &[0, 1]).await?;
    assert_eq!(
        got,
        vec![Some(Value::from("A0")), None],
        "the whole batch resolves from the lowest source A; B's key 1 is never spliced in"
    );
    Ok(())
}

/// The lowest-ordered source with data pins and answers the whole scan. This
/// test proves it through the source-call trace: the decoy source B is never
/// read, so its recorded read count stays zero. The deque's bounds read pins
/// source A, and every later read in the scan addresses that one pinned
/// source.
///
/// Falsify: reverse the snapshot's source-preference order, pinning the
/// highest source instead. Then B pins, its values answer the scan, and both
/// its read count and the value assert go red.
#[tokio::test]
async fn scan_reads_only_pinned_source() -> Result<()> {
    let env = ScriptedEnv::new(deque_state::<JsonCodec>("d-coherent"))?;
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // Both sources hold a full deque, wide enough for the range-scan arm. A
    // is the lowest source and must answer alone. B's values are tagged
    // distinctly so a splice would be visible.
    let mut segments = Vec::new();
    for (group, tp, base, event) in [
        (GROUP_A, tp_a, 0i64, 1u128),
        (GROUP_B, tp_b, 1000i64, 2u128),
    ] {
        let state_key = env
            .commit(group, tp, &key, event, move |h| async move {
                for i in 0..SCAN_ARM_LEN {
                    h.push_back(Value::from(base + i as i64))
                        .await
                        .map_err(|e| eyre!("push: {e}"))?;
                }
                Ok(())
            })
            .await?;
        segments.push(state_key.segment_id);
        env.publish(group, tp).await;
    }
    let (segment_a, segment_b) = (segments[0], segments[1]);

    let reader = env.reader_eager()?;

    let scanned: Vec<Value> = reader
        .stream(key, Direction::Forward)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    let expected: Vec<Value> = (0..SCAN_ARM_LEN).map(|i| Value::from(i as i64)).collect();
    assert_eq!(
        scanned, expected,
        "the whole scan resolves from the lowest source A"
    );
    assert!(env.cells.reads(segment_a) >= 1, "source A was scanned");
    assert_eq!(
        env.cells.reads(segment_b),
        0,
        "source B (decoy) was never opened"
    );
    Ok(())
}

/// Two reads on one reader overlap: they drive cell I/O concurrently, sharing
/// no admission.
///
/// The source holds every committed point read at a two-party meeting point,
/// so both reads must arrive before either returns. Serializing them would
/// park the first there forever; the deadline is only the hang guard, and the
/// rendezvous is the assertion. The read cache is disabled so its same-key
/// single-flight cannot collapse the two reads into one.
///
/// Falsify: route both reads through one shared admission — take a single
/// session-wide permit around the read instead of building an independent
/// session per operation. The second read then never reaches the meeting
/// point and the deadline fires.
#[tokio::test]
async fn concurrent_reads_on_one_reader_overlap() -> Result<()> {
    let mut env = ScriptedEnv::new(
        value_state::<JsonCodec>("probe-concurrent").read_cache(ReadCachePolicy::Disabled),
    )?;
    env.cells.rendezvous(2);
    let key = Key::from("user-1");
    let tp = topic(GROUP_A);
    env.commit(GROUP_A, tp, &key, 1, |handle| async move {
        handle
            .set(Value::from(7_i64))
            .await
            .map_err(|e| eyre!("set: {e}"))?;
        Ok(())
    })
    .await?;
    env.publish(GROUP_A, tp).await;
    let reader = env.reader_eager()?;

    let both = timeout(Duration::from_secs(30), async {
        tokio::join!(reader.get(key.clone()), reader.get(key.clone()))
    })
    .await
    .map_err(|_| eyre!("the two reads never met: they did not overlap"))?;

    assert_eq!(
        both.0.map_err(|e| eyre!("first read: {e}"))?,
        Some(Value::from(7_i64)),
        "the first concurrent read"
    );
    assert_eq!(
        both.1.map_err(|e| eyre!("second read: {e}"))?,
        Some(Value::from(7_i64)),
        "the second concurrent read"
    );
    Ok(())
}

/// One session selects its source once. The first scoped operation probes for
/// a source and publishes its selection onto the session; every later
/// operation on that session seeds from that selection and addresses it
/// directly, so no source below it is probed again.
///
/// The lowest source is published but holds nothing, which is what makes the
/// re-probe observable: the probe must read it (and get `None`) before
/// reaching the source that answers, so a second probe shows up as a second
/// read of the empty source.
///
/// Falsify: have the reader engine start each invocation unselected, or stop
/// publishing the first selection back to the session. The second `get` then
/// re-probes and the empty source's read count rises to two.
#[tokio::test]
async fn one_session_selects_its_source_once() -> Result<()> {
    /// Sorts below [`GROUP_A`], so this source is probed first.
    const EMPTY_GROUP: &str = "group-000";

    let env = ScriptedEnv::new(
        value_state::<JsonCodec>("probe-select").read_cache(ReadCachePolicy::Disabled),
    )?;
    let key = Key::from("user-1");
    let tp_empty = topic("topic-empty");
    let tp_a = topic("topic-a");

    let empty = source_state_key(tp_empty, EMPTY_GROUP, &key, env.count)?.segment_id;
    env.publish(EMPTY_GROUP, tp_empty).await;
    let answering = env
        .commit(GROUP_A, tp_a, &key, 1, |handle| async move {
            handle
                .set(Value::from("A"))
                .await
                .map_err(|e| eyre!("set: {e}"))
        })
        .await?
        .segment_id;
    env.publish(GROUP_A, tp_a).await;

    let reader = env.reader_eager()?;
    let session = reader.session(key).await?;
    let handle = env.descriptor.bind(&session)?;

    assert_eq!(
        handle.get().await.map_err(|e| eyre!("first read: {e}"))?,
        Some(Value::from("A")),
        "the first source with data answers the first read"
    );
    assert_eq!(
        (env.cells.reads(empty), env.cells.reads(answering)),
        (1, 1),
        "the probe read the empty source, then the one that answered"
    );

    assert_eq!(
        handle.get().await.map_err(|e| eyre!("second read: {e}"))?,
        Some(Value::from("A")),
        "the selected source answers the second read"
    );
    assert_eq!(
        (env.cells.reads(empty), env.cells.reads(answering)),
        (1, 2),
        "the second read addressed the selection without probing again"
    );
    Ok(())
}
