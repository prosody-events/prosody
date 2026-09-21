//! Probe-and-pin over multiple sources, driven through the scripted fault
//! source.
//!
//! Selection picks the lowest-`SourceId` source that has data, and skips a
//! source that errored on open. Data always beats a skipped error. An
//! all-empty source set reads `None` or empty. No data plus at least one
//! source error reads `Err`. [`prop_probe_and_pin`] proves all of this
//! together over random fault scripts. It covers the point reads `get` and
//! `len` and the pinned scan `values`. [`prop_probe_and_pin_set`] proves
//! the same selection for set reads. A failed read never reports an empty set,
//! and the store error keeps its category.
//!
//! The [`focused`] tests cover invariants the script model does not express:
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
    DequeDescriptor, SetDescriptor, StateDescriptor, deque_state, map_state, set_state, value_state,
};
use crate::state::order_codec::{I64KeyCodec, Utf8KeyCodec};
use crate::state::{DequeQuery, KeyQuery};
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

/// A deque length above the point-get ceiling. It exercises the range-scan
/// stream arm.
const SCAN_ARM_LEN: usize = 130;

// --- Probe-and-pin property -------------------------------------------------

/// The ordered group pool the fault script assigns sources to. Lexicographic
/// order (`g0 < g1 < …`) makes `SourceId` order match index order. The
/// model's "lowest source" is therefore `sources[0]`.
const GROUP_POOL: [&str; 4] = ["probe-g0", "probe-g1", "probe-g2", "probe-g3"];

/// The per-source deque lengths the script draws from. The small lengths
/// exercise the chunked point-get stream arm. [`SCAN_ARM_LEN`] is one past
/// the range-scan ceiling and exercises that arm.
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
        // Use at least one source and at most the group pool.
        let n = 1 + usize::arbitrary(g) % GROUP_POOL.len();
        Self {
            sources: (0..n).map(|_| SourceDisposition::arbitrary(g)).collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Keep at least one source, because an empty snapshot is a different
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

/// Member `j` of source `idx`. Members sort in insertion order within a source.
fn member(idx: usize, j: usize) -> String {
    format!("{idx:02}-{j:04}")
}

/// The selection the point reads resolve to under a script. If any source
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

/// The lowest-`SourceId` source with committed data answers every read.
/// Selection skips an errored source, and data beats a skipped error. All
/// sources empty reads `None` or empty. No data plus an earlier error reads
/// `Err`. This holds for the point reads `get` and `len` and for the pinned
/// scan `values` in both directions.
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

/// Asserts the reader's point reads and scan match the selection the script
/// resolves to. The point reads are `len` and `get`. The scan is `values`.
async fn assert_probe(reader: &DequeReader, key: &Key, selection: Selection) -> Result<bool> {
    match selection {
        Selection::Pinned { idx, len } => {
            let expected: Vec<Value> = (0..len).map(|j| element(idx, j)).collect();
            let forward = Box::pin(collect_stream(
                reader
                    .values(key.clone(), DequeQuery::new(Direction::Forward))
                    .await?,
            ))
            .await?;
            let backward = Box::pin(collect_stream(
                reader
                    .values(key.clone(), DequeQuery::new(Direction::Backward))
                    .await?,
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
                .values(key.clone(), DequeQuery::new(Direction::Forward))
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
                reader
                    .values(key.clone(), DequeQuery::new(Direction::Forward))
                    .await?,
            ))
            .await?
            .is_empty()),
    }
}

/// Set reads follow the same selection as deque reads: `is_empty`,
/// `contains`, and the `keys` scan. A failed read never establishes that a set
/// is empty. The `ErrOnly` arm keeps the store error's transient category and
/// its message.
///
/// FALSIFICATION: make `membership::is_empty` treat a stream error as an empty
/// set. Every script with faults and no data then reports `true` instead of
/// `Err`.
#[test]
fn prop_probe_and_pin_set() {
    fn property(script: FaultScript) -> Result<bool> {
        block_on(run_probe_and_pin_set(script))
    }
    QuickCheck::new().quickcheck(property as fn(FaultScript) -> Result<bool>);
}

async fn run_probe_and_pin_set(script: FaultScript) -> Result<bool> {
    let env = ScriptedEnv::new(set_state::<Utf8KeyCodec>("probe-set"))?;
    let key = Key::from("user-1");

    for (idx, disposition) in script.sources.iter().enumerate() {
        let group = GROUP_POOL[idx];
        let tp = topic(group);
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
                            .insert(&member(idx, j))
                            .await
                            .map_err(|e| eyre!("insert: {e}"))?;
                    }
                    Ok(())
                })
                .await?;
            }
        }
        env.publish(group, tp).await;
    }

    let reader = env.reader_eager()?;
    Box::pin(assert_set_probe(&reader, &key, selection(&script))).await
}

/// The concrete set reader the set probe property drives.
type SetReader = StateReader<SetDescriptor<Utf8KeyCodec>, JsonCodec, ScriptedReaderBackend>;

/// Asserts the set reader's `is_empty`, `contains`, and `keys` match the
/// selection the script resolves to.
async fn assert_set_probe(reader: &SetReader, key: &Key, selection: Selection) -> Result<bool> {
    let keys = reader.keys(key.clone(), KeyQuery::new(Direction::Forward));
    match selection {
        Selection::Pinned { idx, len } => {
            let expected: Vec<String> = (0..len).map(|j| member(idx, j)).collect();
            Ok(!reader.is_empty(key.clone()).await?
                && reader.contains(key.clone(), &member(idx, 0)).await?
                && Box::pin(collect_stream(keys.await?)).await? == expected)
        }
        Selection::ErrOnly => {
            // No data through a failed source: absence is not provable, so the
            // read errors and the store error keeps its category and message.
            let Err(error) = reader.is_empty(key.clone()).await else {
                return Ok(false);
            };
            Ok(error.classify_error() == ErrorCategory::Transient
                && matches!(
                    &error,
                    StateReaderError::Store { message, .. }
                        if message.contains("scripted cell-source fault")
                )
                && reader.contains(key.clone(), &member(0, 0)).await.is_err())
        }
        Selection::EmptyOnly => Ok(reader.is_empty(key.clone()).await?
            && !reader.contains(key.clone(), &member(0, 0)).await?
            && Box::pin(collect_stream(keys.await?)).await?.is_empty()),
    }
}

mod focused;
