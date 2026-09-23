//! Read-committed staging reads committed bases in batches.
//!
//! A stage reads its survivors' bases with one batch read per section chunk
//! and no point reads. Each chunk's bases pair with exactly the dirty records
//! that produced it. The query-count property derives its expected batch
//! count from the dirty input. The pairing tests seed distinct bases and
//! abort, so a reversed pairing diverges.

use super::stage::{StageOp, apply_stage_ops, cell_in, replay_dirty, touched_cells};
use super::{Fixture, message};
use crate::codec::JsonCodec;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StateAccessError;
use crate::state::cell::Values;
use crate::state::cell_key::{CellKey, Section};
use crate::state::descriptor::value_state;
use crate::state::dirty::{DirtyStore, DirtyVal};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::Promoted;
use crate::state::session::sealed::StateLifecycle;
use crate::state::session::{Finalized, KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::{CELL_BATCH, CellRead};
use crate::state::tests::cell_suite::{MemoryDeduplicationStore, cell_at};
use crate::state::tests::support::{CountingCellStore, admit_collection};
use crate::state::{
    CollectionId, CollectionRef, CommitMode, EventRef, PartitionBackend, StateKey, StateName,
    StateType,
};
use crate::test_util::TEST_RUNTIME;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

/// The counting cell store the query-count fixture mints: a
/// [`CountingCellStore`] over shared in-memory cells, so `batch_reads` /
/// `visible_point_reads` count exactly the stage's `get_many` / `get`.
type CountingCell = CountingCellStore<MemoryCellStore>;
type CountingBackend =
    PartitionBackend<MemoryDeduplicationStore, MemoryDescriptorIdentityStore, CountingCell, ()>;
type CountingSession = KeyedStateSession<CountingBackend, ()>;

/// A single-collection session over a fresh key (row isolation) whose lower
/// store counts every read the stage issues. Keeps the counting-store handle
/// (shares the session's op counters), the shared dirty workspace (read for the
/// stage's input truth before `finalize` drains it), the dedup store (records
/// the message marker so a promoted stage's committed projection resolves), and
/// the collection id / name.
struct CountingFixture {
    session: CountingSession,
    counting: CountingCell,
    dirty: Arc<DirtyStore>,
    dedup: MemoryDeduplicationStore,
    id: CollectionId,
    name: StateName,
    state_key: StateKey,
    dedup_id: Uuid,
    _shutdown_tx: watch::Sender<ShutdownPhase>,
    _cancel_tx: watch::Sender<bool>,
}

impl CountingFixture {
    /// One Value collection named `name`; `read_uncommitted` selects its commit
    /// mode (the registry reads the mode from the `CollectionDef`, not the
    /// descriptor).
    fn new(read_uncommitted: bool, name: &str) -> Result<Self> {
        let state_name = StateName::try_new(name)?;
        let commit_mode = if read_uncommitted {
            CommitMode::ReadUncommitted
        } else {
            CommitMode::ReadCommitted
        };
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &value_state::<JsonCodec>(name),
            CollectionDef {
                commit_mode,
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let dedup = MemoryDeduplicationStore::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let dirty = Arc::new(DirtyStore::new());
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let dedup_id = Uuid::new_v4();
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let session = KeyedStateSession::new(SessionParts {
            cell: counting.clone(),
            dirty: dirty.clone(),
            dedup: dedup.clone(),
            loader: (),
            registry,
            state_key: state_key.clone(),
            event: EventRef::Message { dedup_id },
            dedup_ttl: CompactDuration::new(30),
            checks: (),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        });
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            state_name.clone(),
        );
        Ok(Self {
            session,
            counting,
            dirty,
            dedup,
            id,
            name: state_name,
            state_key,
            dedup_id,
            _shutdown_tx: shutdown_tx,
            _cancel_tx: cancel_tx,
        })
    }

    /// The `get_many` batches the RC stage will issue, derived the way
    /// `stage_collection` groups its input: survivors (dirty cells not subsumed
    /// by a section clear) per section, ceil-divided by `CELL_BATCH`. Zero when
    /// nothing survives. Read from the live dirty store before `finalize`
    /// drains it, so it is the stage's input, never its output.
    fn expected_batches(&self) -> usize {
        let touched = self.dirty.touched(&self.state_key.key);
        let Some((_, cleared, cells)) = touched.iter().find(|((_, n), ..)| *n == self.name) else {
            return 0;
        };
        let mut per_section: HashMap<Section, usize> = HashMap::new();
        for (cell, value) in cells {
            let subsumed = *value == DirtyVal::Cleared && cleared.contains(&cell.section);
            if !subsumed {
                *per_section.entry(cell.section).or_default() += 1;
            }
        }
        per_section
            .values()
            .map(|n| n.div_ceil(CELL_BATCH.get()))
            .sum()
    }

    /// Settles a finalized receipt so the committed projection is readable: a
    /// `Staged` receipt records the message marker (the boundary's post-stage
    /// order) then promotes. `Clean` (RU direct writes, or nothing staged)
    /// needs no settle.
    async fn settle(&self, finalized: Finalized<CountingCell, ()>) -> Result<()> {
        if let Finalized::Staged(staged) = finalized {
            self.dedup.insert(self.dedup_id).await?;
            assert!(matches!(staged.promote(|| false).await, Promoted::Complete));
        }
        Ok(())
    }

    /// Returns the durable committed value of `cell`.
    async fn committed(&self, cell: &CellKey) -> Result<Option<Bytes>> {
        Ok(
            CellRead::<Values>::read(&self.counting, &self.id, cell.as_ref())
                .await?
                .0
                .into_inner(),
        )
    }
}

/// A staging population: one op sequence under a chosen commit mode. `short`
/// makes the lower store drop the last answer of each base batch.
#[derive(Clone, Debug)]
struct StagePop {
    ru: bool,
    short: bool,
    ops: Vec<StageOp>,
}

impl Arbitrary for StagePop {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ru: bool::arbitrary(g),
            short: bool::arbitrary(g),
            ops: Vec::<StageOp>::arbitrary(g).into_iter().take(40).collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let (ru, short) = (self.ru, self.short);
        Box::new(self.ops.shrink().map(move |ops| Self { ru, short, ops }))
    }
}

/// Drives one staging population through the real `finalize` and checks the
/// query-count law plus the committed projection.
async fn run_stage_query_counts(pop: StagePop) -> Result<()> {
    let fx = CountingFixture::new(pop.ru, "qc")?;
    apply_stage_ops(&fx.session, &fx.name, &pop.ops).await;

    // The expected batch count, derived from the stage's dirty input.
    let expected_batches = fx.expected_batches();
    fx.counting.reset();
    if pop.short {
        fx.counting.short_batches();
    }
    let finalized = fx.session.finalize().await;

    // A short base batch fails the stage as Transient, so the source never
    // commits without its state. It writes nothing and keeps the dirty input
    // whole.
    if pop.short && !pop.ru && expected_batches > 0 {
        let Err(error) = finalized else {
            bail!("a short base batch must fail the stage");
        };
        if !matches!(error, StateAccessError::MisalignedBatch(_))
            || error.classify_error() != ErrorCategory::Transient
        {
            bail!("a short base batch failed as {error:?}, expected a transient misaligned batch");
        }
        if fx.counting.durable_writes() != 0 {
            bail!(
                "a failed stage wrote {} times",
                fx.counting.durable_writes()
            );
        }
        if fx.expected_batches() != expected_batches {
            bail!("a failed stage changed its dirty input");
        }
        return Ok(());
    }
    let finalized = finalized?;

    // Query-count law: never a visible point read; RC issues exactly the
    // per-section batch count, RU reads no bases at all.
    if fx.counting.visible_point_reads() != 0 {
        bail!(
            "stage issued {} visible point reads, must be 0",
            fx.counting.visible_point_reads()
        );
    }
    let expected_reads = if pop.ru { 0 } else { expected_batches };
    if fx.counting.batch_reads() != expected_reads {
        bail!(
            "stage issued {} batch reads, expected {expected_reads}",
            fx.counting.batch_reads()
        );
    }

    // Committed projection parity: settle, then every touched cell equals the
    // model's `into_data` (Set → its bytes, Clear / cleared-section / untouched
    // → absent).
    fx.settle(finalized).await?;
    let (cells, _cleared) = replay_dirty(&pop.ops);
    for cell in &touched_cells(&pop.ops) {
        let expected = match cells.get(cell) {
            Some(DirtyVal::Set(b)) => Some(b.clone()),
            Some(DirtyVal::Cleared) | None => None,
        };
        let committed = fx.committed(cell).await?;
        if committed != expected {
            bail!("committed {cell:?} = {committed:?}, expected {expected:?}");
        }
    }
    Ok(())
}

/// The query-count law over random op sequences in both commit modes: the RC
/// stage reads committed bases in exactly `Σ_section ceil(survivors /
/// CELL_BATCH)` batches and zero point reads, RU reads no bases, and the
/// committed projection tracks the model regardless. A short base batch fails
/// an RC stage before any durable write.
#[test]
fn prop_stage_query_counts() {
    fn prop(pop: StagePop) -> TestResult {
        match TEST_RUNTIME.block_on(run_stage_query_counts(pop)) {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(StagePop) -> TestResult);
}

/// `n` distinct cells in one section stage in `ceil(n / CELL_BATCH)` batches
/// and zero point reads — the batch-boundary law at the 127/128/129 edges.
async fn stage_query_count_section_size(n: usize, expected_batches: usize) -> Result<()> {
    let fx = CountingFixture::new(false, "qc")?;
    for c in 0..n {
        fx.session
            .seed(
                StateType::Application,
                &fx.name,
                &cell_in(0, c as u8),
                Some(b"v"),
            )
            .await;
    }
    fx.counting.reset();
    let finalized = fx.session.finalize().await?;
    assert_eq!(fx.counting.visible_point_reads(), 0);
    assert_eq!(fx.counting.batch_reads(), expected_batches);
    fx.settle(finalized).await
}

#[tokio::test]
async fn stage_query_count_section_size_127() -> Result<()> {
    stage_query_count_section_size(127, 1).await
}

#[tokio::test]
async fn stage_query_count_section_size_128() -> Result<()> {
    stage_query_count_section_size(128, 1).await
}

#[tokio::test]
async fn stage_query_count_section_size_129() -> Result<()> {
    stage_query_count_section_size(129, 2).await
}

/// Batches split PER SECTION, never across: 130 cells in one section plus 5 in
/// another read in `ceil(130/128) + ceil(5/128) = 3` batches — a global count
/// split would be 2 and would feed `get_many` a cross-section batch.
#[tokio::test]
async fn stage_query_count_splits_per_section() -> Result<()> {
    let fx = CountingFixture::new(false, "qc")?;
    for c in 0..130u16 {
        fx.session
            .seed(
                StateType::Application,
                &fx.name,
                &cell_in(0, c as u8),
                Some(b"v"),
            )
            .await;
    }
    for c in 0..5u8 {
        fx.session
            .seed(StateType::Application, &fx.name, &cell_in(1, c), Some(b"v"))
            .await;
    }
    fx.counting.reset();
    let finalized = fx.session.finalize().await?;
    assert_eq!(fx.counting.visible_point_reads(), 0);
    assert_eq!(
        fx.counting.batch_reads(),
        3,
        "ceil(130/128) + ceil(5/128) = 2 + 1",
    );
    fx.settle(finalized).await
}

/// A stage with no surviving cells builds no batch and issues no read: a
/// section clear followed by a clear of one of its cells leaves only a
/// `Cleared` cell subsumed by the unsettled section marker. The clears-only
/// stage still writes its marker (a separate counter), but reads nothing.
#[tokio::test]
async fn stage_clears_only_issues_no_read() -> Result<()> {
    let fx = CountingFixture::new(false, "qc")?;
    fx.session
        .seed_section_clear(StateType::Application, &fx.name, Section::new(0))
        .await;
    fx.session
        .seed(StateType::Application, &fx.name, &cell_in(0, 7), None)
        .await;
    fx.counting.reset();
    let finalized = fx.session.finalize().await?;
    assert_eq!(
        fx.counting.batch_reads(),
        0,
        "a subsumed Cleared cell must build no batch",
    );
    assert_eq!(fx.counting.visible_point_reads(), 0);
    fx.settle(finalized).await
}

/// Each staged cell pairs with its OWN committed base: two same-section cells
/// with distinct committed bases `A`/`B`, overwritten and then aborted, restore
/// to `A`/`B` — a reversed base pairing would restore them swapped. The
/// deterministic pairing falsifier (query counts stay green under a swap).
#[tokio::test]
async fn stage_restores_distinct_bases_on_abort() -> Result<()> {
    let fx = Fixture::new()?;
    let c0 = cell_at(0);
    let c1 = cell_at(1);

    // Seed distinct committed bases A / B for the two same-section cells.
    let (event, dedup) = message(1);
    let session = fx.session(event).handle();
    session
        .seed(StateType::Application, &fx.value_name, &c0, Some(b"A"))
        .await;
    session
        .seed(StateType::Application, &fx.value_name, &c1, Some(b"B"))
        .await;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the seeding event must stage");
    };
    fx.dedup.insert(dedup).await?;
    assert!(matches!(staged.promote(|| false).await, Promoted::Complete));

    // Overwrite both, then abort: each cell rolls back to its own base.
    let (event, _dedup) = message(2);
    let session = fx.session(event).handle();
    session
        .seed(StateType::Application, &fx.value_name, &c0, Some(b"X"))
        .await;
    session
        .seed(StateType::Application, &fx.value_name, &c1, Some(b"Y"))
        .await;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the overwriting event must stage");
    };
    drop(staged);
    admit_collection(
        &fx.cell_store(),
        &fx.dedup,
        &CollectionRef::new(fx.value_id(), None),
    )
    .await?;

    assert_eq!(
        CellRead::<Values>::read(&fx.cell_store(), &fx.value_id(), c0.as_ref())
            .await?
            .0
            .into_inner(),
        Some(Bytes::from_static(b"A")),
        "c0 restored to its own base",
    );
    assert_eq!(
        CellRead::<Values>::read(&fx.cell_store(), &fx.value_id(), c1.as_ref())
            .await?
            .0
            .into_inner(),
        Some(Bytes::from_static(b"B")),
        "c1 restored to its own base",
    );
    Ok(())
}
