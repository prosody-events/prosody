//! Properties and pins for the scoped collection operations.
//!
//! The subject is the invocation, not any one collection: a probe layout and a
//! probe handle exercise the generated expansions and the write journal
//! directly, so the invariants hold for every collection that runs through the
//! same scope.
//!
//! The flagship is [`prop_write_invocations_are_atomic`], a trace/model
//! property over generated invocations. Its model is a plain map: the journal's
//! reverse-order fold must answer every in-invocation read, a successful merge
//! must leave the event overlay exactly at the model, and every other exit must
//! leave the overlay exactly as the invocation found it.
//!
//! The sibling [`plans`] module pins the managed stream drivers that a plan
//! feeds. It covers order, error termination, the per-emission fence, and the
//! resolve fan-out.

mod plans;

use super::{
    CellFamily, Collection, CollectionLayout, CollectionRead, CollectionWrite, JOURNAL_INLINE,
    StateSession, collection_layout, collection_methods, decode_cell,
};
use crate::codec::{I64Codec, I64CodecError};
use crate::consumer::middleware::RepinProof;
use crate::loader::MemoryLoader;
use crate::state::cached::Cached;
use crate::state::cell::Values;
use crate::state::cell_key::CellKey;
use crate::state::descriptor::tests::{session_over, session_with_dirty, value_registry};
use crate::state::descriptor::{
    CellStateError, Keyed, StateDescriptor, StructuralIdentity, ValueDescriptor, value_state,
};
use crate::state::dirty::DirtyStore;
use crate::state::fjall::test_db;
use crate::state::identity::CollectionId;
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::session::sealed::StateLifecycle;
use crate::state::store::CELL_BATCH;
use crate::state::tests::support::CountingCellStore;
use crate::state::{CollectionKindId, StateAccessError, StateKey, StateType};
use crate::test_util::TEST_RUNTIME;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use educe::Educe;
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::BTreeMap;
use std::future::Future;
use std::iter::{empty, once};
use std::pin::pin;
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

mod invocation;
mod reads;

/// The probe collection's registered name.
const PROBE: &str = "pair-probe";

/// The probe collection's cell type: an `i64` value addressed by an `i64` key.
type ProbeCell = Keyed<I64KeyCodec, I64Codec>;

/// The probe collection's error type — the shape a real collection's methods
/// return, so the write scope's `From<StateAccessError>` requirement is
/// exercised rather than assumed.
type ProbeError = CellStateError<I64CodecError>;

collection_layout! {
    /// A two-family probe layout with a deliberate gap between its ids and a
    /// deliberate mismatch between declaration order and id order.
    struct PairLayout {
        /// The higher id, declared first.
        #[id(3)]
        RIGHT: ProbeCell,
        /// The lower id, declared second.
        #[id(0)]
        LEFT: ProbeCell,
    }
}

/// A handle over the probe collection, shaped exactly like a real one.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
struct PairHandle<S> {
    cells: Collection<S, PairLayout>,
}

/// Returns a read future after its local key leaves scope.
fn read_family<C>(
    op: &mut C,
    family: CellFamily<C::Layout, ProbeCell>,
    key: i64,
) -> impl Future<Output = Result<Option<i64>, ProbeError>> + use<'_, C>
where
    C: CollectionRead,
{
    op.get(family, &key)
}

/// The mutating twin of [`read_family`].
fn stage_pair<C>(op: &mut C, key: i64, left: i64, right: i64) -> Result<(), ProbeError>
where
    C: CollectionWrite<Layout = PairLayout>,
{
    op.set(PairLayout::LEFT.at(&key), left)?;
    op.set(PairLayout::RIGHT.at(&key), right)
}

#[collection_methods(field = cells, session = S)]
impl<S> PairHandle<S>
where
    S: StateSession,
{
    /// One read command, issued through a free helper.
    #[read(op)]
    async fn left(&self, key: i64) -> Result<Option<i64>, ProbeError> {
        read_family(op, PairLayout::LEFT, key).await
    }

    /// Several commands in one invocation, including a read of a staged write.
    #[write(op)]
    async fn swap(&self, key: i64, left: i64, right: i64) -> Result<Option<i64>, ProbeError> {
        let previous = read_family(op, PairLayout::LEFT, key).await?;
        stage_pair(op, key, left, right)?;
        let staged = read_family(op, PairLayout::LEFT, key).await?;
        assert_eq!(staged, Some(left), "a staged write must read back");
        assert!(!op.journal_spilled(), "two mutations stay inline");
        Ok(previous)
    }

    /// An invocation that stages and then fails.
    #[write(op)]
    async fn stage_then_fail(&self, key: i64, left: i64, right: i64) -> Result<(), ProbeError> {
        stage_pair(op, key, left, right)?;
        Err(CellStateError::Access(StateAccessError::Unavailable))
    }

    /// Takes `LEFT[key]`, **swallows** the result, stages an unrelated write,
    /// and reports whether the take succeeded. The invocation therefore merges
    /// on both paths, and the overlay shows what the take staged.
    ///
    /// This is the only shape that exposes a *failed* command's journal
    /// contribution. An invocation that propagates the error drops the whole
    /// journal.
    #[write(op)]
    async fn take_swallowing(&self, key: i64, marker: i64) -> Result<bool, ProbeError> {
        let took = op.take(PairLayout::LEFT, &key).await.is_ok();
        op.set(PairLayout::RIGHT.at(&key), marker)?;
        Ok(took)
    }
}

/// The registered descriptor the probe binds against. A layout brand is
/// independent of the durable identity, so a Value-kind registration admits a
/// two-family probe.
fn probe_descriptor() -> ValueDescriptor<I64Codec> {
    value_state(PROBE)
}

/// Binds the probe handle over `session`.
fn bind_probe<S: StateSession>(session: &S) -> Result<PairHandle<S>> {
    let collection = Collection::bind(
        session,
        PROBE,
        StateType::Application,
        &StructuralIdentity::of::<I64Codec>(CollectionKindId::Value),
    )
    .map_err(|e| eyre!("probe bind failed: {e}"))?;
    Ok(PairHandle { cells: collection })
}

/// The overlay's staged cells for the probe collection, decoded back into the
/// model's shape. Read straight off the dirty store: nothing resolves, caches,
/// or repairs between a merge and this observation, so nothing can heal a
/// mutation that should not be there.
fn staged_state(dirty: &DirtyStore, id: &CollectionId) -> Result<BTreeMap<(i8, i64), Option<i64>>> {
    let mut state = BTreeMap::new();
    for (cell, data) in dirty.collection_snapshot(id) {
        let key = I64KeyCodec::decode(cell.coordinate.as_bytes())
            .map_err(|e| eyre!("staged coordinate did not decode: {e}"))?;
        let value = match data {
            Some(bytes) => Some(
                decode_cell::<I64Codec>(bytes)
                    .map_err(|e| eyre!("staged cell did not decode: {e}"))?,
            ),
            None => None,
        };
        state.insert((i8::from(cell.section), key), value);
    }
    Ok(state)
}

/// The generated read and write expansions run, not merely typecheck: a
/// multi-command write stages both families and a later invocation sees them.
#[test]
fn generated_expansions_stage_and_read_real_values() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, _dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key);
        let handle = bind_probe(&session)?;

        assert_eq!(handle.left(7).await?, None, "an unset family reads absent");
        assert_eq!(
            handle.swap(7, 11, 22).await?,
            None,
            "the first swap reports no previous value"
        );
        assert_eq!(handle.left(7).await?, Some(11), "the staged left value");
        assert_eq!(
            handle.swap(7, 33, 44).await?,
            Some(11),
            "the second swap reports the first swap's value"
        );
        Ok(())
    })
}

/// A failed invocation leaves the overlay exactly as it was — observed on the
/// raw dirty store, with nothing in between that could heal it.
#[test]
fn failed_write_leaves_the_overlay_unchanged() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key.clone());
        let handle = bind_probe(&session)?;
        let id = CollectionId::new(
            state_key,
            StateType::Application,
            handle.cells.name().clone(),
        );

        handle.swap(1, 10, 20).await?;
        let before = staged_state(&dirty, &id)?;
        assert!(
            handle.stage_then_fail(1, 99, 98).await.is_err(),
            "the invocation must surface its authored error"
        );
        assert_eq!(
            staged_state(&dirty, &id)?,
            before,
            "the failed invocation's mutations must not reach the overlay"
        );
        Ok(())
    })
}

/// A `take` whose read fails stages nothing. The addressed cell keeps the bytes
/// that did not decode. A staged clear would instead have replayed an absence.
///
/// The probe **swallows** the take's error and returns `Ok`. That is the only
/// shape that exposes a failed command's journal contribution, because an
/// invocation that propagates the error drops the whole journal. The test seeds
/// the bytes straight into the overlay, then reads the overlay back raw. No
/// layer between them can heal a clear that must not be there.
#[test]
fn take_error_does_not_clear() -> Result<()> {
    /// Bytes no `I64Codec` cell can decode.
    const BAD: &[u8] = b"not an i64";
    const KEY: i64 = 5;

    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key.clone());
        let handle = bind_probe(&session)?;
        let name = handle.cells.name().clone();
        let id = CollectionId::new(state_key, StateType::Application, name.clone());
        let left = CellKey {
            section: PairLayout::LEFT.section(),
            coordinate: I64KeyCodec::encode(&KEY),
        };
        session
            .seed(StateType::Application, &name, &left, Some(BAD))
            .await;

        assert!(
            !handle.take_swallowing(KEY, 42).await?,
            "the seeded bytes must not decode, so the take must have failed"
        );

        let staged: BTreeMap<CellKey, Option<Bytes>> =
            dirty.collection_snapshot(&id).into_iter().collect();
        assert_eq!(
            staged
                .get(&left)
                .and_then(Option::as_ref)
                .map(Bytes::as_ref),
            Some(BAD),
            "a failed take must leave the addressed cell exactly as it found it"
        );
        assert!(
            staged.contains_key(&CellKey {
                section: PairLayout::RIGHT.section(),
                coordinate: I64KeyCodec::encode(&KEY),
            }),
            "the invocation still merged the write staged after the failed take"
        );
        Ok(())
    })
}

/// A cancelled invocation drops its journal and releases admission: the overlay
/// is unchanged and the next invocation acquires the gate immediately.
///
/// The invocation future is polled to `Pending` inside its scope and then
/// dropped — no clock and no sleep.
#[test]
fn cancelled_write_drops_the_journal_and_releases_admission() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key.clone());
        let handle = bind_probe(&session)?;
        handle.swap(2, 1, 2).await?;
        let id = CollectionId::new(
            state_key,
            StateType::Application,
            handle.cells.name().clone(),
        );
        let before = staged_state(&dirty, &id)?;

        let parked = Notify::new();
        let pending = {
            let mut invocation = pin!(handle.cells.write(async |op| {
                op.set(PairLayout::LEFT.at(&2), 77)?;
                parked.notified().await;
                Ok::<(), ProbeError>(())
            }));
            futures::poll!(invocation.as_mut()).is_pending()
        };
        assert!(pending, "the invocation must park inside its scope");

        assert_eq!(
            staged_state(&dirty, &id)?,
            before,
            "a cancelled invocation stages nothing"
        );
        // Admission is RAII: this call would hang if the dropped invocation had
        // leaked its permit, so completing it is the assertion.
        assert_eq!(
            handle.left(2).await?,
            Some(1),
            "the pre-cancel value stands"
        );
        Ok(())
    })
}

/// The generated section set follows the declared ids, not the declaration
/// order, and a family token addresses exactly its declared id.
#[test]
fn generated_layout_is_id_sorted() {
    assert_eq!(
        <PairLayout as CollectionLayout>::SECTIONS
            .iter()
            .map(|section| i8::from(*section))
            .collect::<Vec<_>>(),
        vec![0, 3],
        "the canonical section set is id-sorted"
    );
    assert!(
        <PairLayout as CollectionLayout>::RESERVED.is_empty(),
        "the probe layout has removed no family"
    );
    assert_eq!(
        i8::from(PairLayout::LEFT.section()),
        0,
        "a family token addresses its declared id"
    );
}
