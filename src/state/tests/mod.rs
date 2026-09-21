use crate::state::CommitDecision;
use crate::state::store::CellRead;
use crate::state::store::CommittedBatch;
use crate::state::tests::support::{StageInspection, evidence};
use crate::test_util::TEST_RUNTIME;
mod cached_suite;
pub(crate) mod cell_suite;
pub(crate) mod collection_suite;
mod gate_suite;
pub(crate) mod identity_suite;
pub(crate) mod publication_suite;
pub(crate) mod support;

use self::cell_suite::{
    ApplyTrace, BatchReadTrace, FailingCellStore, MemoryDeduplicationStore, MemoryShapeProbe,
    OverlayTrace, OverwriteTrace, PoisonHandle, RawBatchTrace, ScanTrace, Trace,
    run_apply_idempotence, run_batch_alignment, run_batch_duplicate_co_observation,
    run_batch_read_parity_trace, run_blind_write_leaves_clears_free_marker, run_bottom_scan_trace,
    run_crash_equivalence_trace, run_overlay_precedence_pin, run_overlay_trace,
    run_overwrite_trace, run_raw_batch_ascending_output, run_raw_batch_no_side_effects,
    run_raw_batch_parity_trace,
};
use self::cell_suite::{SECTIONS, bytes, cell_in};
use self::collection_suite::{
    DequeCapacityShape, DequeConstraints, DequeHoles, DequeInterleave, DequeTrace, MapGetManyInput,
    MapInterleave, MapKeyHoles, MapTrace, StreamConstraints, finalize_and_promote,
    run_deque_capacity_convergence, run_deque_constraint_parity, run_deque_holes,
    run_deque_stream_interleave, run_deque_trace, run_map_get_many_parity_trace,
    run_map_key_scan_holes, run_map_keyset_exact_trace, run_map_query_trace,
    run_map_stream_interleave, run_map_ttl_keyset_refresh_trace,
};
use self::publication_suite::{PublicationTrace, run_publication_trace};
use self::support::{CountingCellStore, CountingResolver, ResolveCounter, fresh_collection};
use super::cell::{Cell, Committed, ProvisionalWrite, Values};
use super::cell_key::CellKey;
use super::descriptor::{StateDescriptor, WithResolver, deque, deque_state, map_state};
use super::marker::EventMarker;
use super::memory::{
    MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore,
};
use super::order_codec::{I64KeyCodec, OrderedKeyCodec};
use super::registry::{CollectionDef, CollectionDefRegistry};
use super::resolve::{EvidenceLookup, resolve_event_marker};
use super::session::{KeyedStateSession, SessionParts, TerminationWatch};
use super::store::{CELL_BATCH, CellBuffer, CellStore, CoordinateBatch, dedupe};
use super::{
    CELLS_INLINE, CollectionId, CollectionRef, CommitMode, Coordinate, Direction, EventRef,
    PartitionBackend, StateKey, StateName, StateType,
};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::{StreamExt, pin_mut, stream};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::sync::watch;
use uuid::Uuid;

/// The bounded-deque capacity the lifecycle properties run under. `match`, not
/// `NonZeroUsize::new(..).unwrap_or(..)`: `Option::unwrap_or` is not const, and
/// the tests forbid `unwrap`.
const BOUNDED_TEST_CAP: NonZeroUsize = match NonZeroUsize::new(2) {
    Some(n) => n,
    None => NonZeroUsize::MIN,
};

/// The default batch read preserves the TTL from each projected point read.
#[test]
fn forwarding_default_preserves_ttl() -> Result<()> {
    use self::support::TtlStub;

    let ttl = CompactDuration::new(3_600);
    let store = TtlStub::new(bytes(7), Some(ttl));
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("key")),
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let batch = CoordinateBatch::chunks([0u8, 1].map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("non-empty read list must yield one batch"))?;
    let got = TEST_RUNTIME.block_on(async {
        CellRead::<Values>::read_many(&store, &id, SECTIONS[0], &batch).await
    })?;
    assert_eq!(got.len(), 2, "every position answered");
    for (_, remaining) in &got {
        assert_eq!(
            *remaining,
            Some(ttl),
            "the inherited default carries the TTL through"
        );
    }
    Ok(())
}

/// The per-partition backend over a [`CountingCellStore`], so a directed test
/// can test the lower-store scan count a collection op issues.
type CountingBackend = PartitionBackend<
    MemoryDeduplicationStore,
    MemoryDescriptorIdentityStore,
    CountingCellStore<MemoryCellStore>,
    (),
>;

/// Mints a session over `counting` carrying `loader` for one event. Dropped
/// senders are fine — `watch::Receiver::borrow` keeps returning the last value.
fn session_with_loader<L>(
    counting: &CountingCellStore<MemoryCellStore>,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
    loader: L,
) -> KeyedStateSession<CountingBackend, L> {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    KeyedStateSession::new(SessionParts::<CountingBackend, _> {
        cell: counting.clone(),
        dirty: Arc::default(),
        dedup: dedup.clone(),
        loader,
        registry: registry.clone(),
        state_key: state_key.clone(),
        event,
        dedup_ttl: CompactDuration::new(30),
        checks: (),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    })
}

/// Mints a session over `counting` for one event with the default in-memory
/// loader.
pub(super) fn counting_session(
    counting: &CountingCellStore<MemoryCellStore>,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
) -> KeyedStateSession<CountingBackend, MemoryLoader<Value>> {
    session_with_loader(
        counting,
        dedup,
        registry,
        state_key,
        event,
        MemoryLoader::new(),
    )
}

mod cells;
mod laziness;
mod map;
mod models;
mod projections;
mod windows;
use laziness::*;
mod markers;
