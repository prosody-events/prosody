//! Deterministic KV4 pins for the per-event session operation gate.
//!
//! The sequential transparency property cannot exercise `join!`-shaped
//! schedules, so these pins force them: a real [`KeyedStateSession`] over
//! `Cached<HoldingCellStore<CountingCellStore<Memory>>>`, with the holding
//! seam withholding one lower response so the racing op is **forced** into
//! the bad interleaving (a post-race assert alone proves nothing). Each pin
//! asserts the outcome equals *some* serial order of the two ops; each goes
//! red by deleting the permit acquisition in the relevant handle op. A
//! cancel-safety pin covers the futurelock posture's safe half (dropping a
//! holding or queued session-op future releases the gate), and the closure
//! pin proves settlement fences mutators while post-settle reads still
//! answer.
//!
//! Per-test `current_thread` runtimes keep the schedules deterministic: a
//! spawned op only progresses while the test body awaits, so "parked on the
//! gate" and "parked in the hold" are stable states the test observes via the
//! hold's `entered` signal, never via timing.

use super::super::cached::Cached;
use super::super::descriptor::{
    CellStateError, StateDescriptor, deque, deque_state, map_state, value_state,
};
use super::super::manager::ArmedKeys;
use super::super::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use super::super::order_codec::{I64KeyCodec, OrderedKeyCodec};
use super::super::registry::{CollectionDef, CollectionDefRegistry};
use super::super::session::sealed::StateLifecycle;
use super::super::session::{KeyedStateSession, SessionParts, TerminationWatch};
use super::super::store::CellStore;
use super::super::{
    CollectionId, CollectionRef, Direction, PartitionBackend, StateAccessError, StateKey,
    StateName, StateType,
};
use super::cell_suite::{ScriptedOracle, value_cell};
use super::collection_suite::finalize_and_promote;
use super::support::{CountingCellStore, HoldingCellStore, Holds, probe};
use crate::codec::{JsonCodec, JsonCodecError};
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;

use super::super::fjall::test_db;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Report, Result, bail, eyre};
use futures::StreamExt;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::time::timeout;
use uuid::Uuid;

/// The hang-guard for acquisitions that must proceed — never the assertion.
const HANG_GUARD: Duration = Duration::from_secs(30);

/// Yields until a just-spawned task has reached its park point (the gate
/// acquire); 8 yields covers the deepest spawn → acquire chain, and the
/// pins stay correct regardless (the gate serializes either way).
async fn let_task_park() {
    for _ in 0..8_u8 {
        yield_now().await;
    }
}

/// The gate suite's lower store: holds beneath counters beneath memory.
type GateStore = HoldingCellStore<CountingCellStore<MemoryCellStore<ScriptedOracle>>>;

/// The per-partition backend the gate-suite sessions run over.
type GateBackend =
    PartitionBackend<ScriptedOracle, MemoryDescriptorIdentityStore, Cached<GateStore>>;

/// One test's fixture: the composed cache, its seams, and session minting.
struct GateFixture {
    cached: Cached<GateStore>,
    counting: CountingCellStore<MemoryCellStore<ScriptedOracle>>,
    holds: Arc<Holds>,
    cells: MemoryCells,
    oracle: ScriptedOracle,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    armed: ArmedKeys,
}

impl GateFixture {
    /// Builds the fixture over the shared fjall database keyspace `name`,
    /// registering the suite's value/map/deque collections.
    fn new(name: &str) -> Result<Self> {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let mut registry = CollectionDefRegistry::new(None);
        registry.register(&value_state::<JsonCodec>("v"), CollectionDef::new(None))?;
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("m"),
            CollectionDef::new(None),
        )?;
        registry.register(&deque_state::<JsonCodec>("d"), CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(
            cells.clone(),
            oracle.clone(),
            registry.clone(),
        ));
        let holding = HoldingCellStore::new(counting.clone());
        let holds = holding.holds();
        let cached = Cached::new(test_db::cache(name)?, holding);
        Ok(Self {
            cached,
            counting,
            holds,
            cells,
            oracle,
            registry,
            state_key: StateKey::new(Uuid::new_v4(), Arc::from("key")),
            armed: Arc::default(),
        })
    }

    /// Mints a session for dedup id `n`. Dropped senders are fine —
    /// `watch::Receiver::borrow` keeps returning the last value.
    fn session(&self, n: u128) -> KeyedStateSession<GateBackend, MemoryLoader<Value>> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        KeyedStateSession::new(SessionParts::<GateBackend, _> {
            cell: self.cached.clone(),
            dirty: Arc::default(),
            oracle: self.oracle.clone(),
            loader: MemoryLoader::new(),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event: probe(n),
            recovery_delay: CompactDuration::new(30),
            armed: self.armed.clone(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        })
    }

    /// The [`CollectionId`] of the registered collection `name`.
    fn id(&self, name: &str) -> Result<CollectionId> {
        Ok(CollectionId::new(
            self.state_key.clone(),
            StateType::Application,
            StateName::try_new(name)?,
        ))
    }
}

/// A fresh single-thread runtime per pin, so spawned ops progress only while
/// the test awaits — the deterministic-schedule requirement.
fn runtime() -> Result<Runtime> {
    Ok(Builder::new_current_thread().enable_all().build()?)
}

/// KV4 pin (a): a get-fill suspended across a `commit()` of the same cell.
/// The harness FORCES the round-2 schedule — the fill's lower read completes
/// (linearizing before the commit's durable write), its publish is withheld,
/// the commit's write-through would land, then the fill resumes. With the
/// gate, the commit parks until the whole get (read + publish) completes, so
/// the re-get answers the committed value WARM (zero lower reads). Red-proven
/// by deleting the handle-level permit acquisition: the stale fill publish
/// overwrites the commit's write-through and the warm re-get answers the
/// pre-commit value.
#[test]
fn gate_serializes_fill_against_commit() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_fill_commit")?;
        let cref = CollectionRef::new(fx.id("v")?, None);
        // Committed base "A", seeded beneath the cache so the fill is cold.
        fx.counting
            .write_resolved(
                &cref,
                &[(
                    value_cell(),
                    Some(Bytes::from(serde_json::to_vec(&Value::from("A"))?)),
                )],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Suspend the fill after its lower read, before its publish.
        fx.holds.get_for_cache().arm(1);
        let get_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.get().await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the fill never reached its hold"))?;

        // The racing set+commit parks on the gate the get holds.
        let commit_task = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle.set(Value::from("B")).await?;
                handle.commit().await?;
                Ok::<_, CellStateError<JsonCodecError>>(())
            }
        });
        let_task_park().await;
        fx.holds.get_for_cache().release();

        let got = timeout(HANG_GUARD, get_task)
            .await
            .map_err(|_| eyre!("get hung"))??
            .map_err(|e| eyre!("get: {e}"))?;
        assert_eq!(
            got,
            Some(Value::from("A")),
            "the fill read the pre-commit value"
        );
        timeout(HANG_GUARD, commit_task)
            .await
            .map_err(|_| eyre!("commit hung"))??
            .map_err(|e| eyre!("commit: {e}"))?;

        // The serial-order assert, on the BUDGET (a value-only assert could be
        // healed by a fall-through): the re-get answers the committed B warm.
        fx.counting.reset();
        let after = handle.get().await.map_err(|e| eyre!("re-get: {e}"))?;
        assert_eq!(
            after,
            Some(Value::from("B")),
            "the fill never overwrote the newer write-through"
        );
        assert_eq!(
            fx.counting.lower_reads(),
            0,
            "the re-get is warm — the commit's write-through survived the suspended fill"
        );
        Ok(())
    })
}

/// KV4 pin (b): a `set` racing `commit()`'s snapshot→drain window. The
/// commit's lower write is withheld after it lands; the racing set parks on
/// the gate, so its cell is buffered strictly after the drain and survives to
/// the settle — nothing is lost. Red-proven by deleting the permit
/// acquisition: the set buffers into the snapshot→drain window and the drain
/// silently drops it (the pre-existing lost-update this gate closes).
#[test]
fn gate_serializes_set_against_commit_drain() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_set_drain")?;
        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        handle
            .set(1, Value::from(10_i64))
            .await
            .map_err(|e| eyre!("{e}"))?;

        // Withhold the commit's durable write (it lands; the response parks).
        fx.holds.write_resolved().arm(1);
        let commit_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.commit().await }
        });
        timeout(HANG_GUARD, fx.holds.write_resolved().entered())
            .await
            .map_err(|_| eyre!("the commit never reached its hold"))?;

        // The racing set parks on the gate the commit holds.
        let set_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(2, Value::from(20_i64)).await }
        });
        let_task_park().await;
        fx.holds.write_resolved().release();
        timeout(HANG_GUARD, commit_task)
            .await
            .map_err(|_| eyre!("commit hung"))??
            .map_err(|e| eyre!("commit: {e}"))?;
        timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;

        // Settle the event; BOTH cells must be durable — the parked set was
        // buffered after the drain, not swallowed by it.
        finalize_and_promote(
            &session,
            &fx.oracle,
            Uuid::from_u128(1),
            &fx.cells,
            &fx.id("m")?,
        )
        .await?;
        let verify = fx.session(2);
        let fresh = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&verify)
            .map_err(|e| eyre!("bind: {e}"))?;
        assert_eq!(
            fresh.get(&1).await.map_err(|e| eyre!("{e}"))?,
            Some(Value::from(10_i64)),
            "the committed cell survived"
        );
        assert_eq!(
            fresh.get(&2).await.map_err(|e| eyre!("{e}"))?,
            Some(Value::from(20_i64)),
            "the racing set was never dropped by the drain"
        );
        Ok(())
    })
}

/// KV4 pin (c): a `set` racing `clear()`. The set is suspended mid-body (its
/// bound-ratchet read withheld) while holding the gate; the clear parks
/// behind it, so the outcome equals the serial set-then-clear order — entry
/// AND bounds gone. Red-proven by deleting the permit acquisition: the clear
/// interleaves between the entry buffer and the bound ratchet, leaving a
/// half-applied state (entry gone, bounds present) no serial order explains.
#[test]
fn gate_serializes_set_against_clear() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_set_clear")?;
        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // The set buffers its entry, then parks at its Min-bound read (a cold
        // meta cell → a held lower read) while HOLDING the gate.
        fx.holds.get_for_cache().arm(1);
        let set_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(1, Value::from(10_i64)).await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the set never reached its hold"))?;

        let clear_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.clear().await }
        });
        let_task_park().await;
        fx.holds.get_for_cache().release();
        timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;
        timeout(HANG_GUARD, clear_task)
            .await
            .map_err(|_| eyre!("clear hung"))??
            .map_err(|e| eyre!("clear: {e}"))?;

        // Settle, then probe the physical state: the outcome must equal ONE
        // serial order — set-then-clear (all gone) or clear-then-set (entry
        // AND bounds present). The half-applied interleaving (entry gone,
        // bounds present) is what the gate excludes.
        finalize_and_promote(
            &session,
            &fx.oracle,
            Uuid::from_u128(1),
            &fx.cells,
            &fx.id("m")?,
        )
        .await?;
        let verify = fx.session(2);
        let fresh = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&verify)
            .map_err(|e| eyre!("bind: {e}"))?;
        let entry = fresh.get(&1).await.map_err(|e| eyre!("{e}"))?;
        let (min_cell, _) = super::super::descriptor::map::bound_cells();
        let min = fx
            .counting
            .get(&fx.id("m")?, &min_cell, probe(99))
            .await?
            .into_inner();
        assert_eq!(
            entry.is_some(),
            min.is_some(),
            "the outcome equals a serial order: entry and bounds live or die together"
        );
        Ok(())
    })
}

/// The ratchet-lost-update pin (the pre-existing `ratchet_bounds`
/// read-modify-write race the gate closes): two racing sets of the extremes
/// serialize under the gate, so the final bounds hold BOTH extremes and a
/// stream yields both entries. Red-proven by deleting the permit acquisition:
/// the parked set's stale bound reads overwrite the other's ratchet, the
/// bounds lose an extension, and the loose-superset invariant breaks (the
/// stream drops the out-of-bounds entry).
#[test]
fn gate_serializes_racing_ratchets() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_ratchet")?;
        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // set(1) parks at its bound read while holding the gate; set(9) parks
        // on the gate behind it.
        fx.holds.get_for_cache().arm(1);
        let first = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(1, Value::from(1_i64)).await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("set(1) never reached its hold"))?;
        let second = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(9, Value::from(9_i64)).await }
        });
        let_task_park().await;
        fx.holds.get_for_cache().release();
        timeout(HANG_GUARD, first)
            .await
            .map_err(|_| eyre!("set(1) hung"))??
            .map_err(|e| eyre!("set(1): {e}"))?;
        timeout(HANG_GUARD, second)
            .await
            .map_err(|_| eyre!("set(9) hung"))??
            .map_err(|e| eyre!("set(9): {e}"))?;

        finalize_and_promote(
            &session,
            &fx.oracle,
            Uuid::from_u128(1),
            &fx.cells,
            &fx.id("m")?,
        )
        .await?;
        let verify = fx.session(2);
        let fresh = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&verify)
            .map_err(|e| eyre!("bind: {e}"))?;
        let mut keys = Vec::new();
        {
            let stream = fresh.stream(Direction::Forward);
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (key, _) = item.map_err(|e| eyre!("stream: {e}"))?;
                keys.push(key);
            }
        }
        assert_eq!(
            keys,
            vec![1, 9],
            "serialized ratchets hold both extremes — no lost bound extension"
        );
        Ok(())
    })
}

/// Item 11's stream pin: the deque's bounded materialization holds the gate,
/// so a racing `pop_back` + `commit()` cannot interleave — the stream yields
/// the whole pre-commit window in order, and the commit applies after. The
/// window is wider than the prefetch width, so without the gate the parked
/// materialization's unlaunched tail reads WOULD observe the commit (the
/// red). Also re-checks the counting pin's scan half: zero scans (the full
/// `len + 1`-gets pin lives in `state::tests::deque_stream_issues_no_scans`).
#[test]
fn gate_excludes_commit_during_bounded_materialization() -> Result<()> {
    /// Wider than `WINDOW_PREFETCH` (16), under the scan threshold (128).
    const LEN: usize = 20;

    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_stream")?;
        // Seed the window beneath the cache (cold), valued by index.
        let id = fx.id("d")?;
        let dref = CollectionRef::new(id.clone(), None);
        let mut seeded = vec![(
            deque::meta_cell(),
            Some(Bytes::from(deque::seed_frame(0, i64::try_from(LEN)?))),
        )];
        for i in 0..LEN {
            let index = i64::try_from(i)?;
            seeded.push((
                deque::entry_cell_for(&I64KeyCodec::encode(&index)),
                Some(Bytes::from(serde_json::to_vec(&Value::from(index))?)),
            ));
        }
        fx.counting.write_resolved(&dref, &seeded, &[]).await?;

        let session = fx.session(1);
        let handle = deque_state::<JsonCodec>("d")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        // Warm the bounds cell so the armed hold lands on the FIRST ENTRY read.
        assert_eq!(handle.len().await.map_err(|e| eyre!("{e}"))?, LEN);

        fx.holds.get_for_cache().arm(1);
        let stream_task = tokio::spawn({
            let handle = handle.clone();
            async move {
                let mut out = Vec::new();
                let stream = handle.stream(Direction::Forward);
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    out.push(item?);
                }
                Ok::<_, deque::DequeStateError<JsonCodecError>>(out)
            }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the materialization never reached its hold"))?;

        // The racing pop+commit parks on the gate the materialization holds.
        let pop_task = tokio::spawn({
            let handle = handle.clone();
            async move {
                let popped = handle.pop_back().await.map_err(|e| eyre!("pop: {e}"))?;
                handle.commit().await.map_err(|e| eyre!("commit: {e}"))?;
                Ok::<_, Report>(popped)
            }
        });
        let_task_park().await;
        fx.counting.reset();
        fx.holds.get_for_cache().release();

        let yielded = timeout(HANG_GUARD, stream_task)
            .await
            .map_err(|_| eyre!("stream hung"))??
            .map_err(|e| eyre!("stream: {e}"))?;
        let want: Vec<Value> = (0..LEN)
            .map(|i| i64::try_from(i).map(Value::from))
            .collect::<Result<_, _>>()?;
        assert_eq!(
            yielded, want,
            "the materialization yields the whole pre-commit window, in index order"
        );
        assert_eq!(
            fx.counting.lower_scans(),
            0,
            "the bounded materialization issues no scans"
        );
        let popped = timeout(HANG_GUARD, pop_task)
            .await
            .map_err(|_| eyre!("pop hung"))??
            .map_err(|e| eyre!("{e}"))?;
        assert_eq!(
            popped,
            Some(Value::from(i64::try_from(LEN - 1)?)),
            "the pop applied after the materialization"
        );
        assert_eq!(
            handle.len().await.map_err(|e| eyre!("{e}"))?,
            LEN - 1,
            "the commit landed after the stream's window"
        );
        Ok(())
    })
}

/// The cancel-safety pin (the futurelock posture's safe half): dropping a
/// session-op future — while it HOLDS the gate, and while it is QUEUED on it —
/// releases the gate, so the next op and the settle acquire both proceed. The
/// hang-guard deadline is exactly that — a hang-guard, never the assertion.
#[test]
fn dropped_session_op_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_cancel")?;
        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Drop a HOLDING op: a get parked in its withheld fill, gate held.
        fx.holds.get_for_cache().arm(1);
        let holding = tokio::spawn({
            let handle = handle.clone();
            async move { handle.get().await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the holding op never reached its hold"))?;
        holding.abort();
        assert!(
            holding.await.is_err(),
            "the holding op was dropped mid-gate"
        );

        // The next op proceeds.
        timeout(HANG_GUARD, handle.set(Value::from(1_i64)))
            .await
            .map_err(|_| eyre!("hang-guard: the gate was not released by the drop"))?
            .map_err(|e| eyre!("set: {e}"))?;

        // Drop a QUEUED op: A holds (withheld), B queues, B is dropped, A
        // completes, and the settle acquire still proceeds. A holds via a map
        // key the session never buffered — the value cell's set above would
        // answer from the dirty overlay and never reach the withheld lower
        // read.
        let map = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        fx.holds.get_for_cache().arm(1);
        let holding = tokio::spawn({
            let map = map.clone();
            async move { map.get(&42).await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the second holding op never reached its hold"))?;
        let queued = tokio::spawn({
            let handle = handle.clone();
            async move { handle.get().await }
        });
        let_task_park().await;
        queued.abort();
        assert!(queued.await.is_err(), "the queued op was dropped");
        fx.holds.get_for_cache().release();
        timeout(HANG_GUARD, holding)
            .await
            .map_err(|_| eyre!("the holding op hung"))??
            .map_err(|e| eyre!("get: {e}"))?;

        // The settle acquire proceeds (the drop-is-safe futurelock half).
        let permit = timeout(HANG_GUARD, session.close_gate())
            .await
            .map_err(|_| eyre!("hang-guard: settle's close never acquired the gate"))?;
        drop(permit);
        Ok(())
    })
}

/// The closure pin (the mutator fence): after the settle boundary closes the
/// gate, a detached mutator errors [`StateAccessError::SessionClosed`] while
/// a read still answers — the post-settle apply-hook read contract, made
/// explicit at the session level (`hook_visibility` is its unmodified
/// middleware-level witness).
#[test]
fn closed_session_fences_mutators_but_serves_hook_reads() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_closed")?;
        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        handle
            .set(Value::from(7_i64))
            .await
            .map_err(|e| eyre!("set: {e}"))?;
        finalize_and_promote(
            &session,
            &fx.oracle,
            Uuid::from_u128(1),
            &fx.cells,
            &fx.id("v")?,
        )
        .await?;

        // The settle boundary's close: acquire once, mark Closed, drop the
        // permit before the hooks fire.
        let permit = session.close_gate().await;
        drop(permit);

        // A detached mutator errors SessionClosed.
        let denied = handle.set(Value::from(8_i64)).await;
        match denied {
            Err(CellStateError::Access(StateAccessError::SessionClosed)) => {}
            other => bail!("a closed session must fence mutators, got {other:?}"),
        }
        // rollback answers NoOp (its infallible containment posture).
        assert_eq!(
            handle.rollback().await,
            super::super::StoreOutcome::NoOp,
            "rollback on a closed session discards nothing"
        );
        // A read still answers — the apply hooks read state through it.
        assert_eq!(
            handle.get().await.map_err(|e| eyre!("hook read: {e}"))?,
            Some(Value::from(7_i64)),
            "post-settle reads serve the settled state"
        );
        Ok(())
    })
}
