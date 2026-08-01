//! Deterministic KV4 pins for the per-event session operation gate.
//!
//! The sequential transparency property cannot exercise `join!`-shaped
//! schedules, so these pins force them: a real [`KeyedStateSession`] over
//! `Cached<HoldingCellStore<CountingCellStore<Memory>>>`, with the holding
//! seam withholding one lower response so the racing op is **forced** into
//! the bad interleaving (a post-race assert alone proves nothing). Each pin
//! asserts the outcome equals *some* serial order of the two ops; each goes
//! red by removing the relevant op's admission — deleting the handle-level
//! permit acquisition for the kinds that still take one, or making
//! `OwnerEngine::begin_write` hand back a witness over an already-released
//! permit for the kinds that run as scoped operations. A
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
    CellStateError, MapStateError, STREAM_CHUNK, StateDescriptor, deque, deque_state, map,
    map_state, value_state,
};
use super::super::dirty::DirtyStore;
use super::super::manager::ArmedKeys;
use super::super::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use super::super::order_codec::{I64KeyCodec, OrderedKeyCodec};
use super::super::registry::{CollectionDef, CollectionDefRegistry};
use super::super::session::sealed::StateLifecycle;
use super::super::session::{KeyedStateSession, SessionParts, TerminationWatch};
use super::super::store::{CELL_BATCH, CellStore};
use super::super::{
    CollectionId, CollectionRef, Direction, PartitionBackend, StateAccessError, StateKey,
    StateName, StateType, StoreOutcome,
};
use super::cell_suite::{ScriptedOracle, value_cell};
use super::collection_suite::finalize_and_promote;
use super::support::{CountingCellStore, HoldingCellStore, Holds, probe};
use crate::codec::{JsonCodec, JsonCodecError};
use crate::consumer::middleware::RepinProof;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;

use super::super::fjall::test_db;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::time::timeout;
use uuid::Uuid;

/// The hang-guard for acquisitions that must proceed — never the assertion.
const HANG_GUARD: Duration = Duration::from_secs(30);

/// The `*_stream_error_yield_releases_the_gate` pins seed exactly two items and
/// assert the first yielded item is the `Err` — chunk-atomicity, which requires
/// both items to land in one point-get chunk. At `STREAM_CHUNK == 1` each key
/// is its own chunk, the valid entry's `Ok` surfaces first, and the pins would
/// go red on correct code; enforce the premise so breaking it is uncompilable.
const _: () = assert!(
    STREAM_CHUNK >= 2,
    "the *_stream_error_yield pins need two items in one chunk to prove chunk-atomicity"
);

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
        let mut registry = CollectionDefRegistry::default();
        registry.register(&value_state::<JsonCodec>("v"), CollectionDef::new(None))?;
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("m"),
            CollectionDef::new(None),
        )?;
        registry.register(&deque_state::<JsonCodec>("d"), CollectionDef::new(None))?;
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("ks"),
            CollectionDef {
                keyset_limit: 3,
                ..CollectionDef::new(None)
            },
        )?;
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
        self.session_with_dirty(n, Arc::default())
    }

    /// [`Self::session`] over a caller-owned dirty workspace, so a pin can read
    /// exactly what an invocation staged.
    fn session_with_dirty(
        &self,
        n: u128,
        dirty: Arc<DirtyStore>,
    ) -> KeyedStateSession<GateBackend, MemoryLoader<Value>> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        KeyedStateSession::new(SessionParts::<GateBackend, _> {
            cell: self.cached.clone(),
            dirty,
            oracle: self.oracle.clone(),
            loader: MemoryLoader::new(),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event: probe(n),
            recovery_delay: CompactDuration::new(30),
            armed: self.armed.clone(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
            publisher: None,
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

/// KV4 — a get-fill suspended across a `commit()` of the same cell.
/// The harness FORCES the round-2 schedule — the fill's lower read completes
/// (linearizing before the commit's durable write), its publish is withheld,
/// the commit's write-through would land, then the fill resumes. With the
/// gate, the commit parks until the whole get (read + publish) completes, so
/// the re-get answers the committed value WARM (zero lower reads). Red-proven
/// by making `OwnerEngine::begin_read` hand back a witness over an
/// already-released permit: the stale fill publish overwrites the commit's
/// write-through and the warm re-get answers the pre-commit value.
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

/// KV4 — a `set` racing `commit()`'s snapshot→drain window. The
/// commit's lower write is withheld after it lands; the racing set parks on
/// the gate, so its cell is buffered strictly after the drain and survives to
/// the settle — nothing is lost. Red-proven by making
/// `OwnerEngine::begin_write` hand back a witness over an already-released
/// permit: the set buffers into the snapshot→drain window and the drain
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

/// KV4 — a `set` racing `clear()`, proving the map's core invariant —
/// **every live entry is covered by a present keyset** (`KeysetPresence`) —
/// survives the race. The teeth need a *non-empty* map: on an empty map both
/// serial orders leave a valid state, so the invariant can't be violated.
/// Seeded cold with `{0,1,2}` and keyset `Tracked{0,1,2}`, then `set(1)` (a key
/// already tracked, so on a non-TTL map its keyset write is suppressed) parks
/// at its cold keyset read HOLDING the gate. `clear()` is polled exactly once:
/// with the gate it parks (a single deterministic `Poll::Pending`, no scheduler
/// heuristic) and runs only after the set completes — set-then-clear leaves the
/// map empty. Red-proven by making `OwnerEngine::begin_write` return a witness
/// over an already-released permit (so `set` and `clear` no longer exclude each
/// other): the first poll runs `clear` to completion, then the resumed `set`
/// writes ONLY the entry (its keyset write suppressed), stranding a live entry
/// with an absent keyset — the invariant the gate protects.
#[test]
fn gate_serializes_set_against_clear() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_set_clear")?;
        let id = fx.id("m")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed a valid, cold, non-TTL map: entries {0,1,2} and a three-key
        // Tracked keyset. Cold so the event's meta read lands on the armed hold.
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (
                        map::keyset_cell(),
                        Some(Bytes::from(tracked_frame(&[0, 1, 2]))),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&0)),
                        Some(json_entry(0)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(json_entry(1)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(json_entry(2)?),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // set(1) parks at its cold keyset read (a held lower read) while HOLDING
        // the gate. 1 is already tracked, so once it resumes its only write is
        // the entry.
        fx.holds.get_for_cache().arm(1);
        let set_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(1, Value::from(99_i64)).await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the set never reached its hold"))?;

        // Poll clear ONCE. With the gate it hits the held permit and returns
        // Pending deterministically; without it, clear runs to completion in
        // this single poll (every op is Ready).
        let clear = handle.clear();
        futures::pin_mut!(clear);
        let first_clear_poll = futures::poll!(clear.as_mut());

        fx.holds.get_for_cache().release();
        timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;
        match first_clear_poll {
            Poll::Ready(result) => result.map_err(|e| eyre!("clear: {e}"))?,
            Poll::Pending => timeout(HANG_GUARD, clear)
                .await
                .map_err(|_| eyre!("clear hung"))?
                .map_err(|e| eyre!("clear: {e}"))?,
        }

        // Settle, then probe the physical state: no live entry may survive with
        // an absent keyset. With the gate the outcome is set-then-clear (empty);
        // the injected race strands entry 1 with a cleared keyset.
        finalize_and_promote(&session, &fx.oracle, Uuid::from_u128(1), &fx.cells, &id).await?;
        let verify = fx.session(2);
        let fresh = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&verify)
            .map_err(|e| eyre!("bind: {e}"))?;
        let entry = fresh.get(&1).await.map_err(|e| eyre!("{e}"))?;
        let keyset = fx
            .counting
            .get(&id, &map::keyset_cell(), probe(99))
            .await?
            .into_inner();
        assert!(
            entry.is_none() || keyset.is_some(),
            "a live entry must be covered by a present keyset (entry={:?}, keyset={:?})",
            entry.is_some(),
            keyset.is_some(),
        );
        Ok(())
    })
}

/// The keyset read-modify-write race pin (the pre-existing lost-update the gate
/// closes): two racing fresh-key sets serialize under the gate, so the keyset
/// is the UNION of both keys (not a last-wins singleton) and a stream yields
/// both entries. Red-proven by making `OwnerEngine::begin_write` return a
/// witness over an already-released permit: the parked
/// set's stale keyset read overwrites the other's update, the keyset loses a
/// key, and the current-membership invariant breaks.
#[test]
fn gate_serializes_racing_keyset_rmw() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_keyset_rmw")?;
        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // set(1) parks at its keyset read while holding the gate; set(9) parks
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
            "serialized keyset updates track both keys — no lost membership"
        );

        // The keyset is the UNION {1, 9}, not a last-wins singleton.
        let id = fx.id("m")?;
        let keyset = fx
            .counting
            .get(&id, &map::keyset_cell(), probe(99))
            .await?
            .into_inner()
            .ok_or_else(|| eyre!("missing keyset cell"))?;
        assert_eq!(
            keyset[..],
            tracked_frame(&[1, 9]),
            "the serialized keyset updates union both keys"
        );
        Ok(())
    })
}

/// The exact `Tracked` frame over ascending `i64` `keys`, built from the real
/// codec: tag `0`, `u32` BE count, then per key a `u32` BE length and its
/// coordinate bytes.
fn tracked_frame(keys: &[i64]) -> Vec<u8> {
    let mut frame = vec![0u8];
    frame.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for k in keys {
        let coordinate = I64KeyCodec::encode(k);
        frame.extend_from_slice(&(coordinate.as_bytes().len() as u32).to_be_bytes());
        frame.extend_from_slice(coordinate.as_bytes());
    }
    frame
}

/// A JSON-encoded map entry payload for the seed writes.
fn json_entry(v: i64) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(&Value::from(v))?))
}

/// The set/set-nearly-full keyset pin: two racing sets on a map already at
/// `limit - 1` keys serialize under the gate, so the second observes the
/// first's insert and overflows — the raw keyset is the `Overflowed` sentinel
/// and the map holds all four entries. Red without the gate: both sets read the
/// same two-key keyset, both compute a fitting three-key list, and a last-wins
/// keyset write silently under-tracks (a three-of-four Tracked frame survives).
#[test]
fn gate_overflows_keyset_at_the_limit() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_keyset_overflow")?;
        let id = fx.id("ks")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed a two-key keyset (limit 3) beneath the cache, so event ops read
        // cold and land on the armed hold.
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (
                        map::keyset_cell(),
                        Some(Bytes::from(tracked_frame(&[1, 2]))),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(json_entry(1)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(json_entry(2)?),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("ks")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // set(3) parks in its cold meta read while holding the gate; set(4)
        // parks on the gate behind it.
        fx.holds.get_for_cache().arm(1);
        let first = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(3, Value::from(3_i64)).await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("set(3) never reached its hold"))?;
        let second = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(4, Value::from(4_i64)).await }
        });
        let_task_park().await;
        fx.holds.get_for_cache().release();
        timeout(HANG_GUARD, first)
            .await
            .map_err(|_| eyre!("set(3) hung"))??
            .map_err(|e| eyre!("set(3): {e}"))?;
        timeout(HANG_GUARD, second)
            .await
            .map_err(|_| eyre!("set(4) hung"))??
            .map_err(|e| eyre!("set(4): {e}"))?;

        finalize_and_promote(&session, &fx.oracle, Uuid::from_u128(1), &fx.cells, &id).await?;

        // The serial second set exceeds the limit → Overflowed.
        let keyset = fx
            .counting
            .get(&id, &map::keyset_cell(), probe(99))
            .await?
            .into_inner()
            .ok_or_else(|| eyre!("missing keyset cell"))?;
        assert_eq!(
            keyset[..],
            [1],
            "the second set over the limit collapses the keyset to Overflowed"
        );

        // No entry was lost: the map holds all four keys (via the scan path).
        let verify = fx.session(2);
        let fresh = map_state::<I64KeyCodec, JsonCodec>("ks")
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
            vec![1, 2, 3, 4],
            "no entry was dropped by the overflow"
        );
        Ok(())
    })
}

/// Rotating map stays `Tracked` (the live-size bound): a map churned across
/// more distinct keys than the limit — each event removing the oldest key
/// before setting a new one, so live size never exceeds the limit — keeps a
/// `Tracked` keyset forever, because `remove` subtracts. A fresh stream then
/// takes the point-get arm and issues **zero** scans. Red-proven by making
/// `remove`'s subtract a no-write: the keyset never shrinks, the fourth
/// distinct key overflows, and the stream degrades to a full-section scan
/// (`lower_scans() == 1`).
#[test]
fn map_keyset_rotating_stays_tracked() -> Result<()> {
    /// Distinct keys churned — strictly greater than the limit (3), so a
    /// keyset that never subtracts would overflow.
    const STEPS: i64 = 6;

    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_rotating")?;
        let id = fx.id("ks")?;
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("ks");

        // Each committed event removes the oldest key (once the window is full)
        // then sets a new one, so live size stays ≤ 3 while total distinct = 6.
        for step in 0..STEPS {
            let event = Uuid::from_u128(u128::try_from(step)? + 1);
            let session = fx.session(u128::try_from(step)? + 1);
            let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
            if step >= 3 {
                handle.remove(&(step - 3)).await.map_err(|e| eyre!("{e}"))?;
            }
            handle
                .set(step, Value::from(step))
                .await
                .map_err(|e| eyre!("{e}"))?;
            finalize_and_promote(&session, &fx.oracle, event, &fx.cells, &id).await?;
        }

        // A fresh stream over the live window {3,4,5} takes the Tracked arm.
        fx.counting.reset();
        let verify = fx.session(100);
        let fresh = descriptor.bind(&verify).map_err(|e| eyre!("bind: {e}"))?;
        let mut keys = Vec::new();
        {
            let stream = fresh.stream(Direction::Forward);
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (key, _) = item.map_err(|e| eyre!("stream: {e}"))?;
                keys.push(key);
            }
        }
        assert_eq!(keys, vec![3, 4, 5], "the stream yields the live window");
        assert_eq!(
            fx.counting.lower_scans(),
            0,
            "a rotating map that never overflows streams by point gets, not a scan"
        );
        Ok(())
    })
}

/// Removal heals an oversized frame (the remove-side twin of
/// `map_keyset_oversized_frame_collapses_before_fast_path`): a stored oversized
/// `Tracked` frame degrades the stream to a full-section scan, but once
/// `remove` subtracts enough keys to bring the frame back under the limit, a
/// fresh stream takes the point-get arm again. Red-proven by making
/// `subtract_keyset` write `Overflowed` instead of the shrunk frame: removal
/// never heals, so the post-remove stream still degrades (`lower_scans() ==
/// 1`).
#[test]
fn map_keyset_removal_heals_oversized() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_heal_oversized")?;
        let id = fx.id("ks")?;
        let cref = CollectionRef::new(id.clone(), None);
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("ks");

        // Seed a valid but oversized 5-key Tracked frame (limit 3) + entries.
        let mut seed = vec![(
            map::keyset_cell(),
            Some(Bytes::from(tracked_frame(&[1, 2, 3, 4, 5]))),
        )];
        for k in 1..=5_i64 {
            seed.push((
                map::entry_cell_for(&I64KeyCodec::encode(&k)),
                Some(json_entry(k)?),
            ));
        }
        fx.counting.write_resolved(&cref, &seed, &[]).await?;

        // The oversized frame degrades to a full-section scan.
        fx.counting.reset();
        {
            let verify = fx.session(1);
            let fresh = descriptor.bind(&verify).map_err(|e| eyre!("bind: {e}"))?;
            let stream = fresh.stream(Direction::Forward);
            futures::pin_mut!(stream);
            while (stream.next().await).is_some() {}
        }
        assert_eq!(
            fx.counting.lower_scans(),
            1,
            "an oversized keyset degrades to the full-section scan"
        );

        // A committed event removes keys 1 and 2, bringing the tracked set to
        // {3,4,5} (≤ limit) — remove subtracts, rewriting a smaller Tracked.
        let session = fx.session(2);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        handle.remove(&1).await.map_err(|e| eyre!("{e}"))?;
        handle.remove(&2).await.map_err(|e| eyre!("{e}"))?;
        finalize_and_promote(&session, &fx.oracle, Uuid::from_u128(2), &fx.cells, &id).await?;

        // The healed frame ({3,4,5}) takes the point-get arm — no scan.
        fx.counting.reset();
        let verify = fx.session(3);
        let fresh = descriptor.bind(&verify).map_err(|e| eyre!("bind: {e}"))?;
        let mut keys = Vec::new();
        {
            let stream = fresh.stream(Direction::Forward);
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (key, _) = item.map_err(|e| eyre!("stream: {e}"))?;
                keys.push(key);
            }
        }
        assert_eq!(keys, vec![3, 4, 5], "the stream yields the remaining keys");
        assert_eq!(
            fx.counting.lower_scans(),
            0,
            "removal healed the frame back under the bound: the fast arm is restored"
        );
        Ok(())
    })
}

/// An absent keyset streams nothing with zero entry reads (the `Absent → Empty`
/// fast path resting on `KeysetPresence`): a truly empty collection yields
/// nothing and issues no scan, and the only lower read is the single keyset get
/// itself. Red-proven by changing `stream_plan`'s `Absent` arm to `Scan`: an
/// empty map then issues a full-section scan (`lower_scans() == 1`).
#[test]
fn map_absent_keyset_streams_zero_reads() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_absent_keyset")?;
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("ks");
        let session = fx.session(1);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

        fx.counting.reset();
        let mut yielded = 0usize;
        {
            let stream = handle.stream(Direction::Forward);
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                item.map_err(|e| eyre!("stream: {e}"))?;
                yielded += 1;
            }
        }
        assert_eq!(yielded, 0, "an absent keyset yields nothing");
        assert_eq!(
            fx.counting.lower_scans(),
            0,
            "Absent → Empty issues no scan"
        );
        assert_eq!(
            fx.counting.lower_reads(),
            1,
            "the only lower read is the single keyset get; no entry point-gets"
        );
        Ok(())
    })
}

/// The set-racing-stream pin (the chunked-stream contract): a mutator racing a
/// live stream serializes against the CURRENT chunk fetch and lands **between
/// chunks**, never mid-fetch. The stream snapshots key membership at its init
/// keyset read, then releases the gate before fetching the entry chunk; a `set`
/// parked on the gate during the init read therefore lands first (FIFO) and
/// buffers `1→99` into the shared overlay, so the entry chunk — a fresh gate
/// acquire — reads key 1 through the overlay = 99. Values are read live,
/// chunk by chunk (the point-get arm's per-arm consistency contract); the
/// interleaving property `run_map_stream_interleave` is the stronger successor
/// (named in the commit). Red-proven by making `OwnerEngine::begin_write` (or
/// `OwnerEngine::resume`, the stream's per-chunk acquire) hand back a witness
/// over an already-released permit: without serialization the yield is
/// nondeterministic and the stream can observe a torn state.
#[test]
fn gate_excludes_set_during_keyset_stream() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_keyset_stream")?;
        let id = fx.id("ks")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed {1: 10, 2: 20} with a two-key keyset beneath the cache (cold).
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (
                        map::keyset_cell(),
                        Some(Bytes::from(tracked_frame(&[1, 2]))),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(json_entry(10)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(json_entry(20)?),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("ks")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // The stream's FIRST cold read is the keyset cell — park it there,
        // holding the gate.
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
                Ok::<_, MapStateError<JsonCodecError>>(out)
            }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the stream never reached its keyset hold"))?;

        // The racing set of a listed key parks on the gate.
        let set_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(1, Value::from(99_i64)).await }
        });
        let_task_park().await;
        fx.holds.get_for_cache().release();

        let yielded = timeout(HANG_GUARD, stream_task)
            .await
            .map_err(|_| eyre!("stream hung"))??
            .map_err(|e| eyre!("stream: {e}"))?;
        assert_eq!(
            yielded,
            vec![(1, Value::from(99_i64)), (2, Value::from(20_i64))],
            "the racing set landed between the init keyset read and the entry chunk fetch \
             (chunk-scoped hold, not whole-stream): the chunk read key 1 through the overlay"
        );
        timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;
        assert_eq!(
            handle.get(&1).await.map_err(|e| eyre!("{e}"))?,
            Some(Value::from(99_i64)),
            "the set is durable in the overlay"
        );
        Ok(())
    })
}

/// The single-hold isolation pin for `Map::get_many`: a `> CELL_BATCH` call
/// holds the session gate ONCE across its two internal sub-batches, so a
/// concurrent mutator queued on the gate serializes entirely AFTER the whole
/// read and the read observes no intermediate state. The first sub-batch is
/// cold, so `get_many` parks in its cache-fill holding the gate; a `set` on a
/// sub-batch-2 key is queued (FIFO); on release `get_many` finishes both
/// sub-batches — reading the committed pre-set value for that key — before the
/// set runs. Red-proven by rewriting `Map::get_many` to acquire the read permit
/// PER `CELL_BATCH` sub-batch (drop + reacquire between them): the queued
/// set then wins the gate at the boundary and buffers its dirty write, so
/// sub-batch 2's overlay read answers the NEW value.
#[test]
fn map_get_many_holds_gate_across_sub_batches() -> Result<()> {
    /// Two sub-batches: the first full `CELL_BATCH` chunk, then the remainder.
    const N: i64 = CELL_BATCH as i64 + 2;
    /// First key of sub-batch 2 — the one the concurrent set targets.
    const TARGET: i64 = CELL_BATCH as i64;

    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_get_many_isolation")?;
        let cref = CollectionRef::new(fx.id("m")?, None);

        // Seed N keys committed BENEATH the cache (cold), value == key.
        let mut seeded = Vec::new();
        for k in 0..N {
            seeded.push((
                map::entry_cell_for(&I64KeyCodec::encode(&k)),
                Some(json_entry(k)?),
            ));
        }
        fx.counting.write_resolved(&cref, &seeded, &[]).await?;

        let session = fx.session(1);
        let map = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        let keys: Vec<i64> = (0..N).collect();

        // Park get_many in sub-batch 1's cold cache-fill (its first lower read),
        // holding the gate.
        fx.holds.get_for_cache().arm(1);
        let reader = tokio::spawn({
            let map = map.clone();
            let keys = keys.clone();
            async move { Box::pin(map.get_many(&keys)).await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("get_many never reached the sub-batch-1 hold"))?;

        // A set on the sub-batch-2 target parks on the gate (get_many holds it).
        let writer = tokio::spawn({
            let map = map.clone();
            async move { map.set(TARGET, Value::from(999_i64)).await }
        });
        let_task_park().await;

        // Release the hold; correct code keeps the gate across the boundary.
        fx.holds.get_for_cache().release();
        let out = timeout(HANG_GUARD, reader)
            .await
            .map_err(|_| eyre!("get_many hung"))??
            .map_err(|e| eyre!("get_many: {e}"))?;
        timeout(HANG_GUARD, writer)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;

        assert_eq!(out.len(), keys.len(), "every position answered");
        assert_eq!(
            out[TARGET as usize],
            Some(Value::from(TARGET)),
            "the sub-batch-2 read observed the committed pre-set value; the queued set serialized \
             after the whole get_many"
        );
        Ok(())
    })
}

/// Journal atomicity at the invocation's final fence: a `set` that reaches
/// **both** of its stages and only then meets a terminated session stages
/// nothing at all — neither the entry nor the keyset write reaches the event
/// overlay.
///
/// The schedule parks the invocation inside its cold keyset read, which sits
/// *below* the read's own liveness guard, so the read still returns `Ok` and
/// the body runs to completion holding write admission. The control run — the
/// identical schedule without the termination — proves the park point is really
/// past both stages: it leaves exactly two staged cells. Red-proven by moving
/// the journal replay above the final validation in `WriteOperation::merge`:
/// the fenced run then stages the same two cells the control does.
#[test]
fn map_set_fenced_at_the_final_check_stages_nothing() -> Result<()> {
    runtime()?.block_on(async {
        let control = parked_set("fence_journal_control", false).await?;
        assert!(control.outcome.is_ok(), "the control set must succeed");
        assert_eq!(
            control.staged, 2,
            "the control stages the entry write and the keyset write"
        );

        let fenced = parked_set("fence_journal_fenced", true).await?;
        match &fenced.outcome {
            Err(error) if map_item_terminated(error) => {}
            other => bail!(
                "the fenced set must report Terminated, got ok={}",
                other.is_ok()
            ),
        }
        assert_eq!(
            fenced.staged, 0,
            "a fenced invocation replays nothing: both staged mutations are discarded"
        );
        assert_eq!(
            fenced.cleared, 0,
            "a fenced invocation stages no section clear either"
        );
        Ok(())
    })
}

/// What one parked-`set` run produced: the call's outcome and what its event
/// overlay holds afterwards, read straight off the dirty store.
struct ParkedSet {
    outcome: Result<(), MapStateError<JsonCodecError>>,
    staged: usize,
    cleared: usize,
}

/// Seeds a two-key tracked map cold, parks a `set` of a fresh key inside its
/// keyset read, optionally terminates the session while it is parked, and
/// reports what the invocation left behind.
async fn parked_set(name: &str, terminate: bool) -> Result<ParkedSet> {
    let fx = GateFixture::new(name)?;
    let id = fx.id("m")?;
    let cref = CollectionRef::new(id.clone(), None);
    let mut seed = vec![(
        map::keyset_cell(),
        Some(Bytes::from(tracked_frame(&[1, 2]))),
    )];
    for k in 1..=2_i64 {
        seed.push((
            map::entry_cell_for(&I64KeyCodec::encode(&k)),
            Some(json_entry(k)?),
        ));
    }
    fx.counting.write_resolved(&cref, &seed, &[]).await?;

    let dirty: Arc<DirtyStore> = Arc::default();
    let session = fx.session_with_dirty(1, dirty.clone());
    let map = map_state::<I64KeyCodec, JsonCodec>("m")
        .bind(&session)
        .map_err(|e| eyre!("bind: {e}"))?;

    // Park in the keyset read's cold cache-fill: past the read's liveness
    // guard, holding write admission, with both stages still ahead.
    fx.holds.get_for_cache().arm(1);
    let writer = tokio::spawn({
        let map = map.clone();
        async move { map.set(9, Value::from(9_i64)).await }
    });
    timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
        .await
        .map_err(|_| eyre!("the set never reached the keyset-read hold"))?;
    if terminate {
        session.terminate();
    }
    fx.holds.get_for_cache().release();
    let outcome = timeout(HANG_GUARD, writer)
        .await
        .map_err(|_| eyre!("the set hung"))??;

    Ok(ParkedSet {
        outcome,
        staged: dirty.collection_snapshot(&id).len(),
        cleared: dirty.cleared_sections(&id).len(),
    })
}

/// The error-yield gate-release pin (map): a `Tracked` map stream whose entry
/// holds undecodable bytes yields `Err` — and MUST release the session gate
/// before that yield reaches user code. Otherwise a caller that catches the
/// error and, with the stream still alive, issues another op on the same
/// session deadlocks: the suspended generator holds the gate the next op waits
/// on (the chunked-stream contract on `SessionGate` — the gate is never held
/// across a yield to user code, error items included). Mechanism under
/// chunking: the corrupt bytes **fetch AND fail to decode under the chunk
/// permit**; the permit still dies with the chunk future's scope before the
/// forwarding loop's `chunk?` yields the `Err`, so the gate is released before
/// the `Err` reaches user code.
///
/// This pin **also pins chunk-atomicity**: a valid lower-sorting entry (key 3)
/// precedes the corrupt key (7) in the same chunk, so prefix-yield semantics
/// would surface key 3's `Ok` first and fail `first.is_err()`. Two red
/// recipes: (i) chunk-atomicity — make the chunk yield per-item instead of
/// collecting; (ii) gate-release — hold the permit across the yield by
/// returning it in the unfold state (`Some((chunk, permit, keys))`) so the
/// follow-up `get` hangs.
#[test]
fn map_stream_error_yield_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_map_stream_error")?;
        let id = fx.id("m")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed a two-key `Tracked` keyset: a valid lower-sorting entry (3) ahead
        // of the corrupt entry (7) in the same chunk, so a chunk-atomic error
        // surfaces the corrupt key's `Err` first (prefix-yield would leak 3's
        // `Ok`), and the corrupt bytes fail to decode under the chunk permit.
        let valid = 3_i64;
        let corrupt = 7_i64;
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (
                        map::keyset_cell(),
                        Some(Bytes::from(tracked_frame(&[valid, corrupt]))),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&valid)),
                        Some(Bytes::from_static(b"null")),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&corrupt)),
                        Some(Bytes::from_static(b"\x00\x01\x02 not json")),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        let stream = handle.stream(Direction::Forward);
        futures::pin_mut!(stream);
        // Both keys land in one chunk; the chunk-atomic collect short-circuits
        // at the corrupt key, so the FIRST yielded item is the `Err` — key 3's
        // `Ok` is never surfaced.
        let first = stream
            .next()
            .await
            .ok_or_else(|| eyre!("the stream ended without yielding the decode error"))?;
        assert!(
            first.is_err(),
            "the corrupt entry must surface as the first (chunk-atomic) Err item, not key 3's Ok"
        );

        // The stream is still alive (held in scope, not dropped). A follow-up op
        // on the same session must not be starved by a gate the suspended stream
        // holds: post-fix the permit was released before the Err yield, so this
        // completes; pre-fix it parks forever and the guard trips.
        let absent = timeout(HANG_GUARD, handle.get(&999))
            .await
            .map_err(|_| {
                eyre!(
                    "a session op after a stream Err hung: the stream held the gate across the \
                     error yield"
                )
            })?
            .map_err(|e| eyre!("probe get: {e}"))?;
        assert!(absent.is_none(), "the probe key is absent");
        Ok(())
    })
}

/// The error-yield gate-release pin (deque twin of
/// [`map_stream_error_yield_releases_the_gate`]): a point-get deque stream
/// whose element holds undecodable bytes yields `Err` and MUST release the gate
/// before the yield. Same mechanism (corrupt bytes fetch AND fail to decode
/// under the chunk permit, which dies with the chunk future before `chunk?`
/// yields the `Err`), and — with index 0 valid ahead of the corrupt index 1 in
/// one chunk — it likewise pins chunk-atomicity: prefix-yield would surface
/// index 0's `Ok` first. Same reds, on the structural twin.
#[test]
fn deque_stream_error_yield_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_deque_stream_error")?;
        let id = fx.id("d")?;
        let dref = CollectionRef::new(id.clone(), None);

        // Seed a two-element window `[0, 2)`: index 0 valid, index 1 corrupt, so
        // a chunk-atomic error surfaces index 1's `Err` first (prefix-yield
        // would leak index 0's `Ok`), and the corrupt bytes fail to decode under
        // the chunk permit.
        fx.counting
            .write_resolved(
                &dref,
                &[
                    (
                        deque::meta_cell(),
                        Some(Bytes::from(deque::seed_frame(0, 2))),
                    ),
                    (
                        deque::entry_cell_for(&I64KeyCodec::encode(&0)),
                        Some(Bytes::from_static(b"null")),
                    ),
                    (
                        deque::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(Bytes::from_static(b"\x00\x01\x02 not json")),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = deque_state::<JsonCodec>("d")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        let stream = handle.stream(Direction::Forward);
        futures::pin_mut!(stream);
        // Both indices land in one chunk; the chunk-atomic collect short-circuits
        // at index 1, so the FIRST yielded item is the `Err` — index 0's `Ok` is
        // never surfaced.
        let first = stream
            .next()
            .await
            .ok_or_else(|| eyre!("the stream ended without yielding the decode error"))?;
        assert!(
            first.is_err(),
            "the corrupt element must surface as the first (chunk-atomic) Err item, not index 0's \
             Ok"
        );

        // The stream is still alive: a follow-up op must not be starved.
        let empty = timeout(HANG_GUARD, handle.is_empty())
            .await
            .map_err(|_| {
                eyre!(
                    "a session op after a stream Err hung: the stream held the gate across the \
                     error yield"
                )
            })?
            .map_err(|e| eyre!("probe is_empty: {e}"))?;
        assert!(!empty, "the seeded window is non-empty");
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

/// The chunk-fetch cancellation pin (the stream cousin of
/// [`dropped_session_op_releases_the_gate`]): a stream whose chunk fetch parks
/// holding the gate is dropped — while it HOLDS the gate, and while a second op
/// is QUEUED behind it — and the RAII permit is released, so the next op and
/// settle's close acquire both proceed. Drop-releases-via-RAII is green by
/// construction; the falsification that guards it is detaching the chunk fetch
/// into a `tokio::spawn` (the design's forbidden detachment) so an abort of the
/// stream task cannot cancel the fetch — then the gate stays held and the
/// follow-up op's hang-guard trips.
#[test]
fn dropped_stream_chunk_fetch_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_stream_cancel")?;
        // Seed a small deque window cold, valued by index.
        let id = fx.id("d")?;
        let dref = CollectionRef::new(id.clone(), None);
        let mut seeded = vec![(
            deque::meta_cell(),
            Some(Bytes::from(deque::seed_frame(0, 3))),
        )];
        for i in 0..3_i64 {
            seeded.push((
                deque::entry_cell_for(&I64KeyCodec::encode(&i)),
                Some(Bytes::from(serde_json::to_vec(&Value::from(i))?)),
            ));
        }
        fx.counting.write_resolved(&dref, &seeded, &[]).await?;

        let session = fx.session(1);
        let handle = deque_state::<JsonCodec>("d")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        // Warm the bounds cell so the armed hold lands on the first ENTRY read —
        // i.e. inside a chunk fetch, holding the gate.
        assert_eq!(handle.len().await.map_err(|e| eyre!("{e}"))?, 3);

        // Drop a HOLDING stream: its chunk fetch parks in the withheld entry
        // read, gate held.
        fx.holds.get_for_cache().arm(1);
        let stream_task = tokio::spawn({
            let handle = handle.clone();
            async move {
                let stream = handle.stream(Direction::Forward);
                futures::pin_mut!(stream);
                let _ = stream.next().await;
            }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the chunk fetch never reached its hold"))?;
        stream_task.abort();
        assert!(
            stream_task.await.is_err(),
            "the stream was dropped mid-chunk-fetch"
        );

        // The next op proceeds — the dropped generator released the gate.
        timeout(HANG_GUARD, handle.is_empty())
            .await
            .map_err(|_| {
                eyre!("hang-guard: the chunk fetch's permit was not released by the drop")
            })?
            .map_err(|e| eyre!("is_empty: {e}"))?;

        // Drop a QUEUED next(): A (a chunk fetch) holds, a queued op B waits, B
        // is dropped, A completes, and settle's close still proceeds.
        fx.holds.get_for_cache().arm(1);
        let holding = tokio::spawn({
            let handle = handle.clone();
            async move {
                let stream = handle.stream(Direction::Forward);
                futures::pin_mut!(stream);
                stream.next().await.transpose()
            }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the second chunk fetch never reached its hold"))?;
        let queued = tokio::spawn({
            let handle = handle.clone();
            async move { handle.len().await }
        });
        let_task_park().await;
        queued.abort();
        assert!(queued.await.is_err(), "the queued op was dropped");
        fx.holds.get_for_cache().release();
        timeout(HANG_GUARD, holding)
            .await
            .map_err(|_| eyre!("the holding stream hung"))??
            .map_err(|e| eyre!("stream: {e}"))?;

        // Settle's close acquire proceeds (the drop-is-safe half).
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
            StoreOutcome::NoOp,
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

// ==========================================================================
// Attempt-epoch fence pins
// ==========================================================================
//
// The mechanics of the per-event attempt epoch (`AttemptEpoch`): a handle,
// stream, or session clone pins the epoch that was live when it was minted, and
// every cell op fails `Terminated` once a later attempt boundary (`reset`)
// bumped it. These pins drive `reset`/`repin` directly through
// `RepinProof::for_test()` at the typed layer — no retry loop — so each fence
// rule is isolated.

/// Whether a fenced cell op returned the `Terminated` access error.
fn is_terminated(err: &CellStateError<JsonCodecError>) -> bool {
    matches!(err, CellStateError::Access(StateAccessError::Terminated))
}

/// Seeds `value` as the committed base of collection `v` beneath the cache, so
/// a later cold `get` fills through the lower store.
async fn seed_committed_v(fx: &GateFixture, value: &Value) -> Result<()> {
    let cref = CollectionRef::new(fx.id("v")?, None);
    fx.counting
        .write_resolved(
            &cref,
            &[(value_cell(), Some(Bytes::from(serde_json::to_vec(value)?)))],
            &[],
        )
        .await?;
    Ok(())
}

/// Settles `session`, then asserts a fresh event's read of `v` answers
/// `expected` — the zero-store-effect verification every fence pin ends with.
async fn settle_and_verify(
    fx: &GateFixture,
    session: &KeyedStateSession<GateBackend, MemoryLoader<Value>>,
    expected: Option<Value>,
    msg: &str,
) -> Result<()> {
    finalize_and_promote(
        session,
        &fx.oracle,
        Uuid::from_u128(1),
        &fx.cells,
        &fx.id("v")?,
    )
    .await?;
    let fresh = value_state::<JsonCodec>("v")
        .bind(&fx.session(2))
        .map_err(|e| eyre!("bind: {e}"))?;
    assert_eq!(
        fresh.get().await.map_err(|e| eyre!("verify: {e}"))?,
        expected,
        "{msg}"
    );
    Ok(())
}

/// A typed handle op after `reset()` errors `Terminated`, with zero store
/// effect — the fenced `set` never reaches the committed cell. Red-proven by
/// deleting the `!session.attempt_current()` pin compare in `ensure_live`: the
/// stale-pinned `get`/`set` then answer live instead of `Terminated`.
#[test]
fn handle_op_after_reset_is_terminated() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_reset_op")?;
        seed_committed_v(&fx, &Value::from("A")).await?;
        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Attempt boundary: discard + bump. `handle` keeps the stale pin.
        session.reset(RepinProof::for_test()).await;

        match handle.get().await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("get after reset must be Terminated, got {other:?}"),
        }
        match handle.set(Value::from("B")).await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("set after reset must be Terminated, got {other:?}"),
        }

        // Zero store effect: settle the (now attempt-N+1) session — the fenced
        // set never buffered, so nothing stages — and a fresh event still reads
        // the seeded "A".
        settle_and_verify(
            &fx,
            &session,
            Some(Value::from("A")),
            "the fenced set left the committed cell unchanged",
        )
        .await?;
        Ok(())
    })
}

/// `Map::get_many` after a `reset()` epoch bump errors `Terminated` as a whole
/// — never a partial `Vec`. The batch twin of
/// `handle_op_after_reset_is_terminated`: `ensure_live` fences at the first
/// `raw_get_many` before any cell is read, and the `Result<Vec<_>>` shape makes
/// a partial answer unrepresentable. Red-proven by deleting the
/// `!session.attempt_current()` pin compare in `ensure_live`: the stale-pinned
/// `get_many` then answers live.
#[test]
fn map_get_many_after_reset_is_terminated() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_reset_get_many")?;
        let session = fx.session(1);
        let map = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Attempt boundary: discard + bump. `map` keeps the stale pin.
        session.reset(RepinProof::for_test()).await;

        match Box::pin(map.get_many(&[0, 1, 2])).await {
            Err(ref e) if map_item_terminated(e) => {}
            other => bail!("get_many after reset must be Terminated, got {other:?}"),
        }
        Ok(())
    })
}

/// A leaked attempt-N session clone (and a clone of a clone) stays fenced after
/// `reset()`, while a `repin`-ed clone is live. Red-proven by deleting the
/// `!session.attempt_current()` pin compare in `ensure_live`: the leaked
/// clones' reads then answer live instead of `Terminated`.
#[test]
fn leaked_clone_is_fenced_repin_is_live() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_leaked_clone")?;
        let session = fx.session(1);
        session.reset(RepinProof::for_test()).await; // epoch N -> N+1

        // A leaked attempt-N clone vends a previously-unbound collection; its
        // first op errors.
        let leaked = session.clone();
        let leaked_handle = value_state::<JsonCodec>("v")
            .bind(&leaked)
            .map_err(|e| eyre!("bind: {e}"))?;
        match leaked_handle.get().await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("a leaked attempt-N clone op must be Terminated, got {other:?}"),
        }

        // A clone of a clone carries the same stale pin.
        let leaked2 = session.clone().clone();
        let handle2 = value_state::<JsonCodec>("v")
            .bind(&leaked2)
            .map_err(|e| eyre!("bind: {e}"))?;
        match handle2.get().await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("a clone-of-a-clone op must be Terminated, got {other:?}"),
        }

        // A `repin`-ed clone is pinned to the live epoch and reads normally.
        let live = session.repin(RepinProof::for_test());
        let live_handle = value_state::<JsonCodec>("v")
            .bind(&live)
            .map_err(|e| eyre!("bind: {e}"))?;
        assert_eq!(
            live_handle
                .get()
                .await
                .map_err(|e| eyre!("live get: {e}"))?,
            None,
            "the live attempt reads normally"
        );
        Ok(())
    })
}

/// The epoch bump sticks even with NOTHING vended before the boundary — the
/// first handle bound from a stale-pinned clone after `reset()` errors on its
/// first op. Red-proven by deleting the `!session.attempt_current()` pin
/// compare in `ensure_live`: the first op then answers live instead of
/// `Terminated`.
#[test]
fn reset_bump_sticks_with_nothing_vended() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_bump_sticks")?;
        let session = fx.session(1);
        // No handle/stream vended before the boundary.
        session.reset(RepinProof::for_test()).await;
        // The FIRST handle, bound from a stale-pinned clone, errors on first op.
        let stale = session.clone();
        let handle = value_state::<JsonCodec>("v")
            .bind(&stale)
            .map_err(|e| eyre!("bind: {e}"))?;
        match handle.get().await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("first op after a vend-free bump must be Terminated, got {other:?}"),
        }
        Ok(())
    })
}

/// A stale `rollback()` (pinned N) after `reset()` returns `NoOp` and leaves
/// attempt N+1's dirty buffer untouched. Red-proven by deleting the
/// `!self.attempt_current()` pin term from the session `rollback`'s
/// self-admission: the stale rollback then drains attempt N+1's live buffer and
/// the fresh event reads `None`.
#[test]
fn stale_rollback_is_noop_and_spares_next_attempt() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_stale_rollback")?;
        let session = fx.session(1);
        let stale = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Attempt boundary, then seed N+1's dirty through a live (repin-ed)
        // handle over the SAME collection.
        session.reset(RepinProof::for_test()).await;
        let live = session.repin(RepinProof::for_test());
        let live_handle = value_state::<JsonCodec>("v")
            .bind(&live)
            .map_err(|e| eyre!("bind: {e}"))?;
        live_handle
            .set(Value::from("keep"))
            .await
            .map_err(|e| eyre!("set: {e}"))?;

        // Without the pin check the stale rollback would drain the live buffer.
        assert_eq!(
            stale.rollback().await,
            StoreOutcome::NoOp,
            "a stale rollback discards nothing"
        );

        // N+1 settles with the seeded value intact.
        settle_and_verify(
            &fx,
            &live,
            Some(Value::from("keep")),
            "attempt N+1's dirty survived the stale rollback",
        )
        .await?;
        Ok(())
    })
}

/// A stale queued write (pinned N) issued after the whole one-hold reset
/// transition errors instead of buffering, and attempt N+1 settles with no
/// residue of it — the `mutate_permit` pin fence, distinct from the queued-set
/// interleaving race (`racing_set_never_joins_next_attempt`). Red-proven only
/// by deleting BOTH `mutate_permit`'s pin check AND `ensure_live`'s pin
/// compare: either alone leaves the other to fence this stale `set`, so both
/// must go for the write to buffer and its residue to surface.
#[test]
fn stale_write_after_reset_errors_and_leaves_no_residue() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_stale_write")?;
        let session = fx.session(1);
        let stale = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        session.reset(RepinProof::for_test()).await; // discard + bump, one hold

        match stale.set(Value::from("leak")).await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("a stale write after reset must be Terminated, got {other:?}"),
        }

        // Settle N+1: nothing staged from the fenced write.
        settle_and_verify(
            &fx,
            &session,
            None,
            "the fenced stale write left no committed residue",
        )
        .await?;
        Ok(())
    })
}

/// A `set` forced to queue on the gate behind the reset transition never joins
/// attempt N+1's committed transaction (paused-time-free but deterministic via
/// FIFO gate ordering). A parked fill holds the gate; reset queues first, the
/// stale set second; releasing lets reset bump, then the set's admission pin
/// check fences it — the interleaving case where the check-then-mint race is
/// resolved by the held permit. Red-proven only by deleting BOTH
/// `mutate_permit`'s pin check AND `ensure_live`'s pin compare: either alone
/// still fences the racing set, so both must go for it to join attempt N+1.
#[test]
fn racing_set_never_joins_next_attempt() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_race_set")?;
        // A committed base so the gate-holding get triggers a cold fill.
        seed_committed_v(&fx, &Value::from("A")).await?;
        let session = fx.session(1);
        let stale = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Park a fill holding the gate (epoch still N, so its own `ensure_live`
        // admitted it before the bump).
        fx.holds.get_for_cache().arm(1);
        let get_task = tokio::spawn({
            let stale = stale.clone();
            async move { stale.get().await }
        });
        timeout(HANG_GUARD, fx.holds.get_for_cache().entered())
            .await
            .map_err(|_| eyre!("the fill never parked on the gate"))?;

        // Reset queues on the gate FIRST (behind the parked fill).
        let reset_task = tokio::spawn({
            let session = session.clone();
            async move { session.reset(RepinProof::for_test()).await }
        });
        let_task_park().await;
        // The stale set queues on the gate SECOND (behind reset).
        let set_task = tokio::spawn({
            let stale = stale.clone();
            async move { stale.set(Value::from("B")).await }
        });
        let_task_park().await;

        // Release the fill: reset acquires (discard+bump), then the set.
        fx.holds.get_for_cache().release();
        timeout(HANG_GUARD, get_task)
            .await
            .map_err(|_| eyre!("get hung"))??
            .map_err(|e| eyre!("get: {e}"))?;
        timeout(HANG_GUARD, reset_task)
            .await
            .map_err(|_| eyre!("reset hung"))??;
        let set_result = timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??;
        match set_result {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("the set that lost the gate to reset must be Terminated, got {other:?}"),
        }

        // N+1 settles with no trace of "B".
        settle_and_verify(
            &fx,
            &session,
            Some(Value::from("A")),
            "the racing set never joined attempt N+1's transaction",
        )
        .await?;
        Ok(())
    })
}

/// Whether a fenced map outcome — a stream item or a call's error — is the
/// `Terminated` access error.
fn map_item_terminated(item: &MapStateError<JsonCodecError>) -> bool {
    matches!(
        item,
        MapStateError::Cell(CellStateError::Access(StateAccessError::Terminated))
    )
}

/// Scan-shell fence, RANGE source: the map degrade arm streams through the
/// gate-free range source, and an emission after an observed attempt bump
/// errors `Terminated` — the managed stream's per-emission fence catches it,
/// not the source (which keeps producing). The first item crosses pre-bump; the
/// range source holds no admission, so the `reset` between pulls bumps
/// immediately. Red proven by dropping the `fenced(...)` wrapper in
/// `RangePlan::entries` (return the raw source): the post-bump pull then
/// yields a second `Ok` item.
#[test]
fn range_scan_stream_fences_after_bump() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_range_scan")?;
        let cref = CollectionRef::new(fx.id("ks")?, None);
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("ks");

        // An oversized 4-key Tracked frame (limit 3) degrades the stream to the
        // full-section range source; ≥ 2 entries so a second emission exists.
        let mut seed = vec![(
            map::keyset_cell(),
            Some(Bytes::from(tracked_frame(&[1, 2, 3, 4]))),
        )];
        for k in 1..=4_i64 {
            seed.push((
                map::entry_cell_for(&I64KeyCodec::encode(&k)),
                Some(json_entry(k)?),
            ));
        }
        fx.counting.write_resolved(&cref, &seed, &[]).await?;

        let session = fx.session(1);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        let stream = handle.stream(Direction::Forward);
        futures::pin_mut!(stream);

        match stream.next().await {
            Some(Ok(_)) => {}
            other => bail!("the first range item must cross pre-bump, got {other:?}"),
        }
        // Attempt boundary: the range source holds no permit, so reset bumps now.
        session.reset(RepinProof::for_test()).await;
        match stream.next().await {
            Some(Err(ref e)) if map_item_terminated(e) => {}
            other => bail!("the post-bump range emission must be Terminated, got {other:?}"),
        }
        Ok(())
    })
}

/// Scan-shell fence, COORDINATE source: the map tracked arm point-gets a chunk,
/// collects it into a bounded buffer, and releases the permit before the first
/// yield; a buffered entry never crosses the fence after an observed bump. Both
/// keys land in one chunk (`STREAM_CHUNK >= 2`), so the first entry's fence
/// check passes pre-bump and the second's runs post-bump. Red proven by
/// dropping the `fenced(...)` wrapper in `CoordinatePlan::entries`: the
/// buffered second entry then crosses as an `Ok`.
#[test]
fn coordinate_stream_fences_buffered_entries_after_bump() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_coord_scan")?;
        let cref = CollectionRef::new(fx.id("m")?, None);
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("m");

        // Exactly two tracked keys: one chunk, both entries buffered together.
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (
                        map::keyset_cell(),
                        Some(Bytes::from(tracked_frame(&[1, 2]))),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(json_entry(10)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(json_entry(20)?),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        let stream = handle.stream(Direction::Forward);
        futures::pin_mut!(stream);

        // The chunk is fetched and the permit dropped before this first yield.
        match stream.next().await {
            Some(Ok((1, _))) => {}
            other => bail!("the first buffered entry must cross pre-bump, got {other:?}"),
        }
        // Attempt boundary: the chunk permit is already dropped, so reset bumps.
        session.reset(RepinProof::for_test()).await;
        // The SECOND buffered entry's emission check runs post-bump.
        match stream.next().await {
            Some(Err(ref e)) if map_item_terminated(e) => {}
            other => bail!("the buffered second entry must be fenced Terminated, got {other:?}"),
        }
        Ok(())
    })
}

/// A stale-pinned mutation on an already-closed session classifies
/// `Terminated`, never `SessionClosed` — `mutate_permit` checks the pin before
/// the closed flag, so a dead-attempt op is fenced uniformly regardless of the
/// close. Isolates the admission ORDER: `ensure_live` never runs when the
/// permit errors `SessionClosed` first, so the order flip is observable only
/// here. Red-proven by swapping the pin and closed checks in `mutate_permit`:
/// the stale `set` then returns `SessionClosed`.
#[test]
fn stale_mutator_on_closed_session_is_terminated_not_closed() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_stale_closed")?;
        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Stale pin, then close the gate as the settle boundary would.
        session.reset(RepinProof::for_test()).await;
        drop(session.close_gate().await);

        match handle.set(Value::from(8_i64)).await {
            Err(ref e) if is_terminated(e) => {}
            other => {
                bail!("a stale mutator on a closed session must be Terminated, got {other:?}")
            }
        }
        Ok(())
    })
}

/// A conforming within-attempt op for the conforming-handler property.
#[derive(Clone, Debug)]
enum ValueOp {
    Get,
    Set(i64),
    Clear,
    Commit,
    Rollback,
}

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 => ValueOp::Get,
            1 => ValueOp::Set(i64::from(u8::arbitrary(g))),
            2 => ValueOp::Clear,
            3 => ValueOp::Commit,
            _ => ValueOp::Rollback,
        }
    }
}

/// A conforming handler — every op issued within its own attempt, including a
/// scan-then-rollback-then-rescan — NEVER observes the fence. The epoch is
/// stable within one attempt, so the pin always matches; any op that errored
/// `Terminated` would surface here as a failed property (the `?` propagates
/// it). The subject is called for every generated op, so this is a real
/// property, not a tautology. Red-proven by inverting `ensure_live`'s pin
/// compare to `session.attempt_current()`: every conforming op then fences and
/// the property's `?` fails.
#[test]
fn conforming_within_attempt_never_fenced() {
    fn property(ops: Vec<ValueOp>) -> Result<bool> {
        runtime()?.block_on(async {
            let fx = GateFixture::new("fence_conforming")?;
            let session = fx.session(1);
            let value = value_state::<JsonCodec>("v")
                .bind(&session)
                .map_err(|e| eyre!("bind v: {e}"))?;
            for op in ops {
                match op {
                    ValueOp::Get => {
                        value.get().await.map_err(|e| eyre!("get: {e}"))?;
                    }
                    ValueOp::Set(n) => {
                        value
                            .set(Value::from(n))
                            .await
                            .map_err(|e| eyre!("set: {e}"))?;
                    }
                    ValueOp::Clear => {
                        value.clear().await.map_err(|e| eyre!("clear: {e}"))?;
                    }
                    ValueOp::Commit => {
                        value.commit().await.map_err(|e| eyre!("commit: {e}"))?;
                    }
                    ValueOp::Rollback => {
                        // Infallible; within an attempt it is Applied/NoOp,
                        // never fenced.
                        let _ = value.rollback().await;
                    }
                }
            }
            // scan → rollback → rescan on a map, same attempt.
            let map = map_state::<I64KeyCodec, JsonCodec>("m")
                .bind(&session)
                .map_err(|e| eyre!("bind m: {e}"))?;
            for k in 0..3_i64 {
                map.set(k, Value::from(k))
                    .await
                    .map_err(|e| eyre!("map set: {e}"))?;
            }
            {
                let stream = map.stream(Direction::Forward);
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    item.map_err(|e| eyre!("scan: {e}"))?;
                }
            }
            let _ = map.rollback().await;
            {
                let stream = map.stream(Direction::Forward);
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    item.map_err(|e| eyre!("rescan: {e}"))?;
                }
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(Vec<ValueOp>) -> Result<bool>);
}
