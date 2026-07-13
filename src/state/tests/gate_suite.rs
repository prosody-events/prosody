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
    CellStateError, MapStateError, StateDescriptor, deque, deque_state, map, map_state, value_state,
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
use std::task::Poll;
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

/// KV4 pin (c): a `set` racing `clear()`, proving the map's core invariant —
/// **every live entry is covered by a present keyset** (`KeysetPresence`) —
/// survives the race. The teeth need a *non-empty* map: on an empty map both
/// serial orders leave a valid state, so the invariant can't be violated.
/// Seeded cold with `{0,1,2}` and keyset `Tracked{0,1,2}`, then `set(1)` (a key
/// already tracked, so on a non-TTL map its keyset write is suppressed) parks
/// at its cold keyset read HOLDING the gate. `clear()` is polled exactly once:
/// with the gate it parks (a single deterministic `Poll::Pending`, no scheduler
/// heuristic) and runs only after the set completes — set-then-clear leaves the
/// map empty. Red-proven by deleting the permit acquisition in `set` OR
/// `clear`: the first poll runs `clear` to completion, then the resumed `set`
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
        let entry =
            |v: i64| -> Result<Bytes> { Ok(Bytes::from(serde_json::to_vec(&Value::from(v))?)) };
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
                        Some(entry(0)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(entry(1)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(entry(2)?),
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
/// closes; the surviving same-shape race after the bound ratchet was deleted):
/// two racing fresh-key sets serialize under the gate, so the keyset is the
/// UNION of both keys (not a last-wins singleton) and a stream yields both
/// entries. Red-proven by deleting the permit acquisition: the parked set's
/// stale keyset read overwrites the other's update, the keyset loses a key, and
/// the current-membership invariant breaks.
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
        let entry =
            |v: i64| -> Result<Bytes> { Ok(Bytes::from(serde_json::to_vec(&Value::from(v))?)) };
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
                        Some(entry(1)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(entry(2)?),
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
        let entry =
            |v: i64| -> Result<Bytes> { Ok(Bytes::from(serde_json::to_vec(&Value::from(v))?)) };

        // Seed a valid but oversized 5-key Tracked frame (limit 3) + entries.
        let mut seed = vec![(
            map::keyset_cell(),
            Some(Bytes::from(tracked_frame(&[1, 2, 3, 4, 5]))),
        )];
        for k in 1..=5_i64 {
            seed.push((
                map::entry_cell_for(&I64KeyCodec::encode(&k)),
                Some(entry(k)?),
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

/// The set-racing-stream pin (the split-stream bounded-arm materialization
/// contract): the keyset stream materializes under one gate hold, so a racing
/// `set` of a listed key cannot interleave — the drain yields EXACTLY the
/// init-materialized state (stronger than some-serial-order), and the set
/// applies after. Red without the gate: the set completes while the stream is
/// parked at its keyset read, the dirty overlay wins the entry read, and the
/// stream yields the new value.
#[test]
fn gate_excludes_set_during_keyset_stream() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_keyset_stream")?;
        let id = fx.id("ks")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed {1: 10, 2: 20} with a two-key keyset beneath the cache (cold).
        let entry =
            |v: i64| -> Result<Bytes> { Ok(Bytes::from(serde_json::to_vec(&Value::from(v))?)) };
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
                        Some(entry(10)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(entry(20)?),
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
            vec![(1, Value::from(10_i64)), (2, Value::from(20_i64))],
            "the stream yields the init-materialized state, not the racing set"
        );
        timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;
        assert_eq!(
            handle.get(&1).await.map_err(|e| eyre!("{e}"))?,
            Some(Value::from(99_i64)),
            "the set applied after the whole stream materialized"
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

/// The error-yield gate-release pin (map): a `Tracked` map stream whose
/// point-get hits an undecodable entry yields `Err` — and MUST release the
/// session gate before that yield reaches user code. Otherwise a caller that
/// catches the error and, with the stream still alive, issues another op on the
/// same session deadlocks: the suspended generator holds the gate the next op
/// waits on (the split-stream contract on `SessionGate` — the gate is never
/// held across a yield to user code, error items included). Red without the
/// fix: the `try_stream!` `?` yields the error while the init permit is still
/// held (the generator suspends at the yield before the permit drops), so the
/// follow-up `get` parks forever and the hang-guard trips.
#[test]
fn map_stream_error_yield_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_map_stream_error")?;
        let id = fx.id("m")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed a one-key `Tracked` keyset whose entry cell holds undecodable
        // bytes, so the stream's point-get fails and yields `Err`.
        let key = 7_i64;
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (map::keyset_cell(), Some(Bytes::from(tracked_frame(&[key])))),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&key)),
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
        // The Tracked point-get hits the corrupt entry: the stream yields `Err`.
        let first = stream
            .next()
            .await
            .ok_or_else(|| eyre!("the stream ended without yielding the decode error"))?;
        assert!(
            first.is_err(),
            "the corrupt entry must surface as an Err item"
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
/// [`map_stream_error_yield_releases_the_gate`]): a bounded deque stream whose
/// point-get hits an undecodable element yields `Err` and MUST release the gate
/// before the yield. Same mechanism, same red, on the structural twin.
#[test]
fn deque_stream_error_yield_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_deque_stream_error")?;
        let id = fx.id("d")?;
        let dref = CollectionRef::new(id.clone(), None);

        // Seed a one-element window `[0, 1)` whose entry holds undecodable
        // bytes, so the bounded materialization's point-get fails.
        fx.counting
            .write_resolved(
                &dref,
                &[
                    (
                        deque::meta_cell(),
                        Some(Bytes::from(deque::seed_frame(0, 1))),
                    ),
                    (
                        deque::entry_cell_for(&I64KeyCodec::encode(&0)),
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
        let first = stream
            .next()
            .await
            .ok_or_else(|| eyre!("the stream ended without yielding the decode error"))?;
        assert!(
            first.is_err(),
            "the corrupt element must surface as an Err item"
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
