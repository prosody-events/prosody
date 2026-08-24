//! Abnormal-exit fencing through [`guarded_dispatch`] — the single
//! panic-unwind catch site, which `process_event` wraps every dispatch in
//! (above `RetryHandler`'s own `EventHandler` impl). On an unwind the catch
//! runs the gate-held terminal transition (close, discard, terminate — no epoch
//! write) then resumes; on a dropped dispatch future the scope's `Drop` flips
//! termination. Either way a handle the dispatch leaked past its attempt is
//! fenced on the op's next effect, with zero orchestration by any caller.

use super::*;
use crate::codec::{JsonCodec, JsonCodecError};
use crate::consumer::middleware::NextAttempt;
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::state::access::StateAccessError;
use crate::state::descriptor::tests::TestSession;
use crate::state::descriptor::{CellStateError, Registered, ValueHandle, value_state};
use crate::state::dirty::DirtyStore;
use crate::state::manager::EventStateScope;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::tests::cell_suite::value_cell;
use crate::state::{EventRef, StateKey, StateName, StateType};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use parking_lot::Mutex as SyncMutex;
use serde_json::{Value, json};
use tokio::sync::{oneshot, watch};
use tokio::time::timeout;
use uuid::Uuid;

const NAME: &str = "c";
type Ctx = MockEventContext<Value, TestSession>;
type Handle = ValueHandle<TestSession, JsonCodec>;

/// Shared durable + dirty state so the event session and a fresh observer
/// session read the same overlay — the residue probe.
struct Fixture {
    cell: MemoryCellStore<FixedOracle>,
    dirty: Arc<DirtyStore>,
    state_key: StateKey,
    registry: Arc<CollectionDefRegistry>,
}

impl Fixture {
    fn new() -> Result<Self> {
        let mut registry = CollectionDefRegistry::default();
        registry.register(&value_state::<JsonCodec>(NAME), CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let cell = MemoryCellStore::new(
            MemoryCells::new(),
            FixedOracle::committed(),
            registry.clone(),
        );
        Ok(Self {
            cell,
            dirty: Arc::new(DirtyStore::new()),
            state_key: StateKey::new(Uuid::from_u128(0xE), Arc::from("user-1")),
            registry,
        })
    }

    /// A fresh session (new event epoch) over the shared durable + dirty
    /// state.
    fn session(&self) -> TestSession {
        let (_s, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_c, cancel_rx) = watch::channel(false);
        KeyedStateSession::new(SessionParts {
            cell: self.cell.clone(),
            dirty: self.dirty.clone(),
            oracle: FixedOracle::committed(),
            loader: MemoryLoader::new(),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event: EventRef::Message {
                dedup_id: Uuid::new_v4(),
            },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        })
    }

    /// The buffered dirty bytes of `NAME` observed through a fresh,
    /// non-terminated session over the same overlay — the residue probe.
    async fn residue(&self) -> Result<Option<Bytes>> {
        self.session()
            .peek(
                StateType::Application,
                &StateName::try_new(NAME)?,
                &value_cell(),
            )
            .await
            .map_err(|e| eyre!("residue read: {e}"))
    }
}

fn handle(context: &Ctx) -> Result<Handle> {
    context
        .state(Registered::new(value_state::<JsonCodec>(NAME)))
        .map_err(|e| eyre!("bind: {e}"))
}

fn tag(error: &CellStateError<JsonCodecError>) -> String {
    match error {
        CellStateError::Access(StateAccessError::SessionClosed) => "SessionClosed".into(),
        CellStateError::Access(StateAccessError::Terminated) => "Terminated".into(),
        other => format!("other: {other}"),
    }
}

/// Runs `dispatch` through the production catch on a spawned task and
/// returns `Ok(())` once the resumed panic is observed at the join.
async fn expect_unwind<F>(scope: EventStateScope<TestSession>, dispatch: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let joined = spawn(async move {
        guarded_dispatch(&scope, dispatch).await;
    })
    .await;
    match joined {
        Err(e) if e.is_panic() => Ok(()),
        other => bail!("guarded_dispatch must resume the unwind, got {other:?}"),
    }
}

/// Arm A/C — a handler (or final apply hook) panics with no attempt bump,
/// so a handle leaked past it keeps a CURRENT pin. After the catch resumes
/// the panic: a leaked read errors `Terminated`, a leaked `commit()` errors
/// `SessionClosed` (current pin, gate Closed — Closed is checked before
/// termination), and the dirty overlay is empty.
///
/// Falsify: drop the `close_gate()` acquire from the catch arm — the gate
/// stays Open, so the current-pin `commit()` falls through to the
/// termination check and errors `Terminated`, not `SessionClosed`. Closing
/// the gate under the panic is the catch's uniquely-pinned contribution
/// here. The read-`Terminated` and empty-overlay postconditions are *also*
/// produced by [`EventStateScope`]'s `Drop` re-running `terminate` +
/// `discard_dirty` (ungated) as the still-live scope unwinds, so deleting
/// those from the catch alone is masked — Drop's `terminate` is pinned by
/// [`dropped_dispatch_future_terminates_the_session`]. The catch's
/// gate-held discard uniquely defeats residue from a mutator admitted
/// *before* closure (the Arm D no-residue guarantee below), which the
/// ungated Drop cannot.
#[tokio::test]
#[expect(
    clippy::panic,
    reason = "the unwind pins drive a deliberate panic through the catch"
)]
async fn handler_panic_current_pin_leaks_are_fenced_and_overlay_cleared() -> Result<()> {
    let fx = Fixture::new()?;
    let session = fx.session();
    let scope = EventStateScope::new(session.clone());
    let context = MockEventContext::new().with_session(session);
    // Leak a current-pin handle and buffer a write, both at attempt 1.
    let leaked = handle(&context)?;
    leaked.set(json!("leaked")).await?;

    expect_unwind(scope, async move {
        let _hold = context;
        panic!("handler boom");
    })
    .await?;

    match leaked.get().await {
        Err(CellStateError::Access(StateAccessError::Terminated)) => {}
        other => bail!("leaked read must be Terminated, got {other:?}"),
    }
    match leaked.commit().await {
        Err(e) if tag(&e) == "SessionClosed" => {}
        other => bail!("current-pin leaked commit must be SessionClosed, got {other:?}"),
    }
    assert_eq!(
        fx.residue().await?,
        None,
        "the catch discarded the handler's buffered overlay",
    );
    Ok(())
}

/// Arm B — an intermediate `after_abort` panics mid retry loop, AFTER a
/// `next_attempt` bump, so the leaked handle predates the bump and is
/// STALE. Its `commit()` then errors `Terminated` (the pin compare fires
/// before the closed-gate check), distinguishing it from the current-pin
/// arms. Falsify: swap the pin/closed order in `mutate_permit` → this flips
/// to `SessionClosed`.
#[tokio::test]
#[expect(
    clippy::panic,
    reason = "the unwind pins drive a deliberate panic through the catch"
)]
async fn intermediate_bump_makes_leaked_commit_terminated() -> Result<()> {
    let fx = Fixture::new()?;
    let session = fx.session();
    let scope = EventStateScope::new(session.clone());
    let context = MockEventContext::new().with_session(session);
    // Leak a handle at attempt 1 BEFORE the bump.
    let leaked = handle(&context)?;

    expect_unwind(scope, async move {
        // Advance the attempt boundary via the real verb — exactly what
        // retry runs between attempts — then panic mid-loop.
        let context = context.next_attempt().await;
        let _hold = context;
        panic!("intermediate after_abort boom");
    })
    .await?;

    match leaked.commit().await {
        Err(e) if tag(&e) == "Terminated" => Ok(()),
        other => bail!("stale-pin leaked commit must be Terminated, got {other:?}"),
    }
}

/// Arm E — the dispatch future is DROPPED mid-flight (task cancellation),
/// which no catch ever sees. The scope's `Drop` flips termination
/// synchronously, so a handle leaked past it errors `Terminated`. (No
/// no-residue claim: the ungated drop cannot revoke an already-admitted
/// parked mutator — the documented drop-path residual.) Falsify: remove
/// `terminate()` from `EventStateScope::Drop` → the leaked read returns
/// `Ok`.
#[tokio::test]
async fn dropped_dispatch_future_terminates_the_session() -> Result<()> {
    let fx = Fixture::new()?;
    let session = fx.session();
    let context = MockEventContext::new().with_session(session.clone());
    let leaked = handle(&context)?;

    let (parked_tx, parked_rx) = oneshot::channel::<()>();
    let (_never_tx, never_rx) = oneshot::channel::<()>();
    // A dispatch future owning the scope that parks forever; aborting the
    // task drops the future, running the scope's Drop.
    let task = spawn(async move {
        let scope = EventStateScope::new(session);
        guarded_dispatch(&scope, async move {
            parked_tx.send(()).ok();
            let _ = never_rx.await;
        })
        .await;
    });
    parked_rx
        .await
        .map_err(|_| eyre!("dispatch never parked"))?;
    task.abort();
    let _ = task.await;

    match leaked.get().await {
        Err(CellStateError::Access(StateAccessError::Terminated)) => Ok(()),
        other => bail!("a dropped-future leak must error Terminated, got {other:?}"),
    }
}

/// A handler that leaks a bound keyed-state handle into a detached task,
/// then panics — the same current-pin leak as
/// [`handler_panic_current_pin_leaks_are_fenced_and_overlay_cleared`], but
/// exercised through the production `process_event` wiring rather than a
/// direct [`guarded_dispatch`] call. The leaked task parks until the test
/// releases it (strictly after the catch has run), then reports its
/// `commit()` error tag.
struct PanicLeakHandler {
    /// Fires with the in-attempt `set` outcome the instant before the
    /// panic, so the test knows the message was dispatched (ruling out a
    /// `Draining` drop race) and the leaked handle was admitted while the
    /// attempt was live.
    reached: SyncMutex<Option<oneshot::Sender<Result<(), String>>>>,
    /// Releases the leaked task's `commit()` — the test sends this only
    /// after `shutdown()` has joined the panicked partition task.
    go: SyncMutex<Option<oneshot::Receiver<()>>>,
    /// Reports the leaked `commit()`'s error tag back to the test.
    tag: SyncMutex<Option<oneshot::Sender<String>>>,
}

impl EventHandler for PanicLeakHandler {
    type Payload = Value;

    #[expect(
        clippy::panic,
        reason = "the unwind pin drives a deliberate panic through the catch"
    )]
    fn on_message<C>(
        &self,
        context: C,
        _message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let reached = self.reached.lock().take();
        let go = self.go.lock().take();
        let tag_tx = self.tag.lock().take();
        async move {
            let (Some(reached), Some(go), Some(tag_tx)) = (reached, go, tag_tx) else {
                return;
            };
            let handle = match context.state(Registered::new(value_state::<JsonCodec>(NAME))) {
                Ok(handle) => handle,
                Err(e) => {
                    reached.send(Err(format!("bind: {e}"))).ok();
                    return;
                }
            };
            let set_outcome = handle.set(json!("leaked")).await.map_err(|e| tag(&e));
            // Leak the CURRENT-pin handle into a detached task that outlives
            // the attempt; it commits only once the test releases `go`,
            // which it does strictly after the catch has closed the gate.
            spawn(async move {
                if go.await.is_err() {
                    return;
                }
                let outcome = match handle.commit().await {
                    Ok(_) => "Ok".to_owned(),
                    Err(e) => tag(&e),
                };
                tag_tx.send(outcome).ok();
            });
            reached.send(set_outcome).ok();
            panic!("handler boom");
        }
    }

    async fn on_excise<C>(
        &self,
        _context: C,
        message: UncommittedMessage<()>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        message.commit().await;
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
    }

    async fn shutdown(self) {}
}

/// Abnormal-exit fencing through the PRODUCTION entry — the same
/// current-pin leak as
/// [`handler_panic_current_pin_leaks_are_fenced_and_overlay_cleared`], but
/// driven through `process_event` (which wraps every dispatch in
/// [`guarded_dispatch`]) instead of calling `guarded_dispatch` directly, so
/// the zero-orchestration production wiring is what fences the leak. A
/// handler leaks a live-attempt handle into a detached task and panics;
/// after `shutdown()` joins the panicked partition task — which completes
/// only once the catch has run `close_gate` → `resume_unwind` — the leaked
/// `commit()` errors `SessionClosed`: current pin, closed gate, no epoch
/// bump.
///
/// Falsify: in `process_event`'s message arm, replace
/// `guarded_dispatch(&scope, …).await` with `….await` (keeping the trailing
/// `cloned_context.invalidate();`). With no catch, only
/// [`EventStateScope`]'s `Drop` runs during the unwind (terminate +
/// discard, gate left OPEN), so the leaked `commit()` falls through to the
/// termination check and errors `Terminated`, not `SessionClosed`. This is
/// the half of the abnormal-exit fencing the direct-`guarded_dispatch` unit
/// arms above cannot reach; the stale-pin-through-`RetryHandler` half lives
/// in `retry::tests`.
#[tokio::test]
async fn process_event_wires_the_catch_for_a_panicking_handler() -> Result<()> {
    init_test_logging();
    let (reached_tx, reached_rx) = oneshot::channel();
    let (go_tx, go_rx) = oneshot::channel();
    let (tag_tx, tag_rx) = oneshot::channel();
    let handler = PanicLeakHandler {
        reached: SyncMutex::new(Some(reached_tx)),
        go: SyncMutex::new(Some(go_rx)),
        tag: SyncMutex::new(Some(tag_tx)),
    };

    let mut registry = CollectionDefRegistry::default();
    registry
        .register(&value_state::<JsonCodec>(NAME), CollectionDef::new(None))
        .map_err(|e| eyre!("register: {e}"))?;
    let mut config = default_config();
    config.state_provider = memory_state_provider(registry);
    let partition_manager = PartitionManager::new(config, handler, "test-topic".into(), 0);

    partition_manager
        .try_send_record(ConsumerRecord::Message(create_test_message(0, "key")?))
        .map_err(|_| eyre!("message send rejected"))?;

    // The handler ran and buffered a set on the live attempt and is about
    // to panic; awaiting this before shutting down rules out any `Draining`
    // message-drop race. The deadline is only a hang-guard.
    let set_outcome = timeout(Duration::from_secs(5), reached_rx)
        .await
        .map_err(|_| eyre!("handler never reached the leak point"))?
        .map_err(|_| eyre!("reached sender dropped"))?;
    if let Err(t) = set_outcome {
        bail!("in-attempt set must succeed, got {t}");
    }

    // Joins the panicked partition task: `guarded_dispatch`'s catch (close
    // gate, discard, terminate, resume) has fully run once shutdown returns.
    partition_manager.shutdown().await;

    // Release the leaked handle's commit, now strictly after the catch.
    if go_tx.send(()).is_err() {
        bail!("leaked task dropped its release channel before committing");
    }
    let tag = timeout(Duration::from_secs(5), tag_rx)
        .await
        .map_err(|_| eyre!("leaked commit never reported"))?
        .map_err(|_| eyre!("tag sender dropped"))?;
    if tag != "SessionClosed" {
        bail!(
            "leaked current-pin commit through the production catch must be SessionClosed, got \
             {tag}"
        );
    }
    Ok(())
}

// Arm D (the paused-time admitted-mutator FIFO race — a detached `set`
// parked mid-op while the catch's `close_gate()` waits behind its permit)
// is not a unit-level pin: forcing the park *between* `mutate_permit` and
// the buffer write needs store instrumentation the memory backend does not
// expose. Its no-residue outcome is covered by
// `handler_panic_current_pin_leaks_are_fenced_and_overlay_cleared` (the
// catch discards the handler's buffered write); the FIFO ordering itself —
// `close_gate().await` strictly before the discard in `guarded_dispatch` —
// is a code invariant backed by the gate-serialization pins in
// `state::tests::gate_suite`.
