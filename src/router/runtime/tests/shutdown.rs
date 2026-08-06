//! The order one process stops in, and what it leaves behind.
//!
//! The shutdown future is polled by hand here rather than awaited, because the
//! claim is about what has and has not happened between two steps. A failure
//! before shutdown returns leaves the process's listener running; the test
//! process ends with the failure, which is what releases it.

use super::{ALPHA, PlainProcess, Process, Shared, TIMEOUT, frame_cap, header, plain_process};
use crate::codec::Codec;
use crate::requester::config::RequesterConfiguration;
use crate::response::RequestId;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::tests::CountingCodec;
use crate::response::sender::TypedSender;
use crate::router::ResponseSender;
use crate::router::directory::NodeDirectory;
use crate::router::directory::tests::support::finish;
use crate::router::fleet::{Refusal, Reservation};
use crate::router::grpc::client::GrpcSender;
use crate::router::loopback::HANG_GUARD;
use crate::subsystem::SubsystemName;
use crate::test_util::{TEST_RUNTIME, integration_test_count};
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use futures::poll;
use opentelemetry::Context;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Acquire, Release};
use tokio::task::yield_now;
use tokio::time::Instant;

/// The response drain has not run.
const NOT_STARTED: u8 = 0;

/// The response drain ran while the admission gate was open or occupied.
const GATE_STILL_OPEN: u8 = 1;

/// The response drain ran after the admission gate closed and emptied.
const CLOSED_AND_DRAINED: u8 = 2;

/// What one process holds when its shutdown starts.
///
/// Both floors are load-bearing rather than taste. With no parked request the
/// registry is already empty, so removing the step that wakes waiters would
/// change nothing observable. With no held reservation the whole sequence can
/// finish inside one poll, so the assertions that must see it suspended would
/// never run.
#[derive(Clone, Copy, Debug)]
struct Interleaving {
    /// Responses queued and held in flight.
    queued: usize,
    /// Parked requests. Never fewer than one.
    waiting: usize,
    /// Reservations held across the start of shutdown. Never fewer than one.
    hooks: usize,
}

impl Arbitrary for Interleaving {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            queued: usize::arbitrary(g) % 5,
            waiting: 1 + usize::arbitrary(g) % 4,
            hooks: 1 + usize::arbitrary(g) % 3,
        }
    }

    /// Shrinks every count and keeps both floors.
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(
            (self.queued, self.waiting - 1, self.hooks - 1)
                .shrink()
                .map(|(queued, waiting, hooks)| Self {
                    queued,
                    waiting: waiting + 1,
                    hooks: hooks + 1,
                }),
        )
    }
}

/// A process registers before `start` returns, and a clean shutdown removes
/// both rows.
///
/// A process that enables no peer feature still takes its place in the
/// directory: registration is unconditional, which is what makes any node
/// reachable from any other.
#[test]
fn runtime_registers_on_start_and_deregisters_on_shutdown() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let PlainProcess {
            runtime,
            directory,
            membership,
            bound_port,
        } = plain_process().await?;
        let node = runtime.node();
        let outcome: Result<()> = async {
            let registered = directory
                .read(node)
                .await?
                .ok_or_else(|| eyre!("a started runtime must already resolve"))?;
            ensure!(
                registered.node == node,
                "the published row must belong to this process"
            );
            ensure!(
                registered.direct.port == bound_port,
                "the runtime must publish the port the listener bound"
            );
            ensure!(
                registered.group.as_ref() == Some(&membership),
                "the runtime must publish the group it was started with"
            );
            ensure!(
                registered.advertised.is_none() && registered.network.is_none(),
                "an unconfigured process publishes no entry point and no network"
            );
            ensure!(
                runtime.router.addresses.resolve(node).await?.as_deref() == Some(&registered),
                "the runtime must resolve its own node through its cache"
            );
            Ok(())
        }
        .await;
        runtime.shutdown(|| async {}).await?;
        outcome?;
        ensure!(
            directory.read(node).await?.is_none(),
            "shutdown must remove the node row"
        );
        Ok(())
    })
}

/// Shutdown closes and drains admission before it drains queued responses, from
/// every generated mix of held reservations, queued responses and parked
/// requests.
///
/// See [`PeerRuntime::shutdown`](super::super::PeerRuntime::shutdown) for why
/// the gate closes before the drain.
/// Every one of these is true when shutdown returns: no reservation survives,
/// every parked request was released, every queued response was delivered, this
/// node's rows are gone, and the listener's socket is closed. That a released
/// request reports `ShuttingDown` to its caller is pinned by
/// `shutdown_discards_partial_results`.
#[test]
fn prop_shutdown_leaves_no_registration_and_no_reservation() {
    /// Runs one generated schedule against live Cassandra and a real listener.
    fn ordered(schedule: Interleaving) -> TestResult {
        finish(TEST_RUNTIME.block_on(async move {
            let Process {
                runtime,
                sender,
                workers,
                shared,
            } = Process::new().await?;
            let hooks = schedule.hooks;
            let held = match arrange(schedule, &sender, &shared) {
                Ok(held) => held,
                Err(error) => {
                    shared.barrier.add_permits(1);
                    runtime
                        .shutdown(|| async move {
                            drop(sender);
                            workers.join().await;
                        })
                        .await?;
                    return Err(error);
                }
            };

            let witness = Arc::new(AtomicU8::new(NOT_STARTED));
            let drain = {
                let fleet = Arc::clone(&shared.fleet);
                let witness = Arc::clone(&witness);
                move || async move {
                    witness.store(
                        if fleet.is_closed() && fleet.tickets_held() == 0 {
                            CLOSED_AND_DRAINED
                        } else {
                            GATE_STILL_OPEN
                        },
                        Release,
                    );
                    drop(sender);
                    workers.join().await;
                }
            };
            let mut shutting = pin!(runtime.shutdown(drain));
            // The registry emptying is what says the steps before the gate ran.
            drive_until(
                &mut shutting,
                &witness,
                "shutdown never woke the waiting requests",
                || shared.pending.len() == 0,
            )
            .await?;
            drive_until(
                &mut shutting,
                &witness,
                "shutdown never closed the admission gate",
                || shared.fleet.is_closed(),
            )
            .await?;
            ensure!(
                witness.load(Acquire) == NOT_STARTED,
                "the response drain began while a reservation still held the gate"
            );
            ensure!(
                shared.fleet.tickets_held() == u64::try_from(hooks)?,
                "the gate must still count every held reservation"
            );

            drop(held);
            shared.barrier.add_permits(1);
            shutting.await?;

            ensure!(
                witness.load(Acquire) == CLOSED_AND_DRAINED,
                "the response drain must find a closed and empty gate"
            );
            settled(&shared, schedule.queued).await
        }))
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(ordered as fn(Interleaving) -> TestResult);
}

/// A process with nothing queued, parked or reserved still stops cleanly.
///
/// This is the boundary the ordering property excludes: with no held
/// reservation the whole sequence can finish inside one poll, and it must.
#[test]
fn shutdown_from_a_quiet_process_terminates_clean() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let PlainProcess {
            runtime, directory, ..
        } = plain_process().await?;
        let node = runtime.node();
        let fleet = Arc::clone(&runtime.router.fleet);

        runtime.shutdown(|| async {}).await?;
        ensure!(
            directory.read(node).await?.is_none(),
            "shutdown must remove the node row"
        );
        ensure!(
            matches!(fleet.reserve(node), Err(Refusal::ShuttingDown)),
            "a quiet process must still close its admission gate"
        );
        Ok(())
    })
}

/// Queues the responses, parks the requests, and takes the reservations one
/// generated schedule calls for.
///
/// The reservations are taken from the fleet handle rather than from the
/// runtime, so they outlive the shutdown that consumes it.
fn arrange<'a>(
    schedule: Interleaving,
    sender: &TypedSender<CountingCodec>,
    shared: &'a Shared,
) -> Result<Vec<Reservation<'a>>> {
    for index in 0..schedule.queued {
        let payload = vec![u8::try_from(index)?];
        sender
            .send(
                header(shared.destination, RequestId::new(), ALPHA)?,
                Context::current(),
                payload,
            )
            .map_err(|_| eyre!("response {index} was refused before shutdown began"))?;
    }
    let awaited = [SubsystemName::try_new(ALPHA)?];
    for _ in 0..schedule.waiting {
        shared
            .pending
            .register_unguarded(&awaited, CountingCodec::FORMAT_ID, TIMEOUT)?;
    }
    ensure!(
        shared.pending.len() == schedule.waiting,
        "every parked request must still hold its entry"
    );
    (0..schedule.hooks)
        .map(|_| shared.fleet.reserve(shared.destination))
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

/// Polls `shutting` until `reached` answers true.
///
/// Fails when shutdown finishes first, when the response drain starts first, or
/// when `stalled` describes a step that never happened. The deadline is a hang
/// guard on a step that has no signal to wait on; the assertion is the state
/// each poll reads.
async fn drive_until<F: Future, C: FnMut() -> bool>(
    shutting: &mut Pin<&mut F>,
    witness: &AtomicU8,
    stalled: &'static str,
    mut reached: C,
) -> Result<()> {
    let deadline = Instant::now() + HANG_GUARD;
    while !reached() {
        ensure!(
            poll!(shutting.as_mut()).is_pending(),
            "shutdown finished while a reservation still held the gate"
        );
        ensure!(
            witness.load(Acquire) == NOT_STARTED,
            "the response drain began before the admission gate closed"
        );
        ensure!(Instant::now() < deadline, "{stalled}");
        yield_now().await;
    }
    Ok(())
}

/// Asserts everything one finished shutdown leaves behind.
async fn settled(shared: &Shared, queued: usize) -> Result<()> {
    ensure!(
        matches!(
            shared.fleet.reserve(shared.destination),
            Err(Refusal::ShuttingDown)
        ),
        "a closed fleet must refuse every new reservation"
    );
    ensure!(
        shared.pending.len() == 0,
        "shutdown must leave no request registered"
    );
    ensure!(
        shared.pending.available_permits() == RequesterConfiguration::default().max_in_flight,
        "shutdown must return every request admission permit"
    );
    ensure!(
        shared.counters.sent() == u64::try_from(queued)?,
        "the drain must deliver every response queued before shutdown"
    );
    ensure!(
        shared.counters.dropped() == 0,
        "the drain must strand no queued response"
    );
    ensure!(
        shared.directory.read(shared.node).await?.is_none(),
        "shutdown must remove the node row"
    );
    let transport = GrpcSender::new(frame_cap()?, &shared.fleet);
    let mut encoder = FrameEncoder::new(CountingCodec::default(), frame_cap()?);
    let probe = header(shared.node, RequestId::new(), ALPHA)?;
    let staged = encoder.stage(&probe, vec![0xA5])?;
    ensure!(
        transport
            .deliver(&shared.listener, &staged, Instant::now() + HANG_GUARD,)
            .await
            .is_err(),
        "the listener socket must be closed once shutdown has returned"
    );
    Ok(())
}
