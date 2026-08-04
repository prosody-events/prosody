//! The typed half of delivery: what holds a queued result, encodes it, and
//! hands it to the transport.

mod metrics;
mod worker;

use self::metrics::{DropReason, Stage};
use self::worker::{Job, WorkerContext, run_worker};
use crate::codec::Codec;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::router::Router;
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::{FleetConfigurationError, validate_scratch_budget};
use opentelemetry::Context;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use tokio::sync::mpsc::{Sender, channel};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::warn;

#[cfg(test)]
mod tests;

/// The typed half of delivery.
///
/// A queued response is a moved handler result, and one process may run several
/// consumers whose results are different types. So the queues are typed and
/// belong to the layer that responds, while the capacity they draw on is
/// untyped and belongs to the process. This type is the typed half: it holds
/// the queued results, encodes them, and reserves from the shared fleet rather
/// than from a budget of its own.
///
/// Because the fleet is process-owned, a response queued before a partition is
/// revoked is still delivered.
pub(crate) struct TypedSender<C: Codec> {
    /// The fleet every send reserves from. Held rather than read from the
    /// router on each send, so the cell a reservation names always indexes the
    /// queues below: both are the length one configuration gave.
    fleet: Arc<DestinationFleet>,
    /// One queue per fleet cell, and one worker draining each of them. Both are
    /// built once, so a send neither allocates a queue nor starts a task.
    ///
    /// One worker per queue, not several. `tokio::sync::mpsc` has exactly one
    /// receiver. More workers would therefore need a shared receiver behind an
    /// async lock, or a round robin over private queues. A round robin strands
    /// jobs behind one stalled worker.
    ///
    /// One destination therefore sees at most one send in flight from this
    /// sender. A process that runs several senders overlaps that many, still
    /// under the destination's shared slot bound and its shared rate limit.
    ///
    /// A queue holds work for at most one destination at a time, whatever
    /// occupies its cell over the process's life: a queued job holds one of its
    /// destination's slots, and a cell is only evictable while every slot of it
    /// is free.
    queues: Box<[Sender<Job<C>>]>,
    workers: Box<[JoinHandle<()>]>,
    send_deadline: Duration,
    counters: Arc<SendCounters>,
}

/// What one sender's deliveries came to.
///
/// Every job a worker dequeues moves exactly one of these. A response the
/// destination's queue could not take moves `dropped` alone, because no worker
/// ever sees it.
///
/// This is the in-process account, which this module's suites assert on. The
/// operator's account is in [`metrics`], and it names each outcome rather than
/// totalling two.
#[derive(Debug, Default)]
pub(crate) struct SendCounters {
    sent: AtomicU64,
    dropped: AtomicU64,
}

impl<C: Codec> TypedSender<C> {
    /// Builds a sender over `router`'s fleet, with `cap` as the frame ceiling.
    ///
    /// One queue, one codec and one encode scratch at the cap are built here
    /// per fleet cell, and one worker is spawned to drive each of them. A send
    /// therefore reserves no buffer, no queue and no task. Admitting a
    /// destination the fleet does not hold yet costs one record, which
    /// [`DestinationFleet::new`] accounts for. Call this from inside a runtime:
    /// the workers are spawned tasks.
    ///
    /// # Errors
    ///
    /// Returns [`FleetConfigurationError::ScratchBudget`] when one encode
    /// buffer per destination at `cap` exceeds what one sender may commit to.
    pub(crate) fn new<R: Router>(
        router: &R,
        cap: FrameCap,
    ) -> Result<Self, FleetConfigurationError> {
        let fleet = Arc::clone(router.fleet());
        let config = fleet.config();
        validate_scratch_budget(config.max_destinations, cap)?;
        let counters = Arc::new(SendCounters::default());
        let mut queues = Vec::with_capacity(config.max_destinations);
        let mut workers = Vec::with_capacity(config.max_destinations);
        for _ in 0..config.max_destinations {
            let (jobs, queue) = channel(config.slots_each);
            queues.push(jobs);
            workers.push(tokio::spawn(run_worker(
                queue,
                WorkerContext {
                    router: router.clone(),
                    attempts: config.max_send_attempts,
                    counters: Arc::clone(&counters),
                },
                FrameEncoder::new(C::default(), cap),
            )));
        }
        Ok(Self {
            fleet,
            queues: queues.into_boxed_slice(),
            workers: workers.into_boxed_slice(),
            send_deadline: config.send_deadline,
            counters,
        })
    }

    /// Queues one response for delivery, in the trace `trace` names.
    ///
    /// `trace` is the requester's trace, captured by `Answering` in the respond
    /// layer, which states why it is a context rather than an ambient span.
    ///
    /// Never awaits. An apply hook calls this, and apply hooks are per-key
    /// serialized: the next event for the same key waits for the hook to
    /// return. So every step here refuses rather than waits — the reservation
    /// and the queue alike.
    ///
    /// # Errors
    ///
    /// Returns the payload, unencoded, when the fleet refused a slot or when
    /// the destination's queue could not take the job. The caller owns the
    /// result again and disposes of it. The rate of each refusal is already
    /// counted: a fleet refusal by the fleet, a queue refusal by
    /// [`SendCounters::dropped`], and both by name in [`metrics`].
    pub(crate) fn send(
        &self,
        header: FrameHeader,
        trace: Context,
        payload: C::Payload,
    ) -> Result<(), C::Payload> {
        Stage::Attempted.record();
        let reservation = match self.fleet.reserve(header.target) {
            Ok(reservation) => reservation,
            Err(refusal) => {
                DropReason::from(refusal).record();
                return Err(payload);
            }
        };
        // The cell a reservation names is one of this fleet's, and this
        // sender's queues are the same fleet's length.
        let jobs = &self.queues[reservation.slot()];
        let destination = Arc::clone(reservation.destination());
        let expires_at = Instant::now() + self.send_deadline;
        reservation.commit(|slot| {
            let job = Job {
                header,
                payload,
                trace,
                destination,
                slot,
                expires_at,
            };
            if let Err(error) = jobs.try_send(job) {
                // `Full` cannot happen. A queue is as deep as its destination
                // has slots, every job in it holds one of them, and a cell only
                // changes occupant while every slot of it is free. A job that
                // got a slot therefore always has room. `Closed` means the
                // worker ended, which only a drain or a panic does. Dropping
                // the returned job releases its slot. This is the one drop that
                // no dequeued job accounts for.
                DropReason::from(&error).record();
                self.counters.dropped.fetch_add(1, Relaxed);
                return Err(error.into_inner().payload);
            }
            Stage::Enqueued.record();
            Ok(())
        })
    }

    /// What this sender's deliveries came to. The counters outlive this sender,
    /// so a caller may hold them while the workers finish.
    pub(crate) fn counters(&self) -> Arc<SendCounters> {
        Arc::clone(&self.counters)
    }

    /// Stops taking work and returns once every worker has finished what it
    /// holds.
    ///
    /// Each response a worker still holds ends at its own expiry, so the wait
    /// is at most one deadline past the last response queued. That deadline is
    /// measured between polls: a codec that never returns holds its worker, and
    /// this wait with it. Shutdown runs this after the fleet's admission gate
    /// has drained, so nothing can be queued while it waits.
    pub(crate) async fn drain(self) {
        let Self {
            queues, workers, ..
        } = self;
        // Dropping the queues is what tells a worker no more work is coming;
        // it delivers what it already holds and then exits.
        drop(queues);
        for worker in Vec::from(workers) {
            if let Err(error) = worker.await {
                warn!(%error, "a response delivery worker did not exit cleanly");
            }
        }
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "no production reader yet: the consumer wiring that hands a consumer its \
                  responder will report these totals; the sender's and the respond layer's suites \
                  read them today"
    )
)]
impl SendCounters {
    /// How many responses reached their destination.
    pub(crate) fn sent(&self) -> u64 {
        self.sent.load(Relaxed)
    }

    /// How many responses this sender gave up on: what a worker dequeued and
    /// could not deliver, plus a job the destination's queue could not take.
    /// A fleet refusal is not here, because the fleet counts its own. A queue
    /// refusal counts here and still hands its payload back to the caller.
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Relaxed)
    }
}
