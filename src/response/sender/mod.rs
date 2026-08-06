//! The typed half of delivery: what holds a queued result, encodes it, and
//! hands it to the transport.

mod metrics;
mod witness;
mod worker;

use self::metrics::{DropReason, Stage};
use self::witness::DeliveryWitness;
use self::worker::{Job, ResponseRoute, Then, WorkerContext, run_worker};
use crate::codec::Codec;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::{FleetConfigurationError, validate_scratch_budget};
use crate::router::{LocalTarget, RelayHop, Router, RouterHandle};
use opentelemetry::Context;
use std::sync::Arc;
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
    send_deadline: Duration,
    witness: DeliveryWitness,
}

/// The response delivery workers, joined once every send handle is gone.
///
/// This value holds no sender. Each queue has exactly one [`Sender`], all of
/// them live in the [`TypedSender`] this value was built beside, and that
/// sender is never cloned. A worker ends when its queue closes, so every worker
/// ends together with that one sender. In a consumer the sender sits inside the
/// shared responder, which a partition handler clones instead. The drop of the
/// last responder clone therefore starts the join.
///
/// No deadline guards the join. The shutdown order is the whole guarantee:
/// [`ProsodyConsumer::shutdown`](crate::consumer::ProsodyConsumer::shutdown)
/// sweeps every partition manager before the peer teardown runs, which is what
/// makes the wait finite. A consumer dropped without that shutdown runs no
/// sweep; that consumer's `Drop` states what it leaves behind.
#[must_use = "dropping this detaches every delivery worker, so the wait is lost"]
pub(crate) struct ResponseWorkers(Box<[JoinHandle<()>]>);

#[cfg(test)]
pub(crate) use witness::SendCounters;

impl<C: Codec> TypedSender<C> {
    /// Builds a sender over `router`'s fleet, with `cap` as the frame ceiling.
    ///
    /// Returns the sender and its [`ResponseWorkers`]. The caller owes that
    /// second value the join, in the order [`ResponseWorkers`] states.
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
    #[cfg(test)]
    pub(crate) fn new_without_local<R: Router>(
        router: &R,
        cap: FrameCap,
    ) -> Result<(Self, ResponseWorkers), FleetConfigurationError> {
        Self::build(router.fleet(), router, cap)
    }

    /// Builds a sender that deposits responses for `local` without gRPC.
    pub(crate) fn new<S, D>(
        router: &RouterHandle<S, D>,
        cap: FrameCap,
    ) -> Result<(Self, ResponseWorkers), FleetConfigurationError>
    where
        RouterHandle<S, D>: Router,
    {
        Self::build(
            router.fleet(),
            &Then(router.local().clone(), router.clone()),
            cap,
        )
    }

    /// Builds a sender that can only reach this process.
    pub(crate) fn new_local(
        local: &LocalTarget,
        fleet: &Arc<DestinationFleet>,
        cap: FrameCap,
    ) -> Result<(Self, ResponseWorkers), FleetConfigurationError> {
        Self::build(fleet, local, cap)
    }

    fn build<R: ResponseRoute>(
        fleet: &Arc<DestinationFleet>,
        router: &R,
        cap: FrameCap,
    ) -> Result<(Self, ResponseWorkers), FleetConfigurationError> {
        let fleet = Arc::clone(fleet);
        let config = fleet.config();
        validate_scratch_budget(config.max_destinations, cap)?;
        let witness = DeliveryWitness::new();
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
                    witness: witness.clone(),
                },
                FrameEncoder::new(C::default(), cap),
            )));
        }
        Ok((
            Self {
                fleet,
                queues: queues.into_boxed_slice(),
                send_deadline: config.send_deadline,
                witness,
            },
            ResponseWorkers(workers.into_boxed_slice()),
        ))
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
    /// result again and disposes of it. [`metrics`] counts every refusal by
    /// name. The test witness also counts a queue refusal as dropped.
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
            // Recorded before the job is published, so a worker cannot deliver
            // it and count that before this stage is counted. An exporter would
            // otherwise read more delivered than enqueued.
            Stage::Enqueued.record();
            if let Err(error) = jobs.try_send(job) {
                // `Full` cannot happen. A queue is as deep as its destination
                // has slots, every job in it holds one of them, and a cell only
                // changes occupant while every slot of it is free. A job that
                // got a slot therefore always has room. `Closed` means the
                // worker ended, which only a drain or a panic does. Dropping
                // the returned job releases its slot. This is the one drop that
                // no dequeued job accounts for.
                DropReason::from(&error).record();
                self.witness.dropped();
                return Err(error.into_inner().payload);
            }
            Ok(())
        })
    }

    /// What this sender's deliveries came to. The counters outlive this sender,
    /// so a caller may hold them while the workers finish.
    #[cfg(test)]
    pub(crate) fn counters(&self) -> Arc<SendCounters> {
        self.witness.counters()
    }
}

impl ResponseWorkers {
    /// Waits until every worker has finished what it holds.
    ///
    /// A dropped queue sender tells its worker that no more work can arrive.
    /// The worker then delivers what it already holds and exits. Each of those
    /// responses ends at its own expiry, so this wait is at most one send
    /// deadline past the last response queued. That deadline is measured
    /// between polls: a codec that never returns holds its worker, and this
    /// wait with it.
    pub(crate) async fn join(self) {
        for worker in self.0 {
            if let Err(error) = worker.await {
                warn!(%error, "a response delivery worker did not exit cleanly");
            }
        }
    }
}
