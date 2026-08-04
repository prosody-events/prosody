//! The typed half of delivery: what holds a queued result, encodes it, and
//! hands it to the transport.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the respond layer is this module's production caller; every item here is \
                  exercised by this module's tests"
    )
)]

use crate::codec::Codec;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::router::directory::Endpoint;
use crate::router::fleet::config::{FleetConfigurationError, validate_scratch_budget};
use crate::router::fleet::{Destination, DestinationFleet};
use crate::router::{Framed, ResponseSender, Router, SendFailure};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use tokio::select;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until, timeout_at};
use tracing::warn;

#[cfg(test)]
mod tests;

/// What fraction of a response's remaining budget the first of two endpoints
/// may spend. The rest is left for the second, so an endpoint that answers
/// nothing at all cannot make the fallback unreachable.
const PROBE_DIVISOR: u32 = 4;

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

/// What every worker of one sender shares.
struct WorkerContext<R> {
    router: R,
    attempts: u32,
    counters: Arc<SendCounters>,
}

/// One response, waiting for its turn on a destination.
struct Job<C: Codec> {
    header: FrameHeader,
    payload: C::Payload,
    /// The destination this response is paced against. Carried by the job
    /// rather than by the queue, so a queue outlives every occupant of its cell
    /// and a worker never paces one destination against another's schedule.
    destination: Arc<Destination>,
    /// Released when the send ends: by delivery, by a terminal status, by the
    /// deadline, or by the drain.
    slot: OwnedSemaphorePermit,
    expires_at: Instant,
}

/// What one sender's deliveries came to.
///
/// Every job a worker dequeues moves exactly one of these. A response the
/// destination's queue could not take moves `dropped` alone, because no worker
/// ever sees it.
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

    /// Queues one response for delivery.
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
    /// [`SendCounters::dropped`].
    pub(crate) fn send(&self, header: FrameHeader, payload: C::Payload) -> Result<(), C::Payload> {
        let Ok(reservation) = self.fleet.reserve(header.target) else {
            return Err(payload);
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
                self.counters.dropped.fetch_add(1, Relaxed);
                return Err(error.into_inner().payload);
            }
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

/// One cell's worker: it drains the queue, encodes each response into its own
/// scratch, and delivers it.
///
/// `encoder` carries the codec and the scratch this worker keeps for its whole
/// life, so a response is encoded into a buffer that already exists. The loop
/// keeps draining after its queue handle drops, so a queued response outlives
/// the layer that queued it.
///
/// Exactly one counter moves per dequeued job. The deadline is the biased arm
/// of the select, so a job whose deadline has already passed is dropped before
/// the pipeline is polled at all — nothing is paced, encoded or sent for it.
/// Work already inside one poll still finishes: this is a deadline the pipeline
/// is measured against between polls, never an absolute wall-clock cut.
async fn run_worker<C: Codec, R: Router>(
    mut queue: Receiver<Job<C>>,
    context: WorkerContext<R>,
    mut encoder: FrameEncoder<C>,
) {
    while let Some(job) = queue.recv().await {
        let Job {
            header,
            payload,
            destination,
            slot,
            expires_at,
        } = job;
        // One deadline over the whole pipeline — the pacing wait, the address
        // read, the encode and every attempt — so a transport that never
        // answers still releases the slot.
        let delivered = select! {
            biased;
            () = sleep_until(expires_at) => false,
            delivered = deliver_job(
                &mut encoder,
                &destination,
                &context,
                header,
                payload,
                expires_at,
            ) => {
                delivered
            }
        };
        if delivered {
            context.counters.sent.fetch_add(1, Relaxed);
        } else {
            context.counters.dropped.fetch_add(1, Relaxed);
        }
        // A worker can wait for its next response for as long as its
        // destination is quiet, so it gives the response's bytes back first.
        encoder.release();
        drop(slot);
    }
}

/// Resolves one response's route, frames it and delivers it.
///
/// Returns `true` only when the destination accepted the frame. The caller
/// counts the outcome, so one dequeued job moves one counter.
///
/// The attempt budget applies to each endpoint rather than to both together.
/// Sharing it would make the fallback unreachable whenever one attempt is
/// configured. Both endpoints together stay inside the job's single deadline,
/// which the worker's biased select already enforces.
///
/// The *time* budget is split the same way, and for the same reason. Where a
/// route offers a second endpoint, the first one gets [`PROBE_DIVISOR`] of what
/// is left and the second gets the rest. An address that drops packets instead
/// of refusing them would otherwise spend the whole deadline unanswered, and
/// the endpoint that works would never be tried — which is exactly the
/// misapplied label the fallback exists for.
async fn deliver_job<C: Codec, R: Router>(
    encoder: &mut FrameEncoder<C>,
    destination: &Destination,
    context: &WorkerContext<R>,
    header: FrameHeader,
    payload: C::Payload,
    expires_at: Instant,
) -> bool {
    // No address originates anywhere but a registration: a node the directory
    // does not hold is not dialed at all, and a node the rules refuse to reach
    // from here is not dialed either.
    let route = match context.router.route(header.target).await {
        Ok(Some(route)) => route,
        Ok(None) => return false,
        Err(error) => {
            warn!(%error, node = %header.target, "peer route lookup failed");
            return false;
        }
    };
    let staged = match encoder.stage(&header, payload) {
        Ok(staged) => staged,
        Err(error) => {
            warn!(%error, node = %header.target, "response could not be framed");
            return false;
        }
    };
    let mut remembered = None;
    let mut last_failure = None;
    let candidates = route.candidates(destination.preferred());
    let probed = candidates[1].is_some();
    for (index, (preference, address)) in candidates.into_iter().flatten().enumerate() {
        let until = if probed && index == 0 {
            probe_deadline(expires_at)
        } else {
            expires_at
        };
        match deliver(
            context.router.sender(),
            destination,
            address,
            &staged,
            context.attempts,
            until,
        )
        .await
        {
            Ok(()) => {
                destination.prefer(Some(preference));
                return true;
            }
            Err(failure) => {
                last_failure = Some(failure);
                if !failure.is_wrong_endpoint() {
                    // Only an answer from the node proves this endpoint is the
                    // reachable one. Whatever the last candidate did, an
                    // endpoint that said nothing is not worth remembering.
                    remembered = failure.answered().then_some(preference);
                    break;
                }
            }
        }
    }
    destination.prefer(remembered);
    if let Some(failure) = last_failure {
        warn!(%failure, node = %header.target, "response delivery failed");
    }
    false
}

/// Delivers one frame to one endpoint, trying again only for a failure another
/// attempt could fix, and giving up at `until`.
///
/// Every attempt claims the destination's pacing, so the rate limit bounds what
/// one destination is asked for rather than what it receives. A response that
/// falls back enters here a second time, and that endpoint's attempts claim
/// too.
///
/// `until` bounds this endpoint as a whole, rather than only the `grpc-timeout`
/// each attempt states. The channel lookup and the readiness wait both run
/// before that header is written, so an address that drops packets instead of
/// refusing them answers nothing and states nothing. Giving up therefore reads
/// as [`SendFailure::Unreachable`], which is what it is.
async fn deliver<S: ResponseSender, F: Framed + Sync>(
    sender: &S,
    destination: &Destination,
    address: &Endpoint,
    frame: &F,
    attempts: u32,
    until: Instant,
) -> Result<(), SendFailure> {
    let walk = async {
        sleep_until(destination.next_send()).await;
        let mut outcome = sender.deliver(address, frame, until).await;
        for _ in 1..attempts {
            match outcome {
                Ok(()) => return Ok(()),
                Err(failure) if !failure.is_ambiguous() => return Err(failure),
                Err(_) => {
                    sleep_until(destination.next_send()).await;
                    outcome = sender.deliver(address, frame, until).await;
                }
            }
        }
        outcome
    };
    match timeout_at(until, walk).await {
        Ok(outcome) => outcome,
        Err(_) => Err(SendFailure::Unreachable),
    }
}

/// What the first of two endpoints may spend of what is left.
fn probe_deadline(expires_at: Instant) -> Instant {
    let now = Instant::now();
    now + expires_at.saturating_duration_since(now) / PROBE_DIVISOR
}
