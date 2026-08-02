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
use crate::router::fleet::config::{
    FleetConfiguration, FleetConfigurationError, validate_scratch_budget,
};
use crate::router::fleet::{Destination, Refusal, Reservation};
use crate::router::{Framed, ResponseSender, Router, SendFailure};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::{Instant, sleep_until, timeout_at};
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
pub(crate) struct TypedSender<C: Codec, R> {
    router: R,
    /// One cell per fleet cell, allocated once and never pushed. A lane is
    /// replaced when its generation no longer matches the reservation's, which
    /// is both how an evicted destination's lane is removed and how a lane
    /// whose worker ended is rebuilt.
    lanes: Mutex<Vec<Option<Lane<C>>>>,
    cap: FrameCap,
    config: FleetConfiguration,
    counters: Arc<SendCounters>,
}

/// What every worker of one sender shares.
struct LaneContext<R> {
    router: R,
    attempts: u32,
    counters: Arc<SendCounters>,
}

/// One destination's queue, and which occupant of its cell it feeds.
///
/// One worker per destination, not several. `tokio::sync::mpsc` has exactly one
/// receiver, so more workers would need either a shared receiver behind an
/// async lock or a round robin over private queues, and the second strands jobs
/// behind one stalled worker. Per-destination concurrency would only buy
/// throughput against a slow peer, which the rate limit already bounds.
struct Lane<C: Codec> {
    generation: u64,
    jobs: Sender<Job<C>>,
}

/// One response, waiting for its turn on a destination.
struct Job<C: Codec> {
    header: FrameHeader,
    payload: C::Payload,
    /// Released when the send ends: by delivery, by a terminal status, by the
    /// deadline, or by the drain.
    slot: OwnedSemaphorePermit,
    expires_at: Instant,
}

/// What one sender's deliveries came to.
#[derive(Debug, Default)]
pub(crate) struct SendCounters {
    sent: AtomicU64,
    dropped: AtomicU64,
}

impl<C: Codec, R: Router> TypedSender<C, R> {
    /// Builds a sender over `router`'s fleet, with `cap` as the frame ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`FleetConfigurationError::ScratchBudget`] when one encode
    /// buffer per destination at `cap` exceeds what the process may commit
    /// to.
    pub(crate) fn new(router: R, cap: FrameCap) -> Result<Self, FleetConfigurationError> {
        let config = router.fleet().config();
        validate_scratch_budget(config.max_destinations, cap)?;
        let mut lanes = Vec::with_capacity(config.max_destinations);
        lanes.resize_with(config.max_destinations, || None);
        Ok(Self {
            router,
            lanes: Mutex::new(lanes),
            cap,
            config,
            counters: Arc::new(SendCounters::default()),
        })
    }

    /// Queues one response for delivery.
    ///
    /// Never awaits. An apply hook calls this, and apply hooks are per-key
    /// serialized: the next event for the same key waits for the hook to
    /// return. So every step here refuses rather than waits — the reservation,
    /// the lane and the queue alike.
    ///
    /// # Errors
    ///
    /// Returns [`Refused::Fleet`] when the fleet refused a slot, and
    /// [`Refused::Queue`] when the destination's lane could not take the job.
    pub(crate) fn send(&self, header: FrameHeader, payload: C::Payload) -> Result<(), Refused> {
        let reservation = self.router.fleet().reserve(header.target)?;
        let jobs = self.lane(&reservation);
        let expires_at = Instant::now() + self.config.send_deadline;
        reservation.commit(|slot| {
            let job = Job {
                header,
                payload,
                slot,
                expires_at,
            };
            if jobs.try_send(job).is_err() {
                // `Full` cannot happen: every lane on one destination draws
                // from that destination's one set of slots, and every queued
                // job holds one, so a job that got a slot always has room.
                // `Closed` means the worker ended between the lane check and
                // here. Dropping the returned job releases its slot.
                self.counters.dropped.fetch_add(1, Relaxed);
                return Err(Refused::Queue);
            }
            Ok(())
        })
    }

    /// What this sender's deliveries came to.
    pub(crate) fn counters(&self) -> &Arc<SendCounters> {
        &self.counters
    }

    /// The lane for this reservation's destination, built when the cell holds
    /// none or holds one for an earlier occupant.
    ///
    /// Replacing a lane drops its queue handle; the retired worker delivers
    /// what it already holds and then exits. That is a lane's removal path.
    fn lane(&self, reservation: &Reservation<'_>) -> Sender<Job<C>> {
        let mut lanes = self.lanes.lock();
        let cell = &mut lanes[reservation.slot()];
        if let Some(lane) = cell.as_ref()
            && lane.generation == reservation.generation()
            && !lane.jobs.is_closed()
        {
            return lane.jobs.clone();
        }
        let (jobs, queue) = channel(self.config.slots_each);
        drop(tokio::spawn(run_lane(
            queue,
            Arc::clone(reservation.destination()),
            LaneContext {
                router: self.router.clone(),
                attempts: self.config.max_send_attempts,
                counters: Arc::clone(&self.counters),
            },
            self.cap,
        )));
        *cell = Some(Lane {
            generation: reservation.generation(),
            jobs: jobs.clone(),
        });
        jobs
    }
}

impl SendCounters {
    /// How many responses reached their destination.
    pub(crate) fn sent(&self) -> u64 {
        self.sent.load(Relaxed)
    }

    /// How many responses were given up on, for any reason.
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Relaxed)
    }
}

/// One destination's worker: it drains the lane, encodes each response into its
/// own scratch, and delivers it.
///
/// The codec and the scratch are built once here, so the steady-state send path
/// allocates nothing. The loop keeps draining after its queue handle drops,
/// which is what makes a queued response outlive the layer that queued it.
async fn run_lane<C: Codec, R: Router>(
    mut queue: Receiver<Job<C>>,
    destination: Arc<Destination>,
    context: LaneContext<R>,
    cap: FrameCap,
) {
    let mut encoder = FrameEncoder::new(C::default(), cap);
    while let Some(job) = queue.recv().await {
        let Job {
            header,
            payload,
            slot,
            expires_at,
        } = job;
        // One deadline over the whole pipeline — the pacing wait, the address
        // read, the encode and every attempt — so a transport that never
        // answers still releases the slot. A job that sat in the queue past its
        // deadline is refused here rather than encoded.
        let pipeline = deliver_job(&mut encoder, &destination, &context, header, payload);
        if timeout_at(expires_at, pipeline).await.is_err() {
            context.counters.dropped.fetch_add(1, Relaxed);
        }
        drop(slot);
    }
}

/// Paces one response, resolves its destination, frames it and delivers it.
///
/// The pacing wait comes first, so a response whose deadline expires while it
/// waits is never encoded at all.
async fn deliver_job<C: Codec, R: Router>(
    encoder: &mut FrameEncoder<C>,
    destination: &Destination,
    context: &LaneContext<R>,
    header: FrameHeader,
    payload: C::Payload,
) {
    sleep_until(destination.next_send()).await;
    // No address originates anywhere but a registration: a node the directory
    // does not hold is not dialed at all.
    let address = match context.router.address(header.target).await {
        Ok(Some(address)) => address,
        Ok(None) => {
            context.counters.dropped.fetch_add(1, Relaxed);
            return;
        }
        Err(error) => {
            warn!(%error, node = %header.target, "peer address lookup failed");
            context.counters.dropped.fetch_add(1, Relaxed);
            return;
        }
    };
    let staged = match encoder.stage(&header, payload) {
        Ok(staged) => staged,
        Err(error) => {
            warn!(%error, node = %header.target, "response could not be framed");
            context.counters.dropped.fetch_add(1, Relaxed);
            return;
        }
    };
    match deliver(
        context.router.sender(),
        destination,
        &address,
        &staged,
        context.attempts,
    )
    .await
    {
        Ok(()) => {
            context.counters.sent.fetch_add(1, Relaxed);
        }
        Err(failure) => {
            warn!(%failure, node = %header.target, "response delivery failed");
            context.counters.dropped.fetch_add(1, Relaxed);
        }
    }
}

/// Delivers one frame, trying again only for a failure another attempt could
/// fix.
///
/// Every retry claims the destination's pacing too, so the rate limit bounds
/// what one destination receives rather than what it is asked for.
async fn deliver<S: ResponseSender, F: Framed + Sync>(
    sender: &S,
    destination: &Destination,
    address: &Endpoint,
    frame: &F,
    attempts: u32,
) -> Result<(), SendFailure> {
    let mut outcome = sender.deliver(address, frame).await;
    for _ in 1..attempts {
        match outcome {
            Ok(()) => return Ok(()),
            Err(failure) if !failure.is_ambiguous() => return Err(failure),
            Err(_) => {
                sleep_until(destination.next_send()).await;
                outcome = sender.deliver(address, frame).await;
            }
        }
    }
    outcome
}

/// Why a response could not be queued.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum Refused {
    /// The fleet refused a send slot.
    #[error(transparent)]
    Fleet(#[from] Refusal),

    /// The destination's lane could not take the response.
    #[error("the destination's queue could not take the response")]
    Queue,
}
