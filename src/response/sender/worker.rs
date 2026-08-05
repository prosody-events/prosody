//! One cell's delivery worker: what it dequeues, how it frames it, and how far
//! it will chase a destination that does not answer.

use super::metrics::{DropReason, Stage, record_fallback, record_rate_limited};
use crate::codec::Codec;
use crate::otel::carry_parent;
use crate::response::frame::FrameHeader;
use crate::response::frame::encode::FrameEncoder;
use crate::router::directory::Endpoint;
use crate::router::fleet::Destination;
use crate::router::{Framed, Preference, ResponseSender, Router, SendFailure};
use opentelemetry::Context;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use tokio::select;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, sleep_until, timeout_at};
use tracing::field::Empty;
use tracing::{Instrument, debug_span, warn};

use super::SendCounters;

/// One in this many of the deadline that is left is what an endpoint keeps for
/// the fallback behind it — and all an endpoint that has never answered may
/// spend. See [`Share`].
const FALLBACK_DIVISOR: u32 = 4;

/// What every worker of one sender shares.
pub(super) struct WorkerContext<R> {
    pub(super) router: R,
    pub(super) attempts: u32,
    pub(super) counters: Arc<SendCounters>,
}

/// One response, waiting for its turn on a destination.
pub(super) struct Job<C: Codec> {
    pub(super) header: FrameHeader,
    pub(super) payload: C::Payload,
    /// The trace the message that asked for this response belongs to.
    /// [`Answering`](crate::consumer::middleware::respond::Answering) states
    /// why it travels as a context.
    pub(super) trace: Context,
    /// The destination this response is paced against. Carried by the job
    /// rather than by the queue, so a queue outlives every occupant of its cell
    /// and a worker never paces one destination against another's schedule.
    pub(super) destination: Arc<Destination>,
    /// Released when the send ends: by delivery, by a terminal status, by the
    /// deadline, or by the drain.
    pub(super) slot: OwnedSemaphorePermit,
    pub(super) expires_at: Instant,
}

/// How much of what is left of a response's deadline one endpoint may spend.
///
/// While a route still has a candidate untried, no endpoint gets the whole
/// budget. An address which drops packets instead of refusing them would
/// otherwise spend the deadline unanswered and leave the endpoint that works
/// untried — and that is exactly what a misapplied label reaches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Share {
    /// An endpoint with a fallback behind it that this destination has never
    /// answered on. One [`FALLBACK_DIVISOR`]th of what is left.
    Probe,
    /// An endpoint with a fallback behind it that this destination answered on
    /// before. Everything but the [`FALLBACK_DIVISOR`]th it keeps for that
    /// fallback, because an endpoint that already answered is worth waiting
    /// for.
    Most,
    /// The last endpoint of a route. Everything that is left.
    Rest,
}

impl Share {
    /// The instant this endpoint must give up at, measured from now.
    ///
    /// Read after the pacing wait rather than before it, so a share is a slice
    /// of the time that is left to reach the network with.
    fn until(self, expires_at: Instant) -> Instant {
        let now = Instant::now();
        let left = expires_at.saturating_duration_since(now);
        let reserved = left / FALLBACK_DIVISOR;
        match self {
            Self::Probe => now + reserved,
            Self::Most => now + left.saturating_sub(reserved),
            Self::Rest => expires_at,
        }
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
/// Every dequeued job ends as exactly one outcome: it moves one stage or one
/// drop reason, one of this sender's two counters, and the `peer.disposition`
/// attribute on its own span. A delivered job also records `peer.preference`
/// and counts one fallback transition when its walk made one. Every count of a
/// job's outcome sits in this one match, so no two of them can disagree about
/// what that job came to. The deadline is the biased arm of the select, so a
/// job whose deadline has already passed is dropped before the pipeline is
/// polled at all — nothing is paced, encoded or sent for it.
/// Work already inside one poll still finishes: this is a deadline the pipeline
/// is measured against between polls, never an absolute wall-clock cut.
///
/// The `peer.response.send` span is opened here and covers the delivery alone.
/// It is a child of the trace the job carries, so the listener's
/// `peer.response.receive` — parented on the context this span's own injection
/// writes — lands under the call that asked for the response.
pub(super) async fn run_worker<C: Codec, R: Router>(
    mut queue: Receiver<Job<C>>,
    context: WorkerContext<R>,
    mut encoder: FrameEncoder<C>,
) {
    while let Some(job) = queue.recv().await {
        let Job {
            header,
            payload,
            trace,
            destination,
            slot,
            expires_at,
        } = job;
        let span = debug_span!(
            "peer.response.send",
            otel.kind = "client",
            peer.target = %header.target,
            peer.request = %header.request,
            peer.subsystem = %header.subsystem,
            peer.disposition = Empty,
            peer.preference = Empty,
        );
        carry_parent(&span, trace);
        // One deadline over the whole pipeline — the pacing wait, the address
        // read, the encode and every attempt — so a transport that never
        // answers still releases the slot.
        let outcome = select! {
            biased;
            () = sleep_until(expires_at) => Err(DropReason::Deadline),
            outcome = deliver_job(
                &mut encoder,
                &destination,
                &context,
                header,
                payload,
                expires_at,
            ).instrument(span.clone()) => {
                outcome
            }
        };
        // Recorded through the owned handle rather than the current span: a
        // level-disabled span never becomes current, and the deadline arm has
        // already left the instrumented future in any case.
        match outcome {
            Ok((preference, from)) => {
                span.record("peer.disposition", "delivered");
                span.record("peer.preference", preference.label());
                if let Some(from) = from {
                    record_fallback(from, preference);
                }
                Stage::Delivered.record();
                context.counters.sent.fetch_add(1, Relaxed);
            }
            Err(reason) => {
                span.record("peer.disposition", reason.label());
                reason.record();
                context.counters.dropped.fetch_add(1, Relaxed);
            }
        }
        // A worker can wait for its next response for as long as its
        // destination is quiet, so it gives the response's bytes back first.
        encoder.release();
        drop(slot);
    }
}

/// Resolves one response's route, frames it and delivers it.
///
/// `Ok` carries the candidate that accepted the frame, and the candidate tried
/// before it when the walk fell back; every other outcome names why the
/// response was dropped. The transition travels out rather than being counted
/// here, so the caller counts the whole outcome of one dequeued job in one
/// place.
///
/// The attempt budget applies to each endpoint rather than to both together.
/// Sharing it would make the fallback unreachable whenever one attempt is
/// configured. Both endpoints together stay inside the job's single deadline,
/// which the worker's biased select already enforces.
///
/// The *time* budget is split the same way, and for the same reason. [`Share`]
/// owns how much of it one endpoint may spend, and the endpoint this
/// destination remembers is the one that gets the larger part.
async fn deliver_job<C: Codec, R: Router>(
    encoder: &mut FrameEncoder<C>,
    destination: &Destination,
    context: &WorkerContext<R>,
    header: FrameHeader,
    payload: C::Payload,
    expires_at: Instant,
) -> Result<(Preference, Option<Preference>), DropReason> {
    // No address originates anywhere but a registration: a node the directory
    // does not hold is not dialed at all, and a node the rules refuse to reach
    // from here is not dialed either.
    let route = match context.router.route(header.target).await {
        Ok(Some(route)) => route,
        Ok(None) => return Err(DropReason::UnresolvableNode),
        Err(error) => {
            warn!(%error, node = %header.target, "peer route lookup failed");
            return Err(DropReason::LookupFailed);
        }
    };
    let staged = match encoder.stage(&header, payload) {
        Ok(staged) => staged,
        Err(error) => {
            warn!(%error, node = %header.target, "response could not be framed");
            return Err(DropReason::EncodeFailed);
        }
    };
    Stage::Framed.record();
    let mut remembered = None;
    let mut last_failure = None;
    // The candidate that failed the turn before this one. Inside the loop it
    // proves this is no longer the first candidate, and it is the `from` of a
    // fallback.
    let mut previous = None;
    let preferred = destination.preferred();
    let candidates = route.candidates(preferred);
    let has_fallback = candidates[1].is_some();
    let proven = candidates[0].is_some_and(|(preference, _)| Some(preference) == preferred);
    for (preference, address) in candidates.into_iter().flatten() {
        let share = if previous.is_some() || !has_fallback {
            Share::Rest
        } else if proven {
            Share::Most
        } else {
            Share::Probe
        };
        match deliver(
            context.router.sender(),
            destination,
            address,
            &staged,
            context.attempts,
            expires_at,
            share,
        )
        .await
        {
            Ok(()) => {
                destination.prefer(Some(preference));
                return Ok((preference, previous));
            }
            Err(failure) => {
                last_failure = Some((preference, failure));
                if !failure.is_wrong_endpoint() {
                    // A failure that is not a wrong endpoint is a status the
                    // path answered, so this endpoint is the one that reaches
                    // the node — refusal and all. Every other failure proves
                    // nothing about which endpoint serves the node, so it
                    // leaves nothing remembered.
                    remembered = Some(preference);
                    break;
                }
                previous = Some(preference);
            }
        }
    }
    destination.prefer(remembered);
    if let Some((preference, failure)) = last_failure {
        // What the walk did, not what the route offered. The last turn sets
        // `previous` as well, so a route of one candidate needs both terms.
        warn!(
            %failure,
            node = %header.target,
            preference = preference.label(),
            fell_back = has_fallback && previous.is_some(),
            "response delivery failed"
        );
    }
    Err(DropReason::SendFailed)
}

/// Delivers one frame to one endpoint, trying again only for a failure another
/// attempt could fix, and giving up on this endpoint at what `share` allows.
///
/// Every attempt claims the destination's pacing, so the rate limit bounds what
/// one destination is asked for rather than what it receives. A response that
/// falls back enters here a second time, and that endpoint's attempts claim
/// too.
///
/// The first attempt's pacing wait sits outside this endpoint's share: the
/// share is read after that wait, so a destination whose schedule is far ahead
/// does not read as an endpoint that answered nothing. A retry's wait is inside
/// the share, because the share is fixed once. That is deliberate — recomputing
/// it would let the first candidate spend past what [`Share`] leaves for the
/// fallback. A claimed turn is spent whether or not the send happens, so a
/// retry into a share that is already gone is not made at all.
async fn deliver<S: ResponseSender, F: Framed + Sync>(
    sender: &S,
    destination: &Destination,
    address: &Endpoint,
    frame: &F,
    attempts: u32,
    expires_at: Instant,
    share: Share,
) -> Result<(), SendFailure> {
    pace(destination).await;
    let until = share.until(expires_at);
    let mut outcome = attempt(sender, address, frame, expires_at, until).await;
    for _ in 1..attempts {
        match outcome {
            Ok(()) => return Ok(()),
            Err(failure) if !failure.is_ambiguous() => return Err(failure),
            Err(_) if Instant::now() >= until => return outcome,
            Err(_) => {
                pace(destination).await;
                outcome = attempt(sender, address, frame, expires_at, until).await;
            }
        }
    }
    outcome
}

/// Claims this destination's next turn and waits for it, counting the attempts
/// that really waited.
///
/// A claimed turn that is already due is not a rate limit, so the counter reads
/// as "how often pacing held a response back" rather than as an attempt count.
async fn pace(destination: &Destination) {
    let at = destination.next_send();
    if at > Instant::now() {
        record_rate_limited();
    }
    sleep_until(at).await;
}

/// One attempt, bounded twice over: by `expires_at` on the wire and by `until`
/// here.
///
/// The two deadlines are different on purpose. `expires_at` is what the peer is
/// told to answer inside, so a `DEADLINE_EXCEEDED` still means what it says —
/// the whole response ran out of time — rather than "this process moved on".
/// `until` is what this process spends on this one endpoint, and it covers the
/// channel lookup and the readiness wait as well, neither of which the
/// `grpc-timeout` header reaches. Giving up on it therefore reads as
/// [`SendFailure::Unreachable`]: nothing answered here, and the next candidate
/// keeps what the response has left.
async fn attempt<S: ResponseSender, F: Framed + Sync>(
    sender: &S,
    address: &Endpoint,
    frame: &F,
    expires_at: Instant,
    until: Instant,
) -> Result<(), SendFailure> {
    match timeout_at(until, sender.deliver(address, frame, expires_at)).await {
        Ok(outcome) => outcome,
        Err(_) => Err(SendFailure::Unreachable),
    }
}
