//! What crosses a real hop: the loop stop, the caller's budget, and the trace.

use super::{ALPHA, BUDGET, CAP_BYTES, FixedRouter, Live, PAYLOAD, Pair, TargetRoute, named};
use crate::codec::Codec;
use crate::requester::registry::PendingRegistry;
use crate::response::frame::encode::{FrameEncoder, Staged};
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::response::sender::TypedSender;
use crate::response::{RequestId, ResponseStatus};
use crate::router::directory::{Endpoint, NetworkId, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::TRANSPORT;
use crate::router::grpc::client::GrpcSender;
use crate::router::loopback::HANG_GUARD;
use crate::router::{Host, NodeId, Preference, ResponseSender, Router, SendFailure};
use crate::subsystem::SubsystemName;
use crate::test_util::{GlobalSpans, TEST_RUNTIME};
use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use opentelemetry::Value;
use opentelemetry_sdk::trace::SpanData;
use std::slice::from_ref;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, timeout};
use tonic::Code;
use tracing::{Instrument, info_span};

/// The span the delivery is made from, so every later span has one root to be
/// measured against.
const CALLER: &str = "peer.test.call";

/// The span a process opens for one frame it received.
const RECEIVED: &str = "peer.response.receive";

/// The span a process opens for one frame it sends on.
const FORWARDED: &str = "peer.response.forward";

/// The attribute carrying what is left of the caller's budget.
const DEADLINE_MS: &str = "peer.deadline_ms";

/// The budget a caller states. Well under the ceiling a process applies on its
/// own, so a hop that inherited nothing is unmistakable.
const CALLER_BUDGET: Duration = Duration::from_secs(30);

/// The ceiling the relaying process applies to one forward. Well under the
/// budget its caller states, so what the relay hands on is a number no caller
/// stated and no process would apply on its own.
const RELAY_CEILING: Duration = Duration::from_secs(5);

/// The ceiling a process applies if a caller stated none. Far above both.
const PROCESS_BUDGET: Duration = Duration::from_mins(1);

/// The two network labels the crossing case declares.
const HERE: &str = "here";
const THERE: &str = "there";

/// Two processes whose directory entries name each other pass a frame neither
/// owns exactly once.
///
/// The frame names a third node, so neither process can accept it. The first
/// sends it on and writes its own id into it; the second sees an id already
/// there and refuses. Without that refusal the two would pass the frame back
/// and forth until a budget ended it, so the answered status is the assertion:
/// `FAILED_PRECONDITION` is the refusal, and `DEADLINE_EXCEEDED` is the loop.
#[test]
fn a_frame_this_process_already_relayed_is_never_relayed_again() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pair = Pair::start(PROCESS_BUDGET, PROCESS_BUDGET, TargetRoute::Relay).await?;
        let forwarded = TRANSPORT.forwarded();
        let outcome = async {
            let answered = call(&pair.relay, NodeId::new(), RequestId::new(), BUDGET).await?;
            ensure(
                answered == Code::FailedPrecondition,
                format!(
                    "a frame neither process owns must be refused once it names a relay, not \
                     answered {answered:?}"
                ),
            )?;
            ensure(
                TRANSPORT.forwarded() == forwarded + 1,
                format!(
                    "exactly one process may send the frame on, but {} did",
                    TRANSPORT.forwarded() - forwarded
                ),
            )
        }
        .await;
        pair.stop().await?;
        outcome
    })
}

/// The budget a process states on the hop it makes is what is left of the
/// budget it is spending itself, never a budget it was handed.
///
/// The frame names a third node, so the second process refuses it — but it
/// refuses it after reading the budget it was given, which is the number this
/// case is about. Three budgets are deliberately distinct, so each way of
/// getting the number wrong records a different one: the caller states 30 s,
/// the relaying process holds that to its own 5 s ceiling, and a process given
/// no budget at all would apply a 60 s ceiling. So a hop that copied the
/// caller's stated duration through records 30 s, one that stated nothing
/// leaves the target recording 60 s, and only a recomputed one records what is
/// left of the relay's own 5 s. The two hops are compared with `<=` rather than
/// `<` because the attribute is whole milliseconds and two instants
/// microseconds apart round onto one number.
#[test]
fn a_forward_carries_what_is_left_of_the_caller_budget() -> Result<()> {
    let spans = GlobalSpans::install()?;
    TEST_RUNTIME.block_on(async {
        let pair = Pair::start(RELAY_CEILING, PROCESS_BUDGET, TargetRoute::Nowhere).await?;
        let outcome = async {
            let answered =
                call(&pair.relay, NodeId::new(), RequestId::new(), CALLER_BUDGET).await?;
            ensure(
                answered == Code::FailedPrecondition,
                format!(
                    "the second process must refuse a frame that already names a relay, not \
                     answer {answered:?}"
                ),
            )?;
            let ended = spans.ended();
            let (relay, target) = two_hops(&ended)?;
            let relay_ms = deadline_ms(relay)?;
            let target_ms = deadline_ms(target)?;
            let stated = i64::try_from(CALLER_BUDGET.as_millis())?;
            let ceiling = i64::try_from(PROCESS_BUDGET.as_millis())?;
            ensure(
                target_ms > 0,
                format!("the second hop was given {target_ms} ms, which is no budget at all"),
            )?;
            ensure(
                target_ms <= relay_ms,
                format!(
                    "the second hop was given {target_ms} ms, more than the {relay_ms} ms the \
                     first hop was spending, so the budget was handed on rather than recomputed"
                ),
            )?;
            ensure(
                relay_ms <= stated,
                format!("the first hop was given {relay_ms} ms, more than the {stated} ms stated"),
            )?;
            ensure(
                target_ms < ceiling,
                format!(
                    "the second hop was given {target_ms} ms, which is this process's own \
                     {ceiling} ms ceiling rather than what the caller left"
                ),
            )
        }
        .await;
        pair.stop().await?;
        outcome
    })
}

/// One relayed response reads as one trace, each link an immediate parent.
///
/// The chain is the call, the first process's receive, the hop it made, and the
/// second process's receive. The hop is a child of the first process's own
/// receive rather than of the responder's send: the receive sits between them,
/// so the relation to the send holds through it rather than directly.
#[test]
fn a_relayed_response_reads_as_one_trace() -> Result<()> {
    let spans = GlobalSpans::install()?;
    TEST_RUNTIME.block_on(async {
        let pair = Pair::start(PROCESS_BUDGET, PROCESS_BUDGET, TargetRoute::Nowhere).await?;
        let outcome = async {
            let request = awaited(&pair.target.registry)?;
            let answered = call(&pair.relay, pair.target.node, request, BUDGET).await?;
            ensure(
                answered == Code::Ok,
                format!("the target must accept the relayed response, not answer {answered:?}"),
            )?;
            let ended = spans.ended();
            let caller = named(&ended, CALLER)?;
            let (relay, target) = two_hops(&ended)?;
            let forward = named(&ended, FORWARDED)?;
            for (name, span, parent) in [
                (RECEIVED, relay, caller),
                (FORWARDED, forward, relay),
                (RECEIVED, target, forward),
            ] {
                ensure(
                    span.parent_span_id == parent.span_context.span_id(),
                    format!("{name} is not the immediate child of {}", parent.name),
                )?;
                ensure(
                    span.span_context.trace_id() == caller.span_context.trace_id(),
                    format!("{name} is in another trace than the call that caused it"),
                )?;
            }
            Ok(())
        }
        .await;
        pair.stop().await?;
        outcome
    })
}

/// A response crosses two networks and is stored by the process it names.
///
/// This is the whole path the two halves exist for, and neither half shows it
/// alone. The target publishes a label this responder does not share, so the
/// declared rules refuse its direct address and choose its entry point — and
/// that entry point is a process which is not the target. It sends the frame on
/// to the target's direct endpoint, and the target stores it. The label rule on
/// its own dials nothing, and a relay on its own is never the address the rules
/// picked.
#[test]
fn a_response_crosses_two_networks_through_a_relay() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pair = Pair::start(PROCESS_BUDGET, PROCESS_BUDGET, TargetRoute::Nowhere).await?;
        let outcome = crossing(&pair).await;
        pair.stop().await?;
        outcome
    })
}

/// Resolves the target the declared way, sends one response, and reports what
/// the target holds.
///
/// The responder's own fleet and transport are the production ones, so the
/// response is queued, paced, framed and dialed exactly as a responder does it.
/// `drain` returns once every worker has finished, so what the target holds is
/// settled without waiting on a clock.
async fn crossing(pair: &Pair) -> Result<()> {
    let cap = FrameCap::new(CAP_BYTES)?;
    let request = awaited(&pair.target.registry)?;
    let elsewhere = NodeRegistration {
        node: pair.target.node,
        direct: pair.target.address.clone(),
        advertised: Some(pair.relay.address.clone()),
        network: Some(NetworkId::make(THERE)),
        group: None,
        hostname: Host::make("crossing"),
    };
    let router = FixedRouter::new(cap, Some(elsewhere), Some(NetworkId::make(HERE)))?;
    let route = router
        .route(pair.target.node)
        .await?
        .ok_or_else(|| eyre!("a node in another network must be reachable through its entry"))?;
    let [first, second] = route.candidates(None);
    ensure(
        first == Some((Preference::Advertised, &pair.relay.address)) && second.is_none(),
        format!("the rules chose {route:?}, which is not the target's entry point alone"),
    )?;

    let forwarded = TRANSPORT.forwarded();
    let sender = TypedSender::<CountingCodec>::new(&router, cap)?;
    sender
        .send(
            FrameHeader {
                target: pair.target.node,
                request,
                subsystem: SubsystemName::try_new(ALPHA)?,
                status: ResponseStatus::Success,
                relay: None,
            },
            PAYLOAD.to_vec(),
        )
        .map_err(|_| eyre!("the fleet refused the response"))?;
    if timeout(HANG_GUARD, sender.drain()).await.is_err() {
        bail!("the delivery workers did not finish");
    }
    ensure(
        pair.target
            .registry
            .stored_payload(request, &SubsystemName::try_new(ALPHA)?)
            .is_some(),
        "the response never reached the process it named".to_owned(),
    )?;
    ensure(
        TRANSPORT.forwarded() == forwarded + 1,
        format!(
            "the entry point must send the frame on once, but {} frames were sent on",
            TRANSPORT.forwarded() - forwarded
        ),
    )
}

/// Delivers one frame for `target` into `live`, under a budget of `granted`.
async fn call(live: &Live, target: NodeId, request: RequestId, granted: Duration) -> Result<Code> {
    let cap = FrameCap::new(CAP_BYTES)?;
    let fleet = DestinationFleet::new(FleetConfiguration::default())?;
    let sender = GrpcSender::new(cap, &fleet);
    let mut encoder = FrameEncoder::new(CountingCodec::default(), cap);
    let header = FrameHeader {
        target,
        request,
        subsystem: SubsystemName::try_new(ALPHA)?,
        status: ResponseStatus::Success,
        relay: None,
    };
    let staged = encoder.stage(&header, PAYLOAD.to_vec())?;
    let delivered = timeout(
        HANG_GUARD,
        deliver(&sender, &live.address, &staged, granted).instrument(info_span!("peer.test.call")),
    )
    .await;
    match delivered {
        Ok(Ok(())) => Ok(Code::Ok),
        Ok(Err(SendFailure::Status(code))) => Ok(code),
        Ok(Err(failure)) => bail!("the listener answered nothing at all: {failure}"),
        Err(_) => bail!("the delivery never finished"),
    }
}

/// One delivery, under a budget stated as an instant the caller will not go
/// past.
async fn deliver(
    sender: &GrpcSender,
    address: &Endpoint,
    staged: &Staged<'_>,
    granted: Duration,
) -> Result<(), SendFailure> {
    sender
        .deliver(address, staged, Instant::now() + granted)
        .await
}

/// Registers one request the target process waits for.
fn awaited(registry: &Arc<PendingRegistry>) -> Result<RequestId> {
    let subsystem = SubsystemName::try_new(ALPHA)?;
    Ok(registry.register_unguarded(from_ref(&subsystem), CountingCodec::FORMAT_ID, BUDGET)?)
}

/// The receive span of the process that forwarded, and of the one it forwarded
/// to.
///
/// The first is the child of the call; the second is the other one. Exactly two
/// must exist, or the frame did not cross a hop at all.
fn two_hops(spans: &[SpanData]) -> Result<(&SpanData, &SpanData)> {
    let caller = named(spans, CALLER)?;
    let received: Vec<&SpanData> = spans.iter().filter(|span| span.name == RECEIVED).collect();
    if received.len() != 2 {
        bail!(
            "one relayed frame must be received twice, not {} times",
            received.len()
        );
    }
    let relay = received
        .iter()
        .find(|span| span.parent_span_id == caller.span_context.span_id())
        .ok_or_else(|| eyre!("no received frame is a child of the call"))?;
    let target = received
        .iter()
        .find(|span| span.span_context.span_id() != relay.span_context.span_id())
        .ok_or_else(|| eyre!("only one process received the frame"))?;
    Ok((relay, target))
}

/// What is left of the caller's budget, as one span recorded it.
fn deadline_ms(span: &SpanData) -> Result<i64> {
    let recorded = span
        .attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == DEADLINE_MS)
        .ok_or_else(|| eyre!("a received frame recorded no {DEADLINE_MS}"))?;
    match recorded.value {
        Value::I64(milliseconds) => Ok(milliseconds),
        ref other => bail!("{DEADLINE_MS} was recorded as {other:?} rather than a number"),
    }
}

/// Fails with `message` when `held` does not hold.
///
/// The suites here must stop their listeners on every path, so a failed claim
/// becomes a value the caller carries out rather than an unwind.
fn ensure(held: bool, message: String) -> Result<()> {
    if held { Ok(()) } else { bail!(message) }
}
