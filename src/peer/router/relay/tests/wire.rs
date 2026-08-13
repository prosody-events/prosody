//! What crosses a real hop: the loop stop, the caller's budget, and the trace.

use super::{ALPHA, BUDGET, Live, PAYLOAD, Pair, TargetRoute};
use crate::peer::requester::registry::PendingRegistry;
use crate::peer::requester::registry::tests::TestRegistration;
use crate::peer::response::RequestId;
use crate::peer::response::frame::FrameHeader;
use crate::peer::response::frame::encode::{Staged, stage_success};
use crate::peer::response::frame::tests::CountingCodec;
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::sender::{deliver_response, stage as stage_response};
use crate::peer::router::cache_config::PeerCacheConfiguration;
use crate::peer::router::directory::{Endpoint, NetworkId, PeerRegistration};
use crate::peer::router::grpc::client::GrpcSender;
use crate::peer::router::loopback::direct_address;
use crate::peer::router::loopback::listener::FixedRouter;
use crate::peer::router::{EndpointKind, Host, NetworkRouter, PeerId, ResponseSender, SendFailure};
use crate::subsystem::SubsystemName;
use crate::test_util::{GlobalMetrics, GlobalSpans, TEST_RUNTIME, label, named};
use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use opentelemetry::Context;
use opentelemetry::Value;
use opentelemetry_sdk::trace::SpanData;
use std::convert::Infallible;
use std::slice::from_ref;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tonic::Code;
use tracing::info_span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The span the delivery is made from, so every later span has one root to be
/// measured against.
const CALLER: &str = "peer.test.call";

/// The span a process opens for one frame it received.
const RECEIVED: &str = "peer.response.receive";

/// The span a process opens for one frame it sends on.
const FORWARDED: &str = "peer.response.forward";

/// The attribute carrying what is left of the caller's budget.
const DEADLINE_MS: &str = "peer.deadline_ms";

/// The counter of delivery attempts a process decided.
const DISPOSITIONS: &str = "prosody.response.dispositions";

/// The budget a caller states. Well under the ceiling a process applies on its
/// own, so a hop that inherited nothing is unmistakable.
const CALLER_BUDGET: Duration = Duration::from_secs(30);

/// The ceiling the relaying process applies to one forward. Well under the
/// budget its caller states, so what the relay hands on is a number no caller
/// stated and no process would apply on its own.
/// The two network labels the crossing case declares.
const HERE: &str = "here";
const THERE: &str = "there";

static LARGE_RESPONSE: [u8; 64 * 1024] = [0; 64 * 1024];

/// A large response crosses the relay without a Prosody-specific size limit.
#[test]
fn a_large_response_crosses_a_relay() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pair = Pair::start(TargetRoute::Nowhere).await?;
        let request = awaited(&pair.target.registry)?;
        let outcome = async {
            let answered = call_with_payload(
                &pair.relay,
                pair.target.peer,
                request.id(),
                BUDGET,
                &LARGE_RESPONSE,
            )
            .await?;
            ensure(
                answered == Code::Ok,
                format!("the target must accept the large response, not {answered:?}"),
            )?;
            Ok(())
        }
        .await;
        pair.stop().await?;
        outcome
    })
}

/// Two processes whose directory entries name each other pass a frame neither
/// owns exactly once.
///
/// The frame names a third peer, so neither process can accept it. The first
/// sends it on and writes its own id into it; the second sees an id already
/// there and refuses. Without that refusal the two would pass the frame back
/// and forth until a budget ended it, so the answered status is the assertion:
/// `FAILED_PRECONDITION` is the refusal, and `DEADLINE_EXCEEDED` is the loop.
#[test]
fn a_frame_this_process_already_relayed_is_never_relayed_again() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pair = Pair::start(TargetRoute::Relay).await?;
        let outcome = async {
            let answered = call(&pair.relay, PeerId::new(), RequestId::new(), BUDGET).await?;
            ensure(
                answered == Code::FailedPrecondition,
                format!(
                    "a frame neither process owns must be refused once it names a relay, not \
                     answered {answered:?}"
                ),
            )?;
            Ok(())
        }
        .await;
        pair.stop().await?;
        outcome
    })
}

/// The budget a process states on the hop it makes is what is left of the
/// budget it is spending itself, never a budget it was handed.
///
/// The frame names a third peer, so the second process refuses it — but it
/// refuses it after reading the budget it was given, which is the number this
/// case is about. Three budgets are deliberately distinct, so each way of
/// getting the number wrong records a different one. The caller states 30 s.
/// Each hop records the time that remains from that deadline. The two hops are
/// compared with `<=` rather than
/// `<` because the attribute is whole milliseconds and two instants
/// microseconds apart round onto one number.
#[test]
fn a_forward_carries_what_is_left_of_the_caller_budget() -> Result<()> {
    let spans = GlobalSpans::install()?;
    TEST_RUNTIME.block_on(async {
        let pair = Pair::start(TargetRoute::Nowhere).await?;
        let outcome = async {
            let answered =
                call(&pair.relay, PeerId::new(), RequestId::new(), CALLER_BUDGET).await?;
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
            Ok(())
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
        let pair = Pair::start(TargetRoute::Nowhere).await?;
        let outcome = async {
            let request = awaited(&pair.target.registry)?;
            let answered = call(&pair.relay, pair.target.peer, request.id(), BUDGET).await?;
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
///
/// Both processes are live in this test process, so the disposition counter is
/// the second claim: one delivery is one point. The relay decided nothing, so
/// it counts nothing, and only the target's `accepted` is there.
#[test]
fn a_response_crosses_two_networks_through_a_relay() -> Result<()> {
    let metrics = GlobalMetrics::install();
    TEST_RUNTIME.block_on(async {
        let pair = Pair::start(TargetRoute::Nowhere).await?;
        let outcome = crossing(&pair).await;
        pair.stop().await?;
        outcome?;
        let counted = metrics.points(DISPOSITIONS)?;
        ensure(
            counted == vec![(label("disposition", "accepted"), 1)],
            format!(
                "one relayed delivery is decided once, so it counts once, but {DISPOSITIONS} \
                 reads {counted:?}"
            ),
        )
    })
}

/// Resolves the target the declared way, sends one response, and reports what
/// the target holds.
///
/// The responder uses the production fleet and transport. Thus, the response
/// follows the same route as a production response.
async fn crossing(pair: &Pair) -> Result<()> {
    let mut request = awaited(&pair.target.registry)?;
    let receiver = request.receiver()?;
    let elsewhere = PeerRegistration {
        peer: pair.target.peer,
        direct: direct_address(&pair.target.address)?,
        advertised: Some(pair.relay.address.clone()),
        network: Some(NetworkId::make(THERE)),
        hostname: Host::make("crossing"),
    };
    let router = FixedRouter::new(
        PeerCacheConfiguration::default(),
        Some(elsewhere),
        Some(NetworkId::make(HERE)),
    );
    let route = router
        .route(pair.target.peer)
        .await?
        .ok_or_else(|| eyre!("a peer in another network must be reachable through its entry"))?;
    let (kind, endpoint) = route.endpoint();
    ensure(
        kind == EndpointKind::Advertised && endpoint.uri() == pair.relay.address.uri(),
        format!("the rules chose {route:?}, which is not the target's entry point alone"),
    )?;

    let payload = PAYLOAD.to_vec();
    let prepared = stage_response::<CountingCodec, Infallible>(
        FrameHeader {
            target: pair.target.peer,
            request: request.id(),
            subsystem: SubsystemName::try_new(ALPHA)?,
            relay: None,
        },
        Ok(&payload),
    );
    deliver_response(
        &router,
        prepared,
        Context::current(),
        RequestDeadline::from_unix_micros(4_102_444_800_000_000),
    )
    .await;
    receiver
        .await
        .map_err(|_| eyre!("the response never reached the process it named"))?;
    Ok(())
}

/// Delivers one frame for `target` into `live`, under a budget of `granted`.
async fn call(live: &Live, target: PeerId, request: RequestId, granted: Duration) -> Result<Code> {
    call_with_payload(live, target, request, granted, PAYLOAD).await
}

async fn call_with_payload(
    live: &Live,
    target: PeerId,
    request: RequestId,
    granted: Duration,
    payload: &[u8],
) -> Result<Code> {
    let sender = GrpcSender::new(PeerCacheConfiguration::default());
    let header = FrameHeader {
        target,
        request,
        subsystem: SubsystemName::try_new(ALPHA)?,
        relay: None,
    };
    let staged = stage_success::<CountingCodec>(&header, &payload.to_vec())?;
    let caller = info_span!("peer.test.call");
    let context = caller.context();
    let delivered = deliver(&sender, &live.address, &staged, granted, &context).await;
    drop(caller);
    match delivered {
        Ok(()) => Ok(Code::Ok),
        Err(SendFailure::Status(code)) => Ok(code),
        Err(failure) => bail!("the listener answered nothing at all: {failure}"),
    }
}

/// One delivery, under a budget stated as an instant the caller will not go
/// past.
async fn deliver(
    sender: &GrpcSender,
    address: &Endpoint,
    staged: &Staged,
    granted: Duration,
    context: &Context,
) -> Result<(), SendFailure> {
    sender
        .deliver(address, staged, Instant::now() + granted, context)
        .await
}

/// Registers one request the target process waits for.
fn awaited(registry: &Arc<PendingRegistry>) -> Result<TestRegistration> {
    let subsystem = SubsystemName::try_new(ALPHA)?;
    TestRegistration::new(registry, from_ref(&subsystem), BUDGET)
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
