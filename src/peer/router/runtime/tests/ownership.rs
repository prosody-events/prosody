//! What the runtime owns, and what its handles reach.
//!
//! One process has one peer identity. The tests here read that identity from
//! all three places it appears — the listener the runtime serves, the entry the
//! directory holds, and the address the runtime's own router resolves — and
//! require them to agree.

use super::{ALPHA, Process, Shared, TIMEOUT, header};
use crate::peer::requester::registry::tests::TestRegistration;
use crate::peer::response::frame::encode::stage_success;
use crate::peer::response::frame::tests::CountingCodec;
use crate::peer::response::frame::{FrameResult, ResponseSuccess};
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::sender::{
    PeerMetricSource, ResponseRoute, Then, deliver_response, stage as stage_response,
};
use crate::peer::router::cache_config::PeerCacheConfiguration;
use crate::peer::router::directory::PeerDirectory;
use crate::peer::router::grpc::client::GrpcSender;
use crate::peer::router::loopback::HANG_GUARD;
use crate::peer::router::{NetworkRoute, PeerId, RelayHop, ResponseSender, SendFailure};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::Context;
use std::convert::Infallible;
use std::slice::from_ref;
use tokio::time::Instant;

/// The payload one delivered response carries.
const PAYLOAD: &[u8] = b"through the runtime's own router";

/// The listener answers for the id the runtime minted, and for no other.
///
/// The id it answers for is the one the directory row carries, so a peer that
/// resolves this peer reaches the process that owns that id.
#[test]
fn the_listener_answers_only_for_the_peer_the_runtime_minted() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let Process { runtime, shared } = Process::new().await?;
        let outcome: Result<()> = async {
            let awaited = [SubsystemName::try_new(ALPHA)?];
            let request = TestRegistration::new(&shared.pending, &awaited, TIMEOUT)?;
            let transport = GrpcSender::new(PeerCacheConfiguration::default());
            let addressed_here = header(shared.peer, request.id(), ALPHA)?;
            let mine = stage_success::<CountingCodec>(&addressed_here, &PAYLOAD.to_vec())?;
            transport
                .deliver(
                    &shared.listener,
                    &mine,
                    Instant::now() + HANG_GUARD,
                    &Context::new(),
                )
                .await
                .map_err(|failure| {
                    eyre!("the listener refused a frame for its own peer: {failure}")
                })?;
            let addressed_elsewhere = header(PeerId::new(), request.id(), ALPHA)?;
            let foreign = stage_success::<CountingCodec>(&addressed_elsewhere, &PAYLOAD.to_vec())?;
            ensure!(
                matches!(
                    transport
                        .deliver(
                            &shared.listener,
                            &foreign,
                            Instant::now() + HANG_GUARD,
                            &Context::new(),
                        )
                        .await,
                    Err(SendFailure::Status(_))
                ),
                "a frame addressed to another peer must never be accepted here"
            );
            let registered = shared
                .directory
                .read(shared.peer)
                .await?
                .ok_or_else(|| eyre!("a started runtime must already resolve"))?;
            ensure!(
                registered.peer == shared.peer,
                "the directory row must carry the id the listener answers for"
            );
            Ok(())
        }
        .await;
        runtime.shutdown(|| async {}).await?;
        outcome
    })
}

/// A response for this process reaches its registry without gRPC.
///
/// The local route skips address resolution and transport work.
#[test]
fn a_same_peer_response_uses_the_local_registry() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let Process { runtime, shared } = Process::new().await?;
        let network = runtime.network.clone();
        let own = Then(runtime.local.clone(), network.clone());
        let outcome = delivered_to_itself(&network, &own, &shared).await;
        runtime.shutdown(|| async {}).await?;
        outcome
    })
}

/// Sends one response to this process's own peer id and waits for the registry.
async fn delivered_to_itself<D: PeerDirectory, R: ResponseRoute + PeerMetricSource>(
    network: &NetworkRoute<GrpcSender, D>,
    own: &R,
    shared: &Shared,
) -> Result<()> {
    ensure!(
        network
            .direct(shared.peer)
            .await
            .map_err(|error| eyre!("{error}"))?
            .is_some_and(|endpoint| endpoint.uri() == shared.listener.uri()),
        "the runtime's router must resolve this process's own listener"
    );
    let subsystem = SubsystemName::try_new(ALPHA)?;
    let mut request = TestRegistration::new(&shared.pending, from_ref(&subsystem), TIMEOUT)?;
    let receiver = request.receiver()?;
    let payload = PAYLOAD.to_vec();
    let prepared = stage_response::<CountingCodec, Infallible, _>(
        own,
        header(shared.peer, request.id(), ALPHA)?,
        Ok(&payload),
    );
    deliver_response(
        own,
        prepared,
        Context::current(),
        RequestDeadline::from_unix_micros(4_102_444_800_000_000),
    )
    .await;

    let stored = receiver
        .await
        .map_err(|_| eyre!("the same-peer response stored no payload"))?;
    let FrameResult::Success(ResponseSuccess { payload, .. }) = stored.result else {
        return Err(eyre!("the same-peer response stored a handler error"));
    };
    ensure!(
        payload.as_ref() == PAYLOAD,
        "the registry stored other payload bytes"
    );
    Ok(())
}
