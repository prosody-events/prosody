//! What the runtime owns, and what its handles reach.
//!
//! One process has one node identity. The tests here read that identity from
//! all three places it appears — the listener the runtime serves, the entry the
//! directory holds, and the address the runtime's own router resolves — and
//! require them to agree.

use super::{ALPHA, Process, Shared, TIMEOUT, header};
use crate::requester::registry::tests::TestRegistration;
use crate::response::frame::encode::stage;
use crate::response::frame::tests::CountingCodec;
use crate::response::headers::RequestDeadline;
use crate::response::sender::{ResponseRoute, Then, deliver_response, stage as stage_response};
use crate::router::directory::NodeDirectory;
use crate::router::grpc::client::GrpcSender;
use crate::router::loopback::HANG_GUARD;
use crate::router::{NetworkRoute, NodeId, RelayHop, ResponseSender, SendFailure};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::Context;
use std::slice::from_ref;
use std::sync::Arc;
use tokio::time::Instant;

/// The payload one delivered response carries.
const PAYLOAD: &[u8] = b"through the runtime's own router";

/// The listener answers for the id the runtime minted, and for no other.
///
/// The id it answers for is the one the directory row carries, so a peer that
/// resolves this node reaches the process that owns that id.
#[test]
fn the_listener_answers_only_for_the_node_the_runtime_minted() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let Process { runtime, shared } = Process::new().await?;
        let outcome: Result<()> = async {
            let awaited = [SubsystemName::try_new(ALPHA)?];
            let request = TestRegistration::new(&shared.pending, &awaited, TIMEOUT)?;
            let transport = GrpcSender::new(&shared.fleet);
            let addressed_here = header(shared.node, request.id(), ALPHA)?;
            let mine = stage::<CountingCodec>(&addressed_here, &PAYLOAD.to_vec())?;
            transport
                .deliver(
                    &shared.listener,
                    &mine,
                    Instant::now() + HANG_GUARD,
                    &Context::new(),
                )
                .await
                .map_err(|failure| {
                    eyre!("the listener refused a frame for its own node: {failure}")
                })?;
            let addressed_elsewhere = header(NodeId::new(), request.id(), ALPHA)?;
            let foreign = stage::<CountingCodec>(&addressed_elsewhere, &PAYLOAD.to_vec())?;
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
                "a frame addressed to another node must never be accepted here"
            );
            let registered = shared
                .directory
                .read(shared.node)
                .await?
                .ok_or_else(|| eyre!("a started runtime must already resolve"))?;
            ensure!(
                registered.node == shared.node,
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
fn a_same_node_response_uses_the_local_registry() -> Result<()> {
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

/// Sends one response to this process's own node id and waits for the registry.
async fn delivered_to_itself<D: NodeDirectory, R: ResponseRoute>(
    network: &NetworkRoute<GrpcSender, D>,
    own: &R,
    shared: &Shared,
) -> Result<()> {
    ensure!(
        Arc::ptr_eq(&network.fleet, &shared.fleet),
        "the runtime's router must reserve from the process's own fleet"
    );
    ensure!(
        network
            .direct(shared.node)
            .await
            .map_err(|error| eyre!("{error}"))?
            .is_some_and(|endpoint| endpoint.uri() == shared.listener.uri()),
        "the runtime's router must resolve this process's own listener"
    );
    let subsystem = SubsystemName::try_new(ALPHA)?;
    let mut request = TestRegistration::new(&shared.pending, from_ref(&subsystem), TIMEOUT)?;
    let receiver = request.receiver()?;
    let payload = PAYLOAD.to_vec();
    let prepared =
        stage_response::<CountingCodec>(header(shared.node, request.id(), ALPHA)?, &payload);
    deliver_response(
        own,
        prepared,
        Context::current(),
        RequestDeadline::from_unix_micros(4_102_444_800_000_000),
    )
    .await;

    let stored = receiver
        .await
        .map_err(|_| eyre!("the same-node response stored no payload"))?;
    ensure!(
        stored.payload.as_ref() == PAYLOAD,
        "the registry stored a payload the sender never wrote"
    );
    Ok(())
}
