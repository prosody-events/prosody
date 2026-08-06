//! What the runtime owns, and what its handles reach.
//!
//! One process has one node identity. The tests here read that identity from
//! all three places it appears — the listener the runtime serves, the entry the
//! directory holds, and the address the runtime's own router resolves — and
//! require them to agree.

use super::{ALPHA, Process, Shared, TIMEOUT, frame_cap, header};
use crate::codec::Codec;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::tests::CountingCodec;
use crate::response::sender::TypedSender;
use crate::router::directory::NodeDirectory;
use crate::router::grpc::TRANSPORT;
use crate::router::grpc::client::GrpcSender;
use crate::router::loopback::HANG_GUARD;
use crate::router::{NodeId, RelayHop, ResponseSender, RouterHandle, SendFailure};
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
/// resolves this node reaches the process that owns that id. The transport
/// counters belong to the process, so each one is read as a difference across
/// the call under test.
#[test]
fn the_listener_answers_only_for_the_node_the_runtime_minted() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let Process {
            runtime,
            sender,
            workers,
            shared,
        } = Process::new().await?;
        let outcome: Result<()> = async {
            let awaited = [SubsystemName::try_new(ALPHA)?];
            let request =
                shared
                    .pending
                    .register_unguarded(&awaited, CountingCodec::FORMAT_ID, TIMEOUT)?;
            let transport = GrpcSender::new(frame_cap()?, &shared.fleet);
            let mut encoder = FrameEncoder::new(CountingCodec::default(), frame_cap()?);

            let before = TRANSPORT.misrouted();
            let addressed_here = header(shared.node, request, ALPHA)?;
            let mine = encoder.stage(&addressed_here, PAYLOAD.to_vec())?;
            transport
                .deliver(&shared.listener, &mine, Instant::now() + HANG_GUARD)
                .await
                .map_err(|failure| {
                    eyre!("the listener refused a frame for its own node: {failure}")
                })?;
            ensure!(
                TRANSPORT.misrouted() == before,
                "a frame for this node must not count as misrouted"
            );

            let before = TRANSPORT.misrouted();
            let addressed_elsewhere = header(NodeId::new(), request, ALPHA)?;
            let foreign = encoder.stage(&addressed_elsewhere, PAYLOAD.to_vec())?;
            ensure!(
                matches!(
                    transport
                        .deliver(&shared.listener, &foreign, Instant::now() + HANG_GUARD)
                        .await,
                    Err(SendFailure::Status(_))
                ),
                "a frame addressed to another node must never be accepted here"
            );
            ensure!(
                TRANSPORT.misrouted() == before + 1,
                "one frame for another node must count as one misroute"
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
        runtime
            .shutdown(|| async move {
                drop(sender);
                workers.join().await;
            })
            .await?;
        outcome
    })
}

/// A response for this process reaches its registry without gRPC.
///
/// The response still reserves from the process's fleet and uses its bounded
/// queue. An explicit lookup preserves the router ownership proof. The local
/// worker itself skips address resolution and transport work.
#[test]
fn a_same_node_response_uses_the_local_registry() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let Process {
            runtime,
            sender,
            workers,
            shared,
        } = Process::new().await?;
        let router = runtime.router();
        let (own, own_workers) = match TypedSender::<CountingCodec>::new(&router, frame_cap()?) {
            Ok(parts) => parts,
            Err(error) => {
                runtime
                    .shutdown(|| async move {
                        drop(sender);
                        workers.join().await;
                    })
                    .await?;
                return Err(error.into());
            }
        };
        let outcome = delivered_to_itself(&router, &own, &shared).await;
        runtime
            .shutdown(|| async {
                drop((own, sender));
                own_workers.join().await;
                workers.join().await;
            })
            .await?;
        outcome
    })
}

/// Sends one response to this process's own node id and waits for the registry.
async fn delivered_to_itself<D: NodeDirectory>(
    router: &RouterHandle<GrpcSender, D>,
    own: &TypedSender<CountingCodec>,
    shared: &Shared,
) -> Result<()> {
    ensure!(
        Arc::ptr_eq(router.fleet(), &shared.fleet),
        "the runtime's router must reserve from the process's own fleet"
    );
    ensure!(
        router
            .direct(shared.node)
            .await
            .map_err(|error| eyre!("{error}"))?
            .as_ref()
            == Some(&shared.listener),
        "the runtime's router must resolve this process's own listener"
    );
    let subsystem = SubsystemName::try_new(ALPHA)?;
    let request = shared.pending.register_unguarded(
        from_ref(&subsystem),
        CountingCodec::FORMAT_ID,
        TIMEOUT,
    )?;
    let attempted = router.sender().attempts();
    own.send(
        header(shared.node, request, ALPHA)?,
        Context::current(),
        PAYLOAD.to_vec(),
    )
    .map_err(|_| eyre!("the runtime's own router refused the response"))?;

    let stored = shared
        .pending
        .wait_for_payload(request, &subsystem)
        .await
        .ok_or_else(|| eyre!("the same-node response stored no payload"))?;
    ensure!(
        stored.as_ref() == PAYLOAD,
        "the registry stored a payload the sender never wrote"
    );
    ensure!(
        router.sender().attempts() == attempted,
        "a same-node response must not enter the gRPC sender"
    );
    Ok(())
}
