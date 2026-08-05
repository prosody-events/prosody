//! What the runtime owns, and what its handles reach.
//!
//! One process has one node identity. Two tests here read that identity from
//! all three places it appears — the listener the runtime serves, the row the
//! directory holds, and the address the runtime's own router resolves — and
//! require them to agree. The third reads the lease those answers are served
//! within.

use super::super::{PeerInputs, PeerRuntime, RouterConfiguration};
use super::{ALPHA, CONTACT, Process, Shared, TIMEOUT, frame_cap, header, listener, requester};
use crate::codec::Codec;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::tests::CountingCodec;
use crate::response::sender::TypedSender;
use crate::router::directory::NodeDirectory;
use crate::router::directory::RegistrationTtl;
use crate::router::directory::tests::support::cassandra_directory;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::TRANSPORT;
use crate::router::grpc::client::GrpcSender;
use crate::router::loopback::{HANG_GUARD, TestHealth};
use crate::router::{NodeId, ResponseSender, Router, SendFailure};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::Context;
use std::slice::from_ref;
use std::sync::Arc;
use tokio::task::yield_now;
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
        runtime.shutdown(|| sender.drain()).await?;
        outcome
    })
}

/// A response sent through the router the runtime hands out reaches this
/// process's own listener.
///
/// The router is not a second set of machinery: it reserves from the process's
/// one fleet, so closing that fleet at shutdown governs it too. The delivery
/// then proves the rest of the handle — the resolver reads this node's own
/// published address, and the transport dials it.
#[test]
fn a_response_through_the_runtime_router_reaches_this_process() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let Process {
            runtime,
            sender,
            shared,
        } = Process::new().await?;
        let router = runtime.router();
        let own = match TypedSender::<CountingCodec>::new(&router, frame_cap()?) {
            Ok(own) => own,
            Err(error) => {
                runtime.shutdown(|| sender.drain()).await?;
                return Err(error.into());
            }
        };
        let outcome = delivered_to_itself(&router, &own, &shared).await;
        runtime
            .shutdown(|| async {
                own.drain().await;
                sender.drain().await;
            })
            .await?;
        outcome
    })
}

/// The runtime resolves through a cache aged on the lease it publishes under.
///
/// The directory carries that one lease, and it governs both halves of
/// reachability: how long this node's own row survives without a refresh, and
/// how long a peer's address is still served after that peer's row is gone. A
/// cache built with any other lease would hand a dialer an address that answers
/// for nobody, for a time no operator asked for.
///
/// The bound is read rather than waited out. What the bound *means* — an entry
/// is served until it, and read again past it — belongs to
/// [`AddressCache`](crate::router::directory::cache::AddressCache) and is
/// proved on a mock clock by the cache's own property. What is left for this
/// test is the wiring, and equality proves that exactly, where a wall clock
/// could only bracket it.
#[test]
fn the_resolver_ages_entries_on_the_lease_this_process_publishes_under() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        // Not the default lease, so a cache aged on a constant fails here.
        let lease = RegistrationTtl::try_from(RegistrationTtl::MIN)?;
        let router = RouterConfiguration::default();
        let requester = requester();
        let runtime = PeerRuntime::start(PeerInputs {
            directory: cassandra_directory(lease.duration()).await?,
            listener: listener().await?,
            health: TestHealth::new(true, true),
            contact: CONTACT,
            group: None,
            router: &router,
            fleet: FleetConfiguration::default(),
            requester: &requester,
        })
        .await?;
        let node = runtime.node();
        let outcome: Result<()> = async {
            let addresses = runtime.addresses();
            ensure!(
                addresses.resolve(node).await?.is_some(),
                "a started runtime must resolve its own node"
            );
            ensure!(
                addresses.ttl() == lease.duration(),
                "the resolver serves an entry for {:?}, not the {:?} this process publishes under",
                addresses.ttl(),
                lease.duration()
            );
            Ok(())
        }
        .await;
        runtime.shutdown(|| async {}).await?;
        outcome
    })
}

/// Sends one response to this process's own node id and waits for the registry
/// to hold it.
///
/// The deadline is a hang guard on a delivery that reports through the registry
/// rather than through a signal; the assertion is the stored payload.
async fn delivered_to_itself<R: Router>(
    router: &R,
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
    own.send(
        header(shared.node, request, ALPHA)?,
        Context::current(),
        PAYLOAD.to_vec(),
    )
    .map_err(|_| eyre!("the runtime's own router refused the response"))?;

    let deadline = Instant::now() + HANG_GUARD;
    loop {
        if let Some(stored) = shared.pending.stored_payload(request, &subsystem) {
            ensure!(
                stored.as_ref() == PAYLOAD,
                "the registry stored a payload the sender never wrote"
            );
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "a response sent through the runtime's router never reached its own listener"
        );
        yield_now().await;
    }
}
