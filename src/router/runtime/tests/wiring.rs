//! What the runtime wires into the pieces it builds.
//!
//! Two arguments decide that wiring, and neither is visible from the piece it
//! reaches. The router is given this process's own network label, which decides
//! every route it answers. The listener's service is given a relay over that
//! router, so a frame this process does not own leaves again instead of being
//! refused. Every other suite builds those pieces by hand, so only a live
//! runtime can hold either argument.

use super::super::{PeerInputs, PeerRuntime, RouterConfiguration};
use super::{
    ALPHA, CONTACT, LEASE, TIMEOUT, frame_cap, header, listener, requester, start_runtime,
};
use crate::codec::Codec;
use crate::heartbeat::HeartbeatRegistry;
use crate::requester::config::RequesterConfiguration;
use crate::requester::registry::PendingRegistry;
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::tests::CountingCodec;
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::directory::tests::support::cassandra_directory;
use crate::router::directory::{Endpoint, NetworkId, NodeDirectory, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::client::GrpcSender;
use crate::router::grpc::service::PeerService;
use crate::router::grpc::{BoundListener, TransportConfiguration, serve};
use crate::router::loopback::{HANG_GUARD, TestHealth, TestRouter, config as fleet_config};
use crate::router::relay::Relay;
use crate::router::{Host, LocalTarget, NodeId, Preference, ResponseSender, Router};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Report;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use std::net::{Ipv4Addr, SocketAddr};
use std::slice::from_ref;
use std::sync::Arc;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// The label the process under test publishes, and the one its neighbour
/// publishes too.
const NETWORK: &str = "one-network";

/// Where the neighbour says it is. Nothing dials either port: the order the
/// route puts them in is the whole subject.
const NEIGHBOUR_DIRECT: u16 = 9401;
const NEIGHBOUR_ENTRY: u16 = 9402;

/// The payload the forwarded frame carries.
const PAYLOAD: &[u8] = b"sent on by the listener the runtime served";

/// Destinations and slots the fleets in this suite hold.
const DESTINATIONS: usize = 2;
const SLOTS: usize = 2;

/// One more listener, answering for one more node.
struct Elsewhere {
    node: NodeId,
    registry: Arc<PendingRegistry>,
    address: Endpoint,
    stop: Option<Sender<()>>,
    served: Option<JoinHandle<()>>,
}

/// The label a process was configured with is the label its router routes by.
///
/// A peer that published the same label is a neighbour, so its direct endpoint
/// leads and its entry point is only the fallback. A router built without the
/// label puts every neighbour behind its entry point instead, and nothing below
/// the runtime can tell the two apart: the label reaches the router at one
/// argument that no other suite passes.
#[test]
fn the_router_routes_by_the_network_label_the_process_was_configured_with() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = cassandra_directory(LEASE).await?;
        let config = RouterConfiguration::builder().network(NETWORK).build()?;
        let requester = requester();
        let runtime = start_runtime(PeerInputs {
            directory: directory.clone(),
            listener: listener().await?,
            heartbeats: HeartbeatRegistry::test(),
            probe: Some(CONTACT),
            router: &config,
            fleet: FleetConfiguration::default(),
            requester: &requester,
        })
        .await?;
        let neighbour = NodeRegistration {
            node: NodeId::new(),
            direct: Endpoint {
                host: Host::make("10.0.0.11"),
                port: NEIGHBOUR_DIRECT,
            },
            advertised: Some(Endpoint {
                host: Host::make("gateway.example"),
                port: NEIGHBOUR_ENTRY,
            }),
            network: Some(NetworkId::make(NETWORK)),
            hostname: Host::make("neighbour"),
        };
        let outcome: Result<()> = async {
            directory.register(&neighbour).await?;
            let route = runtime
                .router
                .clone()
                .route(neighbour.node)
                .await?
                .ok_or_else(|| eyre!("a published neighbour must resolve"))?;
            let walked: Vec<Preference> = route
                .candidates(None)
                .into_iter()
                .flatten()
                .map(|(preference, _)| preference)
                .collect();
            ensure!(
                walked == [Preference::Direct, Preference::Advertised],
                "a neighbour's route is {walked:?}, not its direct endpoint before its entry point"
            );
            Ok(())
        }
        .await;
        runtime.shutdown(|| async {}).await?;
        outcome
    })
}

/// A frame that names another node leaves the runtime's listener again and
/// arrives there.
///
/// The runtime hands its listener's service a relay over its own router, the
/// frame ceiling that listener enforces, and a forward budget. All three reach
/// the service at one call. The second listener's registry holding the payload
/// is the assertion, because the relay is the only path to it.
#[test]
fn a_frame_for_another_node_leaves_the_runtime_listener_again() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = cassandra_directory(LEASE).await?;
        let elsewhere = Elsewhere::start().await?;
        let started = start_over(&directory, &elsewhere).await;
        let (runtime, here) = match started {
            Ok(started) => started,
            Err(error) => {
                elsewhere.stop().await?;
                return Err(error);
            }
        };
        let outcome = sent_on(&elsewhere, &here).await;
        runtime.shutdown(|| async {}).await?;
        elsewhere.stop().await?;
        outcome
    })
}

impl Elsewhere {
    /// Binds and serves one more listener, answering for a node of its own.
    ///
    /// Its own relay never runs: every frame this suite sends it names it.
    async fn start() -> Result<Self> {
        let node = NodeId::new();
        let registry = PendingRegistry::test(&RequesterConfiguration::default())?;
        let bound = bind().await?;
        let address = local(bound.address().port());
        let cap = bound.frame_cap();
        let (unused, _deliveries) = TestRouter::new(fleet_config(DESTINATIONS, SLOTS))?;
        let (stop, stopped) = channel();
        let served = serve(
            bound,
            PeerService::new(
                LocalTarget::new(node, Arc::clone(&registry)),
                Relay::new(unused),
                cap,
                TIMEOUT,
            ),
            TestHealth::new(true, true),
            async move { stopped.await.unwrap_or(()) },
        )?;
        Ok(Self {
            node,
            registry,
            address,
            stop: Some(stop),
            served: Some(served),
        })
    }

    /// Stops this listener and waits for it to finish.
    async fn stop(mut self) -> Result<()> {
        drop(self.stop.take());
        if let Some(served) = self.served.take() {
            served.await?;
        }
        Ok(())
    }
}

/// Publishes `elsewhere` and starts the process under test, reporting where its
/// own listener is.
async fn start_over(
    directory: &CassandraNodeDirectory,
    elsewhere: &Elsewhere,
) -> Result<(PeerRuntime<CassandraNodeDirectory>, Endpoint), Report> {
    directory
        .register(&NodeRegistration {
            node: elsewhere.node,
            direct: elsewhere.address.clone(),
            advertised: None,
            network: None,
            hostname: Host::make("elsewhere"),
        })
        .await?;
    let bound = bind().await?;
    let here = local(bound.address().port());
    let config = RouterConfiguration::default();
    let requester = requester();
    let runtime = start_runtime(PeerInputs {
        directory: directory.clone(),
        listener: bound,
        heartbeats: HeartbeatRegistry::test(),
        probe: Some(CONTACT),
        router: &config,
        fleet: FleetConfiguration::default(),
        requester: &requester,
    })
    .await?;
    Ok((runtime, here))
}

/// Delivers one frame naming `elsewhere` into the listener at `here`, and holds
/// that it arrived where it was addressed.
///
/// The listener answers only once its forward is over, so the stored payload is
/// readable the moment the delivery returns.
async fn sent_on(elsewhere: &Elsewhere, here: &Endpoint) -> Result<()> {
    let subsystem = SubsystemName::try_new(ALPHA)?;
    let request = elsewhere.registry.register_unguarded(
        from_ref(&subsystem),
        CountingCodec::FORMAT_ID,
        TIMEOUT,
    )?;
    let fleet = DestinationFleet::new(fleet_config(DESTINATIONS, SLOTS))?;
    let sender = GrpcSender::new(frame_cap()?, &fleet);
    let mut encoder = FrameEncoder::new(CountingCodec::default(), frame_cap()?);
    let addressed = header(elsewhere.node, request, ALPHA)?;
    let staged = encoder.stage(&addressed, PAYLOAD.to_vec())?;
    sender
        .deliver(here, &staged, Instant::now() + HANG_GUARD)
        .await
        .map_err(|failure| eyre!("the listener did not send the frame on: {failure}"))?;
    let stored = elsewhere
        .registry
        .stored_payload(request, &subsystem)
        .ok_or_else(|| eyre!("the frame never reached the node it named"))?;
    ensure!(
        stored.as_ref() == PAYLOAD,
        "the node the frame named stored a payload nothing sent"
    );
    Ok(())
}

/// A listener on the loopback interface, on a port the operating system
/// chooses.
async fn bind() -> Result<BoundListener> {
    Ok(BoundListener::bind(&TransportConfiguration {
        bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        frame_cap: frame_cap()?,
        ..TransportConfiguration::default()
    })
    .await?)
}

/// Where a listener on `port` is, as a peer on this machine dials it.
fn local(port: u16) -> Endpoint {
    Endpoint {
        host: Host::make("127.0.0.1"),
        port,
    }
}
