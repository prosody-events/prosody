//! What the runtime wires into the pieces it builds.
//!
//! Two arguments decide that wiring, and neither is visible from the piece it
//! reaches. The router is given this process's own network label, which decides
//! every route it answers. The listener's service is given a relay over that
//! router, so a frame this process does not own leaves again instead of being
//! refused. Every other suite builds those pieces by hand, so only a live
//! runtime can hold either argument.

use super::super::{PeerInputs, PeerRuntime, RouterConfiguration};
use super::{ALPHA, LEASE, Process, TIMEOUT, header, listener, start_runtime};
use crate::heartbeat::HeartbeatRegistry;
use crate::peer::requester::registry::PendingRegistry;
use crate::peer::requester::registry::tests::TestRegistration;
use crate::peer::response::frame::encode::stage_success;
use crate::peer::response::frame::tests::CountingCodec;
use crate::peer::response::frame::{FrameResult, ResponseSuccess};
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::sender::{deliver_response, stage as stage_response};
use crate::peer::router::cache_config::PeerCacheConfiguration;
use crate::peer::router::directory::cassandra::CassandraPeerDirectory;
use crate::peer::router::directory::tests::support::cassandra_directory;
use crate::peer::router::directory::{
    DirectAddress, Endpoint, NetworkId, PeerDirectory, PeerRegistration,
};
use crate::peer::router::grpc::client::GrpcSender;
use crate::peer::router::grpc::service::PeerService;
use crate::peer::router::grpc::{BoundListener, serve};
use crate::peer::router::loopback::{
    HANG_GUARD, TestRouter, config as fleet_config, direct_address,
};
use crate::peer::router::relay::Relay;
use crate::peer::router::{Host, LocalTarget, NetworkRouter, PeerId, Preference, ResponseSender};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Report;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::slice::from_ref;
use std::sync::Arc;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tonic::transport::Error as TransportError;

/// The label the process under test publishes, and the one its neighbour
/// publishes too.
const NETWORK: &str = "one-network";

/// The payload the forwarded frame carries.
const PAYLOAD: &[u8] = b"sent on by the listener the runtime served";

/// Two peer runtimes resolve and contact each other through Cassandra.
#[test]
fn two_peer_clients_communicate_through_their_socket_addresses() -> Result<()> {
    init_test_logging();
    let hostname = whoami::hostname()?;
    TEST_RUNTIME.block_on(async move {
        let sender = Process::new().await?;
        let receiver = Process::new().await?;
        let outcome: Result<()> = async {
            let published = sender
                .shared
                .directory
                .read(receiver.shared.peer)
                .await?
                .ok_or_else(|| eyre!("the second peer did not publish its address"))?;
            ensure!(
                published.hostname.as_str() == hostname,
                "the peer directory did not store the local hostname"
            );
            let listener: SocketAddr = receiver
                .shared
                .listener
                .uri()
                .authority()
                .ok_or_else(|| eyre!("the second peer listener has no authority"))?
                .as_str()
                .parse()?;
            ensure!(
                published.direct.socket() == listener,
                "the peer directory did not store the bound socket address"
            );
            let subsystem = SubsystemName::try_new(ALPHA)?;
            let mut request =
                TestRegistration::new(&receiver.shared.pending, from_ref(&subsystem), TIMEOUT)?;
            let response = request.receiver()?;
            let payload = PAYLOAD.to_vec();
            let prepared = stage_response::<CountingCodec, Infallible>(
                header(receiver.shared.peer, request.id(), ALPHA)?,
                Ok(&payload),
            );
            deliver_response(
                &sender.runtime.network,
                prepared,
                opentelemetry::Context::new(),
                RequestDeadline::from_unix_micros(4_102_444_800_000_000),
            )
            .await;
            let stored = response
                .await
                .map_err(|_| eyre!("the second peer did not receive the response"))?;
            let FrameResult::Success(ResponseSuccess { payload, .. }) = stored.result else {
                return Err(eyre!("the second peer received a handler error"));
            };
            ensure!(
                payload.as_ref() == PAYLOAD,
                "the second peer received other bytes"
            );
            Ok(())
        }
        .await;
        sender.runtime.shutdown(|| async {}).await?;
        receiver.runtime.shutdown(|| async {}).await?;
        outcome
    })
}

/// One more listener, answering for one more peer.
struct Elsewhere {
    peer: PeerId,
    registry: Arc<PendingRegistry>,
    address: Endpoint,
    stop: Option<Sender<()>>,
    served: Option<JoinHandle<()>>,
}

/// The label a process was configured with is the label its router routes by.
///
/// A peer that published the same label is a neighbour, so the router selects
/// its direct endpoint. The label reaches the router at one argument that no
/// other suite passes.
#[test]
fn the_router_routes_by_the_network_label_the_process_was_configured_with() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = cassandra_directory(LEASE).await?;
        let config = RouterConfiguration::builder().network(NETWORK).build()?;
        let runtime = start_runtime(PeerInputs {
            directory: directory.clone(),
            listener: listener().await?,
            heartbeats: HeartbeatRegistry::test(),
            router: &config,
            cache: PeerCacheConfiguration::default(),
        })
        .await?;
        let neighbour = PeerRegistration {
            peer: PeerId::new(),
            direct: DirectAddress::new(SocketAddr::from(([10, 0, 0, 11], 12_001)))?,
            advertised: Some(Endpoint::from_static("http://gateway.example:12002")),
            network: Some(NetworkId::make(NETWORK)),
            hostname: Host::make("neighbour"),
        };
        let outcome: Result<()> = async {
            directory.register(&neighbour).await?;
            let route = runtime
                .network
                .clone()
                .route(neighbour.peer)
                .await?
                .ok_or_else(|| eyre!("a published neighbour must resolve"))?;
            let (preference, _) = route.endpoint();
            ensure!(
                preference == Preference::Direct,
                "a neighbour's route is {preference:?}, not its direct endpoint"
            );
            Ok(())
        }
        .await;
        runtime.shutdown(|| async {}).await?;
        outcome
    })
}

/// A frame that names another peer leaves the runtime's listener again and
/// arrives there.
///
/// The runtime gives its listener a relay over its own router. The second
/// listener's registry holds the payload only when that relay works.
#[test]
fn a_frame_for_another_peer_leaves_the_runtime_listener_again() -> Result<()> {
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
    /// Binds and serves one more listener, answering for a peer of its own.
    ///
    /// Its own relay never runs: every frame this suite sends it names it.
    async fn start() -> Result<Self> {
        let peer = PeerId::new();
        let registry = PendingRegistry::new();
        let bound = bind().await?;
        let address = local(bound.address())?;
        let (unused, _deliveries) = TestRouter::new()?;
        let (stop, stopped) = channel();
        let served = serve(
            bound,
            PeerService::new(
                LocalTarget::new(peer, Arc::clone(&registry)),
                Relay::new(unused),
            ),
            async move { stopped.await.unwrap_or(()) },
        )
        .await?;
        Ok(Self {
            peer,
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
    directory: &CassandraPeerDirectory,
    elsewhere: &Elsewhere,
) -> Result<(PeerRuntime<CassandraPeerDirectory>, Endpoint), Report> {
    directory
        .register(&PeerRegistration {
            peer: elsewhere.peer,
            direct: direct_address(&elsewhere.address)?,
            advertised: None,
            network: None,
            hostname: Host::make("elsewhere"),
        })
        .await?;
    let bound = bind().await?;
    let here = local(bound.address())?;
    let config = RouterConfiguration::default();
    let runtime = start_runtime(PeerInputs {
        directory: directory.clone(),
        listener: bound,
        heartbeats: HeartbeatRegistry::test(),
        router: &config,
        cache: PeerCacheConfiguration::default(),
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
    let mut request = TestRegistration::new(&elsewhere.registry, from_ref(&subsystem), TIMEOUT)?;
    let receiver = request.receiver()?;
    let sender = GrpcSender::new(fleet_config());
    let addressed = header(elsewhere.peer, request.id(), ALPHA)?;
    let staged = stage_success::<CountingCodec>(&addressed, &PAYLOAD.to_vec())?;
    sender
        .deliver(
            here,
            &staged,
            Instant::now() + HANG_GUARD,
            &opentelemetry::Context::new(),
        )
        .await
        .map_err(|failure| eyre!("the listener did not send the frame on: {failure}"))?;
    let stored = receiver
        .await
        .map_err(|_| eyre!("the frame never reached the peer it named"))?;
    let FrameResult::Success(ResponseSuccess { payload, .. }) = stored.result else {
        return Err(eyre!("the target stored a handler error"));
    };
    ensure!(
        payload.as_ref() == PAYLOAD,
        "the target stored other payload bytes"
    );
    Ok(())
}

/// A listener on the loopback interface, on a port the operating system
/// chooses.
async fn bind() -> Result<BoundListener> {
    Ok(BoundListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?)
}

/// Where a listener on `port` is, as a peer on this machine dials it.
fn local(address: SocketAddr) -> Result<Endpoint, TransportError> {
    Endpoint::from_shared(format!("http://{address}"))
}
