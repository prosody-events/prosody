//! The process runtime's suites, driven against live Cassandra and a real
//! listener.
//!
//! Isolation follows the Cassandra row rule: every process mints a fresh node
//! id and a fresh group id, so rows are disjoint in the shared `prosody_test`
//! keyspace and no test creates a keyspace of its own.

use super::{PeerInputs, PeerRuntime, RouterConfiguration};
use crate::requester::config::RequesterConfiguration;
use crate::requester::registry::PendingRegistry;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::response::sender::{SendCounters, TypedSender};
use crate::response::{RequestId, ResponseStatus};
use crate::router::directory::tests::support::{directory, membership, store};
use crate::router::directory::{Endpoint, GroupMembership, NodeDirectory, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::{BoundListener, TransportConfiguration};
use crate::router::loopback::{LoopbackSender, Script, TestHealth, port};
use crate::router::{Host, NodeId, RouterHandle};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

mod config;
mod discovery;
mod ownership;
mod shutdown;
mod wiring;

/// The Cassandra contact point the routed-address probe aims at.
const CONTACT: &str = "localhost:9042";

/// The same contact point written as an address. A probe against it exercises
/// no name resolution, so it always aims at one address family.
const NUMERIC_CONTACT: &str = "127.0.0.1:9042";

/// The lease these tests read under. It equals the default a runtime starts
/// with, and its refresh delay is at least a fifth of it, so no refresh runs
/// while a test observes the first write.
const LEASE: Duration = Duration::from_secs(30);

/// The frame ceiling one process uses in both directions.
const FRAME_BYTES: usize = 8 * 1024;

/// How long a parked request stays open. Far longer than any test runs, so no
/// assertion races the registry's deadline sweep.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The subsystem every test request awaits.
const ALPHA: &str = "alpha";

/// Destination cells one process under test holds.
const DESTINATIONS: usize = 2;

/// Responses one destination may hold at once. It is above every count the
/// shutdown property queues and reserves together, so no assertion there is
/// made vacuous by a capacity refusal.
const SLOTS_EACH: usize = 8;

/// One process under test: a live runtime, and one typed sender over the
/// handles that runtime hands out.
struct Process {
    runtime: PeerRuntime,
    sender: TypedSender<CountingCodec>,
    shared: Shared,
}

/// The handles a test keeps after shutdown has consumed the runtime.
struct Shared {
    fleet: Arc<DestinationFleet>,
    pending: Arc<PendingRegistry>,
    counters: Arc<SendCounters>,
    /// A second node, published at the loopback transport's first port. Its row
    /// expires on the lease; nothing deletes it.
    destination: NodeId,
    /// Zero permits, so every delivery attempt is held until a test releases
    /// it.
    barrier: Arc<Semaphore>,
    /// Where this process's own listener is.
    listener: Endpoint,
    /// The id the runtime minted.
    node: NodeId,
    directory: NodeDirectory,
}

/// One runtime with the default peer configuration, and no sender.
struct PlainProcess {
    runtime: PeerRuntime,
    directory: NodeDirectory,
    membership: GroupMembership,
    bound_port: u16,
}

impl Process {
    /// Starts one live process and publishes one held loopback destination.
    ///
    /// The sender delivers over the loopback transport rather than over the
    /// runtime's own router, deliberately: a response this process addressed to
    /// itself would be delivered by the drain, which runs after shutdown has
    /// already stopped this process's listener.
    async fn new() -> Result<Self> {
        let directory = directory(LEASE).await?;
        let cap = frame_cap()?;
        let bound = BoundListener::bind(&TransportConfiguration {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            frame_cap: cap,
            ..TransportConfiguration::default()
        })
        .await?;
        let listener = Endpoint {
            host: Host::make("127.0.0.1"),
            port: bound.address().port(),
        };
        let router = RouterConfiguration::default();
        let requester = requester();
        let runtime = PeerRuntime::start(PeerInputs {
            store: store().await?.clone(),
            listener: bound,
            health: TestHealth::new(true, true),
            // The numeric contact point pins the address family the routed
            // probe answers with, so the discovered host is the loopback
            // address this listener bound and a process can reach itself.
            contact: NUMERIC_CONTACT,
            group: Some(membership()),
            router: &router,
            fleet: FleetConfiguration {
                max_destinations: DESTINATIONS,
                slots_each: SLOTS_EACH,
                // Far longer than any test runs, and a rate no test reaches, so
                // neither the send deadline nor pacing can end a queued
                // response before the drain does.
                send_deadline: Duration::from_mins(1),
                sends_per_second: 10_000,
                ..FleetConfiguration::default()
            },
            requester: &requester,
        })
        .await?;
        let barrier = Arc::new(Semaphore::new(0));
        let destination = NodeId::new();
        let prepared = async {
            directory
                .register(&NodeRegistration {
                    node: destination,
                    direct: Endpoint {
                        host: Host::make("10.0.0.1"),
                        port: port(0),
                    },
                    advertised: None,
                    network: None,
                    group: None,
                    hostname: Host::make("test-destination"),
                })
                .await?;
            let (transport, _deliveries) = LoopbackSender::new();
            transport.script(port(0), Script::Hold(Arc::clone(&barrier)));
            let router = RouterHandle::new(
                runtime.addresses().clone(),
                Arc::clone(runtime.fleet()),
                Arc::new(transport),
                None,
            );
            Ok(TypedSender::<CountingCodec>::new(&router, cap)?)
        }
        .await;
        let sender: TypedSender<CountingCodec> = match prepared {
            Ok(sender) => sender,
            Err(error) => {
                runtime.shutdown(|| async {}).await?;
                return Err(error);
            }
        };
        Ok(Self {
            shared: Shared {
                fleet: Arc::clone(runtime.fleet()),
                pending: Arc::clone(runtime.pending()),
                counters: sender.counters(),
                destination,
                barrier,
                listener,
                node: runtime.node(),
                directory,
            },
            runtime,
            sender,
        })
    }
}

/// A peer listener on a port the operating system chooses.
///
/// Registration reads the bound listener rather than a port number, so a test
/// binds a real one and the published port is always a port that exists.
async fn listener() -> Result<BoundListener> {
    Ok(BoundListener::bind(&TransportConfiguration::default()).await?)
}

/// Starts one runtime with every peer field left at its default.
async fn plain_process() -> Result<PlainProcess> {
    let directory = directory(LEASE).await?;
    let membership = membership();
    let bound = listener().await?;
    let bound_port = bound.address().port();
    let router = RouterConfiguration::default();
    let requester = requester();
    let runtime = PeerRuntime::start(PeerInputs {
        store: store().await?.clone(),
        listener: bound,
        health: TestHealth::new(true, true),
        contact: CONTACT,
        group: Some(membership.clone()),
        router: &router,
        fleet: FleetConfiguration::default(),
        requester: &requester,
    })
    .await?;
    Ok(PlainProcess {
        runtime,
        directory,
        membership,
        bound_port,
    })
}

/// The frame ceiling one process's listener and senders share.
fn frame_cap() -> Result<FrameCap> {
    Ok(FrameCap::new(FRAME_BYTES)?)
}

/// The requester limits one process under test runs with.
///
/// The response ceiling matches the frame ceiling, because `start` refuses a
/// process that would admit a response no frame its own listener accepts could
/// carry.
fn requester() -> RequesterConfiguration {
    RequesterConfiguration {
        max_response_bytes: FRAME_BYTES,
        ..RequesterConfiguration::default()
    }
}

/// A header for one successful response to `request`, addressed to `target`.
fn header(target: NodeId, request: RequestId, subsystem: &str) -> Result<FrameHeader> {
    Ok(FrameHeader {
        target,
        request,
        subsystem: SubsystemName::try_new(subsystem)?,
        status: ResponseStatus::Success,
        relay: None,
    })
}
