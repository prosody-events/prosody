//! The process runtime's suites, driven against live Cassandra and a real
//! listener.
//!
//! Isolation follows the Cassandra row rule: every process mints a fresh node
//! id and a fresh group id, so rows are disjoint in the shared `prosody_test`
//! keyspace and no test creates a keyspace of its own.

use super::{PeerInputs, PeerRuntime, PreparedPeerRuntime, RouterConfiguration};
use crate::heartbeat::HeartbeatRegistry;
use crate::requester::registry::PendingRegistry;
use crate::response::frame::FrameHeader;
use crate::response::{RequestId, ResponseStatus};
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::directory::tests::support::cassandra_directory;
use crate::router::directory::{Endpoint, NodeDirectory};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::BoundListener;
use crate::router::{Host, NodeId};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

mod config;
mod ownership;
mod shutdown;
mod wiring;

/// The lease these tests read under. It equals the default a runtime starts
/// with, and its refresh delay is at least a fifth of it, so no refresh runs
/// while a test observes the first write.
const LEASE: Duration = Duration::from_secs(30);

/// How long a parked request stays open.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The subsystem every test request awaits.
const ALPHA: &str = "alpha";

/// One process under test and the handles its runtime supplies.
struct Process {
    runtime: PeerRuntime<CassandraNodeDirectory>,
    shared: Shared,
}

/// The handles a test keeps after shutdown has consumed the runtime.
struct Shared {
    fleet: Arc<DestinationFleet>,
    pending: Arc<PendingRegistry>,
    /// Where this process's own listener is.
    listener: Endpoint,
    /// The id the runtime minted.
    node: NodeId,
    directory: CassandraNodeDirectory,
}

/// One runtime with the default peer configuration, and no sender.
struct PlainProcess {
    runtime: PeerRuntime<CassandraNodeDirectory>,
    directory: CassandraNodeDirectory,
    bound_port: u16,
}

impl Process {
    /// Starts one live process.
    async fn new() -> Result<Self> {
        let directory = cassandra_directory(LEASE).await?;
        let bound = BoundListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let listener = Endpoint {
            host: Host::make("127.0.0.1"),
            port: bound.address().port(),
        };
        let router = RouterConfiguration::default();
        let runtime = start_runtime(PeerInputs {
            directory: directory.clone(),
            listener: bound,
            heartbeats: HeartbeatRegistry::test(),
            router: &router,
            fleet: FleetConfiguration::default(),
        })
        .await?;
        Ok(Self {
            shared: Shared {
                fleet: Arc::clone(&runtime.network.fleet),
                pending: Arc::clone(runtime.local.pending()),
                listener,
                node: runtime.node(),
                directory,
            },
            runtime,
        })
    }
}

/// A peer listener on a port the operating system chooses.
///
/// Registration reads the bound listener rather than a port number, so a test
/// binds a real one and the published port is always a port that exists.
pub(super) async fn listener() -> Result<BoundListener> {
    Ok(BoundListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).await?)
}

/// Starts one runtime with every peer field left at its default.
async fn plain_process() -> Result<PlainProcess> {
    let directory = cassandra_directory(LEASE).await?;
    let bound = listener().await?;
    let bound_port = bound.address().port();
    let router = RouterConfiguration::default();
    let runtime = start_runtime(PeerInputs {
        directory: directory.clone(),
        listener: bound,
        heartbeats: HeartbeatRegistry::test(),
        router: &router,
        fleet: FleetConfiguration::default(),
    })
    .await?;
    Ok(PlainProcess {
        runtime,
        directory,
        bound_port,
    })
}

pub(in crate::router) async fn start_runtime<D>(inputs: PeerInputs<'_, D>) -> Result<PeerRuntime<D>>
where
    D: NodeDirectory,
{
    let prepared = PreparedPeerRuntime::start(inputs).await?;
    match prepared.activate().await {
        Ok(runtime) => Ok(runtime),
        Err((prepared, error)) => {
            prepared.abandon().await;
            Err(error.into())
        }
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
