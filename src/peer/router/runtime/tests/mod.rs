//! The process runtime's suites, driven against live Cassandra and a real
//! listener.
//!
//! Isolation follows the Cassandra row rule: every process mints a fresh peer
//! id and a fresh group id, so rows are disjoint in the shared `prosody_test`
//! keyspace and no test creates a keyspace of its own.

use super::{PeerInputs, PeerRuntime, PreparedPeerRuntime, RouterConfiguration};
use crate::heartbeat::HeartbeatRegistry;
use crate::peer::requester::registry::PendingRegistry;
use crate::peer::response::RequestId;
use crate::peer::response::frame::FrameHeader;
use crate::peer::router::PeerId;
use crate::peer::router::cache_config::PeerCacheConfiguration;
use crate::peer::router::directory::cassandra::CassandraPeerDirectory;
use crate::peer::router::directory::tests::support::cassandra_directory;
use crate::peer::router::directory::{Endpoint, PeerDirectory};
use crate::peer::router::grpc::BoundListener;
use crate::peer::router::loopback::listener::endpoint as listener_endpoint;
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
    runtime: PeerRuntime<CassandraPeerDirectory>,
    shared: Shared,
}

/// The handles a test keeps after shutdown has consumed the runtime.
struct Shared {
    pending: Arc<PendingRegistry>,
    /// Where this process's own listener is.
    listener: Endpoint,
    /// The id the runtime minted.
    peer: PeerId,
    directory: CassandraPeerDirectory,
}

/// One runtime with the default peer configuration, and no sender.
struct PlainProcess {
    runtime: PeerRuntime<CassandraPeerDirectory>,
    directory: CassandraPeerDirectory,
}

impl Process {
    /// Starts one live process.
    async fn new() -> Result<Self> {
        let directory = cassandra_directory(LEASE).await?;
        let bound = BoundListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let listener = listener_endpoint(&bound)?;
        let router = RouterConfiguration::default();
        let runtime = start_runtime(PeerInputs {
            directory: directory.clone(),
            listener: bound,
            heartbeats: HeartbeatRegistry::test(),
            router: &router,
            cache: PeerCacheConfiguration::default(),
        })
        .await?;
        Ok(Self {
            shared: Shared {
                pending: Arc::clone(runtime.local.pending()),
                listener,
                peer: runtime.peer(),
                directory,
            },
            runtime,
        })
    }
}

/// A peer listener on an address the operating system completes.
///
/// Registration reads the bound listener, so a test binds a real one.
pub(super) async fn listener() -> Result<BoundListener> {
    Ok(BoundListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).await?)
}

/// Starts one runtime with every peer field left at its default.
async fn plain_process() -> Result<PlainProcess> {
    let directory = cassandra_directory(LEASE).await?;
    let bound = listener().await?;
    let router = RouterConfiguration::default();
    let runtime = start_runtime(PeerInputs {
        directory: directory.clone(),
        listener: bound,
        heartbeats: HeartbeatRegistry::test(),
        router: &router,
        cache: PeerCacheConfiguration::default(),
    })
    .await?;
    Ok(PlainProcess { runtime, directory })
}

pub(in crate::peer::router) async fn start_runtime<D>(
    inputs: PeerInputs<'_, D>,
) -> Result<PeerRuntime<D>>
where
    D: PeerDirectory,
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
fn header(target: PeerId, request: RequestId, subsystem: &str) -> Result<FrameHeader> {
    Ok(FrameHeader {
        target,
        request,
        subsystem: SubsystemName::try_new(subsystem)?,
        relay: None,
    })
}
