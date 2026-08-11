//! What one process can only learn by asking the machine it runs on.
//!
//! The machine-name lookup can block. [`discover`] runs it on the blocking
//! pool.
//!
//! Inside this module the rule is read rather than compiled: a function added
//! here could call them on a runtime thread.

use super::config::{RouterConfiguration, validate_label};
use crate::router::directory::{DirectAddress, NetworkId, PeerRegistration};
use crate::router::grpc::BoundListener;
use crate::router::{Host, MAX_LABEL_BYTES, PeerId};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle, spawn_blocking};
use tonic::transport::Error as TransportError;
use whoami::hostname;

#[cfg(test)]
mod tests;

/// The host value a process can only learn from its machine.
///
/// [`discover`] produces one of these, and [`registration`] spends it.
pub(super) struct DiscoveredHost {
    /// This machine's name. It is a label a registration may publish.
    pub(super) hostname: Host,
}

/// Reads this machine's name on the blocking pool.
///
/// The spawn sits inside the awaited expression, so no early return stands
/// between the two. One there would detach a blocking task that tokio cannot
/// abort.
///
/// # Errors
///
/// Returns [`DiscoveryError`] when the machine name cannot be read or
/// published, or when the blocking task does not join.
pub(super) async fn discover() -> Result<DiscoveredHost, DiscoveryError> {
    join_discovery(spawn_blocking(discover_host)).await
}

/// Builds the registration from the bound listener and discovered host.
pub(super) fn registration(
    peer: PeerId,
    listener: &BoundListener,
    discovered: DiscoveredHost,
    config: &RouterConfiguration,
) -> Result<PeerRegistration, DiscoveryError> {
    let DiscoveredHost { hostname } = discovered;
    Ok(PeerRegistration {
        peer,
        direct: DirectAddress::new(listener.address())?,
        advertised: config.advertised.clone(),
        network: config.network.as_deref().map(NetworkId::make),
        hostname,
    })
}

/// Reports what the blocking task returned.
///
/// A task that does not join — one that was cancelled, or that panicked —
/// becomes [`DiscoveryError::Task`]. It never becomes an absent
/// address that a later step fills with a guess. This is a function of its own
/// because that is the claim a test can drive: give it a task that cannot join,
/// and read what it reports.
///
/// # Errors
///
/// Returns [`DiscoveryError`] from the task, or for the task.
async fn join_discovery(
    task: JoinHandle<Result<DiscoveredHost, DiscoveryError>>,
) -> Result<DiscoveredHost, DiscoveryError> {
    task.await?
}

/// Reads the machine name.
///
/// This is discovery's blocking half. [`discover`] is what runs it, and it runs
/// it on the blocking pool.
///
/// # Errors
///
/// Returns [`DiscoveryError`] when the machine name cannot be read or
/// published.
fn discover_host() -> Result<DiscoveredHost, DiscoveryError> {
    let machine = hostname()?;
    // One label rule for both sources. A name an operator may not configure is
    // not a name this machine may publish either.
    validate_label(&machine).map_err(|_| DiscoveryError::Unpublishable {
        bytes: machine.len(),
        limit: MAX_LABEL_BYTES,
    })?;
    Ok(DiscoveredHost {
        hostname: Host::make(&machine),
    })
}

/// What can stop a process from learning what only its machine knows.
#[derive(Debug, Error)]
pub(crate) enum DiscoveryError {
    /// The discovered listener address does not form a Tonic endpoint.
    #[error(transparent)]
    Endpoint(#[from] TransportError),

    /// The machine's own name could not be read. Every registration publishes
    /// it, so startup cannot continue.
    #[error("the machine name could not be read: {0:#}")]
    Name(#[from] whoami::Error),

    /// The blocking discovery task returned no result. The task was cancelled
    /// or panicked. Startup stops because the direct endpoint has no host.
    #[error("the host discovery task returned no result: {0:#}")]
    Task(#[from] JoinError),

    /// The machine name is not a label that a registration can publish.
    /// Startup stops because an oversized label would not resolve.
    #[error("the machine name is {bytes} bytes, outside the 1 to {limit} byte label range")]
    Unpublishable {
        /// The machine name's length.
        bytes: usize,
        /// The longest label a registration may publish.
        limit: usize,
    },
}
