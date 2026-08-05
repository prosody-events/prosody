//! What one process can only learn by asking the machine it runs on.
//!
//! Both lookups block. A name server can hold a resolver for as long as it
//! retries, and the machine name is a system call. Both are private to this
//! module, so no code outside it can call either one. [`discover`] is what the
//! rest of the crate has instead, and it runs them on the blocking pool.
//!
//! Inside this module the rule is read rather than compiled: a function added
//! here could call them on a runtime thread.

use super::config::validate_label;
use crate::router::{Host, MAX_LABEL_BYTES};
use std::net::{ToSocketAddrs, UdpSocket};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle, spawn_blocking};
use whoami::hostname;

#[cfg(test)]
mod tests;

/// The two host values a process can only learn by asking the machine it runs
/// on, and the network beneath it.
///
/// [`discover`] produces one of these, and `discover_registration` spends it.
pub(super) struct DiscoveredHost {
    /// This machine's name. It is a label a registration may publish.
    pub(super) hostname: Host,
    /// The local address that reaches the contact point, where the probe found
    /// one.
    pub(super) routed: Option<Host>,
}

/// Reads this machine's name and the address that reaches `contact`, on the
/// blocking pool.
///
/// The spawn sits inside the awaited expression, so no early return stands
/// between the two. One there would detach a blocking task that tokio cannot
/// abort.
///
/// # Errors
///
/// Returns [`DiscoveryError`] when the machine name cannot be read or
/// published, or when the blocking task does not join.
pub(super) async fn discover(contact: &str) -> Result<DiscoveredHost, DiscoveryError> {
    let contact = contact.to_owned();
    join_discovery(spawn_blocking(move || discover_host(&contact))).await
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

/// Reads the machine name and the address that reaches `contact`.
///
/// This is discovery's blocking half. [`discover`] is what runs it, and it runs
/// it on the blocking pool.
///
/// # Errors
///
/// Returns [`DiscoveryError`] when the machine name cannot be read, or when
/// it is not a label a registration may publish. The routed probe's own failure
/// is not an error: it answers `None`, and `discover_registration` then
/// publishes the machine name.
fn discover_host(contact: &str) -> Result<DiscoveredHost, DiscoveryError> {
    // The machine name is published in its own right, so the lookup is paid
    // once and reused where the routed probe finds no address.
    let machine = hostname()?;
    // One label rule for both sources. A name an operator may not configure is
    // not a name this machine may publish either.
    validate_label(&machine).map_err(|_| DiscoveryError::Unpublishable {
        bytes: machine.len(),
        limit: MAX_LABEL_BYTES,
    })?;
    Ok(DiscoveredHost {
        hostname: Host::make(&machine),
        routed: routed_host(contact),
    })
}

/// The local address the operating system would use to reach `contact`.
///
/// Connecting a UDP socket sends nothing: it only asks the routing table which
/// interface would carry that traffic. The answer is the address that reaches
/// the contact point, and nothing more. A loopback contact point answers with a
/// loopback address, and a host that reaches Cassandra over a management
/// interface answers with the management address. A peer elsewhere reaches
/// neither, which is what
/// [`RouterConfiguration::advertised_host`](super::RouterConfiguration::advertised_host)
/// is for. Any failure — an unresolvable contact point, no route — yields
/// `None`, and the next source in the discovery order answers.
fn routed_host(contact: &str) -> Option<Host> {
    let Ok(mut targets) = contact.to_socket_addrs() else {
        return None;
    };
    let target = targets.next()?;
    // An IPv4-bound socket cannot discover an IPv6 route, so the probe binds
    // the family of the address it aims at.
    let unspecified = if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let Ok(probe) = UdpSocket::bind(unspecified) else {
        return None;
    };
    let Ok(()) = probe.connect(target) else {
        return None;
    };
    let Ok(local) = probe.local_addr() else {
        return None;
    };
    Some(Host::make(&local.ip().to_string()))
}

/// What can stop a process from learning what only its machine knows.
#[derive(Debug, Error)]
pub(crate) enum DiscoveryError {
    /// The machine's own name could not be read. Every registration publishes
    /// it, so the lookup is not optional.
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
