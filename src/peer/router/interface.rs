//! The address of this machine's default network interface.

use std::net::{IpAddr, SocketAddr, SocketAddrV6};
use thiserror::Error;

/// Returns an address of the default network interface on `port`.
///
/// A `family` limits the result to the address family of that IP address.
/// Without one, an IPv4 address comes first. An address that is not loopback
/// or link-local comes before one that is.
///
/// # Errors
///
/// Returns [`InterfaceError`] when the operating system identifies no default
/// interface, or when that interface has no address in the family.
pub(crate) fn default_address(
    family: Option<IpAddr>,
    port: u16,
) -> Result<SocketAddr, InterfaceError> {
    let interface = netdev::get_interfaces()
        .into_iter()
        .find(|interface| interface.default)
        .ok_or(InterfaceError::NoDefaultInterface)?;

    let ipv4 = interface
        .ipv4
        .iter()
        .filter(|_| !matches!(family, Some(IpAddr::V6(_))))
        .map(|network| SocketAddr::new(IpAddr::V4(network.addr()), port));
    let ipv6 = interface
        .ipv6
        .iter()
        .filter(|_| !matches!(family, Some(IpAddr::V4(_))))
        .map(|network| {
            let address = network.addr();
            let scope = if address.is_unicast_link_local() {
                interface.index
            } else {
                0
            };
            SocketAddr::V6(SocketAddrV6::new(address, port, 0, scope))
        });
    let mut candidates = ipv4.chain(ipv6);

    candidates
        .clone()
        .find(|address| !is_local(address.ip()))
        .or_else(|| candidates.next())
        .ok_or(InterfaceError::NoAddress)
}

/// Returns whether `ip` is loopback or link-local.
fn is_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unicast_link_local(),
    }
}

/// Why the default network interface has no address to publish.
#[derive(Debug, Error)]
pub(crate) enum InterfaceError {
    /// The operating system did not identify a default interface.
    #[error("the operating system did not identify a default network interface")]
    NoDefaultInterface,
    /// The default interface has no address in the requested family.
    #[error("the default network interface has no address in the requested family")]
    NoAddress,
}
