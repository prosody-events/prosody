//! Tests for how Prosody resolves the peer listener address.

use super::{DEFAULT_PORT, listener_address};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

/// A configured bind address. Each shape that resolution treats differently
/// has an equal chance.
#[derive(Clone, Debug)]
struct Configured(Option<SocketAddr>);

impl Arbitrary for Configured {
    fn arbitrary(g: &mut Gen) -> Self {
        let port = u16::arbitrary(g);
        Self(match u8::arbitrary(g) % 4 {
            0 => None,
            1 => Some(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port))),
            2 => Some(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port))),
            _ => Some(SocketAddr::arbitrary(g)),
        })
    }
}

/// Peers can connect to every resolved address. An unspecified IP address
/// keeps its port. A specified address does not change.
#[test]
fn every_listener_address_is_specified() {
    fn property(Configured(configured): Configured) -> TestResult {
        let resolved = match listener_address(configured) {
            Ok(resolved) => resolved,
            Err(error) => return TestResult::error(format!("{error:#}")),
        };
        TestResult::from_bool(match configured {
            None => !resolved.ip().is_unspecified() && resolved.port() == DEFAULT_PORT,
            Some(bind) if bind.ip().is_unspecified() => {
                !resolved.ip().is_unspecified() && resolved.port() == bind.port()
            }
            Some(bind) => resolved == bind,
        })
    }
    QuickCheck::new().quickcheck(property as fn(Configured) -> TestResult);
}
