//! gRPC client address and method construction.

use crate::router::Host;
use crate::router::SendFailure;
use crate::router::directory::Endpoint;
use crate::router::grpc::client::{
    DELIVER_RESPONSE, GRPC_TIMEOUT_LIMIT, outbound_timeout, peer_uri,
};
use crate::router::grpc::generated::peer_server::SERVICE_NAME;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use std::time::Duration;
use tokio::time::Instant;
use tonic::transport::Endpoint as Dialled;

/// Every host a node can publish makes a URI the dialer parses.
///
/// An IPv6 literal is the case that needs the brackets: unbracketed, its own
/// colons split the authority, nothing parses it, and every response to that
/// node is reported unreachable. A routed probe on an IPv6 host publishes
/// exactly such a literal.
#[test]
fn every_published_host_makes_a_dialable_uri() -> Result<()> {
    for host in ["127.0.0.1", "fd00::5", "::1", "peer.example"] {
        let uri = peer_uri(&Endpoint {
            host: Host::make(host),
            port: 8080,
        });
        Dialled::from_shared(uri.clone())
            .map_err(|error| eyre!("{host} produced {uri}, which does not parse: {error}"))?;
    }
    Ok(())
}

/// The path the client calls names the generated service, so a renamed proto
/// cannot leave the client misrouting quietly.
#[test]
fn the_method_path_names_the_generated_service() -> Result<()> {
    ensure!(
        DELIVER_RESPONSE.as_str() == format!("/{SERVICE_NAME}/DeliverResponse"),
        "the client calls {}, which is not the generated service's method",
        DELIVER_RESPONSE.as_str()
    );
    Ok(())
}

/// An outbound deadline always fits Tonic's gRPC timeout encoder.
#[test]
fn an_extreme_deadline_stays_inside_the_grpc_range() {
    let deadline = Instant::now() + GRPC_TIMEOUT_LIMIT + Duration::from_hours(1);
    assert_eq!(
        outbound_timeout(deadline),
        Err(SendFailure::Status(tonic::Code::InvalidArgument)),
        "an extreme deadline reached Tonic's infallible timeout encoder"
    );
}
