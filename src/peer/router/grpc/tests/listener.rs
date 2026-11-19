//! Peer listener registration and reflection.

use super::Harness;
use crate::heartbeat::HeartbeatRegistry;
use crate::peer::router::cache_config::PeerCacheConfiguration;
use crate::peer::router::directory::tests::support::cassandra_directory;
use crate::peer::router::directory::{PeerDirectory, RegistrationTtl};
use crate::peer::router::grpc::{BoundListener, TransportError};
use crate::peer::router::loopback::listener::bind_address;
use crate::peer::router::runtime::{PeerInputs, RouterConfiguration, start_runtime};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tonic::transport::Endpoint as Dialled;
use tonic_reflection::pb::v1::ServerReflectionRequest;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;

#[test]
fn a_registration_publishes_the_bound_address() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let bound = BoundListener::bind(bind_address()).await?;
        let expected = Dialled::from_shared(format!("http://{}", bound.address()))?;
        let router = RouterConfiguration::default();
        let directory = cassandra_directory(RegistrationTtl::DEFAULT.duration()).await?;
        let runtime = start_runtime(PeerInputs {
            directory: directory.clone(),
            listener: bound,
            heartbeats: HeartbeatRegistry::test(),
            router: &router,
            cache: PeerCacheConfiguration::default(),
        })
        .await?;
        let outcome = async {
            let published = directory
                .read(runtime.peer())
                .await?
                .ok_or_else(|| eyre!("a started runtime must resolve"))?;
            ensure!(
                published.direct.endpoint().uri() == expected.uri(),
                "the runtime published another address"
            );
            Ok(())
        }
        .await;
        let shutdown = runtime.shutdown(|| async {}).await;
        outcome.and(shutdown.map_err(Into::into))
    })
}

/// A held port stops a strict bind. The other bind moves to a port that the
/// operating system selects, on the same IP address.
#[test]
fn only_a_movable_bind_leaves_a_held_port() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let holder = BoundListener::bind(bind_address()).await?;
        let held = holder.address();

        let strict = BoundListener::bind(held).await;
        ensure!(
            matches!(&strict, Err(TransportError::Bind(error)) if error.kind() == ErrorKind::AddrInUse),
            "a strict bind left the held port"
        );

        let moved = BoundListener::bind_any_port(held).await?.address();
        ensure!(moved.ip() == held.ip(), "the bind changed the IP address");
        ensure!(moved.port() != held.port(), "the bind used the held port");
        Ok(())
    })
}

/// A wildcard listener accepts on every interface, but it publishes an IPv4
/// interface address that peers can connect to.
#[test]
fn a_wildcard_listener_publishes_an_interface_address() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let bound = BoundListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).await?;
        let published = bound.address();
        ensure!(
            published.is_ipv4() && !published.ip().is_unspecified(),
            "the listener published {published}"
        );

        TcpStream::connect((Ipv4Addr::LOCALHOST, published.port())).await?;
        Ok(())
    })
}

#[test]
fn reflection_is_always_served() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::with(bind_address()).await?;
        let reflected = reflects_peer_service(harness.address.clone()).await;
        harness.stop().await?;
        ensure!(reflected?, "reflection did not publish the peer service");
        Ok(())
    })
}

async fn reflects_peer_service(endpoint: Dialled) -> Result<bool> {
    let channel = endpoint.connect_lazy();
    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut responses = ServerReflectionClient::new(channel)
        .server_reflection_info(tokio_stream::once(request))
        .await?
        .into_inner();
    let response = responses
        .next()
        .await
        .ok_or_else(|| eyre!("reflection returned no response"))??;
    let Some(MessageResponse::ListServicesResponse(list)) = response.message_response else {
        return Err(eyre!("reflection returned another response"));
    };
    Ok(list
        .service
        .iter()
        .any(|service| service.name == "prosody.peer.v1.PeerService"))
}
