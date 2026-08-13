//! Peer listener registration and reflection.

use super::Harness;
use crate::heartbeat::HeartbeatRegistry;
use crate::peer::router::cache_config::PeerCacheConfiguration;
use crate::peer::router::directory::tests::support::cassandra_directory;
use crate::peer::router::directory::{PeerDirectory, RegistrationTtl};
use crate::peer::router::grpc::BoundListener;
use crate::peer::router::loopback::listener::bind_address;
use crate::peer::router::runtime::{PeerInputs, RouterConfiguration, start_runtime};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
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
