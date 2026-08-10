//! Peer listener registration and reflection.

use super::{Harness, RawFramed};
use crate::heartbeat::HeartbeatRegistry;
use crate::router::directory::tests::support::cassandra_directory;
use crate::router::directory::{NodeDirectory, RegistrationTtl};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::BoundListener;
use crate::router::grpc::codec::ClientFrameCodec;
use crate::router::loopback::listener::bind_address;
use crate::router::runtime::{PeerInputs, RouterConfiguration, start_runtime};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure, eyre};
use tonic::client::Grpc;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::Endpoint as Dialled;
use tonic::{Code, Request};

const REFLECTION: &str = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo";

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
            fleet: FleetConfiguration::default(),
        })
        .await?;
        let outcome = async {
            let published = directory
                .read(runtime.node())
                .await?
                .ok_or_else(|| eyre!("a started runtime must resolve"))?;
            ensure!(
                published.direct.uri() == expected.uri(),
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
        let actual = reflect(harness.address.clone()).await;
        harness.stop().await?;
        ensure!(actual? == Code::Ok, "reflection returned another status");
        Ok(())
    })
}

async fn reflect(endpoint: Dialled) -> Result<Code> {
    let channel = endpoint.connect_lazy();
    let mut client = Grpc::new(channel);
    let request = Request::new(tokio_stream::iter(Vec::<RawFramed>::new()));
    if let Err(error) = client.ready().await {
        bail!("the reflection channel never became ready: {error:#}");
    }
    Ok(
        match client
            .streaming(
                request,
                PathAndQuery::from_static(REFLECTION),
                ClientFrameCodec::new(0),
            )
            .await
        {
            Ok(_) => Code::Ok,
            Err(status) => status.code(),
        },
    )
}
