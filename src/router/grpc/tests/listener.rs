//! Peer listener registration and reflection.

use super::{FRAME_CAP, Harness, transport};
use crate::heartbeat::HeartbeatRegistry;
use crate::response::frame::FrameCap;
use crate::router::directory::tests::support::cassandra_directory;
use crate::router::directory::{NodeDirectory, RegistrationTtl};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::BoundListener;
use crate::router::grpc::codec::ClientFrameCodec;
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
fn a_registration_publishes_the_bound_port() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let bound = BoundListener::bind(&transport(FRAME_CAP)?).await?;
        let expected = bound.address().port();
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
                published.direct.port == expected,
                "the runtime published another port"
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
        let harness = Harness::with(transport(FRAME_CAP)).await?;
        let actual = reflect(harness.address.port).await;
        harness.stop().await?;
        ensure!(actual? == Code::Ok, "reflection returned another status");
        Ok(())
    })
}

async fn reflect(port: u16) -> Result<Code> {
    let channel = Dialled::from_shared(format!("http://127.0.0.1:{port}"))?.connect_lazy();
    let mut client = Grpc::new(channel).max_decoding_message_size(FrameCap::MAX_BYTES);
    let request = Request::new(tokio_stream::iter(Vec::new()));
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
