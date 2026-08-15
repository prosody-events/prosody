//! What the gRPC health service answers, and whose verdict it is.

use super::Harness;
use crate::peer::router::grpc::generated::peer_service_server::SERVICE_NAME;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// A name this listener serves nothing under.
const UNSERVED: &str = "prosody.peer.v1.NotAService";

/// `grpc.health.v1` is routed on the peer port itself, not merely built.
///
/// A generic client dials the shared listener and reads all three answers off
/// the socket: the process, the peer service, and a name nothing serves. A
/// listener that never added the health service answers `UNIMPLEMENTED` to the
/// first of them, which no other test would see.
#[test]
fn the_health_service_answers_on_the_peer_port() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let channel = harness.address.connect_lazy();
        let mut client = HealthClient::new(channel);
        for name in ["", SERVICE_NAME] {
            let answered = client.check(request(name)).await?.into_inner().status;
            ensure!(
                answered == i32::from(ServingStatus::Serving),
                "the listener serves a ready and live process, so {name:?} must be SERVING"
            );
        }
        let refused = client.check(request(UNSERVED)).await;
        ensure!(
            matches!(&refused, Err(status) if status.code() == Code::NotFound),
            "a name this listener does not serve must be NOT_FOUND over the wire"
        );
        Ok(())
    })
}

/// One health request for `service`.
fn request(service: &str) -> HealthCheckRequest {
    HealthCheckRequest {
        service: service.to_owned(),
    }
}
