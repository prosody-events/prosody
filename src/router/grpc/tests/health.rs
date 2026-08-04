//! What the gRPC health service answers, and whose verdict it is.

use super::Harness;
use crate::consumer::Managers;
use crate::consumer::probes::ProbeServer;
use crate::heartbeat::HeartbeatRegistry;
use crate::router::grpc::generated::peer_server::SERVICE_NAME;
use crate::router::grpc::health::{ConsumerHealth, PeerHealth};
use crate::router::loopback::TestHealth;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use reqwest::Client;
use reqwest::StatusCode;
use serde_json::Value;
use std::sync::Arc;
use tonic::transport::Endpoint as Dialled;
use tonic::{Code, Request};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::health_server::Health;

/// A name this listener serves nothing under.
const UNSERVED: &str = "prosody.peer.v1.NotAService";

/// The process answers `SERVING` under the empty name exactly when it is both
/// ready and live; the peer service answers `SERVING` whenever it answers at
/// all; every other name is `NOT_FOUND`.
#[test]
fn the_grpc_health_answer_follows_the_predicates() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        // Neither answer depends on the predicates, so both are asserted once
        // rather than inside the loop that varies them.
        let health = PeerHealth::new(TestHealth::new(false, false));
        ensure!(
            check(&health, SERVICE_NAME).await? == Some(i32::from(ServingStatus::Serving)),
            "a listener that answers at all serves the peer method"
        );
        ensure!(
            check(&health, UNSERVED).await?.is_none(),
            "a name this listener does not serve must be NOT_FOUND"
        );
        for (ready, live) in [(true, true), (true, false), (false, true), (false, false)] {
            let health = PeerHealth::new(TestHealth::new(ready, live));
            let expected = if ready && live {
                ServingStatus::Serving
            } else {
                ServingStatus::NotServing
            };
            ensure!(
                check(&health, "").await? == Some(i32::from(expected)),
                "ready = {ready} and live = {live} must answer {expected:?} for the process"
            );
        }
        Ok(())
    })
}

/// One consumer with no partitions assigned, read over both surfaces.
///
/// The two read the same predicates, so `/readyz` and the empty gRPC name must
/// agree that it is unready. They agree by folding, not by matching: `/livez`
/// calls the same process live, while the empty name answers `NOT_SERVING`
/// because it reports ready **and** live. That is the fold the empty name
/// documents, pinned here so a liveness probe is never wired to it by mistake.
#[test]
fn the_two_health_surfaces_agree() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let managers: Arc<Managers<Value>> = Arc::default();
        let heartbeats = HeartbeatRegistry::test();
        let server = ProbeServer::new(0, Arc::clone(&managers), heartbeats.clone())?;
        let address = server.local_addr();
        let health = PeerHealth::new(ConsumerHealth::new(managers, heartbeats));
        let ready_over_http = probe(address.port(), "/readyz").await;
        let live_over_http = probe(address.port(), "/livez").await;
        let over_grpc = check(&health, "").await;
        server.shutdown().await;
        ensure!(
            ready_over_http? == StatusCode::SERVICE_UNAVAILABLE,
            "a consumer with no partitions assigned is unready over HTTP"
        );
        ensure!(
            live_over_http? == StatusCode::OK,
            "the same consumer is live over HTTP"
        );
        ensure!(
            over_grpc? == Some(i32::from(ServingStatus::NotServing)),
            "the empty gRPC name reports ready and live together, so it must not serve"
        );
        Ok(())
    })
}

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
        let channel = Dialled::from_shared(format!("http://127.0.0.1:{}", harness.address.port))?
            .connect_lazy();
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

/// The status one HTTP probe answered.
async fn probe(port: u16, path: &str) -> Result<StatusCode> {
    Ok(Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .send()
        .await?
        .status())
}

/// The serving status one `Check` answered, or `None` when the name is not
/// served here.
async fn check<H: Health>(health: &H, service: &str) -> Result<Option<i32>> {
    match health.check(Request::new(request(service))).await {
        Ok(response) => Ok(Some(response.into_inner().status)),
        Err(status) if status.code() == Code::NotFound => Ok(None),
        Err(status) => Err(status.into()),
    }
}
