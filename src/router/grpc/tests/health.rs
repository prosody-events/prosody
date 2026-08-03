//! What the gRPC health service answers, and whose verdict it is.

use super::TestHealth;
use crate::consumer::Managers;
use crate::consumer::probes::ProbeServer;
use crate::heartbeat::HeartbeatRegistry;
use crate::router::grpc::generated::peer_server::SERVICE_NAME;
use crate::router::grpc::health::{ConsumerHealth, PeerHealth};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use reqwest::Client;
use reqwest::StatusCode;
use serde_json::Value;
use std::sync::Arc;
use tonic::{Code, Request};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
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
        for (ready, live) in [(true, true), (true, false), (false, true), (false, false)] {
            let health = PeerHealth::new(TestHealth::new(ready, live), SERVICE_NAME);
            let expected = if ready && live {
                ServingStatus::Serving
            } else {
                ServingStatus::NotServing
            };
            ensure!(
                check(&health, "").await? == Some(i32::from(expected)),
                "ready = {ready} and live = {live} must answer {expected:?} for the process"
            );
            ensure!(
                check(&health, SERVICE_NAME).await? == Some(i32::from(ServingStatus::Serving)),
                "a listener that answers at all serves the peer method"
            );
            ensure!(
                check(&health, UNSERVED).await?.is_none(),
                "a name this listener does not serve must be NOT_FOUND"
            );
        }
        Ok(())
    })
}

/// The two health surfaces cannot disagree, because they read the same
/// predicates: a consumer with no partitions assigned is unready on both.
#[test]
fn the_two_health_surfaces_agree() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let managers: Arc<Managers<Value>> = Arc::default();
        let heartbeats = HeartbeatRegistry::test();
        let server = ProbeServer::new(0, Arc::clone(&managers), heartbeats.clone())?;
        let address = server.local_addr();
        let health = PeerHealth::new(ConsumerHealth::new(managers, heartbeats), SERVICE_NAME);
        let over_http = Client::new()
            .get(format!("http://127.0.0.1:{}/readyz", address.port()))
            .send()
            .await
            .map(|response| response.status());
        let over_grpc = check(&health, "").await;
        server.shutdown().await;
        ensure!(
            over_http? == StatusCode::SERVICE_UNAVAILABLE,
            "a consumer with no partitions assigned is unready over HTTP"
        );
        ensure!(
            over_grpc? == Some(i32::from(ServingStatus::NotServing)),
            "the same consumer must be unready over gRPC"
        );
        Ok(())
    })
}

/// The serving status one `Check` answered, or `None` when the name is not
/// served here.
async fn check<H: Health>(health: &H, service: &str) -> Result<Option<i32>> {
    let request = Request::new(HealthCheckRequest {
        service: service.to_owned(),
    });
    match health.check(request).await {
        Ok(response) => Ok(Some(response.into_inner().status)),
        Err(status) if status.code() == Code::NotFound => Ok(None),
        Err(status) => Err(status.into()),
    }
}
