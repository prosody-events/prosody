//! gRPC client address and method construction.

use crate::peer::router::SendFailure;
use crate::peer::router::grpc::client::{DELIVER_RESULT, GRPC_TIMEOUT_LIMIT, outbound_timeout};
use crate::peer::router::grpc::generated::peer_service_server::SERVICE_NAME;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use std::time::Duration;
use tokio::time::Instant;

/// The path the client calls names the generated service, so a renamed proto
/// cannot leave the client misrouting quietly.
#[test]
fn the_method_path_names_the_generated_service() -> Result<()> {
    ensure!(
        DELIVER_RESULT.as_str() == format!("/{SERVICE_NAME}/DeliverResult"),
        "the client calls {}, which is not the generated service's method",
        DELIVER_RESULT.as_str()
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
