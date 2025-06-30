//! Health check endpoints for Kafka consumers.
//!
//! This module provides HTTP endpoints that implement standardized health
//! checks compatible with Kubernetes and other container orchestration systems:
//!
//! - **Readiness probe** (`/readyz`): Indicates whether the consumer has
//!   partitions assigned and is ready to process messages
//! - **Liveness probe** (`/livez`): Indicates whether the consumer is actively
//!   processing messages without stalls
//!
//! These probes help orchestration systems make informed decisions about
//! routing traffic, restarting containers, or scaling deployments based on the
//! current operational state of the service.

use crate::consumer::{Managers, get_assigned_partition_count, get_is_stalled};
use crate::heartbeat::Heartbeat;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum_extra::routing::RouterExt;
use educe::Educe;
use futures::executor::block_on;
use std::borrow::Cow;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

/// HTTP server providing health check endpoints for a Kafka consumer.
///
/// `ProbeServer` exposes two standard health check endpoints:
/// - `/readyz` - Readiness probe that checks if the consumer has partitions
///   assigned
/// - `/livez` - Liveness probe that checks if the consumer's processing is
///   stalled
///
/// The server runs in a background task and can be gracefully shut down.
#[derive(Educe)]
#[educe(Debug)]
pub struct ProbeServer {
    /// The bound socket address of the server
    address: SocketAddr,

    /// Channel for signaling server shutdown
    #[educe(Debug(ignore))]
    shutdown_tx: oneshot::Sender<()>,

    /// Handle to the server task for awaiting completion
    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

/// Shared state for health check handlers.
///
/// Contains references to the components needed to determine
/// consumer health status.
#[derive(Clone, Debug)]
struct ProbeState {
    /// Reference to partition managers for checking assignment status
    managers: Arc<Managers>,

    /// Heartbeat for checking if the poll loop is active
    poll_heartbeat: Heartbeat,
}

impl ProbeServer {
    /// Creates a new HTTP server for health check endpoints.
    ///
    /// # Arguments
    ///
    /// * `port` - Port number to bind the server to
    /// * `managers` - Reference to partition managers for status checks
    /// * `poll_heartbeat` - Heartbeat used to detect poll loop stalls
    ///
    /// # Returns
    ///
    /// A new `ProbeServer` instance if binding succeeds
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the server fails to bind to the specified port
    pub fn new(
        port: u16,
        managers: Arc<Managers>,
        poll_heartbeat: Heartbeat,
    ) -> Result<Self, io::Error> {
        // Create application state with references to components
        let state = ProbeState {
            managers,
            poll_heartbeat,
        };

        // Define router with health check endpoints
        let app = Router::new()
            .route_with_tsr("/readyz", get(readiness_probe))
            .route_with_tsr("/livez", get(liveness_probe))
            .with_state(state);

        // Setup shutdown channel and bind to network interface
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let listener = block_on(async { TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await })?;
        let address = listener.local_addr()?;

        // Spawn server in background task
        let handle = spawn(async move {
            // Define graceful shutdown handler
            let shutdown_signal = async move {
                let _ = shutdown_rx.await;
            };

            info!("starting probe server on {address}");

            // Run server until shutdown is signaled
            let Err(error) = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal)
                .await
            else {
                return;
            };

            error!("probe server failed: {error:#}");
        });

        Ok(Self {
            address,
            shutdown_tx,
            handle,
        })
    }

    #[cfg(test)]
    /// Returns the server's bound address.
    ///
    /// This method is only available in test builds and is useful
    /// for connecting to dynamically assigned ports during tests.
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Gracefully shuts down the probe server.
    ///
    /// Sends a shutdown signal to the server task and waits for it to complete.
    pub async fn shutdown(self) {
        debug!("shutting down probe server");

        // Signal the server to shut down
        let _ = self.shutdown_tx.send(());

        // Wait for the server task to complete
        let _ = self.handle.await;

        info!("probe server shutdown");
    }
}

/// Handles readiness probe requests.
///
/// A consumer is considered ready when it has partitions assigned by Kafka.
/// If no partitions are assigned, the consumer is not yet ready to process
/// messages.
///
/// # Arguments
///
/// * `State(ProbeState { managers, .. })` - Shared state containing partition
///   managers
///
/// # Returns
///
/// A tuple containing:
/// - `StatusCode::OK` (200) if partitions are assigned, or
///   `StatusCode::SERVICE_UNAVAILABLE` (503) otherwise
/// - A message describing the current assignment status
async fn readiness_probe(
    State(ProbeState { managers, .. }): State<ProbeState>,
) -> (StatusCode, Cow<'static, str>) {
    let assigned_count = get_assigned_partition_count(&managers);

    if assigned_count == 0 {
        // No partitions assigned yet - service is not ready
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Cow::Borrowed("no partitions assigned"),
        )
    } else {
        // Partitions assigned - service is ready
        (
            StatusCode::OK,
            Cow::Owned(format!("{assigned_count} partitions assigned")),
        )
    }
}

/// Handles liveness probe requests.
///
/// A consumer is considered live when neither the poll loop nor any partition
/// processing has stalled. Stalls indicate that the consumer is no longer
/// making progress and may need to be restarted.
///
/// # Arguments
///
/// * `State(ProbeState { managers, poll_heartbeat })` - Shared state containing
///   partition managers and heartbeat
///
/// # Returns
///
/// A tuple containing:
/// - `StatusCode::OK` (200) if no stalls are detected, or
///   `StatusCode::SERVICE_UNAVAILABLE` (503) otherwise
/// - A message describing the current stall status
async fn liveness_probe(
    State(ProbeState {
        managers,
        poll_heartbeat,
    }): State<ProbeState>,
) -> (StatusCode, &'static str) {
    if poll_heartbeat.is_stalled() || get_is_stalled(&managers) {
        // Either the poll loop or a partition has stalled
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Poll loop or one or more partitions have stalled. See logs for more details.",
        )
    } else {
        // Consumer is actively processing messages
        (StatusCode::OK, "No stalled components.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    #[allow(clippy::expect_used)]
    #[tokio::test]
    async fn test_probe_server_endpoints_respond() {
        // Create mock components
        let managers = Arc::default();
        let heartbeat = Heartbeat::new("test", Duration::from_secs(30));

        // Create ProbeServer instance on a random port (0)
        let server =
            ProbeServer::new(0, managers, heartbeat).expect("Failed to create ProbeServer");

        let address = server.local_addr();

        // Create an HTTP client for testing
        let client = Client::new();

        // Give the server a moment to start up
        sleep(Duration::from_millis(100)).await;

        // Verify both endpoints respond
        let readyz_result = check_endpoint(&client, address, "/readyz").await;
        let livez_result = check_endpoint(&client, address, "/livez").await;

        // Shutdown the server
        server.shutdown().await;

        // Assert after shutdown to ensure we always shutdown even if assertions fail
        assert!(readyz_result, "Readiness probe did not respond");
        assert!(livez_result, "Liveness probe did not respond");
    }

    /// Checks if an endpoint responds to HTTP requests.
    ///
    /// # Arguments
    ///
    /// * `client` - HTTP client to use for requests
    /// * `address` - Server address to connect to
    /// * `path` - Endpoint path to test
    ///
    /// # Returns
    ///
    /// `true` if the endpoint responds within the timeout, `false` otherwise
    #[allow(clippy::print_stdout)]
    async fn check_endpoint(client: &Client, address: SocketAddr, path: &str) -> bool {
        let url = format!("http://localhost:{}{}", address.port(), path);

        // Attempt to connect with timeout
        match timeout(Duration::from_secs(5), client.get(&url).send()).await {
            Ok(Ok(_)) => true,
            Ok(Err(e)) => {
                println!("Error sending request to {path}: {e:?}");
                false
            }
            Err(_) => {
                println!("Timeout sending request to {path}");
                false
            }
        }
    }
}
