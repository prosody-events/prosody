//! Implements health check endpoints for a Kafka consumer.
//!
//! This module provides a `ProbeServer` that serves readiness and liveness
//! probes, which are commonly used in containerized environments to determine
//! if a service is ready to accept traffic and if it's functioning correctly.

use crate::consumer::{get_assigned_partition_count, get_is_stalled, Managers};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
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

/// A server that provides health check endpoints for a Kafka consumer.
#[derive(Educe)]
#[educe(Debug)]
pub struct ProbeServer {
    address: SocketAddr,

    #[educe(Debug(ignore))]
    shutdown_tx: oneshot::Sender<()>,

    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl ProbeServer {
    /// Creates a new `ProbeServer` instance.
    ///
    /// # Arguments
    ///
    /// * `port` - The port number on which the server will listen.
    /// * `managers` - A shared reference to the Kafka partition managers.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the server fails to bind to the specified
    /// port.
    pub fn new(port: u16, managers: Arc<Managers>) -> Result<Self, io::Error> {
        let app = Router::new()
            .route_with_tsr("/readyz", get(readiness_probe))
            .route_with_tsr("/livez", get(liveness_probe))
            .with_state(managers);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let listener = block_on(async { TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await })?;
        let address = listener.local_addr()?;

        let handle = spawn(async move {
            let shutdown_signal = async move {
                let _ = shutdown_rx.await;
            };

            info!("starting probe server on {address}");
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

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Gracefully shuts down the probe server.
    pub async fn shutdown(self) {
        debug!("shutting down probe server");
        let _ = self.shutdown_tx.send(());
        let _ = self.handle.await;
        info!("probe server shutdown");
    }
}

/// Handles the readiness probe request.
///
/// This function checks if any partitions are assigned to the consumer.
///
/// # Arguments
///
/// * `State(managers)` - The shared state containing the partition managers.
///
/// # Returns
///
/// A tuple containing a `StatusCode` and a message. Returns
/// `SERVICE_UNAVAILABLE` if no partitions are assigned, otherwise returns `OK`
/// with the number of assigned partitions.
async fn readiness_probe(State(managers): State<Arc<Managers>>) -> (StatusCode, Cow<'static, str>) {
    let assigned_count = get_assigned_partition_count(&managers);
    if assigned_count == 0 {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Cow::Borrowed("no partitions assigned"),
        )
    } else {
        (
            StatusCode::OK,
            Cow::Owned(format!("{assigned_count} partitions assigned")),
        )
    }
}

/// Handles the liveness probe request.
///
/// This function checks if any partitions have stalled.
///
/// # Arguments
///
/// * `State(managers)` - The shared state containing the partition managers.
///
/// # Returns
///
/// A tuple containing a `StatusCode` and a message. Returns
/// `SERVICE_UNAVAILABLE` if any partitions have stalled, otherwise returns
/// `OK`.
async fn liveness_probe(State(managers): State<Arc<Managers>>) -> (StatusCode, &'static str) {
    if get_is_stalled(&managers) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "one or more partitions have stalled",
        )
    } else {
        (StatusCode::OK, "no stalled partitions")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_probe_server_endpoints_respond() {
        // Create a mock Managers instance
        let managers = Arc::default();

        // Create a ProbeServer instance
        let server = ProbeServer::new(0, managers).expect("Failed to create ProbeServer");
        let address = server.local_addr();

        // Create a reqwest Client
        let client = Client::new();

        // Give the server a moment to start up
        tokio::time::sleep(Duration::from_secs(1)).await;

        let readyz_result = check_endpoint(&client, address, "/readyz").await;
        let livez_result = check_endpoint(&client, address, "/livez").await;

        // Shutdown the server
        server.shutdown().await;

        // Assert after shutdown to ensure we always shutdown even if assertions fail
        assert!(readyz_result, "Readiness probe did not respond");
        assert!(livez_result, "Liveness probe did not respond");
    }

    // Helper function to check if an endpoint responds
    async fn check_endpoint(client: &Client, address: SocketAddr, path: &str) -> bool {
        let url = format!("http://localhost:{}{}", address.port(), path);
        match timeout(Duration::from_secs(5), client.get(&url).send()).await {
            Ok(Ok(_)) => true,
            Ok(Err(e)) => {
                println!("Error sending request to {}: {:?}", path, e);
                false
            }
            Err(_) => {
                println!("Timeout sending request to {}", path);
                false
            }
        }
    }
}
