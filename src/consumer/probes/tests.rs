use super::*;
use color_eyre::Result;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn test_probe_server_endpoints_respond() -> Result<()> {
    // Create mock components
    let managers: Arc<Managers<serde_json::Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::test();

    // Create ProbeServer instance on a random port (0)
    let server = ProbeServer::new(0, managers, heartbeats)?;

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

    Ok(())
}

/// Checks if an endpoint responds to HTTP requests within the timeout.
async fn check_endpoint(client: &Client, address: SocketAddr, path: &str) -> bool {
    let url = format!("http://localhost:{}{}", address.port(), path);

    // Attempt to connect with timeout
    match timeout(Duration::from_secs(5), client.get(&url).send()).await {
        Ok(Ok(_)) => true,
        Ok(Err(e)) => {
            error!("Error sending request to {path}: {e:?}");
            false
        }
        Err(_) => {
            error!("Timeout sending request to {path}");
            false
        }
    }
}
