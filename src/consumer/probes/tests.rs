use super::*;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use reqwest::Client;
use reqwest::StatusCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// A hang guard on one probe request. It is never the assertion.
const REQUEST_GUARD: Duration = Duration::from_secs(5);

/// Both probes answer over HTTP with the verdict the shared predicates give: a
/// consumer with no partitions assigned is unready and live.
///
/// The status is the assertion, not merely that a response arrived. That is
/// what makes the one pair of functions both surfaces call checkable here.
#[tokio::test]
async fn test_probe_server_endpoints_respond() -> Result<()> {
    let managers: Arc<Managers<serde_json::Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::test();
    let server = ProbeServer::new(0, managers, heartbeats)?;
    let address = server.local_addr();
    let client = Client::new();

    // `ProbeServer::new` binds the listener before it returns, so a connection
    // to `local_addr` is accepted from here on. Nothing has to be waited for.
    let readyz = check_endpoint(&client, address, "/readyz").await;
    let livez = check_endpoint(&client, address, "/livez").await;

    // Asserted after shutdown, so the server stops however the probes answered.
    server.shutdown().await;
    ensure!(
        readyz? == StatusCode::SERVICE_UNAVAILABLE,
        "a consumer with no partitions assigned is unready"
    );
    ensure!(livez? == StatusCode::OK, "the same consumer is live");
    Ok(())
}

/// The status one endpoint answered.
async fn check_endpoint(client: &Client, address: SocketAddr, path: &str) -> Result<StatusCode> {
    let url = format!("http://localhost:{}{}", address.port(), path);
    let response = timeout(REQUEST_GUARD, client.get(&url).send()).await??;
    Ok(response.status())
}
