use crate::consumer::{get_assigned_partition_count, get_is_stalled, Managers};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use axum_extra::routing::RouterExt;
use educe::Educe;
use std::borrow::Cow;
use std::io;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

#[derive(Educe)]
#[educe(Debug)]
pub struct ProbeServer {
    #[educe(Debug(ignore))]
    shutdown_tx: oneshot::Sender<()>,

    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl ProbeServer {
    pub fn new(port: u16, managers: Arc<Managers>) -> Result<Self, io::Error> {
        let app = Router::new()
            .route_with_tsr("/readyz", get(readiness_probe))
            .route_with_tsr("/livez", get(liveness_probe))
            .with_state(managers);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let listener = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, port))?;
        let listener = TcpListener::from_std(listener)?;
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
            shutdown_tx,
            handle,
        })
    }

    pub async fn shutdown(self) {
        debug!("shutting down probe server");
        let _ = self.shutdown_tx.send(());
        let _ = self.handle.await;
        info!("probe server shutdown");
    }
}

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
