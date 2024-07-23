use std::thread::{spawn, JoinHandle};

use educe::Educe;
use rdkafka::error::KafkaError;
use rdkafka::mocking::MockCluster as KafkaMock;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

#[derive(Educe)]
#[educe(Debug)]
pub struct MockCluster {
    bootstrap_servers: String,

    #[educe(Debug(ignore))]
    shutdown: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl MockCluster {
    pub async fn new(broker_count: u8) -> Result<Self, MockError> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();

        let handle = spawn(move || match KafkaMock::new(i32::from(broker_count)) {
            Ok(cluster) => {
                let _ = result_tx.send(Ok(cluster.bootstrap_servers()));
                let _ = shutdown_rx.blocking_recv();
            }
            Err(error) => {
                let _ = result_tx.send(Err(error));
            }
        });

        Ok(Self {
            bootstrap_servers: result_rx.await??,
            shutdown: Some((shutdown_tx, handle)),
        })
    }

    #[must_use]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
}

impl Drop for MockCluster {
    fn drop(&mut self) {
        let Some((shutdown_tx, handle)) = self.shutdown.take() else {
            return;
        };

        let _ = shutdown_tx.send(());
        let _ = handle.join();
    }
}

#[derive(Debug, Error)]
pub enum MockError {
    #[error("mock cluster Kafka failure: {0:#}")]
    Kafka(#[from] KafkaError),

    #[error("mock cluster initialization failure: {0:#}")]
    Initialization(#[from] RecvError),
}
