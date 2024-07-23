//! A module for managing mock Kafka clusters in a thread-safe manner.
//!
//! This module provides a wrapper around the `MockCluster` from rdkafka,
//! allowing for easy creation and management of mock Kafka clusters in tests.

use std::thread::{spawn, JoinHandle};

use educe::Educe;
use rdkafka::error::KafkaError;
use rdkafka::mocking::MockCluster as KafkaMock;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

/// A wrapper around `MockCluster` from rdkafka that provides a thread-safe
/// interface for managing a mock Kafka cluster in tests.
#[derive(Educe)]
#[educe(Debug)]
pub struct MockCluster {
    bootstrap_servers: String,

    #[educe(Debug(ignore))]
    shutdown: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl MockCluster {
    /// Creates a new `MockCluster` with the specified number of brokers.
    ///
    /// This function spawns a new thread to run the `KafkaMock` and returns
    /// a `MockCluster` that can be used to interact with it.
    ///
    /// # Arguments
    ///
    /// * `broker_count` - The number of brokers to create in the mock cluster.
    ///
    /// # Errors
    ///
    /// Returns a `MockError` if:
    /// - The `KafkaMock` fails to initialize.
    /// - There's an error in communication between threads.
    pub async fn new(broker_count: u8) -> Result<Self, MockError> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();

        // Spawn a new thread to run the KafkaMock
        let handle = spawn(move || match KafkaMock::new(i32::from(broker_count)) {
            Ok(cluster) => {
                // Send the bootstrap servers and wait for shutdown signal
                let _ = result_tx.send(Ok(cluster.bootstrap_servers()));
                let _ = shutdown_rx.blocking_recv();
            }
            Err(error) => {
                // Propagate the error if KafkaMock initialization fails
                let _ = result_tx.send(Err(error));
            }
        });

        // Wait for the result from the spawned thread
        Ok(Self {
            bootstrap_servers: result_rx.await??,
            shutdown: Some((shutdown_tx, handle)),
        })
    }

    /// Returns the bootstrap servers string for the mock cluster.
    ///
    /// This string can be used to configure Kafka clients to connect to the
    /// mock cluster.
    #[must_use]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
}

impl Drop for MockCluster {
    fn drop(&mut self) {
        // Send shutdown signal and join the thread if it exists
        let Some((shutdown_tx, handle)) = self.shutdown.take() else {
            return;
        };

        let _ = shutdown_tx.send(());
        let _ = handle.join();
    }
}

/// Errors that can occur when working with `MockCluster`.
#[derive(Debug, Error)]
pub enum MockError {
    /// Indicates a Kafka-related error from the mock cluster.
    #[error("mock cluster Kafka failure: {0:#}")]
    Kafka(#[from] KafkaError),

    /// Indicates a failure in initializing the mock cluster.
    #[error("mock cluster initialization failure: {0:#}")]
    Initialization(#[from] RecvError),
}
