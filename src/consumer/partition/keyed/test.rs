//! Unit tests for the `KeyManager` struct in the consumer partition keyed
//! module.
//!
//! This module contains QuickCheck-based property tests to verify the correct
//! functioning of the `KeyManager`, focusing on concurrent execution prevention
//! and complete message processing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::consumer::Keyed;
use crate::consumer::partition::keyed::KeyManager;
use crate::heartbeat::Heartbeat;
use crate::test_util::TEST_RUNTIME;
use ahash::HashMapExt;
use futures::stream::iter;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use scc::{HashMap, HashSet};
use tokio::sync::watch;
use tokio::time::sleep;

/// A sequence of messages with keys and values for testing.
#[derive(Clone, Debug)]
struct Messages(Vec<(u8, u16)>);

/// A wrapper for a vector of u8 values used in `QuickCheck` tests.
#[derive(Clone, Debug)]
struct SimpleMessages(Vec<u8>);

/// Verifies that `KeyManager` prevents concurrent execution of messages with
/// the same key.
///
/// # Arguments
///
/// * `messages` - A `SimpleMessages` instance containing the test messages.
///
/// # Returns
///
/// A `TestResult` indicating whether the test passed or failed.
#[quickcheck]
fn prevents_concurrent_key_execution(messages: SimpleMessages) -> TestResult {
    TEST_RUNTIME.block_on(prevents_concurrent_key_execution_impl(messages))
}

/// Verifies that `KeyManager` processes messages for each key in order.
///
/// # Arguments
///
/// * `messages` - A `Messages` instance containing the test messages.
///
/// # Returns
///
/// A `TestResult` indicating whether the test passed or failed.
#[quickcheck]
fn processes_messages_in_order(messages: Messages) -> TestResult {
    TEST_RUNTIME.block_on(processes_messages_in_order_impl(messages))
}

/// Implements the test for preventing concurrent execution of messages with the
/// same key.
///
/// # Arguments
///
/// * `messages` - A `SimpleMessages` instance containing the test messages.
///
/// # Returns
///
/// A `TestResult` indicating whether the test passed or failed.
async fn prevents_concurrent_key_execution_impl(
    SimpleMessages(messages): SimpleMessages,
) -> TestResult {
    let failed = Arc::new(AtomicBool::new(false));
    let active_keys = Arc::new(HashSet::with_capacity(messages.len()));
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    // Define the message processing function
    let process_fn = |key: u8| {
        let failed = failed.clone();
        let active_keys = active_keys.clone();

        async move {
            // Attempt to insert the key into the active set
            if active_keys.insert_async(key).await.is_ok() {
                // Simulate processing time
                sleep(Duration::from_micros(key.saturating_mul(100).into())).await;
                active_keys.remove_async(&key).await;
            } else {
                // If insertion fails, it means the key is already being processed
                failed.store(true, Ordering::Release);
            }
        }
    };

    // Process all messages using the KeyManager
    KeyManager::new(process_fn)
        .process_messages(
            iter(messages),
            Heartbeat::new("test", Duration::from_secs(30)),
            shutdown_rx,
            Duration::from_millis(100),
        )
        .await;

    // Check if any concurrent execution was detected
    if failed.load(Ordering::Acquire) {
        TestResult::failed()
    } else {
        TestResult::passed()
    }
}

/// Implements the test for verifying that messages for each key are processed
/// in order.
///
/// # Arguments
///
/// * `messages` - A `Messages` instance containing the test messages.
///
/// # Returns
///
/// A `TestResult` indicating whether the test passed or failed.
async fn processes_messages_in_order_impl(Messages(messages): Messages) -> TestResult {
    let processed: Arc<HashMap<u8, Vec<u16>>> = Arc::new(HashMap::new());
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    KeyManager::new(|(key, value)| {
        let processed = processed.clone();
        async move {
            processed.entry_async(key).await.or_default().push(value);
        }
    })
    .process_messages(
        iter(messages.clone()),
        Heartbeat::new("test", Duration::from_secs(30)),
        shutdown_rx,
        Duration::from_millis(100),
    )
    .await;

    let mut expected = ahash::HashMap::with_capacity(messages.len());
    for (key, value) in messages {
        expected.entry(key).or_insert_with(Vec::new).push(value);
    }

    for (key, expected_values) in expected {
        let Some(processed_values) = processed.get_sync(&key) else {
            return TestResult::failed();
        };

        if processed_values.get() != &expected_values {
            return TestResult::failed();
        }
    }

    TestResult::passed()
}

impl Arbitrary for Messages {
    /// Generates an arbitrary `Messages` instance for `QuickCheck` tests.
    ///
    /// # Arguments
    ///
    /// * `g` - A mutable reference to a `Gen` instance for random generation.
    fn arbitrary(g: &mut Gen) -> Self {
        let count = g.size();
        let messages = (0..count)
            .map(|_| (u8::arbitrary(g), u16::arbitrary(g)))
            .collect();
        Self(messages)
    }
}

impl Arbitrary for SimpleMessages {
    /// Generates an arbitrary `SimpleMessages` instance for `QuickCheck` tests.
    ///
    /// # Arguments
    ///
    /// * `g` - A mutable reference to a `Gen` instance for random generation.
    fn arbitrary(g: &mut Gen) -> Self {
        let mut messages: Vec<u8> = Vec::arbitrary(g);
        for message in &mut messages {
            *message = *g.choose(&[1, 2, 3, 4]).unwrap_or(&1);
        }
        Self(messages)
    }
}

impl Keyed for (u8, u16) {
    type Key = u8;

    fn key(&self) -> &Self::Key {
        &self.0
    }
}

impl Keyed for u8 {
    type Key = u8;

    fn key(&self) -> &Self::Key {
        self
    }
}
