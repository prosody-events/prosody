//! Unit tests for the `KeyManager` struct in the consumer partition keyed
//! module.
//!
//! This module contains QuickCheck-based property tests to verify the correct
//! functioning of the `KeyManager`, focusing on concurrent execution prevention
//! and complete message processing.

use std::cmp::max;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ahash::HashMapExt;
use futures::stream::iter;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use scc::{HashMap, HashSet};
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::time::sleep;

use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::Keyed;

/// Represents a sequence of messages with keys and values for testing.
#[derive(Clone, Debug)]
struct Messages(Vec<(u8, u16)>);

/// A simple wrapper for a vector of u8 values used in `QuickCheck` tests.
#[derive(Clone, Debug)]
struct SimpleMessages(Vec<u8>);

/// Verifies that KeyManager prevents concurrent execution of messages with the
/// same key.
///
/// # Arguments
/// * `messages` - A `SimpleMessages` instance containing the test messages.
/// * `max_enqueued` - The maximum number of messages to enqueue per key.
#[quickcheck]
fn prevents_concurrent_key_execution(messages: SimpleMessages, max_enqueued: u8) -> TestResult {
    let Ok(runtime) = Builder::new_multi_thread().enable_time().build() else {
        return TestResult::error("failed to initialize runtime");
    };

    runtime.block_on(prevents_concurrent_key_execution_impl(
        messages,
        max_enqueued,
    ))
}

/// Verifies that KeyManager processes messages for each key in order.
///
/// # Arguments
/// * `messages` - A `SimpleMessages` instance containing the test messages.
/// * `max_enqueued` - The maximum number of messages to enqueue per key.
#[quickcheck]
fn processes_messages_in_order(messages: Messages, max_enqueued: u8) -> TestResult {
    let Ok(runtime) = Builder::new_multi_thread().enable_time().build() else {
        return TestResult::error("failed to initialize runtime");
    };

    runtime.block_on(processes_messages_in_order_impl(messages, max_enqueued))
}

/// Implements the test for preventing concurrent execution of messages with the
/// same key.
///
/// # Arguments
/// * `messages` - A `SimpleMessages` instance containing the test messages.
/// * `max_enqueued` - The maximum number of messages to enqueue per key.
///
/// # Returns
/// A `TestResult` indicating whether the test passed or failed.
async fn prevents_concurrent_key_execution_impl(
    SimpleMessages(messages): SimpleMessages,
    max_enqueued: u8,
) -> TestResult {
    let max_enqueued = max(max_enqueued as usize, 1);
    let failed = Arc::new(AtomicBool::new(false));
    let active_keys = Arc::new(HashSet::with_capacity(messages.len()));
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);

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
    KeyManager::new(process_fn, max_enqueued)
        .process_messages(
            iter(messages),
            shutdown_tx,
            Some(Duration::from_millis(100)),
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
/// * `messages` - A `SimpleMessages` instance containing the test messages.
/// * `max_enqueued` - The maximum number of messages to enqueue per key.
///
/// # Returns
/// A `TestResult` indicating whether the test passed or failed.
async fn processes_messages_in_order_impl(
    Messages(messages): Messages,
    max_enqueued: u8,
) -> TestResult {
    let max_enqueued = max(max_enqueued as usize, 1);
    let processed: Arc<HashMap<u8, Vec<u16>>> = Arc::new(HashMap::new());
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);

    KeyManager::new(
        |(key, value)| {
            let processed = processed.clone();
            async move {
                processed.entry_async(key).await.or_default().push(value);
            }
        },
        max_enqueued,
    )
    .process_messages(iter(messages.clone()), shutdown_tx, None)
    .await;

    let mut expected = ahash::HashMap::with_capacity(messages.len());
    for (key, value) in messages {
        expected.entry(key).or_insert_with(Vec::new).push(value);
    }

    for (key, expected_values) in expected {
        let Some(processed_values) = processed.get(&key) else {
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

    /// Returns a reference to the key, which is the u8 value itself.
    fn key(&self) -> &Self::Key {
        self
    }
}
