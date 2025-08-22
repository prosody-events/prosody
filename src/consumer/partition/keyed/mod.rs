//! Manages and processes messages keyed by hash values.
//!
//! This module implements a concurrent processing system that ensures messages
//! with the same key are processed in order while allowing concurrent
//! processing of messages with different keys.
//!
//! The key features of this module include:
//!
//! - Processing messages with the same key sequentially to maintain order
//! - Processing messages with different keys concurrently to maximize
//!   throughput
//! - Managing backpressure through configurable queue size limits per key
//! - Graceful shutdown with timeout for in-flight operations
//! - Heartbeat monitoring to detect stalled processing

use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::time::Duration;

use crate::consumer::Keyed;
use crate::consumer::partition::util::WithValue;
use crate::heartbeat::Heartbeat;
use ahash::RandomState;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt, pin_mut};
use nohash_hasher::{IntMap, IntSet};
use tokio::select;
use tokio::sync::watch;
use tokio::task::coop::{Coop, cooperative};
use tokio::time::sleep;
use tracing::warn;
use tracing::{debug, instrument};

#[cfg(test)]
mod test;

/// A hash value used for keying messages.
type HashValue = u64;

/// Manages and processes messages by key, ensuring sequential processing within
/// each key.
///
/// This manager maintains message ordering guarantees for each unique key while
/// allowing concurrent processing across different keys. It tracks which keys
/// are currently being processed, manages queues of pending messages for each
/// key, and schedules message processing tasks.
///
/// # Key Features
///
/// - Preserves message order within each key
/// - Enables concurrent processing across different keys
/// - Enforces per-key backpressure through configurable queue limits
/// - Provides graceful shutdown with timeout for in-flight operations
pub struct KeyManager<M, F, Fut>
where
    Fut: Future,
{
    /// Maximum number of messages allowed to be queued per key
    max_enqueued_per_key: usize,

    /// Function that processes a message and returns a future
    process: F,

    /// Currently executing futures with their associated hash values
    executing: FuturesUnordered<Coop<WithValue<HashValue, Fut>>>,

    /// Set of keys that are currently being processed
    busy: IntSet<HashValue>,

    /// Queues of pending messages, indexed by key hash
    enqueued: IntMap<HashValue, VecDeque<M>>,

    /// Hash function state for computing key hashes
    hash_state: RandomState,
}

impl<M, F, Fut> KeyManager<M, F, Fut>
where
    Fut: Future,
{
    /// Creates a new `KeyManager` with the specified processing function and
    /// queue limits.
    ///
    /// # Arguments
    ///
    /// * `process` - A function that processes a message and returns a future
    /// * `max_enqueued_per_key` - The maximum number of messages that can be
    ///   queued per key
    ///
    /// # Returns
    ///
    /// A new `KeyManager` instance
    ///
    /// # Panics
    ///
    /// Panics if `max_enqueued_per_key` is zero, as it would prevent message
    /// processing
    pub fn new(process: F, max_enqueued_per_key: usize) -> Self {
        debug_assert!(
            max_enqueued_per_key > 0,
            "max_enqueued_per_key cannot be zero"
        );

        Self {
            max_enqueued_per_key,
            process,
            executing: FuturesUnordered::default(),
            busy: IntSet::default(),
            enqueued: IntMap::default(),
            hash_state: RandomState::default(),
        }
    }

    /// Processes incoming messages from a stream until shutdown is signaled.
    ///
    /// This is the main processing loop that:
    /// - Consumes messages from the input stream
    /// - Dispatches messages to the appropriate key queue
    /// - Schedules message processing based on availability
    /// - Handles completion of processed messages
    /// - Manages graceful shutdown when requested
    ///
    /// # Arguments
    ///
    /// * `messages` - A stream of incoming messages to process
    /// * `heartbeat` - A heartbeat used to detect stalled processing
    /// * `shutdown_rx` - A receiver for shutdown signals
    /// * `max_execution_count` - Maximum number of concurrent tasks allowed
    /// * `shutdown_timeout` - How long to wait for in-flight tasks during
    ///   shutdown
    pub async fn process_messages<S>(
        mut self,
        messages: S,
        heartbeat: Heartbeat,
        mut shutdown_rx: watch::Receiver<bool>,
        max_execution_count: usize,
        shutdown_timeout: Duration,
    ) where
        S: Stream<Item = M>,
        M: Keyed + Debug,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        debug_assert!(max_execution_count > 0, "max_concurrency cannot be zero");

        pin_mut!(messages);
        let shutdown_rx = &mut shutdown_rx;

        // Main processing loop - runs until shutdown is signaled or stream ends
        loop {
            heartbeat.beat();

            let execution_count = self.executing.len();
            if execution_count < max_execution_count {
                // Capacity available - can process new messages
                debug!(
                    %execution_count, %max_execution_count,
                    "waiting for new message or existing execution to complete"
                );
                select! {
                    // Process the next available message from the executing queue
                    Some(hash_value) = self.executing.next() => {
                        self.handle_completion(shutdown_rx, hash_value);
                    }

                    // Ensure heartbeat is recorded even if nothing happens
                    () = heartbeat.next() => {}

                    // Check for shutdown signal
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }

                    // Fetch and process the next message from the input stream
                    maybe_message = messages.next() => match maybe_message {
                        None => break, // Stream ended
                        Some(message) => self.dispatch_message(shutdown_rx, message).await,
                    },
                }
            } else {
                // At capacity - can only process completions
                debug!(
                    %execution_count, %max_execution_count,
                    "limit reached; waiting for existing executions to complete"
                );
                select! {
                    // Wait for a task to complete
                    Some(hash_value) = self.executing.next() => {
                        self.handle_completion(shutdown_rx, hash_value);
                    }

                    // Ensure heartbeat is recorded
                    () = heartbeat.next() => {}

                    // Check for shutdown signal
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        }

        // Graceful shutdown - process remaining tasks until timeout
        let deadline = sleep(shutdown_timeout);
        pin_mut!(deadline);

        loop {
            heartbeat.beat();

            select! {
                biased;

                // Exit if the shutdown deadline is reached
                () = &mut deadline => {
                    let count = self.executing.len();
                    warn!("shutdown timeout reached with {count} tasks in progress");
                    break;
                }

                // Ensure heartbeat is recorded
                () = heartbeat.next() => {}

                // Process remaining tasks
                maybe_hash_value = self.executing.next() => {
                    match maybe_hash_value {
                        None => break, // All tasks completed
                        Some(hash_value) => self.handle_completion(shutdown_rx, hash_value),
                    }
                }
            }
        }
    }

    /// Dispatches a message for processing based on its key.
    ///
    /// This function determines the hash value for the message's key and either
    /// processes it immediately or enqueues it for later processing based on
    /// whether the key is currently busy.
    ///
    /// # Arguments
    ///
    /// * `shutdown_rx` - Receiver for shutdown signals
    /// * `message` - The message to dispatch
    #[instrument(level = "debug", skip(self, shutdown_rx))]
    async fn dispatch_message(&mut self, shutdown_rx: &mut watch::Receiver<bool>, message: M)
    where
        M: Keyed + Debug,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        // Compute a hash value for the message's key
        let key = message.key();
        let hash_value = self.hash_state.hash_one(key);

        debug!(hash_value, "dispatching message");

        // If the key is busy, enqueue the message; otherwise process it immediately
        if self.busy.contains(&hash_value) {
            self.enqueue_message(shutdown_rx, message, hash_value).await;
        } else {
            self.process_message(shutdown_rx, message, hash_value);
        }
    }

    /// Handles the completion of message processing for a key.
    ///
    /// When a message is processed, this function:
    /// - Removes the key from the busy set
    /// - Checks if there are more messages queued for the key
    /// - If there are, dequeues and processes the next message
    ///
    /// # Arguments
    ///
    /// * `shutdown_rx` - Receiver for shutdown signals
    /// * `hash_value` - The hash value of the key whose processing completed
    #[instrument(level = "debug", skip(self, shutdown_rx))]
    fn handle_completion(&mut self, shutdown_rx: &mut watch::Receiver<bool>, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
        M: Debug,
    {
        debug!(hash_value, "handling completion");

        // Remove the key from the busy set
        self.busy.remove(&hash_value);

        // Check if there are more messages queued for this key
        let Entry::Occupied(mut occupied) = self.enqueued.entry(hash_value) else {
            return;
        };

        // Dequeue the next message if available
        let Some(message) = occupied.get_mut().pop_front() else {
            occupied.remove();
            return;
        };

        // Process the next message
        self.process_message(shutdown_rx, message, hash_value);
    }

    /// Enqueues a message for a key that is currently busy.
    ///
    /// If the queue for the key is at capacity, this function waits for space
    /// to become available before enqueuing the message.
    ///
    /// # Arguments
    ///
    /// * `shutdown_rx` - Receiver for shutdown signals
    /// * `message` - The message to enqueue
    /// * `hash_value` - The hash value of the message's key
    #[instrument(level = "debug", skip(self, shutdown_rx))]
    async fn enqueue_message(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
        message: M,
        hash_value: HashValue,
    ) where
        F: FnMut(M) -> Fut,
        Fut: Future,
        M: Debug,
    {
        debug!(?message, hash_value, "enqueuing message");

        // Check the current queue length
        let queue_len = self
            .enqueued
            .get(&hash_value)
            .map(VecDeque::len)
            .unwrap_or_default();

        // Wait for space in the queue if it's full
        if queue_len >= self.max_enqueued_per_key {
            debug!("queue {hash_value} is full with {queue_len} items; processing paused");

            // Wait for any task to complete, looking for our hash value
            while let Some(value) = self.executing.next().await {
                self.handle_completion(shutdown_rx, value);

                // If the completed task was for our key, we now have space
                if value == hash_value {
                    debug!("queue {hash_value} is no longer full");
                    break;
                }
            }
        }

        // Add the message to the queue for this key
        self.enqueued
            .entry(hash_value)
            .or_default()
            .push_back(message);
    }

    /// Processes a message immediately or aborts if shutdown is in progress.
    ///
    /// This function:
    /// - Checks if shutdown is in progress
    /// - If not, creates a future for processing the message
    /// - Marks the key as busy
    /// - Adds the future to the execution queue
    ///
    /// # Arguments
    ///
    /// * `shutdown_rx` - Receiver for shutdown signals
    /// * `message` - The message to process
    /// * `hash_value` - The hash value of the message's key
    #[instrument(level = "debug", skip(self, shutdown_rx))]
    fn process_message(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
        message: M,
        hash_value: HashValue,
    ) where
        F: FnMut(M) -> Fut,
        M: Debug,
    {
        debug!(?message, hash_value, "processing message");

        // Check if shutdown is in progress
        if *shutdown_rx.borrow() {
            debug!(?message, "aborting message processing due to shutdown");
            return;
        }

        // Create a future for processing the message
        let future = cooperative(WithValue::new(hash_value, (self.process)(message)));

        // Mark the key as busy and add the future to the execution queue
        self.busy.insert(hash_value);
        self.executing.push(future);
    }
}
