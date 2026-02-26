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
/// - Provides graceful shutdown with timeout for in-flight operations
///
/// # Backpressure
///
/// `KeyManager` does **not** apply any backpressure of its own. It will
/// accept messages from the input stream as fast as they arrive, queuing them
/// internally without bound. The input stream must be backpressured externally
/// to prevent unbounded memory growth. In the standard wiring, this is provided
/// by the bounded `mpsc` channel in [`super::PartitionManager`] — messages are only
/// pulled into `KeyManager` after they have been accepted from that channel.
pub struct KeyManager<M, F, Fut>
where
    Fut: Future,
{
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
    /// Creates a new `KeyManager` with the specified processing function.
    ///
    /// # Arguments
    ///
    /// * `process` - A function that processes a message and returns a future
    ///
    /// # Returns
    ///
    /// A new `KeyManager` instance
    pub fn new(process: F) -> Self {
        Self {
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
    /// * `shutdown_timeout` - How long to wait for in-flight tasks during
    ///   shutdown
    pub async fn process_messages<S>(
        mut self,
        messages: S,
        heartbeat: Heartbeat,
        mut shutdown_rx: watch::Receiver<bool>,
        shutdown_timeout: Duration,
    ) where
        S: Stream<Item = M>,
        M: Keyed + Debug,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        pin_mut!(messages);
        let shutdown_rx = &mut shutdown_rx;

        // Main processing loop - runs until shutdown is signaled or stream ends
        loop {
            heartbeat.beat();

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
                    Some(message) => self.dispatch_message(shutdown_rx, message),
                },
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
    fn dispatch_message(&mut self, shutdown_rx: &mut watch::Receiver<bool>, message: M)
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
            self.enqueue_message(message, hash_value);
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
    /// # Arguments
    ///
    /// * `message` - The message to enqueue
    /// * `hash_value` - The hash value of the message's key
    #[instrument(level = "debug", skip(self))]
    fn enqueue_message(&mut self, message: M, hash_value: HashValue)
    where
        M: Debug,
    {
        debug!(?message, hash_value, "enqueuing message");

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
