//! Provides the `KeyManager` for managing and processing messages keyed by hash
//! values.
//!
//! This module implements a concurrent processing system that ensures messages
//! with the same key are processed in order while allowing parallel processing
//! of messages with different keys.

use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::time::Duration;

use ahash::RandomState;
use futures::stream::FuturesUnordered;
use futures::{pin_mut, Stream, StreamExt};
use nohash_hasher::{IntMap, IntSet};
use tokio::select;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{error, warn};

use crate::consumer::partition::util::WithValue;
use crate::consumer::Keyed;

#[cfg(test)]
mod test;

/// Represents a hash value used for keying messages.
type HashValue = u64;

/// Manages and processes messages keyed by hash values, ensuring concurrent
/// processing constraints within the same thread.
///
/// This manager tracks the state of each key and manages a queue of messages
/// awaiting processing in a single thread, using a combination of
/// `FuturesUnordered` to execute and manage message processing concurrently. It
/// maintains several internal states like executing tasks, busy keys,
/// and queued messages, ensuring no more than the specified number of messages
/// per key are processed concurrently.
pub struct KeyManager<M, F, Fut> {
    max_enqueued_per_key: usize,
    process: F,
    executing: FuturesUnordered<WithValue<HashValue, Fut>>,
    busy: IntSet<HashValue>,
    enqueued: IntMap<HashValue, VecDeque<M>>,
    hash_state: RandomState,
}

impl<M, F, Fut> KeyManager<M, F, Fut> {
    /// Creates a new `KeyManager` instance with specified processing function
    /// and constraints, ensuring that all keys are processed in the same
    /// thread.
    ///
    /// # Arguments
    ///
    /// * `process` - A function that defines how each message is processed.
    /// * `max_enqueued_per_key` - The maximum number of messages allowed per
    ///   key before blocking further enqueue.
    ///
    /// # Returns
    ///
    /// Returns a new instance of `KeyManager`.
    ///
    /// # Panics
    ///
    /// Panics if `max_enqueued_per_key` is zero, as it would prevent any
    /// message processing.
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

    /// Processes incoming messages from a stream until shutdown.
    ///
    /// # Arguments
    ///
    /// * `messages` - Stream of incoming messages.
    /// * `shutdown_tx` - Sender for shutdown signal.
    /// * `shutdown_timeout` - Optional duration to wait before forcefully
    ///   shutting down.
    ///
    /// # Errors
    ///
    /// This function will return an error if the processing function returns an
    /// error.
    pub async fn process_messages<S>(
        mut self,
        messages: S,
        shutdown_tx: watch::Sender<bool>,
        shutdown_timeout: Option<Duration>,
    ) where
        S: Stream<Item = M>,
        M: Keyed,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        pin_mut!(messages);

        loop {
            select! {
                biased;

                // Process the next available message from the executing queue.
                Some(hash_value) = self.executing.next() => {
                    self.handle_completion(hash_value);
                }

                // Fetch the next message from the stream and handle it.
                maybe_message = messages.next() => match maybe_message {
                    None => break,
                    Some(message) => self.handle_message(message).await,
                },
            }
        }

        if let Err(error) = shutdown_tx.send(true) {
            error!("failed to send shutdown signal: {error:#}");
        }

        // Shutdown handling: proceed if a timeout is specified, otherwise immediately
        // return.
        let Some(timeout) = shutdown_timeout else {
            let count = self.executing.len();
            if count > 0 {
                warn!("shutting down with {count} tasks in progress");
            };

            return;
        };

        // Create the shutdown deadline future.
        let deadline = sleep(timeout);
        pin_mut!(deadline);

        loop {
            select! {
                biased;

                // Exit if the shutdown deadline is reached.
                () = &mut deadline => {
                    let count = self.executing.len();
                    warn!("shutdown timeout reached with {count} tasks in progress");
                    break;
                }

                // Continue processing remaining tasks.
                maybe_hash_value = self.executing.next() => {
                    match maybe_hash_value {
                        None => break,
                        Some(hash_value) => self.handle_completion(hash_value),
                    };
                }
            }
        }
    }

    /// Handles a new incoming message by determining its hash value and either
    /// queuing or directly processing it based on the current system load
    /// and key status.
    ///
    /// This function is a primary entry point for messages into the
    /// `KeyManager`. It computes the hash value for the message, checks the
    /// state of the key (busy or not), and decides whether to enqueue the
    /// message for later processing or to process it immediately.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to be handled.
    async fn handle_message(&mut self, message: M)
    where
        M: Keyed,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        // Compute a unique hash value for the message's key
        let key = message.key();
        let hash_value = self.hash_state.hash_one(key);

        // Decide whether to enqueue the message or process it immediately based on
        // key's current status
        if self.busy.contains(&hash_value) {
            self.enqueue_message(message, hash_value).await;
        } else {
            self.process_message(message, hash_value);
        }
    }

    /// Handles the removal of a key from the busy set after message processing
    /// completes.
    ///
    /// This method checks for remaining messages in the queue for the completed
    /// key and schedules them if the key is not marked busy.
    ///
    /// # Arguments
    ///
    /// * `hash_value` - The hash value of the completed message.
    fn handle_completion(&mut self, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
    {
        // Remove the completed message's hash value from the busy set
        self.busy.remove(&hash_value);

        // Check for remaining messages and process the next one if available
        let Entry::Occupied(mut occupied) = self.enqueued.entry(hash_value) else {
            return;
        };

        let Some(message) = occupied.get_mut().pop_front() else {
            occupied.remove();
            return;
        };

        self.process_message(message, hash_value);
    }

    /// Enqueues a message associated with a specific hash value into the
    /// waiting queue if the key is currently busy, otherwise it starts
    /// processing the message directly.
    ///
    /// This method is used to manage the queuing of messages for keys that are
    /// currently unable to accept new messages for processing due to
    /// concurrency limits. It ensures that messages are not dropped and
    /// are handled in a first-come, first-served basis per key.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to be enqueued.
    /// * `hash_value` - The hash value associated with the message to manage
    ///   keying and load distribution.
    async fn enqueue_message(&mut self, message: M, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        // Determine the current queue length for the hash value
        let queue_len = self
            .enqueued
            .get(&hash_value)
            .map(VecDeque::len)
            .unwrap_or_default();

        // Wait until space is available in the queue if it's full
        if queue_len >= self.max_enqueued_per_key {
            while let Some(value) = self.executing.next().await {
                self.handle_completion(value);

                if value == hash_value {
                    break;
                }
            }
        }

        // Add the message to the queue for later processing
        self.enqueued
            .entry(hash_value)
            .or_default()
            .push_back(message);
    }

    /// Enqueues a message for processing or directly processes it if the key is
    /// not currently busy.
    ///
    /// This method manages the lifecycle of a message from reception to queuing
    /// or direct processing, based on the current load and state of the
    /// related key.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to process.
    /// * `hash_value` - Hash value used to key and manage the message.
    fn process_message(&mut self, message: M, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
    {
        // Create a future for processing the message
        let future = WithValue::new(hash_value, (self.process)(message));

        // Mark the hash value as busy and add the future to the executing queue
        self.busy.insert(hash_value);
        self.executing.push(future);
    }
}
