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

use crate::consumer::Keyed;
use crate::consumer::partition::ShutdownPhase;
use crate::consumer::partition::util::WithValue;
use crate::heartbeat::Heartbeat;
use ahash::RandomState;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt, pin_mut};
use nohash_hasher::{IntMap, IntSet};
use tokio::select;
use tokio::sync::watch;
use tokio::task::coop::{Coop, cooperative};
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
/// by the bounded `mpsc` channel in [`super::PartitionManager`] — messages are
/// only pulled into `KeyManager` after they have been accepted from that
/// channel.
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

    /// Processes incoming messages from a stream until draining is signaled or
    /// the stream ends, then waits for in-flight tasks to complete.
    ///
    /// # Arguments
    ///
    /// * `messages` - A stream of incoming messages to process
    /// * `heartbeat` - A heartbeat used to detect stalled processing
    /// * `shutdown_rx` - Receiver for [`ShutdownPhase`] transitions; the main
    ///   loop exits at `Draining` and the drain loop exits at `Terminating`
    pub async fn process_messages<S>(
        mut self,
        messages: S,
        heartbeat: Heartbeat,
        mut shutdown_rx: watch::Receiver<ShutdownPhase>,
    ) where
        S: Stream<Item = M>,
        M: Keyed + Debug,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        pin_mut!(messages);
        let shutdown_rx = &mut shutdown_rx;

        // Main processing loop. Biased so the drain check always wins when
        // ready; other arms only fire while drain has not yet been signaled.
        loop {
            heartbeat.beat();

            select! {
                biased;

                // Exit as soon as draining begins
                _ = shutdown_rx.wait_for(|v| *v >= ShutdownPhase::Draining) => break,

                // Process the next available message from the executing queue
                Some(hash_value) = self.executing.next() => {
                    self.handle_completion(hash_value);
                }

                // Fetch and process the next message from the input stream
                maybe_message = messages.next() => match maybe_message {
                    None => break, // Stream ended
                    Some(message) => self.dispatch_message(message),
                },

                // Ensure heartbeat is recorded even if nothing happens
                () = heartbeat.next() => {}
            }
        }

        // Drain in-flight tasks. The Terminating phase (sent after the full
        // shutdown_timeout) acts as the hard deadline; completing all tasks
        // naturally exits first via the None arm.
        loop {
            heartbeat.beat();

            select! {
                _ = shutdown_rx.wait_for(|v| *v >= ShutdownPhase::Terminating) => {
                    let count = self.executing.len();
                    warn!("shutdown timeout reached with {count} tasks in progress");
                    break;
                }

                // Exit when all in-flight tasks complete; remaining enqueued
                // messages are dropped with self
                option = self.executing.next() => {
                    if option.is_none() {
                        break;
                    }
                }

                // Ensure heartbeat is recorded
                () = heartbeat.next() => {}
            }
        }
    }

    /// Dispatches a message for processing based on its key.
    ///
    /// Called only from the main loop while draining has not yet been
    /// signaled (biased select guarantees this).
    ///
    /// # Arguments
    ///
    /// * `message` - The message to dispatch
    #[instrument(level = "debug", skip(self))]
    fn dispatch_message(&mut self, message: M)
    where
        M: Keyed + Debug,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        let key = message.key();
        let hash_value = self.hash_state.hash_one(key);

        debug!(hash_value, "dispatching message");

        if self.busy.contains(&hash_value) {
            self.enqueue_message(message, hash_value);
        } else {
            self.process_message(message, hash_value);
        }
    }

    /// Handles the completion of message processing for a key.
    ///
    /// Called only from the main loop while draining has not yet been
    /// signaled (biased select guarantees this).
    ///
    /// # Arguments
    ///
    /// * `hash_value` - The hash value of the key whose processing completed
    #[instrument(level = "debug", skip(self))]
    fn handle_completion(&mut self, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
        M: Debug,
    {
        debug!(hash_value, "handling completion");

        self.busy.remove(&hash_value);

        let Entry::Occupied(mut occupied) = self.enqueued.entry(hash_value) else {
            return;
        };

        let Some(message) = occupied.get_mut().pop_front() else {
            occupied.remove();
            return;
        };

        self.process_message(message, hash_value);
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

    /// Starts processing a message.
    ///
    /// Called only while draining has not yet been signaled — no drain check
    /// needed.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to process
    /// * `hash_value` - The hash value of the message's key
    #[instrument(level = "debug", skip(self))]
    fn process_message(&mut self, message: M, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
        M: Debug,
    {
        debug!(?message, hash_value, "processing message");

        let future = cooperative(WithValue::new(hash_value, (self.process)(message)));

        self.busy.insert(hash_value);
        self.executing.push(future);
    }
}
