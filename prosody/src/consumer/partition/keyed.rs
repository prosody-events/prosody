use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;

use ahash::RandomState;
use futures::{pin_mut, Stream, StreamExt};
use futures::stream::FuturesUnordered;
use nohash_hasher::{IntMap, IntSet};
use tokio::select;

use crate::consumer::Keyed;
use crate::consumer::partition::util::WithValue;

type HashValue = u64;

struct KeyManager<M, F, Fut> {
    max_enqueued: usize,
    process: F,
    executing: FuturesUnordered<WithValue<HashValue, Fut>>,
    busy: IntSet<HashValue>,
    enqueued: IntMap<HashValue, VecDeque<M>>,
    hash_state: RandomState,
}

impl<M, F, Fut> KeyManager<M, F, Fut> {
    pub fn new(process: F, max_enqueued: usize) -> Self {
        debug_assert!(max_enqueued > 0);

        Self {
            max_enqueued,
            process,
            executing: FuturesUnordered::default(),
            busy: IntSet::default(),
            enqueued: IntMap::default(),
            hash_state: RandomState::default(),
        }
    }

    pub async fn process_messages<S>(&mut self, messages: S)
    where
        S: Stream<Item = M>,
        M: Keyed,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        pin_mut!(messages);

        loop {
            select! {
                maybe_message = messages.next() => match maybe_message {
                    None => break,
                    Some(message) => self.handle_message(message).await,
                },

                Some(hash_value) = self.executing.next() => {
                    self.handle_completion(hash_value);
                }
            }
        }
    }

    async fn handle_message(&mut self, message: M)
    where
        M: Keyed,
        M::Key: Hash,
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        let key = message.key();
        let hash_value = self.hash_state.hash_one(key);

        if self.busy.contains(&hash_value) {
            self.enqueue_message(message, hash_value).await;
        } else {
            self.process_message(message, hash_value);
        }
    }

    fn handle_completion(&mut self, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
    {
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

    async fn enqueue_message(&mut self, message: M, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
        Fut: Future,
    {
        let queue_len = self
            .enqueued
            .get(&hash_value)
            .map(VecDeque::len)
            .unwrap_or_default();

        if queue_len >= self.max_enqueued {
            while let Some(value) = self.executing.next().await {
                self.handle_completion(value);

                if value == hash_value {
                    break;
                }
            }
        }

        self.enqueued
            .entry(hash_value)
            .or_default()
            .push_back(message);
    }

    fn process_message(&mut self, message: M, hash_value: HashValue)
    where
        F: FnMut(M) -> Fut,
    {
        let future = WithValue::new(hash_value, (self.process)(message));
        self.busy.insert(hash_value);
        self.executing.push(future);
    }
}
