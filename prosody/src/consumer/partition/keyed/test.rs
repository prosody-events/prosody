use std::cmp::max;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::iter;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use scc::{HashMap, HashSet};
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::time::sleep;

use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::Keyed;

#[derive(Clone, Debug)]
struct SimpleMessages(Vec<u8>);

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

#[quickcheck]
fn processes_all_messages(messages: Vec<u8>, max_enqueued: u8) -> TestResult {
    let Ok(runtime) = Builder::new_multi_thread().enable_time().build() else {
        return TestResult::error("failed to initialize runtime");
    };

    runtime.block_on(processes_all_messages_impl(messages, max_enqueued))
}

async fn prevents_concurrent_key_execution_impl(
    SimpleMessages(messages): SimpleMessages,
    max_enqueued: u8,
) -> TestResult {
    let max_enqueued = max(max_enqueued as usize, 1);
    let failed = Arc::new(AtomicBool::new(false));
    let active_keys = Arc::new(HashSet::with_capacity(messages.len()));

    let process_fn = |key: u8, _: watch::Receiver<bool>| {
        let failed = failed.clone();
        let active_keys = active_keys.clone();

        async move {
            if active_keys.insert_async(key).await.is_ok() {
                sleep(Duration::from_micros(key.saturating_mul(100).into())).await;
                active_keys.remove_async(&key).await;
            } else {
                failed.store(true, Ordering::Release);
            }
        }
    };

    KeyManager::new(process_fn, max_enqueued)
        .process_messages(iter(messages), Some(Duration::from_millis(100)))
        .await;

    if failed.load(Ordering::Acquire) {
        TestResult::failed()
    } else {
        TestResult::passed()
    }
}

async fn processes_all_messages_impl(messages: Vec<u8>, max_enqueued: u8) -> TestResult {
    let max_enqueued = max(max_enqueued as usize, 1);
    let expected: HashMap<u8, usize> = HashMap::new();
    let actual: Arc<HashMap<u8, usize>> = Arc::default();

    for message in &messages {
        *expected.entry_async(*message).await.or_default().get_mut() += 1;
    }

    let process_fn = |key: u8, _: watch::Receiver<bool>| {
        let processed = actual.clone();
        async move {
            *processed.entry_async(key).await.or_default().get_mut() += 1;
        }
    };

    KeyManager::new(process_fn, max_enqueued)
        .process_messages(iter(messages), Some(Duration::from_millis(100)))
        .await;

    if &expected == actual.as_ref() {
        TestResult::passed()
    } else {
        TestResult::failed()
    }
}

impl Arbitrary for SimpleMessages {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut messages: Vec<u8> = Vec::arbitrary(g);
        for message in &mut messages {
            *message = *g.choose(&[1, 2, 3, 4]).unwrap_or(&1);
        }
        Self(messages)
    }
}

impl Keyed for u8 {
    type Key = u8;

    fn key(&self) -> &Self::Key {
        self
    }
}
