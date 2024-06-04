use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::stream::iter;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use scc::HashSet;
use tokio::runtime::Builder;
use tokio::time::sleep;

use crate::consumer::Keyed;
use crate::consumer::partition::keyed::KeyManager;

#[derive(Clone, Debug)]
struct Messages(Vec<u8>);

#[quickcheck]
fn prevents_concurrent_key_execution(messages: Messages) -> TestResult {
    Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(prevents_concurrent_key_execution_impl(messages))
}

async fn prevents_concurrent_key_execution_impl(Messages(messages): Messages) -> TestResult {
    let failed = Arc::new(AtomicBool::new(false));
    let active_keys = Arc::new(HashSet::new());

    let process_fn = |key: u8| {
        let failed = failed.clone();
        let active_keys = active_keys.clone();

        async move {
            if active_keys.insert_async(key).await.is_ok() {
                sleep(Duration::from_micros(key.saturating_mul(100) as u64)).await;
                active_keys.remove_async(&key).await;
            } else {
                failed.store(true, Ordering::Release);
            }
        }
    };

    KeyManager::new(process_fn, usize::MAX)
        .process_messages(iter(messages), Some(Duration::from_millis(100)))
        .await;

    if failed.load(Ordering::Acquire) {
        TestResult::failed()
    } else {
        TestResult::passed()
    }
}

impl Arbitrary for Messages {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut messages: Vec<u8> = Vec::arbitrary(g);
        for message in messages.iter_mut() {
            *message = *g.choose(&[1, 2, 3, 4]).unwrap();
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
