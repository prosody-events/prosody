use super::*;
use crate::Key;
use crate::consumer::middleware::defer::segment::MemorySegmentStore;
use crate::test_util::TEST_RUNTIME;
// `QuickCheck` and `TestResult` arrive with the `defer_store_tests!` suite
// invoked at the end of this file.
use color_eyre::eyre::{ensure, eyre};

#[tokio::test]
async fn test_concurrent_access() -> color_eyre::Result<()> {
    let store = MemoryMessageDeferStore::new();

    let key1: Key = Arc::from("test-key-1");
    let key2: Key = Arc::from("test-key-2");

    let store_clone = store.clone();
    let k1 = key1.clone();
    let handle1 = tokio::spawn(async move {
        store_clone
            .defer_first_message(&k1, Offset::from(1_i64))
            .await
    });

    let store_clone = store.clone();
    let k2 = key2.clone();
    let handle2 = tokio::spawn(async move {
        store_clone
            .defer_first_message(&k2, Offset::from(2_i64))
            .await
    });

    assert!(handle1.await.is_ok());
    assert!(handle2.await.is_ok());

    let result1 = store.get_next_deferred_message(&key1).await?;
    assert_eq!(result1, Some((Offset::from(1_i64), 0)));

    let result2 = store.get_next_deferred_message(&key2).await?;
    assert_eq!(result2, Some((Offset::from(2_i64), 0)));

    Ok(())
}

// Property-based tests using model equivalence
defer_store_tests!(async { Ok::<_, color_eyre::Report>(MemoryMessageDeferStore::new()) });

/// Two stores minted by one provider for one segment share the durable
/// substrate, and a store minted for another segment never sees those rows.
///
/// A fresh map per `create_store` would make every durable row vanish with the
/// store that wrote it. A map keyed by the key alone would merge two
/// partitions.
#[test]
fn prop_provider_shares_one_substrate_per_segment() {
    fn property(name: String, offsets: Vec<i64>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async move {
            let provider = MemoryMessageDeferStoreProvider::new(MemorySegmentStore::new());
            let topic = Topic::from("substrate");
            let writer = provider.create_store(topic, 0, "group", 0);
            let reader = provider.create_store(topic, 0, "group", 0);
            let other = provider.create_store(topic, 1, "group", 0);

            let key: Key = Arc::from(format!("substrate-{name}"));
            // The queue always holds a head, so the read below is a real claim.
            let mut queue: BTreeSet<Offset> = offsets.into_iter().collect();
            queue.insert(0);

            let mut queued = queue.iter().copied();
            let first = queued.next().ok_or_else(|| eyre!("queue is empty"))?;
            writer.defer_first_message(&key, first).await?;
            for offset in queued {
                writer.defer_additional_message(&key, offset).await?;
            }

            ensure!(
                reader.get_next_deferred_message(&key).await? == Some((first, 0)),
                "a second store on the same segment must read the writer's queue"
            );
            ensure!(
                other.get_next_deferred_message(&key).await?.is_none(),
                "a store on another segment must not read the writer's queue"
            );
            Ok(())
        }))
    }

    QuickCheck::new().quickcheck(property as fn(String, Vec<i64>) -> TestResult);
}

/// A store or setup failure is a broken environment, never a shrinkable
/// property failure.
fn finish(result: color_eyre::Result<()>) -> TestResult {
    match result {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}
