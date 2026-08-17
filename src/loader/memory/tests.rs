use super::*;
use crate::consumer::Keyed;
use color_eyre::Result;
use serde_json::json;

#[tokio::test]
async fn test_store_and_load() -> Result<()> {
    let loader = MemoryLoader::new();
    let topic = Topic::from("test-topic");
    let key = Key::from("test-key");
    let payload = json!({"key": "value", "num": 42_i32});

    loader.store_message(topic, 0_i32, 100_i64, key.clone(), payload.clone());

    let message = loader.load_message(topic, 0_i32, 100_i64).await?;
    assert_eq!(message.offset(), 100_i64);
    assert_eq!(message.partition(), 0_i32);
    assert_eq!(message.key(), &key);
    assert_eq!(message.record().message(), Some(&payload));
    Ok(())
}

#[tokio::test]
async fn test_not_found() {
    let loader: MemoryLoader<serde_json::Value> = MemoryLoader::new();
    let topic = Topic::from("test-topic");

    let result = loader.load_message(topic, 0, 100).await;
    assert!(matches!(result, Err(MemoryLoaderError::NotFound(..))));
}

#[tokio::test]
async fn test_remove_message() {
    let loader = MemoryLoader::new();
    let topic = Topic::from("test-topic");
    let key = Key::from("test-key");
    let payload = json!({"key": "value"});

    loader.store_message(topic, 0, 100, key, payload);
    assert_eq!(loader.len(), 1);

    loader.remove_message(topic, 0, 100);
    assert_eq!(loader.len(), 0);

    let result = loader.load_message(topic, 0, 100).await;
    assert!(matches!(result, Err(MemoryLoaderError::NotFound(..))));
}

#[tokio::test]
async fn test_clear() {
    let loader = MemoryLoader::new();
    let topic = Topic::from("test-topic");
    let key = Key::from("test-key");

    loader.store_message(topic, 0_i32, 100_i64, key.clone(), json!({"a": 1_i32}));
    loader.store_message(topic, 0_i32, 101_i64, key, json!({"b": 2_i32}));
    assert_eq!(loader.len(), 2_usize);

    loader.clear();
    assert_eq!(loader.len(), 0_usize);
    assert!(loader.is_empty());
}

#[tokio::test]
async fn test_clone_shares_storage() -> Result<()> {
    let loader1 = MemoryLoader::new();
    let topic = Topic::from("test-topic");
    let key = Key::from("test-key");
    let payload = json!({"shared": true});

    loader1.store_message(topic, 0_i32, 100_i64, key, payload.clone());

    let loader2 = loader1.clone();
    let message = loader2.load_message(topic, 0_i32, 100_i64).await?;
    assert_eq!(message.record().message(), Some(&payload));
    Ok(())
}
