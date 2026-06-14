//! Boundary tests for the fjall Value cache decode path.
//!
//! The flagship is the **read-path uniqueness invariant**: a present cell read
//! back from the fjall decode path is uniquely owned
//! (`try_into_mut().is_ok()`). This pins the production fast path
//! `StateHandle::get` relies on — the fjall cache decode mints a fresh `Bytes`,
//! so the read parses in place with zero copy — and guards against a future
//! layer re-introducing a shared clone that would silently demote the read to
//! the copying fallback.

use super::FjallValueStore;
use crate::Key;
use crate::state::value::ValueStore;
use crate::state::{CollectionId, Read, StateKey, StateName, StateType, ValueKind};
use crate::test_util::TEST_RUNTIME;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use fjall::{Config, PartitionCreateOptions};
use quickcheck::{QuickCheck, TestResult};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

/// Opens a fresh tempdir-backed cache partition and wraps it in a store. The
/// returned `TempDir` keeps the keyspace alive for the store's lifetime.
fn setup() -> Result<(TempDir, FjallValueStore)> {
    let dir = tempfile::tempdir()?;
    let keyspace = Config::new(dir.path()).open()?;
    let partition = keyspace.open_partition("value_cache", PartitionCreateOptions::default())?;
    Ok((dir, FjallValueStore::new(partition)))
}

fn collection(name: &str) -> Result<CollectionId<ValueKind>> {
    let key: Key = Arc::from("k");
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), key),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// Read-path uniqueness invariant over the fjall cache: a present cell read
/// back from the decode path is uniquely owned, across random non-empty
/// payloads.
#[test]
fn prop_fjall_present_cell_is_uniquely_owned() {
    async fn check(payload: Vec<u8>) -> Result<bool> {
        let (_dir, store) = setup()?;
        let c = collection("uniq")?;
        store.set(&c, Bytes::from(payload)).await?;
        let Read::Present(bytes) = store.get(&c).await? else {
            return Err(eyre!("expected present cell"));
        };
        Ok(bytes.try_into_mut().is_ok())
    }

    fn prop(payload: Vec<u8>) -> TestResult {
        if payload.is_empty() {
            return TestResult::discard();
        }
        match TEST_RUNTIME.block_on(check(payload)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("present cell was a shared clone, not uniquely owned"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u8>) -> TestResult);
}
