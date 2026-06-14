//! Live-cluster round-trip tests for [`CassandraCellStore`].
//!
//! These run against the local Cassandra node and the shared `prosody_test`
//! keyspace (migrated on [`CassandraStore::new`]). They exercise the part the
//! pure decoder test cannot: `prepare`/`bind`/round-trip of every cell
//! statement, including the promote-of-clear residue read back live. Each test
//! mints a fresh `segment_id` so rows never collide across runs.

use super::{CassandraCellStore, CellQueries};
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::state::cell::{Cell, Committed, ProvisionalWrite};
use crate::state::store::CellStore;
use crate::state::value::ValueKind;
use crate::state::{CollectionId, CollectionRef, EventRef, StateKey, StateName, StateType};
use crate::tracing::init_test_logging;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const TEST_KEYSPACE: &str = "prosody_test";

/// Property-test iteration count for live-backend runs (default 25), from
/// `INTEGRATION_TESTS`. CI cranks it up; dev loops stay fast.
fn get_test_count() -> u64 {
    use std::env;
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25)
}

async fn setup() -> Result<CassandraCellStore> {
    let config = CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: TEST_KEYSPACE.to_owned(),
        user: None,
        password: None,
        retention: Duration::from_mins(10),
    };
    let cassandra = CassandraStore::new(&config).await?;
    let queries = Arc::new(CellQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(CassandraCellStore::new(cassandra, queries))
}

/// A fresh-segment collection so concurrent runs and iterations never collide.
fn collection(name: &str) -> Result<CollectionRef<ValueKind>> {
    let key: crate::Key = Arc::from("k");
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), key),
        StateType::Application,
        StateName::try_new(name)?,
    );
    Ok(CollectionRef::new(id, None))
}

fn event(n: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(n),
    }
}

/// Stage a set, read it back provisional, promote, read back resolved — the
/// hot-path round-trip — then a direct resolved clear reads back absent.
#[tokio::test]
async fn provisional_set_promote_and_resolved_clear_round_trip() -> Result<()> {
    init_test_logging();
    let store = setup().await?;
    let c = collection("cart")?;
    let data = Bytes::from_static(b"v1");

    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
        )
        .await?;
    match store.read_cell(c.id(), &()).await? {
        Cell::Provisional(cell) => {
            assert_eq!(cell.data(), Some(&data));
            assert_eq!(cell.prev(), None);
            assert_eq!(cell.event(), event(1));
        }
        Cell::Resolved(_) => return Err(eyre!("expected provisional after stage")),
    }

    store.mark_resolved(&c, &()).await?;
    assert_eq!(
        store.read_cell(c.id(), &()).await?,
        Cell::Resolved(Committed::new(Some(data)))
    );

    store.write_resolved(&c, &(), None).await?;
    assert_eq!(
        store.read_cell(c.id(), &()).await?,
        Cell::Resolved(Committed::new(None))
    );
    Ok(())
}

/// Stage a clear over a present base, read it back provisional (`data` null,
/// `prev` present), promote, and read back `Resolved(None)` — the
/// promote-of-clear residue decoded live (encoding/version linger, both blobs
/// null).
#[tokio::test]
async fn provisional_clear_over_present_promotes_to_absent() -> Result<()> {
    init_test_logging();
    let store = setup().await?;
    let c = collection("cart")?;
    let old = Bytes::from_static(b"old");

    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(2)),
        )
        .await?;
    match store.read_cell(c.id(), &()).await? {
        Cell::Provisional(cell) => {
            assert_eq!(cell.data(), None);
            assert_eq!(cell.prev(), Some(&old));
        }
        Cell::Resolved(_) => return Err(eyre!("expected provisional after clear-over-present")),
    }

    store.mark_resolved(&c, &()).await?;
    assert_eq!(
        store.read_cell(c.id(), &()).await?,
        Cell::Resolved(Committed::new(None))
    );
    Ok(())
}

/// An absent row reads back as `Resolved(None)`, and `provisional_cells`
/// yields the staged cell then nothing once resolved.
#[tokio::test]
async fn absent_row_and_provisional_cells_stream() -> Result<()> {
    use futures::StreamExt;

    init_test_logging();
    let store = setup().await?;
    let c = collection("cart")?;

    assert_eq!(
        store.read_cell(c.id(), &()).await?,
        Cell::Resolved(Committed::new(None))
    );

    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(
                Some(Bytes::from_static(b"v")),
                Committed::new(None),
                event(3),
            ),
        )
        .await?;
    let staged: Vec<_> = store
        .provisional_cells(c.id())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    assert_eq!(staged.len(), 1);

    store.mark_resolved(&c, &()).await?;
    let resolved: Vec<_> = store
        .provisional_cells(c.id())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    assert!(resolved.is_empty());
    Ok(())
}

/// Read-path uniqueness invariant: a present cell read back from the Cassandra
/// decode path is **uniquely owned** (`try_into_mut().is_ok()`). This pins the
/// production fast path `StateHandle::get` relies on — every backend decode
/// mints a fresh `Bytes`, so the read parses in place with zero copy. It is the
/// regression guard against a future layer re-introducing a shared clone that
/// would silently demote the read to the copying fallback. Run over random
/// non-empty payloads so the property holds across the byte space, not one
/// fixture.
#[test]
fn prop_cassandra_present_cell_is_uniquely_owned() {
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::{QuickCheck, TestResult};

    async fn check(payload: Vec<u8>) -> Result<bool> {
        let store = setup().await?;
        let c = collection("uniq")?;
        let data = Bytes::from(payload);
        store.write_resolved(&c, &(), Some(&data)).await?;
        let Cell::Resolved(committed) = store.read_cell(c.id(), &()).await? else {
            return Err(eyre!("expected resolved cell"));
        };
        let Some(bytes) = committed.into_inner() else {
            return Err(eyre!("expected present committed value"));
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

    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

/// The crash-recovery-equivalence property over the **Cassandra** cell store.
/// Runs the same backend-generic trace runner as the memory suite, so both
/// backends prove identical invariants (parity by transitivity through the
/// model). Each iteration builds a fresh store and a fresh random segment, so
/// the shared keyspace never collides across iterations.
#[test]
fn prop_cassandra_cell_crash_equivalence() {
    use crate::state::tests::cell_suite::{Trace, run_crash_equivalence_trace};
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::{QuickCheck, TestResult};

    fn prop(trace: Trace) -> TestResult {
        let runtime = &*TEST_RUNTIME;
        let store = match runtime.block_on(setup()) {
            Ok(store) => store,
            Err(error) => return TestResult::error(format!("store setup failed: {error:?}")),
        };
        match runtime.block_on(run_crash_equivalence_trace(store, trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(Trace) -> TestResult);
}

/// The reader-projection-soundness property over the **Cassandra** cell store.
#[test]
fn prop_cassandra_cell_projection_is_sound() {
    use crate::state::tests::cell_suite::{ProjTrace, run_projection_trace};
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::{QuickCheck, TestResult};

    fn prop(trace: ProjTrace) -> TestResult {
        let runtime = &*TEST_RUNTIME;
        let store = match runtime.block_on(setup()) {
            Ok(store) => store,
            Err(error) => return TestResult::error(format!("store setup failed: {error:?}")),
        };
        match runtime.block_on(run_projection_trace(store, trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(ProjTrace) -> TestResult);
}

/// The implicit-overwrite-soundness property over the **Cassandra** cell store:
/// each overwrite resolves its predecessor's provisional cell through the
/// oracle on read, with no explicit promote or rollback.
#[test]
fn prop_cassandra_cell_implicit_overwrite() {
    use crate::state::tests::cell_suite::{OverwriteTrace, run_overwrite_trace};
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::{QuickCheck, TestResult};

    fn prop(trace: OverwriteTrace) -> TestResult {
        let runtime = &*TEST_RUNTIME;
        let store = match runtime.block_on(setup()) {
            Ok(store) => store,
            Err(error) => return TestResult::error(format!("store setup failed: {error:?}")),
        };
        match runtime.block_on(run_overwrite_trace(store, trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(OverwriteTrace) -> TestResult);
}
