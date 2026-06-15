//! Property and example tests for [`PartitionStateStore`] over the memory
//! backend, the memory cache, and a scripted oracle.

use super::CommittedCache;
use super::PartitionStateStore;
use crate::Key;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::memory::{MemoryCellStore, MemoryCommittedCache};
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::CellStore;
use crate::state::value::ValueKind;
use crate::state::{
    CollectionId, CollectionRef, CommitDecision, EventRef, StateKey, StateName, StateType,
};
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt};
use std::convert::Infallible;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// Oracle that returns a fixed decision for every event.
#[derive(Clone)]
struct FixedOracle(CommitDecision);

impl CommitOracle for FixedOracle {
    type Error = Infallible;

    async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(self.0)
    }
}

type Store = PartitionStateStore<ValueKind, MemoryCellStore, FixedOracle, MemoryCommittedCache>;

fn make_store(decision: CommitDecision) -> Store {
    PartitionStateStore::new(
        MemoryCellStore::new(),
        FixedOracle(decision),
        MemoryCommittedCache::new(),
        Arc::new(CollectionDefRegistry::default()),
    )
}

fn collection(name: &str) -> Result<CollectionRef<ValueKind>> {
    let key: Key = Arc::from("k");
    let id = CollectionId::new(
        StateKey::new(Uuid::from_u128(0x5E6), key),
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

/// A staged provisional cell read by its own owning event resolves to `prev`,
/// never `data` — the running handler's own write is provably uncommitted.
#[tokio::test]
async fn own_event_reads_prev_not_data() -> Result<()> {
    let store = make_store(CommitDecision::Committed);
    let c = collection("cart")?;
    let e = event(1);
    let prev = Bytes::from_static(b"prev");
    let data = Bytes::from_static(b"data");

    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(Some(data), Committed::new(Some(prev.clone())), e),
        )
        .await?;

    // Even though the oracle would say "committed", own-event short-circuits
    // to prev without consulting it.
    let read = store.committed_value(c.id(), &(), e).await?;
    assert_eq!(read, Some(prev));
    Ok(())
}

/// A foreign committed provisional cell resolves to `data` and promotes.
#[tokio::test]
async fn foreign_committed_resolves_to_data() -> Result<()> {
    let store = make_store(CommitDecision::Committed);
    let c = collection("cart")?;
    let data = Bytes::from_static(b"data");
    let prev = Bytes::from_static(b"prev");

    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(Some(data.clone()), Committed::new(Some(prev)), event(1)),
        )
        .await?;

    // A different event reads it: the oracle says committed → data, promoted.
    let read = store.committed_value(c.id(), &(), event(2)).await?;
    assert_eq!(read, Some(data));
    // After resolution the durable cell is resolved.
    assert!(matches!(
        store.read_cell(c.id(), &()).await?,
        Cell::Resolved(_)
    ));
    Ok(())
}

/// A foreign uncommitted provisional cell resolves to `prev` and writes it
/// back as resolved.
#[tokio::test]
async fn foreign_uncommitted_resolves_to_prev() -> Result<()> {
    let store = make_store(CommitDecision::NotCommitted);
    let c = collection("cart")?;
    let data = Bytes::from_static(b"data");
    let prev = Bytes::from_static(b"prev");

    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(Some(data), Committed::new(Some(prev.clone())), event(1)),
        )
        .await?;

    let read = store.committed_value(c.id(), &(), event(2)).await?;
    assert_eq!(read, Some(prev.clone()));
    let Cell::Resolved(committed) = store.read_cell(c.id(), &()).await? else {
        return Err(eyre!("cell should be resolved after rollback"));
    };
    assert_eq!(committed.get(), Some(&prev));
    Ok(())
}

/// The pure projection equals the committed base of a provisional cell — the
/// external reader's view, without an oracle.
#[tokio::test]
async fn projection_is_prev_for_provisional() -> Result<()> {
    let store = make_store(CommitDecision::Committed);
    let c = collection("cart")?;
    let prev = Bytes::from_static(b"prev");
    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(
                Some(Bytes::from_static(b"data")),
                Committed::new(Some(prev.clone())),
                event(1),
            ),
        )
        .await?;
    let cell = store.read_cell(c.id(), &()).await?;
    assert_eq!(cell.project_committed(), Some(&prev));
    Ok(())
}

/// Promote nulls the provisional side and patches the cache to `data`.
#[tokio::test]
async fn promote_resolves_to_data() -> Result<()> {
    let store = make_store(CommitDecision::Committed);
    let c = collection("cart")?;
    let data = Bytes::from_static(b"data");
    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
        )
        .await?;
    store.promote(&c, &(), Some(&data)).await?;
    assert!(matches!(
        store.read_cell(c.id(), &()).await?,
        Cell::Resolved(_)
    ));
    assert_eq!(
        store.committed_value(c.id(), &(), event(9)).await?,
        Some(data)
    );
    Ok(())
}

/// Sweep over a quiet key converges every provisional cell and reports
/// all-resolved.
#[tokio::test]
async fn sweep_converges_and_reports_resolved() -> Result<()> {
    let store = make_store(CommitDecision::Committed);
    let c = collection("cart")?;
    let data = Bytes::from_static(b"data");
    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
        )
        .await?;
    assert!(store.sweep_collection(&c).await?);
    assert!(matches!(
        store.read_cell(c.id(), &()).await?,
        Cell::Resolved(_)
    ));
    Ok(())
}

/// A committed-value cache that fails every operation, to prove cache errors
/// degrade rather than surface (the cache-holds-committed-only discipline: the
/// durable backend is authoritative).
#[derive(Clone, Default)]
struct FailingCache;

#[derive(Debug, Error)]
#[error("cache is broken")]
struct CacheBroken;

impl CommittedCache<ValueKind> for FailingCache {
    type Error = CacheBroken;

    async fn get<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<Option<Committed>, Self::Error> {
        Err(CacheBroken)
    }

    async fn put<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        (): &'a (),
        _value: &'a Committed,
    ) -> Result<(), Self::Error> {
        Err(CacheBroken)
    }

    async fn invalidate<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<(), Self::Error> {
        Err(CacheBroken)
    }
}

/// Cache failures on every path never propagate: staging, promotion, and the
/// resolving read all succeed against the durable backend, which is the source
/// of truth (invariant 12, cache-holds-committed-only).
#[tokio::test]
async fn cache_failures_never_propagate() -> Result<()> {
    let store: PartitionStateStore<ValueKind, MemoryCellStore, FixedOracle, FailingCache> =
        PartitionStateStore::new(
            MemoryCellStore::new(),
            FixedOracle(CommitDecision::Committed),
            FailingCache,
            Arc::new(CollectionDefRegistry::default()),
        );
    let c = collection("cart")?;
    let data = Bytes::from_static(b"data");

    store
        .write_provisional(
            &c,
            &(),
            &ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
        )
        .await?;
    store.promote(&c, &(), Some(&data)).await?;

    // The cache `get` errors → falls through to the durable read; the value
    // survives despite every cache call failing.
    assert_eq!(
        store.committed_value(c.id(), &(), event(9)).await?,
        Some(data)
    );
    Ok(())
}

/// A cell store that fails the promote (`mark_resolved`) arm with a *permanent*
/// error for one named collection and delegates everything else to an inner
/// memory store. Drives the no-strand invariant: a cell whose resolution can
/// never complete must be skipped without poisoning its siblings.
#[derive(Clone)]
struct PoisonPromoteStore {
    inner: MemoryCellStore,
    poison: StateName,
}

#[derive(Debug, Error)]
#[error("permanent promote poison")]
struct PromotePoisoned;

impl ClassifyError for PromotePoisoned {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

impl CellStore<ValueKind> for PoisonPromoteStore {
    type Error = PromotePoisoned;

    async fn read_cell<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        addr: &'a (),
    ) -> Result<Cell, Self::Error> {
        self.inner.read_cell(collection, addr).await.map_err(never)
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Stream<Item = Result<((), ProvisionalCell), Self::Error>> + Send + 'a {
        self.inner
            .provisional_cells(collection)
            .map(|item| item.map_err(never))
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        writes: &'a [((), ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.inner
            .write_provisional(collection, writes)
            .await
            .map_err(never)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        cells: &'a [((), Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        self.inner
            .write_resolved(collection, cells)
            .await
            .map_err(never)
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addrs: &'a [()],
    ) -> Result<(), Self::Error> {
        if *collection.id().name() == self.poison {
            return Err(PromotePoisoned);
        }
        self.inner
            .mark_resolved(collection, addrs)
            .await
            .map_err(never)
    }
}

/// Lifts the inner store's [`Infallible`] error into [`PromotePoisoned`]; it is
/// never called because [`Infallible`] is uninhabited.
fn never(error: Infallible) -> PromotePoisoned {
    match error {}
}

/// The no-strand invariant (#6): when a per-cell resolution fails permanently,
/// the sweep skips that cell and returns `false` — so the caller keeps the
/// `StateRecovery` backstop armed for a later sweep / first-touch — while a
/// sibling collection still resolves and reports `true`. A poisoned cell must
/// never strand its siblings nor falsely report all-resolved.
#[tokio::test]
async fn sweep_keeps_backstop_when_a_cell_is_permanently_skipped() -> Result<()> {
    let store: PartitionStateStore<
        ValueKind,
        PoisonPromoteStore,
        FixedOracle,
        MemoryCommittedCache,
    > = PartitionStateStore::new(
        PoisonPromoteStore {
            inner: MemoryCellStore::new(),
            poison: StateName::try_new("poison")?,
        },
        FixedOracle(CommitDecision::Committed),
        MemoryCommittedCache::new(),
        Arc::new(CollectionDefRegistry::default()),
    );
    let poison = collection("poison")?;
    let safe = collection("safe")?;
    let data = Bytes::from_static(b"data");

    for c in [&poison, &safe] {
        store
            .write_provisional(
                c,
                &(),
                &ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
            )
            .await?;
    }

    // The poisoned cell can never promote: the sweep skips it and reports
    // unresolved, so the backstop must stay armed.
    assert!(
        !store.sweep_collection(&poison).await?,
        "a permanently-skipped cell must report the collection unresolved",
    );
    assert!(
        matches!(
            store.read_cell(poison.id(), &()).await?,
            Cell::Provisional(_)
        ),
        "the skipped cell stays provisional for first-touch / a later sweep",
    );

    // The sibling resolves normally and reports all-resolved — no strand.
    assert!(
        store.sweep_collection(&safe).await?,
        "a sibling collection must still resolve despite the poisoned cell",
    );
    assert!(matches!(
        store.read_cell(safe.id(), &()).await?,
        Cell::Resolved(_)
    ));
    Ok(())
}
