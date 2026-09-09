use super::*;
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::state::descriptor::{DescriptorIdentity, value_state};
use crate::state::manager::{Admission, PartitionStateManager, test_manager};
use crate::state::marker::EventEvidence;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::timers::test_support::setup_timer_manager;
use std::slice::from_ref;

/// Runs the production admission path over the supplied registered collections.
pub(crate) async fn admit_registered<S: CellStore>(
    store: &S,
    dedup: &MemoryDeduplicationStore,
    collections: &[CollectionRef],
) -> Result<Admission> {
    let first = collections
        .first()
        .ok_or_else(|| color_eyre::eyre::eyre!("admission needs a collection"))?;
    let mut registry = CollectionDefRegistry::default();
    for collection in collections {
        let id = collection.id();
        registry.register_identity(
            id.state_type(),
            id.name().as_str(),
            value_state::<JsonCodec>(id.name().as_str()).structural_identity(),
            CollectionDef::new(collection.ttl()),
        )?;
    }
    let manager = test_manager(
        store.clone(),
        dedup.clone(),
        Arc::new(registry),
        first.id().state_key().segment_id,
        (),
        (),
    );
    let (_stream, timers, shutdown) = setup_timer_manager().await?;
    let admission = manager
        .admit(
            first.id().state_key().key.clone(),
            &timers,
            &shutdown.subscribe(),
        )
        .await;
    shutdown.send_replace(ShutdownPhase::Cancelling);
    Ok(admission)
}

pub(crate) async fn admit_collection<S: CellStore>(
    store: &S,
    dedup: &MemoryDeduplicationStore,
    collection: &CollectionRef,
) -> Result<bool> {
    Ok(admit_registered(store, dedup, from_ref(collection)).await? == Admission::Fresh)
}

/// Checks replay isolation and admission idempotence against the real manager.
pub(crate) async fn run_admit_soundness<S: CellStore>(
    store: S,
    dedup: MemoryDeduplicationStore,
    value: u8,
    committed: bool,
) -> Result<bool> {
    use crate::state::cell::{Committed, ProvisionalWrite};
    use crate::state::marker::{AttemptId, EventMarker};
    use crate::state::tests::cell_suite::{bytes, value_cell};
    use color_eyre::eyre::ensure;
    let key = StateKey::new(Uuid::new_v4(), Arc::from("admit"));
    let collections = collection_pair(&key)?;
    let touched: Arc<[_]> = collections
        .iter()
        .map(|c| (c.id().state_type(), c.id().name().clone()))
        .collect();
    let e = probe(1);
    let c = probe(2);
    let attempt = AttemptId::new();
    let cell = value_cell();
    // The first event commits both collections. Its evidence survives its dedup
    // window.
    for collection in &collections {
        let writes = [(
            cell.clone(),
            ProvisionalWrite::new(Some(bytes(value)), Committed::new(None), e),
        )];
        let evidence = EventEvidence {
            touched: touched.clone(),
            evidence_ttl: None,
            dedup: Some(Uuid::from_u128(1)),
            attempt,
        };
        let marker = EventMarker::frozen(e, &writes, &[], &evidence);
        store
            .write_provisional(collection, &writes, Some(&marker))
            .await?;
        store
            .commit_provisional(collection, &marker, &writes)
            .await?;
    }
    // Seed the dedup store after e expires, with only c retained.
    dedup.insert(Uuid::from_u128(2)).await?;
    let newer = bytes(value.wrapping_add(1));
    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(Some(newer.clone()), Committed::new(Some(bytes(value))), c),
    )];
    let evidence = EventEvidence {
        touched: touched.clone(),
        evidence_ttl: None,
        dedup: Some(Uuid::from_u128(2)),
        attempt: AttemptId::new(),
    };
    let marker = EventMarker::frozen(c, &writes, &[], &evidence);
    store
        .write_provisional(&collections[0], &writes, Some(&marker))
        .await?;
    store
        .commit_provisional(&collections[0], &marker, &writes)
        .await?;

    let replay = bytes(value.wrapping_add(2));
    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(Some(replay.clone()), Committed::new(Some(newer.clone())), e),
    )];
    let evidence = EventEvidence {
        touched: touched.clone(),
        evidence_ttl: None,
        dedup: Some(Uuid::from_u128(1)),
        attempt: AttemptId::new(),
    };
    let marker = EventMarker::frozen(e, &writes, &[], &evidence);
    store
        .write_provisional(&collections[0], &writes, Some(&marker))
        .await?;
    if committed {
        store
            .commit_provisional(&collections[0], &marker, &writes)
            .await?;
    }
    let counted = CountingCellStore::new(store.clone());
    ensure!(admit_registered(&counted, &dedup, &collections).await? == Admission::Fresh);
    let expected = if committed { replay } else { newer };
    ensure!(
        store.get(collections[0].id(), &cell).await?.get() == Some(&expected),
        "an old certificate certified a replay"
    );
    ensure!(store.get(collections[1].id(), &cell).await?.get() == Some(&bytes(value)));
    for collection in &collections {
        ensure!(store.marker_state(collection.id()).await?.staged.is_none());
    }
    ensure!(dedup.exists(Uuid::from_u128(1)).await?);
    counted.reset();
    ensure!(admit_registered(&counted, &dedup, &collections).await? == Admission::Fresh);
    ensure!(
        counted.durable_writes() == 0,
        "a second admit changed state"
    );
    deregistration(&store, &dedup, value, committed).await?;
    Ok(true)
}

/// Seeds the durable state after evidence lands but before the cell chunks
/// finish.
pub(crate) async fn seed_commit_evidence<S: CellStore>(
    store: &S,
    collection: &CollectionRef,
) -> Result<()> {
    let marker = store
        .marker_state(collection.id())
        .await?
        .staged
        .ok_or_else(|| color_eyre::eyre::eyre!("evidence needs a staged attempt"))?;
    store
        .commit_provisional(collection, &marker.committed_payload(), &[])
        .await?;
    store
        .write_provisional(collection, &[], Some(&marker))
        .await?;
    Ok(())
}

/// A later attempt cannot commit residue from a collection that left the
/// registry.
async fn deregistration<S: CellStore>(
    store: &S,
    dedup: &MemoryDeduplicationStore,
    value: u8,
    committed: bool,
) -> Result<()> {
    use crate::state::cell::{Committed, ProvisionalWrite};
    use crate::state::marker::{AttemptId, EventMarker};
    use crate::state::tests::cell_suite::{bytes, value_cell};
    use color_eyre::eyre::ensure;

    let key = StateKey::new(Uuid::new_v4(), Arc::from("deregistered"));
    let collections = collection_pair(&key)?;
    let touched: Arc<[_]> = collections
        .iter()
        .map(|c| (c.id().state_type(), c.id().name().clone()))
        .collect();
    let event = EventRef::Message {
        dedup_id: Uuid::new_v4(),
    };
    let writes = [(
        value_cell(),
        ProvisionalWrite::new(Some(bytes(value)), Committed::new(None), event),
    )];
    let evidence = EventEvidence {
        touched: touched.clone(),
        evidence_ttl: None,
        dedup: match event {
            EventRef::Message { dedup_id } => Some(dedup_id),
            EventRef::Timer(_) => None,
        },
        attempt: AttemptId::new(),
    };
    let marker = EventMarker::frozen(event, &writes, &[], &evidence);
    for collection in &collections {
        store
            .write_provisional(collection, &writes, Some(&marker))
            .await?;
    }
    if committed {
        store
            .commit_provisional(&collections[0], &marker, &writes)
            .await?;
    }
    ensure!(admit_registered(store, dedup, &collections[..1]).await? == Admission::Fresh);
    let residue = store.marker_state(collections[1].id()).await?;
    ensure!(
        residue.staged.is_some() != committed,
        "admit did not apply the registration rule"
    );

    let next = [(
        value_cell(),
        ProvisionalWrite::new(
            Some(bytes(value.wrapping_add(1))),
            Committed::new(committed.then(|| bytes(value))),
            event,
        ),
    )];
    let evidence = EventEvidence {
        touched: vec![(StateType::Application, collections[0].id().name().clone())].into(),
        evidence_ttl: None,
        dedup: marker.dedup(),
        attempt: AttemptId::new(),
    };
    let marker = EventMarker::frozen(event, &next, &[], &evidence);
    store
        .write_provisional(&collections[0], &next, Some(&marker))
        .await?;
    store
        .commit_provisional(&collections[0], &marker, &next)
        .await?;
    let expected = committed.then(|| bytes(value));
    ensure!(
        store
            .get(collections[1].id(), &value_cell())
            .await?
            .into_inner()
            == expected,
        "a replay certified residue from another attempt"
    );
    ensure!(admit_registered(store, dedup, &collections).await? == Admission::Fresh);
    ensure!(
        store
            .marker_state(collections[1].id())
            .await?
            .staged
            .is_none()
    );
    ensure!(
        store
            .get(collections[1].id(), &value_cell())
            .await?
            .into_inner()
            == expected
    );
    Ok(())
}

fn collection_pair(key: &StateKey) -> Result<[CollectionRef; 2]> {
    let [a, b] = ["a", "b"].map(|name| {
        StateName::try_new(name).map(|name| {
            CollectionRef::new(
                CollectionId::new(key.clone(), StateType::Application, name),
                None,
            )
        })
    });
    Ok([a?, b?])
}
