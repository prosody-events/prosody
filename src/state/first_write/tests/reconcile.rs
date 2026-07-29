//! Tests for startup reconciliation: which routing rows
//! [`reconcile_publications`] retires, and which it must leave alone.
//!
//! Every fixture comes from the parent module, so a publication barrier test
//! and a reconciliation test always agree on the group, subsystem, topic, and
//! registry they talk about.

use super::*;
use crate::state::tests::support::PublicationCall;

/// After reconciliation removes the group's row, a private write does not
/// re-create it. Removal stays final because the write path is gated by
/// visibility.
#[tokio::test]
async fn private_write_stays_unpublished_after_reconcile() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    // Seed the group's own row (as if it was published in a prior generation).
    store
        .seed(&subsystem, StateType::Application, &name, &row(GROUP, 3)?)
        .await;

    // The collection is now registered Private; reconciliation removes the row.
    let registry = registry(StateVisibility::Private)?;
    reconcile_publications(&store, &registry, &subsystem, GROUP).await?;
    assert!(
        store
            .rows(&subsystem, StateType::Application, &name)
            .await
            .is_empty(),
        "reconciliation removed the own row"
    );

    // A subsequent private write does not re-publish.
    let publisher = publisher(store.clone(), registry, fixed(3)?)?;
    publisher.ensure_one(StateType::Application, &name).await?;
    assert!(
        store
            .rows(&subsystem, StateType::Application, &name)
            .await
            .is_empty(),
        "the private write must not resurrect the row"
    );
    Ok(())
}

/// Reconciliation removes this group's whole slice — every topic it published
/// the collection under — in one prefix removal, and leaves other groups' rows
/// untouched. Seeding the own group under two topics is what makes the single
/// removal meaningful: a per-topic removal would have to read the rows first
/// and issue one call per topic.
#[tokio::test]
async fn reconcile_removes_own_group_slice_keeps_others() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    let second = Intern::<str>::from("orders-topic-2");
    for row in [
        row(GROUP, 3)?,
        row_on(GROUP, second, 3)?,
        row(OTHER_GROUP, 5)?,
    ] {
        store
            .seed(&subsystem, StateType::Application, &name, &row)
            .await;
    }

    reconcile_publications(
        &store,
        &registry(StateVisibility::Private)?,
        &subsystem,
        GROUP,
    )
    .await?;

    let rows = store.rows(&subsystem, StateType::Application, &name).await;
    assert_eq!(rows.len(), 1, "every own-group topic is removed");
    assert_eq!(
        rows[0].group_id.as_ref(),
        OTHER_GROUP,
        "the other group's row is retained"
    );
    let removes = store
        .calls()
        .iter()
        .filter(|c| matches!(c, PublicationCall::Remove { .. }))
        .count();
    assert_eq!(removes, 1, "one prefix removal, not one call per topic");
    Ok(())
}

/// Reconciliation never reads: it removes each private name's own slice blind.
/// That is what keeps a corrupt sibling row — one that would fail to decode —
/// from blocking the sweep. Over an empty store the removal is a harmless no-op
/// and the store stays empty.
#[tokio::test]
async fn reconcile_removes_blind_without_reading() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    reconcile_publications(
        &store,
        &registry(StateVisibility::Private)?,
        &subsystem,
        GROUP,
    )
    .await?;
    assert!(
        !store
            .calls()
            .iter()
            .any(|c| matches!(c, PublicationCall::Read { .. })),
        "reconciliation must not read the rows it retires"
    );
    assert!(
        store
            .rows(&subsystem, StateType::Application, &cart_name()?)
            .await
            .is_empty(),
        "an empty store stays empty"
    );
    Ok(())
}

/// A `Published` collection's own row survives reconciliation. Only names
/// registered as private are swept, so a published collection keeps its
/// routing row across restart, and a reader never loses the ability to find
/// its still-committed state.
#[tokio::test]
async fn reconcile_keeps_published_collection_row() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    store
        .seed(&subsystem, StateType::Application, &name, &row(GROUP, 3)?)
        .await;

    reconcile_publications(
        &store,
        &registry(StateVisibility::Published)?,
        &subsystem,
        GROUP,
    )
    .await?;

    assert!(
        !store
            .calls()
            .iter()
            .any(|c| matches!(c, PublicationCall::Remove { .. })),
        "a still-published collection's row must not be swept"
    );
    let rows = store.rows(&subsystem, StateType::Application, &name).await;
    assert_eq!(rows.len(), 1, "the published row survives");
    assert_eq!(rows[0].group_id.as_ref(), GROUP);
    Ok(())
}

/// A `Transient` removal failure inside reconciliation propagates, so the
/// caller's build-time retry re-runs rather than the deploy proceeding with a
/// retired collection still advertised.
#[tokio::test]
async fn reconcile_propagates_transient_remove_failure() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    store
        .seed(&subsystem, StateType::Application, &name, &row(GROUP, 3)?)
        .await;
    store.fail_removes_with(ErrorCategory::Transient);

    let result = reconcile_publications(
        &store,
        &registry(StateVisibility::Private)?,
        &subsystem,
        GROUP,
    )
    .await;

    assert!(
        result.is_err(),
        "a Transient removal failure propagates so the build-time retry re-runs"
    );
    assert_eq!(
        store
            .rows(&subsystem, StateType::Application, &name)
            .await
            .len(),
        1,
        "the failed removal left the row in place"
    );
    Ok(())
}
