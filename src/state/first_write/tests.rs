//! Tests for the first-write publication barrier and startup reconciliation.
//!
//! These exercise [`FirstWritePublisher::ensure_one`] and
//! [`reconcile_publications`] directly against a scripted publication store —
//! the cheap, deterministic layer for the visibility gate, the count tripwire,
//! the memo bound, and reconciliation. The settle-boundary ordering arms (a
//! routing row precedes the durable write; a failing store blocks the write)
//! live in the settle test module, which drives the whole sequence.

use std::sync::Arc;

use color_eyre::eyre::{Result, eyre};
use internment::Intern;

use super::{
    FirstWritePublisher, PartitionCounts, PublicationBackend, PublisherTemplate,
    reconcile_publications,
};
use crate::Topic;
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::publication::StatePublication;
use crate::state::registry::{CollectionDef, CollectionDefRegistry, StateVisibility};
use crate::state::tests::support::{PublicationCall, ScriptedPublicationStore};
use crate::state::{StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;

const GROUP: &str = "group-a";
const OTHER_GROUP: &str = "group-b";
const SUBSYSTEM: &str = "orders";
const TOPIC: &str = "orders-topic";

fn subsystem() -> Result<SubsystemName> {
    SubsystemName::try_new(SUBSYSTEM).map_err(|e| eyre!("subsystem: {e}"))
}

fn cart_name() -> Result<StateName> {
    StateName::try_new("cart").map_err(|e| eyre!("name: {e}"))
}

fn topic() -> Topic {
    Intern::<str>::from(TOPIC)
}

fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// A registry with `cart` registered under the given visibility.
fn registry(visibility: StateVisibility) -> Result<CollectionDefRegistry> {
    let mut registry = CollectionDefRegistry::default();
    let def = CollectionDef {
        visibility,
        ..CollectionDef::new(None)
    };
    registry
        .register(&cart(), def)
        .map_err(|e| eyre!("register: {e}"))?;
    Ok(registry)
}

/// A template over the scripted store with `count` partitions and a memo of the
/// given capacity.
fn template(
    store: ScriptedPublicationStore,
    registry: CollectionDefRegistry,
    count: i32,
    capacity: usize,
) -> Result<PublisherTemplate> {
    let counts = PartitionCounts::Memory(PartitionCount::try_from(count)?);
    Ok(PublisherTemplate::with_memo_capacity(
        subsystem()?,
        Arc::from(GROUP),
        Arc::new(PublicationBackend::Scripted(store)),
        Arc::new(counts),
        Arc::new(registry),
        capacity,
    ))
}

fn publisher(
    store: ScriptedPublicationStore,
    registry: CollectionDefRegistry,
    count: i32,
) -> Result<FirstWritePublisher> {
    Ok(template(store, registry, count, 64)?.bind(topic()))
}

/// A row for `(GROUP, TOPIC, count)`.
fn row(group: &str, count: i32) -> Result<StatePublication> {
    Ok(StatePublication {
        group_id: Arc::from(group),
        topic: topic(),
        partition_count: PartitionCount::try_from(count)?,
    })
}

/// A published collection's first write upserts a row with the live count.
#[tokio::test]
async fn published_first_write_upserts_live_count() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let publisher = publisher(store.clone(), registry(StateVisibility::Published)?, 3)?;

    publisher
        .ensure_one(StateType::Application, &cart_name()?)
        .await?;

    assert_eq!(store.upserts_for("cart", TOPIC), 1, "exactly one upsert");
    let rows = store.rows(&subsystem()?, &cart_name()?).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        i32::from(rows[0].partition_count),
        3_i32,
        "row has live count"
    );
    assert_eq!(rows[0].group_id.as_ref(), GROUP);
    Ok(())
}

/// The memo dedups: a second write of the same `(collection, topic)` does not
/// re-upsert.
#[tokio::test]
async fn memo_dedups_second_write() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let publisher = publisher(store.clone(), registry(StateVisibility::Published)?, 3)?;

    publisher
        .ensure_one(StateType::Application, &cart_name()?)
        .await?;
    publisher
        .ensure_one(StateType::Application, &cart_name()?)
        .await?;

    assert_eq!(
        store.upserts_for("cart", TOPIC),
        1,
        "memo dedups the second"
    );
    Ok(())
}

/// Arm (c): a PRIVATE collection's write never consults the store — the
/// visibility gate that makes reconciliation's removal final.
#[tokio::test]
async fn private_collection_never_upserts() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let publisher = publisher(store.clone(), registry(StateVisibility::Private)?, 3)?;

    publisher
        .ensure_one(StateType::Application, &cart_name()?)
        .await?;

    assert!(
        store.calls().is_empty(),
        "a private write must not touch the publication store"
    );
    Ok(())
}

/// Arm (c) continued: after reconciliation removes the group's row, a private
/// write does not re-create it — removal stays final because the write path is
/// visibility-gated.
#[tokio::test]
async fn private_write_stays_unpublished_after_reconcile() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    // Seed the group's own row (as if it was published in a prior generation).
    store.seed(&subsystem, &name, &row(GROUP, 3)?).await;

    // The collection is now registered Private; reconciliation removes the row.
    let registry = registry(StateVisibility::Private)?;
    reconcile_publications(
        &PublicationBackend::Scripted(store.clone()),
        &registry,
        &subsystem,
        GROUP,
    )
    .await?;
    assert!(
        store.rows(&subsystem, &name).await.is_empty(),
        "reconciliation removed the own row"
    );

    // A subsequent private write does not re-publish.
    let publisher = publisher(store.clone(), registry, 3)?;
    publisher.ensure_one(StateType::Application, &name).await?;
    assert!(
        store.rows(&subsystem, &name).await.is_empty(),
        "the private write must not resurrect the row"
    );
    Ok(())
}

/// Arm (d): reconciliation removes this group's own rows and leaves other
/// groups' rows untouched.
#[tokio::test]
async fn reconcile_removes_own_group_keeps_others() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    store.seed(&subsystem, &name, &row(GROUP, 3)?).await;
    store.seed(&subsystem, &name, &row(OTHER_GROUP, 5)?).await;

    reconcile_publications(
        &PublicationBackend::Scripted(store.clone()),
        &registry(StateVisibility::Private)?,
        &subsystem,
        GROUP,
    )
    .await?;

    let rows = store.rows(&subsystem, &name).await;
    assert_eq!(rows.len(), 1, "only the own row is removed");
    assert_eq!(
        rows[0].group_id.as_ref(),
        OTHER_GROUP,
        "the other group's row is retained"
    );
    Ok(())
}

/// Arm (e): a pre-seeded row with a valid but wrong count is overwritten with
/// the live count — a warn-and-overwrite tripwire, never a hard failure.
#[tokio::test]
async fn wrong_stored_count_is_overwritten_not_failed() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    // A valid but stale count (Kafka can only grow partition counts).
    store.seed(&subsystem, &name, &row(GROUP, 1)?).await;

    let publisher = publisher(store.clone(), registry(StateVisibility::Published)?, 3)?;
    // Must succeed (never a hard fail on mismatch).
    publisher.ensure_one(StateType::Application, &name).await?;

    let rows = store.rows(&subsystem, &name).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        i32::from(rows[0].partition_count),
        3_i32,
        "the stale count is overwritten with the live one"
    );
    Ok(())
}

/// Arm (g): the memo is capacity-bounded, so it re-runs the idempotent barrier
/// for evicted entries — the RAM-bound guard. Publishing far more distinct
/// `(collection, topic)` keys than the memo capacity, then re-publishing all of
/// them, forces more upserts than there are keys (the evicted entries re-run).
/// An insert-only memo (the `MarkerMemo.checked` bug class) would keep every
/// key resident, so the second pass would upsert nothing and the total would
/// equal the key count.
#[tokio::test]
async fn memo_is_capacity_bounded() -> Result<()> {
    const KEYS: usize = 256;
    let store = ScriptedPublicationStore::new();
    // A memo far smaller than the key set, so most entries must be evicted.
    let template = template(store.clone(), registry(StateVisibility::Published)?, 3, 4)?;
    let name = cart_name()?;
    let topics: Vec<Topic> = (0..KEYS)
        .map(|i| Intern::<str>::from(format!("t-{i}").as_str()))
        .collect();

    // First pass: publish `cart` under KEYS distinct topics.
    for t in &topics {
        template
            .bind(*t)
            .ensure_one(StateType::Application, &name)
            .await?;
    }
    let after_first: usize = topics
        .iter()
        .map(|t| store.upserts_for("cart", t.as_ref()))
        .sum();
    assert_eq!(after_first, KEYS, "one upsert per distinct topic");

    // Second pass: re-publish all. A bounded memo evicted most entries, so the
    // second pass re-runs the barrier for them — the total exceeds KEYS.
    for t in &topics {
        template
            .bind(*t)
            .ensure_one(StateType::Application, &name)
            .await?;
    }
    let total: usize = topics
        .iter()
        .map(|t| store.upserts_for("cart", t.as_ref()))
        .sum();
    assert!(
        total > KEYS,
        "a capacity-bounded memo re-runs evicted entries (total {total} must exceed {KEYS}); an \
         insert-only memo would keep every entry and leave the total at {KEYS}"
    );
    Ok(())
}

/// Arm (h): the memo key includes the topic, so the same collection published
/// under two topics yields two distinct rows (and two upserts).
#[tokio::test]
async fn distinct_topics_publish_distinct_rows() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    // Two templates, two topics, two different live counts.
    let t1 = Intern::<str>::from("topic-1");
    let t2 = Intern::<str>::from("topic-2");
    template(store.clone(), registry(StateVisibility::Published)?, 3, 64)?
        .bind(t1)
        .ensure_one(StateType::Application, &name)
        .await?;
    template(store.clone(), registry(StateVisibility::Published)?, 7, 64)?
        .bind(t2)
        .ensure_one(StateType::Application, &name)
        .await?;

    let mut rows = store.rows(&subsystem, &name).await;
    rows.sort_by(|a, b| a.topic.as_ref().cmp(b.topic.as_ref()));
    assert_eq!(rows.len(), 2, "one row per topic");
    assert_eq!(rows[0].topic.as_ref(), "topic-1");
    assert_eq!(i32::from(rows[0].partition_count), 3_i32);
    assert_eq!(rows[1].topic.as_ref(), "topic-2");
    assert_eq!(i32::from(rows[1].partition_count), 7_i32);
    Ok(())
}

/// A `read_publications` failure inside reconciliation of a `Permanent`
/// category is logged and skipped, not propagated (a corrupt sibling row must
/// not wedge startup). Here the store is healthy, so reconciliation over an
/// empty set is a no-op — the positive path; the skip-on-permanent branch is a
/// documented degradation exercised by the Cassandra integration variant.
#[tokio::test]
async fn reconcile_over_empty_store_is_a_noop() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    reconcile_publications(
        &PublicationBackend::Scripted(store.clone()),
        &registry(StateVisibility::Published)?,
        &subsystem()?,
        GROUP,
    )
    .await?;
    assert!(
        !store
            .calls()
            .iter()
            .any(|c| matches!(c, PublicationCall::Remove { .. })),
        "nothing to remove from an empty store"
    );
    Ok(())
}
