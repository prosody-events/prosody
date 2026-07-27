//! Tests for the first-write publication barrier, plus the fixtures the
//! reconciliation tests in [`reconcile`] share.
//!
//! These tests drive [`FirstWritePublisher::ensure_one`] directly against a
//! scripted publication store, a cheap deterministic stand-in for the real one.
//! They cover the visibility gate, the observed-count lookup, the count
//! tripwire, and the memo's capacity bound and acknowledgement rule.
//!
//! Settle-boundary ordering (a routing row is written before the durable
//! write, and a failing store blocks the write) is tested separately, in the
//! settle test module that drives the whole sequence.

mod reconcile;

use std::sync::Arc;

use color_eyre::eyre::{Result, eyre};
use internment::Intern;
use tracing::Level;

use super::{
    FirstWritePublisher, PartitionCounts, PublicationBackend, PublisherTemplate,
    reconcile_publications,
};
use crate::Topic;
use crate::consumer::observer::tests::support::{observing, unobserved};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::publication::StatePublication;
use crate::state::registry::{CollectionDef, CollectionDefRegistry, StateVisibility};
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state::{StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use crate::test_util::capture_events;

const GROUP: &str = "group-a";
const OTHER_GROUP: &str = "group-b";
const SUBSYSTEM: &str = "orders";
const TOPIC: &str = "orders-topic";
/// A second topic present in the same observation. Its different count is what
/// makes "the count observed *for this topic*" a falsifiable claim.
const DECOY: &str = "orders-decoy";

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

/// The mock topology's fixed count — the memory arm's source.
fn fixed(count: i32) -> Result<PartitionCounts> {
    Ok(PartitionCounts::Fixed(PartitionCount::try_from(count)?))
}

/// A Kafka observation reporting each `(topic, partition count)` — the
/// production arm's source.
fn observed(topics: &[(&str, i32)]) -> PartitionCounts {
    PartitionCounts::Observed(observing(GROUP, topics))
}

/// A template over the scripted store with the given count source and a memo of
/// the given capacity.
fn template(
    store: ScriptedPublicationStore,
    registry: CollectionDefRegistry,
    counts: PartitionCounts,
    capacity: usize,
) -> Result<PublisherTemplate> {
    Ok(PublisherTemplate::with_memo_capacity(
        subsystem()?,
        Arc::from(GROUP),
        Arc::new(PublicationBackend::Scripted(store)),
        counts,
        Arc::new(registry),
        capacity,
    ))
}

fn publisher(
    store: ScriptedPublicationStore,
    registry: CollectionDefRegistry,
    counts: PartitionCounts,
) -> Result<FirstWritePublisher> {
    Ok(template(store, registry, counts, 64)?.bind(topic()))
}

/// A row for `(group, TOPIC, count)`.
fn row(group: &str, count: i32) -> Result<StatePublication> {
    row_on(group, topic(), count)
}

/// A row for `(group, topic, count)`.
fn row_on(group: &str, topic: Topic, count: i32) -> Result<StatePublication> {
    Ok(StatePublication {
        group_id: Arc::from(group),
        topic,
        partition_count: PartitionCount::try_from(count)?,
    })
}

/// A published collection's first write upserts a row carrying the count the
/// Kafka observation reports **for this topic**. The observation also reports a
/// decoy topic with a different count, so a lookup that ignored the topic would
/// stamp the wrong number.
#[tokio::test]
async fn published_first_write_upserts_live_count() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let publisher = publisher(
        store.clone(),
        registry(StateVisibility::Published)?,
        observed(&[(TOPIC, 3_i32), (DECOY, 7_i32)]),
    )?;

    publisher
        .ensure_one(StateType::Application, &cart_name()?)
        .await?;

    assert_eq!(store.upserts_for("cart", TOPIC), 1, "exactly one upsert");
    let rows = store
        .rows(&subsystem()?, StateType::Application, &cart_name()?)
        .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        i32::from(rows[0].partition_count),
        3_i32,
        "row has the count observed for this topic"
    );
    assert_eq!(rows[0].group_id.as_ref(), GROUP);
    Ok(())
}

/// The memo dedups: a second write of the same `(collection, topic)` does not
/// re-upsert.
#[tokio::test]
async fn memo_dedups_second_write() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let publisher = publisher(
        store.clone(),
        registry(StateVisibility::Published)?,
        fixed(3)?,
    )?;

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

/// A private collection's write never consults the store. This is the
/// visibility gate that makes reconciliation's removal final.
#[tokio::test]
async fn private_collection_never_upserts() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let publisher = publisher(
        store.clone(),
        registry(StateVisibility::Private)?,
        fixed(3)?,
    )?;

    publisher
        .ensure_one(StateType::Application, &cart_name()?)
        .await?;

    assert!(
        store.calls().is_empty(),
        "a private write must not touch the publication store"
    );
    Ok(())
}

/// A pre-seeded row with a valid but stale count is overwritten with the live
/// count, and the `StableRouting` tripwire logs an error-level mismatch event.
/// The write warns and overwrites; it never fails. The test asserts the
/// event as well as the corrected row, because the corrected row alone would
/// not catch a regression that silently dropped the tripwire warning.
#[tokio::test]
async fn wrong_stored_count_is_overwritten_not_failed() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    // A valid but stale count (Kafka can only grow partition counts).
    store
        .seed(&subsystem, StateType::Application, &name, &row(GROUP, 1)?)
        .await;

    let publisher = publisher(
        store.clone(),
        registry(StateVisibility::Published)?,
        observed(&[(TOPIC, 3_i32)]),
    )?;
    let (events, guard) = capture_events(Level::ERROR);
    // Must succeed (never a hard fail on mismatch).
    publisher.ensure_one(StateType::Application, &name).await?;
    drop(guard);

    assert!(
        events.contains("keyed-state publication partition count changed"),
        "the mismatch tripwire must fire an error-level event when the stored count disagrees \
         with the live count"
    );
    let rows = store.rows(&subsystem, StateType::Application, &name).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        i32::from(rows[0].partition_count),
        3_i32,
        "the stale count is overwritten with the live one"
    );
    Ok(())
}

/// The memo is capacity-bounded, so it re-runs the idempotent barrier for
/// evicted entries. This guards against the memo growing without bound.
/// Publishing far more distinct `(collection, topic)` keys than the memo
/// capacity, then re-publishing all of them, forces more upserts than there
/// are keys (the evicted entries re-run). An insert-only memo (the
/// `MarkerMemo.checked` bug class) would keep every key resident, so the
/// second pass would upsert nothing and the total would equal the key count.
#[tokio::test]
async fn memo_is_capacity_bounded() -> Result<()> {
    const KEYS: usize = 256;
    let store = ScriptedPublicationStore::new();
    // A memo far smaller than the key set, so most entries must be evicted.
    let template = template(
        store.clone(),
        registry(StateVisibility::Published)?,
        fixed(3)?,
        4,
    )?;
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

/// One collection published under two topics yields one row per topic, each
/// carrying that topic's own observed count.
///
/// A single template — so a single shared memo — binds both topics. The shared
/// memo is what makes `topic` in `PublicationMemoKey` do real work: drop the
/// field and the second bind hits the first's entry, suppressing its upsert and
/// leaving one row. One observation reports both topics with different counts,
/// so a lookup that ignored the topic would stamp both rows the same.
#[tokio::test]
async fn distinct_topics_publish_distinct_rows() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    let template = template(
        store.clone(),
        registry(StateVisibility::Published)?,
        observed(&[("topic-1", 3_i32), ("topic-2", 7_i32)]),
        64,
    )?;
    for topic in ["topic-1", "topic-2"] {
        template
            .bind(Intern::<str>::from(topic))
            .ensure_one(StateType::Application, &name)
            .await?;
    }

    assert_eq!(
        store.upserts_for("cart", "topic-1"),
        1,
        "the first topic published"
    );
    assert_eq!(
        store.upserts_for("cart", "topic-2"),
        1,
        "the second topic published through the SAME memo — the topic keys it apart"
    );
    let mut rows = store.rows(&subsystem, StateType::Application, &name).await;
    rows.sort_by(|a, b| a.topic.as_ref().cmp(b.topic.as_ref()));
    assert_eq!(rows.len(), 2, "one row per topic despite the shared memo");
    assert_eq!(rows[0].topic.as_ref(), "topic-1");
    assert_eq!(i32::from(rows[0].partition_count), 3_i32);
    assert_eq!(rows[1].topic.as_ref(), "topic-2");
    assert_eq!(i32::from(rows[1].partition_count), 7_i32);
    Ok(())
}

/// An observation that cannot supply this topic's count blocks publication and
/// never touches the store. Both shapes classify `Transient`, so the settle
/// loop retries until a later statistics report repairs the observation.
///
/// The two shapes are the ones a real consumer can be in: no observation
/// installed yet, and an observation that simply does not know this topic. The
/// third shape — a topic present with an incomplete topology — propagates
/// identically here; it is pinned where the rule lives, in
/// `consumer::observer`.
#[tokio::test]
async fn unusable_observation_blocks_publication() -> Result<()> {
    for counts in [
        PartitionCounts::Observed(unobserved(GROUP)),
        observed(&[(DECOY, 7_i32)]),
    ] {
        let store = ScriptedPublicationStore::new();
        let publisher = publisher(store.clone(), registry(StateVisibility::Published)?, counts)?;

        let error = publisher
            .ensure_one(StateType::Application, &cart_name()?)
            .await
            .err()
            .ok_or_else(|| eyre!("an unusable observation must not permit publication"))?;

        assert_eq!(
            error.classify_error(),
            ErrorCategory::Transient,
            "a later statistics report can repair it, so the caller must retry"
        );
        assert!(
            store.calls().is_empty(),
            "the barrier must not read or upsert without a usable count"
        );
    }
    Ok(())
}

/// A failed upsert leaves the memo unlatched, so the caller's retry reaches the
/// store again. Latching before the acknowledgement would turn one transient
/// store failure into a permanently unpublished collection whose durable state
/// no reader can find.
#[tokio::test]
async fn memo_does_not_latch_on_a_failed_upsert() -> Result<()> {
    let store = ScriptedPublicationStore::failing();
    let subsystem = subsystem()?;
    let name = cart_name()?;
    let publisher = publisher(
        store.clone(),
        registry(StateVisibility::Published)?,
        fixed(3)?,
    )?;

    assert!(
        publisher
            .ensure_one(StateType::Application, &name)
            .await
            .is_err(),
        "the failing upsert propagates"
    );
    store.heal();
    publisher.ensure_one(StateType::Application, &name).await?;

    assert_eq!(
        store.upserts_for("cart", TOPIC),
        1,
        "the retry reached the store and applied exactly one upsert"
    );
    assert_eq!(
        store
            .rows(&subsystem, StateType::Application, &name)
            .await
            .len(),
        1,
        "the routing row exists after the retry"
    );
    Ok(())
}
