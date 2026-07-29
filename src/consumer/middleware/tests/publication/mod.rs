//! Tests for first-write publication at the settle boundary, and the fixtures
//! the [`commit`] path's tests share with them.
//!
//! A `Published` collection's routing row is written before its committed
//! state. A failing publication store blocks the durable write rather than
//! settling an unpublished collection. Shutdown during publication abandons
//! the event without staging anything.

mod commit;

use super::*;
use crate::consumer::observer::KafkaObserver;
use crate::consumer::observer::tests::support::{observe, observing};
use crate::loader::MemoryLoader;
use crate::state::cell::Committed;
use crate::state::descriptor::tests::{FixedOracle, TestSession, test_session_with_publisher};
use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
use crate::state::first_write::PublisherTemplate;
use crate::state::memory::MemoryCellStore;
use crate::state::publication::StatePublication;
use crate::state::registry::{CollectionDef, CollectionDefRegistry, StateVisibility};
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::value_cell;
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state::{CollectionId, EventRef, StateKey, StateName, StateType, StoreOutcome};
use crate::subsystem::SubsystemName;
use crate::test_util::capture_events;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, ensure, eyre};
use internment::Intern;
use serde_json::json;
use std::time::Duration;
use tokio::time::{advance, sleep, timeout};
use tracing::Level;
use uuid::Uuid;

type Ctx = MockEventContext<serde_json::Value, TestSession>;

const GROUP: &str = "group-a";
const SUBSYSTEM: &str = "orders";
const TOPIC: &str = "orders-topic";
/// A second topic the observation reports with a different count, so a
/// routing row stamped from the wrong topic is visible.
const DECOY: &str = "orders-decoy";

fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// A registry with every named collection registered `Published`. A test that
/// names two writes only one, proving that only touched collections advertise.
fn published_registry(names: &[&'static str]) -> Result<CollectionDefRegistry> {
    let mut registry = CollectionDefRegistry::default();
    for name in names {
        registry
            .register(
                &value_state(name) as &ValueDescriptor,
                CollectionDef {
                    visibility: StateVisibility::Published,
                    ..CollectionDef::new(None)
                },
            )
            .map_err(|e| eyre!("register {name}: {e}"))?;
    }
    Ok(registry)
}

fn publisher_template(
    store: ScriptedPublicationStore,
    observer: &KafkaObserver,
    names: &[&'static str],
) -> Result<PublisherTemplate<ScriptedPublicationStore, KafkaObserver>> {
    Ok(PublisherTemplate::new(
        subsystem()?,
        Arc::from(GROUP),
        Arc::new(store),
        observer.clone(),
        Arc::new(published_registry(names)?),
    ))
}

/// Reads the collection's single Value cell from the durable store,
/// resolved to its committed value. Uses a probe event identity distinct
/// from the event under test, so the read never aliases it.
async fn committed_value(
    cell_store: &MemoryCellStore<FixedOracle>,
    id: &CollectionId,
) -> Result<Option<Bytes>> {
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    cell_store
        .get(id, &value_cell(), probe)
        .await
        .map(Committed::into_inner)
        .map_err(|e| eyre!("read committed: {e}"))
}

/// A real session over `store`, carrying a first-write publisher for every
/// `Published` collection in `names`, keyed by `key`. Nothing is written yet.
/// Returns the context, the durable store, and `cart`'s cell id.
fn published_context(
    store: ScriptedPublicationStore,
    observer: &KafkaObserver,
    names: &[&'static str],
    key: u128,
) -> Result<(Ctx, MemoryCellStore<FixedOracle>, CollectionId)> {
    let state_key = StateKey::new(Uuid::from_u128(key), Arc::from("user-1"));
    let publisher = publisher_template(store, observer, names)?.bind(Intern::<str>::from(TOPIC));
    let (session, cell_store) = test_session_with_publisher(
        MemoryLoader::new(),
        published_registry(names)?,
        state_key.clone(),
        publisher,
    );
    let cart_id = CollectionId::new(state_key, StateType::Application, state_name("cart")?);
    let context: Ctx = MockEventContext::new().with_session(session);
    Ok((context, cell_store, cart_id))
}

/// Writes one value into a `Published` `cart` on a [`published_context`]. No
/// finalize — `settle` owns the only stage.
async fn buffered_published(
    store: ScriptedPublicationStore,
    observer: &KafkaObserver,
) -> Result<(Ctx, MemoryCellStore<FixedOracle>, CollectionId)> {
    let (context, cell_store, cart_id) = published_context(store, observer, &["cart"], 0x7)?;
    write_cart(&context).await?;
    Ok((context, cell_store, cart_id))
}

/// Binds `cart` on `context` and writes one value into it.
async fn write_cart(context: &Ctx) -> Result<()> {
    let handle = context
        .state(Registered::new(cart()))
        .map_err(|e| eyre!("bind cart: {e}"))?;
    handle.set(json!({ "x": 1_i32 })).await?;
    Ok(())
}

fn subsystem() -> Result<SubsystemName> {
    SubsystemName::try_new(SUBSYSTEM).map_err(|e| eyre!("subsystem: {e}"))
}

fn state_name(name: &str) -> Result<StateName> {
    StateName::try_new(name).map_err(|e| eyre!("name: {e}"))
}

/// The routing rows `store` holds for one collection of this suite's subsystem.
async fn publication_rows(
    store: &ScriptedPublicationStore,
    name: &str,
) -> Result<Vec<StatePublication>> {
    Ok(store
        .rows(&subsystem()?, StateType::Application, &state_name(name)?)
        .await)
}

/// The routing row is written before the durable state. The gated upsert
/// parks in settle step 0, so the cell is not yet durable until finalize
/// runs in step 1. Releasing the gate lets settle stage and commit, and
/// the row lands with the observed partition count.
#[tokio::test]
async fn publication_precedes_the_durable_write() -> Result<()> {
    let store = ScriptedPublicationStore::gated();
    let observer = observing(GROUP, &[(TOPIC, 3_i32), (DECOY, 7_i32)]);
    let (context, cell_store, cart_id) = buffered_published(store.clone(), &observer).await?;
    let handler = ProbeHandler::ok(0);
    let (guard, committed, aborted) = RecordingGuard::new();

    let task = tokio::spawn(async move {
        settle(&handler, context, guard, Ok(0)).await;
    });

    // Wait until the gated upsert has entered settle step 0 and blocked.
    timeout(Duration::from_secs(5), store.wait_entered())
        .await
        .map_err(|_| eyre!("gated upsert never entered"))?;
    // The barrier precedes the stage, so nothing is durable yet.
    assert_eq!(
        committed_value(&cell_store, &cart_id).await?,
        None,
        "the cell must not be durable while publication is still blocked",
    );
    assert_eq!(committed.load(Ordering::SeqCst), 0);

    // Release the barrier; settle now stages, commits, and promotes.
    store.release();
    task.await.map_err(|e| eyre!("settle task: {e}"))?;

    let rows = publication_rows(&store, "cart").await?;
    assert_eq!(rows.len(), 1, "the routing row landed");
    assert_eq!(
        i32::from(rows[0].partition_count),
        3_i32,
        "with the observed count for this topic"
    );
    assert_eq!(committed.load(Ordering::SeqCst), 1);
    assert_eq!(
        aborted.load(Ordering::SeqCst),
        0,
        "the guard never aborts while publication is pending",
    );
    assert!(
        committed_value(&cell_store, &cart_id).await?.is_some(),
        "the cell is durable after release",
    );
    Ok(())
}

/// A failing publication store blocks the durable write. Settle's publish
/// loop must succeed, so it retries forever while the store keeps
/// failing: no cell is durable and nothing commits. Once the store
/// heals, both the row and the cell land, and the guard commits exactly
/// once.
#[tokio::test(start_paused = true)]
async fn failed_publication_blocks_the_write() -> Result<()> {
    let store = ScriptedPublicationStore::failing();
    let observer = observing(GROUP, &[(TOPIC, 3_i32)]);
    let (context, cell_store, cart_id) = buffered_published(store.clone(), &observer).await?;
    let handler = ProbeHandler::ok(0);
    let (guard, committed, aborted) = RecordingGuard::new();

    let task = tokio::spawn(async move {
        settle(&handler, context, guard, Ok(0)).await;
    });

    // The publish loop attempted and failed at least once.
    timeout(Duration::from_secs(5), store.wait_errored())
        .await
        .map_err(|_| eyre!("gated upsert never errored"))?;
    assert_eq!(
        committed_value(&cell_store, &cart_id).await?,
        None,
        "no durable write while publication keeps failing",
    );
    assert_eq!(committed.load(Ordering::SeqCst), 0);

    // Heal the store and advance past the retry backoff so the loop
    // succeeds and settle finishes.
    store.heal();
    advance(Duration::from_secs(2)).await;
    timeout(Duration::from_secs(5), task)
        .await
        .map_err(|_| eyre!("settle did not finish after the store healed"))?
        .map_err(|e| eyre!("settle task: {e}"))?;

    let rows = publication_rows(&store, "cart").await?;
    assert_eq!(
        rows.len(),
        1,
        "the routing row landed once the store healed"
    );
    assert_eq!(
        committed.load(Ordering::SeqCst),
        1,
        "committed exactly once"
    );
    assert_eq!(
        aborted.load(Ordering::SeqCst),
        0,
        "the guard never aborts while publication retries",
    );
    assert!(
        committed_value(&cell_store, &cart_id).await?.is_some(),
        "the cell is durable after the store healed",
    );
    Ok(())
}

/// Shutdown observed at settle step 0 abandons the event before anything
/// stages: the guard aborts, no cell is durable, and no routing row lands.
#[tokio::test]
async fn shutdown_during_publication_abandons() -> Result<()> {
    // A store that would fail forever, but shutdown short-circuits the loop
    // before any upsert is attempted.
    let store = ScriptedPublicationStore::failing();
    let observer = observing(GROUP, &[(TOPIC, 3_i32)]);
    let (context, cell_store, cart_id) = buffered_published(store.clone(), &observer).await?;
    // Request shutdown AFTER the write is buffered: the write itself needs
    // a live session first. Settle's publish loop then sees shutdown at
    // its top.
    context.request_shutdown();
    let handler = ProbeHandler::ok(0);
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(&handler, context, guard, Ok(0)).await;

    assert_eq!(aborted.load(Ordering::SeqCst), 1, "shutdown abandons");
    assert_eq!(committed.load(Ordering::SeqCst), 0);
    assert_eq!(
        committed_value(&cell_store, &cart_id).await?,
        None,
        "nothing staged when shutdown pre-empts publication",
    );
    assert!(
        publication_rows(&store, "cart").await?.is_empty(),
        "no routing row is written on the shutdown-abandon path",
    );
    Ok(())
}

/// Publication advertises only the collections the event actually wrote.
/// `publish_first_writes` iterates the dirty overlay's
/// `touched_collections`, never the full registered set.
///
/// Two `Published` collections are registered; the event writes only
/// `cart`. Settling publishes a row for `cart` and none for the unwritten
/// `wishlist`. Falsify by enumerating the registry instead of the touched
/// overlay in `KeyedStateSession::publish_first_writes`: `wishlist` gains
/// a row and the no-row assertion goes red.
#[tokio::test]
async fn only_written_published_collections_are_advertised() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let observer = observing(GROUP, &[(TOPIC, 3_i32)]);
    let (context, _cell_store, _cart_id) =
        published_context(store.clone(), &observer, &["cart", "wishlist"], 0xB)?;
    // Write ONLY cart; wishlist is Published but untouched by this event.
    write_cart(&context).await?;

    let handler = ProbeHandler::ok(0);
    let (guard, committed, _aborted) = RecordingGuard::new();
    settle(&handler, context, guard, Ok(0)).await;

    assert_eq!(
        committed.load(Ordering::SeqCst),
        1,
        "the written event commits"
    );
    assert_eq!(
        store.upserts_for("cart", TOPIC),
        1,
        "the written collection is advertised"
    );
    assert_eq!(
        store.upserts_for("wishlist", TOPIC),
        0,
        "the unwritten published collection is NOT advertised",
    );
    assert!(
        publication_rows(&store, "wishlist").await?.is_empty(),
        "no routing row for a published collection that never wrote",
    );
    let cart_rows = publication_rows(&store, "cart").await?;
    assert_eq!(
        cart_rows.len(),
        1,
        "exactly the written collection's row lands"
    );
    Ok(())
}

/// A topic the Kafka observation does not know blocks the durable write,
/// and a later statistics report unblocks the same settle call.
///
/// Phase one holds the observation empty of this topic. The barrier is
/// must-succeed, so settle loops on it: nothing stages, nothing reaches the
/// publication store, and the refusal is visible in the retry log. Phase
/// two installs an observation that reports the topic. The gated store
/// then proves the routing row is offered before the cell becomes
/// durable.
///
/// Phase one's deadline is the expected exit: the runtime is paused, so
/// virtual time only advances while every task is idle, and the barrier
/// must have refused across several durability retries before
/// `REFUSAL_WINDOW` elapses. The `settling` and `wait_entered` arms are the
/// failures. Phase two inverts this — `HANG_GUARD` is a hang guard and
/// firing it fails the test.
#[tokio::test(start_paused = true)]
async fn unobserved_topic_blocks_the_write_until_the_snapshot_repairs() -> Result<()> {
    /// Several durability retry delays: long enough for the publish loop to
    /// refuse at least once, and the only way out of phase one.
    const REFUSAL_WINDOW: Duration = Duration::from_secs(3);
    /// Virtual-time hang guard. Failing it is the assertion that the
    /// repaired observation actually unblocked the barrier.
    const HANG_GUARD: Duration = Duration::from_secs(30);

    // An observation that knows a decoy topic but not the one being written.
    let observer = observing(GROUP, &[(DECOY, 7_i32)]);
    let store = ScriptedPublicationStore::gated();
    let (context, cell_store, cart_id) = buffered_published(store.clone(), &observer).await?;
    let handler = ProbeHandler::ok(0);
    let (guard, committed, aborted) = RecordingGuard::new();

    let (events, capture) = capture_events(Level::ERROR);
    let settling = settle(&handler, context, guard, Ok(0));
    tokio::pin!(settling);

    tokio::select! {
        () = &mut settling => bail!("settle completed while the topic was unobserved"),
        () = store.wait_entered() => {
            bail!("the upsert reached the store without an observed count")
        }
        () = sleep(REFUSAL_WINDOW) => {}
    }
    ensure!(
        events.contains("is not in the current Kafka observation"),
        "the barrier must have refused on the missing topic at least once"
    );
    ensure!(
        store.calls().is_empty(),
        "an unusable count must not read or upsert"
    );
    ensure!(
        committed_value(&cell_store, &cart_id).await?.is_none(),
        "nothing may stage while the topic is unobserved"
    );
    ensure!(
        committed.load(Ordering::SeqCst) == 0 && aborted.load(Ordering::SeqCst) == 0,
        "the event neither commits nor aborts while publication is refused"
    );

    // The next statistics report brings the topic in.
    observe(&observer, &[(TOPIC, 3_i32), (DECOY, 7_i32)]);
    tokio::select! {
        () = &mut settling => bail!("settle finished without offering the routing row"),
        () = store.wait_entered() => {}
        () = sleep(HANG_GUARD) => {
            bail!("publication never reached the store after the observation repaired")
        }
    }
    ensure!(
        committed_value(&cell_store, &cart_id).await?.is_none(),
        "the routing row is offered before the cell is durable"
    );

    store.release();
    timeout(HANG_GUARD, settling)
        .await
        .map_err(|_| eyre!("settle never finished after the routing row landed"))?;
    drop(capture);

    let rows = publication_rows(&store, "cart").await?;
    ensure!(rows.len() == 1, "exactly one routing row landed");
    ensure!(
        rows[0].group_id.as_ref() == GROUP && rows[0].topic.as_ref() == TOPIC,
        "the row names this group and the written topic"
    );
    ensure!(
        i32::from(rows[0].partition_count) == 3_i32,
        "the row carries the repaired observation's count for this topic"
    );
    ensure!(
        committed.load(Ordering::SeqCst) == 1 && aborted.load(Ordering::SeqCst) == 0,
        "the event commits exactly once and never aborts"
    );
    ensure!(
        committed_value(&cell_store, &cart_id).await?.is_some(),
        "the cell is durable once the routing row landed"
    );
    Ok(())
}
