//! Assignment ownership and routing-set replacement tests.

use super::*;
use crate::JsonCodec;
use crate::error::ErrorCategory;
use crate::state::descriptor::value_state;
use crate::state::registry::{CollectionDef, StateVisibility};
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state::{StateName, StateType};
use color_eyre::eyre::{Result, eyre};

const GROUP: &str = "group-a";
const OTHER_GROUP: &str = "group-b";
const SUBSYSTEM: &str = "orders";
const LEADER: &str = "orders-a";
const SECOND: &str = "orders-b";

fn subsystem() -> Result<SubsystemName> {
    SubsystemName::try_new(SUBSYSTEM).map_err(|error| eyre!("subsystem: {error}"))
}

fn name(value: &str) -> Result<StateName> {
    StateName::try_new(value).map_err(|error| eyre!("name: {error}"))
}

fn registry() -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::default();
    for (name, visibility) in [
        ("cart", StateVisibility::Published),
        ("wishlist", StateVisibility::Private),
    ] {
        registry
            .register(
                &value_state::<JsonCodec>(name),
                CollectionDef {
                    visibility,
                    ..CollectionDef::new(None)
                },
            )
            .map_err(|error| eyre!("register {name}: {error}"))?;
    }
    Ok(Arc::new(registry))
}

fn topics() -> Result<PublicationTopics> {
    PublicationTopics::new(vec![Topic::from(SECOND), Topic::from(LEADER)])
        .ok_or_else(|| eyre!("publication topics must not be empty"))
}

/// The broker's partition count for each fixture topic.
fn count_for(topic: &str) -> Result<PartitionCount> {
    let count = match topic {
        LEADER => 3_i32,
        SECOND => 7_i32,
        _ => return Err(eyre!("no partition count for {topic}")),
    };
    Ok(PartitionCount::try_from(count)?)
}

fn owner(store: ScriptedPublicationStore) -> Result<PublicationOwner<ScriptedPublicationStore>> {
    Ok(PublicationOwner::new(
        subsystem()?,
        store,
        registry()?,
        topics()?.route(&Arc::from(GROUP), count_for)?,
    ))
}

fn row(group: &str, topic: &str, count: i32) -> Result<StatePublication> {
    Ok(StatePublication {
        group_id: Arc::from(group),
        topic: Topic::from(topic),
        partition_count: PartitionCount::try_from(count)?,
    })
}

/// Only partition zero of the leader topic can touch publication storage.
#[tokio::test]
async fn only_leader_partition_zero_publishes() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let owner = owner(store.clone())?;

    for (topic, partition) in [(LEADER, 1_i32), (SECOND, 0_i32), (SECOND, 4_i32)] {
        owner
            .publish_if_owner(Topic::from(topic), partition)
            .await?;
    }

    assert!(
        store.calls().is_empty(),
        "a non-owner assignment must not touch publication storage"
    );
    owner.publish_if_owner(Topic::from(LEADER), 0).await?;
    assert!(
        !store.calls().is_empty(),
        "the leader topic's partition zero must publish"
    );
    Ok(())
}

/// The owner replaces every registered collection's group slice.
#[tokio::test]
async fn owner_replaces_the_complete_routing_set() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    let subsystem = subsystem()?;
    for (collection, publication) in [
        ("cart", row(GROUP, "retired-topic", 2)?),
        ("cart", row(OTHER_GROUP, "other-topic", 5)?),
        ("wishlist", row(GROUP, "retired-topic", 2)?),
    ] {
        store
            .seed(
                &subsystem,
                StateType::Application,
                &name(collection)?,
                &publication,
            )
            .await;
    }

    owner(store.clone())?
        .publish_if_owner(Topic::from(LEADER), 0)
        .await?;

    let mut cart = store
        .rows(&subsystem, StateType::Application, &name("cart")?)
        .await;
    cart.sort_by_key(|row| row.topic);
    let mut expected = vec![
        row(OTHER_GROUP, "other-topic", 5)?,
        row(GROUP, LEADER, 3)?,
        row(GROUP, SECOND, 7)?,
    ];
    expected.sort_by_key(|row| row.topic);
    assert_eq!(cart, expected, "the owner replaced only its group slice");
    assert!(
        store
            .rows(&subsystem, StateType::Application, &name("wishlist")?)
            .await
            .is_empty(),
        "a private collection must retain no owned route"
    );
    Ok(())
}

/// A routing-store failure keeps its category for the acquisition retry loop.
#[tokio::test]
async fn store_failure_preserves_its_category() -> Result<()> {
    let store = ScriptedPublicationStore::new();
    store.fail_removes_with(ErrorCategory::Terminal);

    let failure = owner(store)?
        .publish_if_owner(Topic::from(LEADER), 0)
        .await
        .err()
        .ok_or_else(|| eyre!("the store failure must prevent acquisition"))?;

    assert_eq!(failure.classify_error(), ErrorCategory::Terminal);
    Ok(())
}
