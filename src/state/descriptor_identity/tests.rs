//! Descriptor-identity acquisition and kind-bucketing tests.
//!
//! The acquisition-side coverage pins the broad invariant-5 union (stored rows
//! plus registered descriptors). The bucketing property pins that
//! [`DurableNames`](super::DurableNames) partitions names by their kind
//! discriminator with no per-kind code — the kind-keyed map the recovery sweep
//! enumerates per lane.

use super::{DurableDescriptorIdentity, DurableNames, acquire_descriptor_identities};
use crate::state::descriptor::{DescriptorIdentity, ValueDescriptor, value_state};
use crate::state::memory::MemoryCellStore;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::{CollectionKindId, StateName, descriptor_identity::DescriptorIdentityStore};
use color_eyre::eyre::Result;
use quickcheck::{QuickCheck, TestResult};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// The durable name set returned by acquisition is the union of the
/// stored rows and the registered descriptors — so a collection whose
/// descriptor was since removed from the application (a stored row with
/// no registry entry) is still enumerated for the recovery sweep
/// (invariant 5). Regression: this fails if acquisition returns only the
/// asserted (registered) names.
#[tokio::test]
async fn returns_stored_names_the_registry_no_longer_holds() -> Result<()> {
    let store = MemoryCellStore::new();
    let segment_id = Uuid::from_u128(0xABC);

    // A prior deployment registered "wishlist" and wrote its identity row.
    let wishlist: ValueDescriptor = value_state("wishlist");
    let wishlist_name = StateName::try_new("wishlist")?;
    store
        .write_descriptor_identities(
            segment_id,
            vec![DurableDescriptorIdentity::from_identity(
                &wishlist_name,
                &wishlist.structural_identity(),
            )],
        )
        .await?;

    // The current deployment registers only "cart".
    let mut registry = CollectionDefRegistry::default();
    let cart: ValueDescriptor = value_state("cart");
    registry.register(&cart, CollectionDef::new(None))?;

    let names = acquire_descriptor_identities(&store, &registry, segment_id).await?;
    let set: HashSet<&str> = names
        .names(CollectionKindId::Value)
        .iter()
        .map(StateName::as_str)
        .collect();

    assert!(set.contains("cart"), "the registered name must be present");
    assert!(
        set.contains("wishlist"),
        "the deregistered durable name must still be swept"
    );
    Ok(())
}

/// An empty registry does no identity I/O and returns no names — the
/// inert state layer.
#[tokio::test]
async fn empty_registry_returns_no_names() -> Result<()> {
    let store = MemoryCellStore::new();
    let registry = CollectionDefRegistry::default();
    let names = acquire_descriptor_identities(&store, &registry, Uuid::from_u128(1)).await?;
    assert!(names.names(CollectionKindId::Value).is_empty());
    Ok(())
}

/// Builds the bucketing inputs from generated `(name-seed, kind)` pairs: a
/// small `c<n>` name pool (so collisions and cross-kind repeats occur) and
/// the raw `i8` discriminant. Returns `None` if a name fails to construct,
/// which `c<n>` never does — surfaced as a property error rather than a
/// panic.
fn build_pairs(seeds: Vec<(u8, i8)>) -> Option<Vec<(StateName, i8)>> {
    seeds
        .into_iter()
        .map(|(seed, kind)| Some((StateName::try_new(format!("c{}", seed % 8)).ok()?, kind)))
        .collect()
}

/// Invariant: [`DurableNames`] buckets each name under its kind
/// discriminator, drops names whose discriminator this build does not
/// recognise (forward-compat), and [`DurableNames::names`] returns exactly
/// the known-kind subset in arrival order — all generically, with no
/// per-kind code.
#[test]
fn prop_durable_names_bucket_by_known_kind() {
    fn prop(seeds: Vec<(u8, i8)>) -> TestResult {
        let Some(pairs) = build_pairs(seeds) else {
            return TestResult::error("c<n> name construction failed");
        };
        let mut durable = DurableNames::default();
        // A plain model bucketing only the known discriminants.
        let mut model: HashMap<CollectionKindId, Vec<StateName>> = HashMap::new();
        for (name, kind) in &pairs {
            durable.push(name.clone(), *kind);
            if let Ok(known) = CollectionKindId::try_from(*kind) {
                model.entry(known).or_default().push(name.clone());
            }
        }
        // Every known kind's bucket matches the model exactly (order +
        // duplicates), and an unknown-only run leaves every bucket empty.
        for known in [CollectionKindId::Value, CollectionKindId::TestSecondary] {
            let expected: &[StateName] = model.get(&known).map_or(&[], Vec::as_slice);
            if durable.names(known) != expected {
                return TestResult::error(format!("bucket {known:?} diverged from the model"));
            }
        }
        TestResult::passed()
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<(u8, i8)>) -> TestResult);
}

/// Frozen example: the Value discriminant (1) buckets under Value; an
/// unknown discriminant (99) is dropped from every bucket.
#[test]
fn durable_names_buckets_value_and_drops_unknown() -> Result<()> {
    let mut names = DurableNames::default();
    names.push(
        StateName::try_new("cart")?,
        i8::from(CollectionKindId::Value),
    );
    names.push(StateName::try_new("ghost")?, 99);

    assert_eq!(
        names.names(CollectionKindId::Value),
        &[StateName::try_new("cart")?],
        "the Value-discriminant name buckets under Value",
    );
    assert!(
        names.names(CollectionKindId::TestSecondary).is_empty(),
        "an unknown discriminant lands in no bucket",
    );
    Ok(())
}
