//! Bound descriptors preserve collection scope.

use super::*;
use crate::state::order_codec::Utf8KeyCodec;

fn wishlist() -> ValueDescriptor {
    value_state("wishlist")
}

fn counts() -> MapDescriptor<Utf8KeyCodec> {
    map_state("counts")
}

fn tags() -> SetDescriptor<Utf8KeyCodec> {
    set_state("tags")
}

fn log() -> DequeDescriptor {
    deque_state("log")
}

/// A registry with sibling collections of every kind registered.
fn registry_with_siblings() -> Result<CollectionDefRegistry> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    registry.register(&wishlist(), CollectionDef::new(None))?;
    registry.register(&counts(), CollectionDef::new(None))?;
    registry.register(&tags(), CollectionDef::new(None))?;
    registry.register(&log(), CollectionDef::new(None))?;
    Ok(registry)
}

/// Sibling descriptors address disjoint cells, though every kind reuses
/// sections and coordinates. The collection binding separates each name
/// within the same session and key.
#[test]
fn prop_sibling_descriptors_do_not_leak() {
    async fn check(a: Value, b: Value) -> Result<bool> {
        let session = test_session(MemoryLoader::new(), registry_with_siblings()?);
        let cart = cart().bind(&session).map_err(|e| eyre!("bind cart: {e}"))?;
        let wishlist = wishlist()
            .bind(&session)
            .map_err(|e| eyre!("bind wishlist: {e}"))?;
        let counts = counts()
            .bind(&session)
            .map_err(|e| eyre!("bind counts: {e}"))?;
        let tags = tags().bind(&session).map_err(|e| eyre!("bind tags: {e}"))?;
        let log = log().bind(&session).map_err(|e| eyre!("bind log: {e}"))?;

        // Distinct writes to each sibling, interleaved.
        cart.set(a.clone()).await?;
        wishlist.set(b.clone()).await?;
        counts.set("qty", b.clone()).await?;
        tags.insert("tag").await?;
        log.push_back(a.clone()).await?;

        // Each handle reads back exactly its own collection's data — no
        // cross-collection or cross-section bleed.
        Ok(cart.get().await? == Some(a.clone())
            && wishlist.get().await? == Some(b.clone())
            && counts.get("qty").await? == Some(b)
            && counts.get("missing").await?.is_none()
            && log.get(0).await? == Some(a)
            && log.len().await? == 1
            && tags.contains("tag").await?
            && !tags.contains("qty").await?
            && counts.get("tag").await?.is_none())
    }
    fn prop(a: ArbJson, b: ArbJson) -> TestResult {
        let input = format!("a={:#?} b={:#?}", a.0, b.0);
        finish_trace(
            TEST_RUNTIME.block_on(check(a.0, b.0)),
            "sibling leakage",
            &input,
        )
    }
    QuickCheck::new().quickcheck(prop as fn(ArbJson, ArbJson) -> TestResult);
}
