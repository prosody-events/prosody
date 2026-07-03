use super::super::cell_key::Section;
use super::*;
use crate::state::identity::{StateKey, StateName, StateType};
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::registry::CollectionDefRegistry;
use crate::state::tests::cell_suite::ScriptedOracle;
use color_eyre::eyre::Result;
use uuid::Uuid;

/// Cache-fill bypasses the dirty overlay: `get_for_cache` must reflect the
/// lower committed projection, never this handler's uncommitted buffer —
/// else a fill would cache the dirty value (and stamp it "never
/// expires").
#[tokio::test]
async fn get_for_cache_reads_lower_committed_not_dirty() -> Result<()> {
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("k")),
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let cref = CollectionRef::new(id.clone(), None);
    let own = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let cell = CellKey {
        section: Section::new(0),
        coordinate: Coordinate::from_bytes(vec![7]),
    };

    let lower = MemoryCellStore::new(
        MemoryCells::new(),
        ScriptedOracle::default(),
        Arc::new(CollectionDefRegistry::default()),
    );
    // Seed a committed value (resolved, no event) in the lower store.
    let committed = Bytes::from_static(b"committed");
    lower
        .write_resolved(&cref, &[(cell.clone(), Some(committed.clone()))])
        .await?;

    // Buffer a *different* dirty value on the same key.
    let overlay = Overlay::new(Arc::new(DirtyStore::new()), lower);
    overlay.buffer_set(&id, &cell, b"dirty");

    // The transactional read sees the dirty overlay...
    let dirty_read = overlay.get(&id, &cell, own).await?;
    assert_eq!(dirty_read.get(), Some(&Bytes::from_static(b"dirty")));

    // ...but cache-fill delegates to the lower committed projection.
    let (fill, _ttl) = overlay.get_for_cache(&id, &cell, own).await?;
    assert_eq!(fill.get(), Some(&committed));
    Ok(())
}
