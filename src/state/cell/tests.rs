use super::{CacheEntry, Cell, Committed, Presence, Projection, ProvisionalCell, Read, Values};
use crate::state::EventRef;
use bytes::Bytes;
use quickcheck::QuickCheck;
use uuid::Uuid;

fn event() -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    }
}

/// The pure committed-value projection (the external reader's view): a
/// resolved cell projects its committed value, a provisional cell projects
/// its `prev` (the committed base, stale by exactly the in-flight event) —
/// never its in-flight `data`. A cleared/rolled-back/absent-base cell all
/// project absence (the `ClearIsAbsence` corollary).
#[test]
fn project_committed_is_prev_for_provisional_and_data_for_resolved() {
    let data = Bytes::from_static(b"data");
    let prev = Bytes::from_static(b"prev");

    // Resolved → its committed value (present or absent).
    assert_eq!(
        Cell::<Values>::Resolved(Committed::new(Some(data.clone()))).project_committed(),
        Some(&data),
    );
    assert_eq!(
        Cell::<Values>::Resolved(Committed::<Values>::new(None)).project_committed(),
        None,
    );

    // Provisional → its `prev`, NOT the in-flight `data`.
    assert_eq!(
        Cell::<Values>::Provisional(ProvisionalCell::new(
            Some(data.clone()),
            Some(prev.clone()),
            event(),
        ))
        .project_committed(),
        Some(&prev),
    );

    // A clear over a present base still projects the (committed) prev.
    assert_eq!(
        Cell::<Values>::Provisional(ProvisionalCell::new(None, Some(prev.clone()), event()))
            .project_committed(),
        Some(&prev),
    );

    // A provisional clear over an absent base (both blobs null) projects
    // absence.
    assert_eq!(
        Cell::<Values>::Provisional(ProvisionalCell::<Values>::new(None, None, event()))
            .project_committed(),
        None,
    );
}

/// Cache entries preserve payloads and distinguish unknown values from absence.
#[test]
fn prop_projection_cache_lattice() {
    fn property(payload: Vec<u8>, present: bool) -> bool {
        let bytes = Bytes::from(payload);
        let value = present.then(|| bytes.clone());
        let presence = present.then_some(());
        let value_entry = Values::into_cached(value.clone());
        let presence_entry = Presence::into_cached(presence);
        assert_eq!(Values::from_value(bytes.clone()), bytes);
        assert_eq!(
            Values::from_cached(value_entry.clone()),
            value.map_or(Read::Absent, Read::Present),
        );
        assert_eq!(
            Presence::from_cached(value_entry),
            presence.map_or(Read::Absent, Read::Present),
        );
        assert_eq!(
            Presence::from_cached(presence_entry.clone()),
            presence.map_or(Read::Absent, Read::Present),
        );
        assert_eq!(
            Values::from_cached(presence_entry),
            if present { Read::Unknown } else { Read::Absent },
        );

        let entries = [
            CacheEntry::Absent,
            CacheEntry::Exists,
            CacheEntry::Value(bytes),
        ];
        for (new_index, new) in entries.iter().enumerate() {
            for (old_index, old) in entries.iter().enumerate() {
                assert_eq!(new.downgrades(old), new_index == 1 && old_index == 2);
            }
        }
        true
    }
    QuickCheck::new().quickcheck(property as fn(Vec<u8>, bool) -> bool);
}
