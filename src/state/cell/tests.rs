use super::{Cell, Committed, ProvisionalCell};
use crate::state::EventRef;
use bytes::Bytes;
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
        Cell::Resolved(Committed::new(Some(data.clone()))).project_committed(),
        Some(&data),
    );
    assert_eq!(
        Cell::Resolved(Committed::new(None)).project_committed(),
        None,
    );

    // Provisional → its `prev`, NOT the in-flight `data`.
    assert_eq!(
        Cell::Provisional(ProvisionalCell::new(
            Some(data.clone()),
            Some(prev.clone()),
            event(),
        ))
        .project_committed(),
        Some(&prev),
    );

    // A clear over a present base still projects the (committed) prev.
    assert_eq!(
        Cell::Provisional(ProvisionalCell::new(None, Some(prev.clone()), event()))
            .project_committed(),
        Some(&prev),
    );

    // A provisional clear over an absent base (both blobs null) projects
    // absence.
    assert_eq!(
        Cell::Provisional(ProvisionalCell::new(None, None, event())).project_committed(),
        None,
    );
}
