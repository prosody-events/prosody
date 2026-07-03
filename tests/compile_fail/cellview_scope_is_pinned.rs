//! `CellView` pins its partition coordinates: the wrapped session and the
//! `(state_type, name)` are private fields, and `session()` is crate-private
//! (`pub(in crate::state)`). So a downstream holder of a `CellView` cannot read
//! out its session to re-point a handle at another partition — the
//! CollectionScopeContainment invariant. The expected `.stderr` is pinned
//! (regenerate with `TRYBUILD=overwrite`).

use prosody::state::descriptor::CellView;
use prosody::state::session::CellSession;

fn repoint<S: CellSession>(view: &CellView<S>) -> &S {
    // `CellView::session` is `pub(in crate::state)` — unreachable downstream.
    view.session()
}

fn main() {}
