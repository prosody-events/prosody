//! A Deque handle minted over a `CellRead`-only session exposes
//! `stream`/`get`/`len`/`is_empty` but has **no** mutator: `push_*`/`pop_*` live
//! in an `impl<S: CellSession>` block, and a read-only session does not satisfy
//! `CellSession`. Naming `push_back` is therefore a compile error — the
//! ReadOnlyHandleCannotMutate invariant (inv 8). The expected `.stderr` is
//! pinned (regenerate with `TRYBUILD=overwrite`).

use prosody::JsonCodec;
use prosody::state::descriptor::DequeHandle;
use prosody::state::session::CellRead;
use serde_json::Value;

async fn reader_cannot_mutate<S: CellRead>(handle: DequeHandle<S, JsonCodec>) {
    // `push_back` is gated on `S: CellSession`; a `CellRead`-only session has no
    // such method, so this does not compile.
    let _ = handle.push_back(Value::Null).await;
}

fn main() {}
