//! A collection handle minted over a `CellRead`-only session exposes the read
//! methods but has **no** mutator: `set`/`clear`/`flush` live in an
//! `impl<S: CellSession>` block, and a read-only session does not satisfy
//! `CellSession`. Naming `set` is therefore a compile error (the method does
//! not exist on a reader handle) — the ReadOnlyHandleCannotMutate invariant
//! (§8 inv 8). The expected `.stderr` is pinned (regenerate with
//! `TRYBUILD=overwrite`).

use prosody::JsonCodec;
use prosody::state::descriptor::{Passthrough, StateHandle};
use prosody::state::session::CellRead;
use serde_json::Value;

async fn reader_cannot_mutate<S: CellRead>(handle: StateHandle<S, JsonCodec, Passthrough<Value>>) {
    // `set` is gated on `S: CellSession`; a `CellRead`-only session has no such
    // method, so this does not compile.
    let _ = handle.set(Value::Null).await;
}

fn main() {}
