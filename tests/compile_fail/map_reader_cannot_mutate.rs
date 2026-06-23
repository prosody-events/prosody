//! A Map handle minted over a `CellRead`-only session exposes `get`/`stream`
//! but has **no** mutator: `set`/`remove` live in an `impl<S: CellSession>`
//! block, and a read-only session does not satisfy `CellSession`. Naming `set`
//! is therefore a compile error — the ReadOnlyHandleCannotMutate invariant (inv
//! 8). The expected `.stderr` is pinned (regenerate with `TRYBUILD=overwrite`).

use prosody::JsonCodec;
use prosody::state::descriptor::MapHandle;
use prosody::state::order_codec::Utf8KeyCodec;
use prosody::state::session::CellRead;
use serde_json::Value;

async fn reader_cannot_mutate<S: CellRead>(handle: MapHandle<S, Utf8KeyCodec, JsonCodec>) {
    // `set` is gated on `S: CellSession`; a `CellRead`-only session has no such
    // method, so this does not compile.
    let _ = handle.set(&"k".to_owned(), Value::Null).await;
}

fn main() {}
