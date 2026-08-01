# prosody-macros

Framework-internal procedural macros for [`prosody`](https://github.com/prosody-events/prosody)
keyed-state collection authors: `collection_layout!` declares a collection's
durable section layout, and `#[collection_methods]` wraps authored collection
methods in one scoped read or write operation.

The generated code names `crate::state::collection::…` paths, so the macros are
usable only from inside `prosody` itself.
