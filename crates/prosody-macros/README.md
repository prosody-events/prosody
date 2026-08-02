# prosody-macros

This crate provides procedural macros for
[`prosody`](https://crates.io/crates/prosody) keyed-state collections.

`collection_layout!` declares the durable section layout for a collection.
`#[collection_methods]` puts each collection method in one read or write operation.

The macros generate paths to private Prosody modules. Use the `prosody` crate
instead of this crate directly.

## Version compatibility

Use the same version of `prosody-macros` and `prosody`. The `prosody` crate
selects the correct version automatically.

## License

Prosody uses the [MIT license](https://github.com/prosody-events/prosody/blob/main/LICENSE).
