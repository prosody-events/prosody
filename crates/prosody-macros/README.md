# prosody-macros

This crate provides procedural macros for
[`prosody`](https://crates.io/crates/prosody) keyed-state collections.

`collection_layout!` declares the durable section layout for a collection.
`#[collection_methods]` puts each collection method in one read or write operation.

The macros generate paths to private Prosody modules. Use the `prosody` crate
instead of this crate directly.

## Version compatibility

Release Please gives both crates the same version for each release. Cargo can
select a newer compatible macro version.

Each macro release must follow Cargo's compatibility rules. The `prosody` crate
selects the compatible version automatically.

## License

Prosody uses the [MIT license](https://github.com/prosody-events/prosody/blob/main/LICENSE).
