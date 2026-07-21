//! Compile-fail proofs for the crate's capability seals.
//!
//! These cases *are* the compile-time feature: a [`Registered`] token is
//! unforgeable (private field, `pub(crate)` constructor), and
//! [`EventContext::state`] accepts only that token — never a raw descriptor.
//! Together they make "use a descriptor you never registered" unrepresentable.
//! The settlement cases seal the durability boundary the same way: a chain
//! component without the crate-internal settlement classification never
//! reaches `EventHandler`, and the oracle marker write's capability type is
//! unnameable outside the settlement module.
//!
//! The expected `.stderr` is pinned (regenerate with `TRYBUILD=overwrite`).
//!
//! [`Registered`]: prosody::state::descriptor::Registered
//! [`EventContext::state`]: prosody::consumer::event_context::EventContext::state

#[test]
fn registration_capability_is_unforgeable() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
