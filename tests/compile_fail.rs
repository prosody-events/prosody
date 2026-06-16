//! Compile-fail proofs for the keyed-state registration capability handle.
//!
//! These cases *are* the compile-time feature: a [`Registered`] token is
//! unforgeable (private field, `pub(crate)` constructor), and
//! [`EventContext::state`] accepts only that token — never a raw descriptor.
//! Together they make "use a descriptor you never registered" unrepresentable.
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
