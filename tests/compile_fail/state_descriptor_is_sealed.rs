//! `StateDescriptor` is sealed by the crate-private `SealedDescriptor`
//! supertrait: a downstream crate can neither name it nor add a
//! `StateDescriptor` impl, so no custom descriptor can receive the raw,
//! gate-free `CellRead` from `bind` and reach cells outside the KV4 gate.

use prosody::state::descriptor::SealedDescriptor;

struct Foo;

// `SealedDescriptor` is `pub(crate)` — unnameable and unimplementable here, so
// `impl StateDescriptor for Foo` (which requires it) is impossible downstream.
impl SealedDescriptor for Foo {}

fn main() {}
