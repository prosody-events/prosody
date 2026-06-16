//! `Registered` cannot be minted outside the crate: the field is private and
//! the only constructor is `pub(crate)`. This is the unforgeability invariant
//! — a live `Registered<D>` always witnesses a real registration.

use prosody::JsonCodec;
use prosody::state::descriptor::{Registered, value_state};

fn main() {
    // `Registered::new` is `pub(crate)` — unreachable from a downstream crate.
    let _forged = Registered::new(value_state::<JsonCodec>("cart"));
}
