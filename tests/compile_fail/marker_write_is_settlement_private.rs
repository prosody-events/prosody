//! The oracle marker write is private to the settlement module: `MarkerWrite`
//! — the capability the sealed lifecycle's `record_marker` demands — lives in
//! the private `settle` module with a crate-internal re-export, so external
//! code cannot even name it, let alone construct one. (In-crate, the private
//! unit field makes `MarkerWrite(())` unwritable outside `settle.rs`; rustc
//! enforces that at every build.)

use prosody::consumer::middleware::MarkerWrite;

fn main() {
    // Unnameable outside the crate: the re-export is `pub(crate)`.
    let _proof: MarkerWrite;
}
