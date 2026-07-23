//! Memory-backed property and example suites for the standalone reader.
//!
//! Every arm is deterministic: committed state is seeded through the **real**
//! owner [`KeyedStateSession`](crate::state::session::KeyedStateSession)
//! (set/finalize/promote), the reader then reads it back over the oracle-free
//! carriers, and time is an injected [`ReaderClock`](super::cache::ReaderClock)
//! — never a sleep. The scaffolding (owner-write harness, scripted fault
//! source, counting identity store, source-call trace) lives in [`support`];
//! the test families split by invariant.

pub(super) mod support;

mod cache_tests;
mod probe_tests;
mod reader_tests;
mod refresh_tests;
