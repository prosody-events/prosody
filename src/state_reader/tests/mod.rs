//! Property and example suites for the standalone reader.
//!
//! Every arm is deterministic: committed state is seeded through the **real**
//! owner [`KeyedStateSession`](crate::state::session::KeyedStateSession)
//! (set/finalize/promote), the reader then reads it back over the oracle-free
//! carriers, and time is an injected [`ReaderClock`](super::cache::ReaderClock)
//! — never a sleep. The scaffolding (backend-generic owner-write harness,
//! scripted fault source, counting identity store, source-call trace) lives in
//! [`support`]; the test families split by invariant.
//!
//! The committed==oracle invariant is proven **once** by the backend-generic
//! [`reader_suite`] runner (`run_reader_{value,map,deque}_trace`), instantiated
//! for the memory reader in [`reader_tests`] and for a **live-Cassandra**
//! reader in [`cassandra_tests`] — the `cell_suite` idiom. Fault, refresh, and
//! cache invariants stay scripted/clock-only (production backends cannot inject
//! faults): [`probe_tests`], [`refresh_tests`], [`cache_tests`].

pub(crate) mod support;

mod cache_tests;
mod cassandra_tests;
mod probe_tests;
mod reader_suite;
mod reader_tests;
mod refresh_tests;
