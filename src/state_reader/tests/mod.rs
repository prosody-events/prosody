//! Property and example suites for the standalone reader.
//!
//! Every arm is deterministic. Committed state is seeded through the
//! **real** owner
//! [`KeyedStateSession`](crate::state::session::KeyedStateSession),
//! using its set/finalize/promote calls. The reader then reads that state
//! back through the stores that read collection evidence. Time comes from a
//! mocked [`quanta::Clock`], advanced explicitly, never a sleep.
//!
//! The scaffolding lives in [`support`]: a backend-generic owner-write
//! harness, a scripted fault source, a counting identity store, and a
//! source-call trace. The test families below split by invariant.
//!
//! The invariant that committed state always matches the oracle is proven
//! once, by the backend-generic [`reader_suite`] runner
//! (`run_reader_{value,map,set,deque}_trace`). It runs against the memory
//! reader in [`reader_tests`] and against a **live Cassandra** reader in
//! [`cassandra_tests`], following the same pattern as `cell_suite`. Fault,
//! refresh, and cache invariants stay scripted and clock-only, since
//! production backends cannot inject faults. They live in [`probe`],
//! [`refresh_tests`], and [`cache_tests`].

pub(crate) mod support;

mod borrowed;
mod cache_tests;
mod cassandra_tests;
mod laziness;
mod probe;
mod reader_suite;
mod reader_tests;
mod refresh_tests;
