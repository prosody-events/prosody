use super::suite::{
    DirectoryTrace, STABLE_LEASE, SUITE_CAPACITY, expected_answers, first_divergence,
    run_directory_trace, run_idempotent_deregister_case, run_label_bound_case,
};
use super::support::{finish, membership, memory_directory, registration};
use crate::router::NodeId;
use crate::router::directory::memory::MemoryNodeDirectory;
use crate::router::directory::{NodeDirectory, RegistrationTtl};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use quanta::Clock;
use quickcheck::{QuickCheck, TestResult};
use std::num::NonZeroUsize;
use std::time::Duration;

/// A memory directory answers every read as the plain map oracle answers it.
///
/// Nothing here names a Cassandra symbol, so this is the directory's default
/// loop and it needs no cluster.
#[test]
fn prop_memory_directory_matches_the_model() {
    fn property(trace: DirectoryTrace) -> TestResult {
        finish(TEST_RUNTIME.block_on(async move {
            let directory = memory_directory(STABLE_LEASE)?;
            let actual = run_directory_trace(&directory, &trace).await?;
            let expected = expected_answers(&trace);
            if let Some(divergence) = first_divergence(&trace, &actual, &expected) {
                return Err(eyre!("memory and model: {divergence}"));
            }
            Ok(())
        }))
    }

    init_test_logging();
    QuickCheck::new().quickcheck(property as fn(DirectoryTrace) -> TestResult);
}

/// Every label obeys the byte bound in the memory directory.
#[test]
fn memory_directory_enforces_the_label_bound() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async { run_label_bound_case(&memory_directory(STABLE_LEASE)?).await })
}

/// Repeated deletion stays harmless in the memory directory.
#[test]
fn memory_directory_deregisters_idempotently() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME
        .block_on(async { run_idempotent_deregister_case(&memory_directory(STABLE_LEASE)?).await })
}

/// The memory directory never holds more entries than its fixed capacity.
///
/// That bound is what makes an entry keyed by a node id safe to hold: a stream
/// of fresh ids cannot grow the map.
#[test]
fn memory_directory_never_holds_more_than_its_capacity() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let capacity = SUITE_CAPACITY.get();
        let directory = memory_directory(STABLE_LEASE)?;
        for index in 0..capacity + 4 {
            directory
                .register(&registration(NodeId::new(), membership()))
                .await?;
            assert!(
                directory.len() <= capacity,
                "registration {index}: the directory holds {} entries, above its capacity of \
                 {capacity}",
                directory.len()
            );
        }
        Ok(())
    })
}

/// The memory directory stops resolving an entry after its lease.
///
/// The lease is read on a mock clock rather than waited out, so the test states
/// the bound instead of bracketing it.
#[test]
fn memory_directory_stops_resolving_past_the_lease() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let ttl = RegistrationTtl::try_from(RegistrationTtl::MIN)?;
        let (clock, mock) = Clock::mock();
        let directory = MemoryNodeDirectory::with_clock(NonZeroUsize::MIN, ttl, clock);
        let written = registration(NodeId::new(), membership());
        directory.register(&written).await?;
        assert_eq!(
            directory.read(written.node).await?,
            Some(written.clone()),
            "a fresh registration must resolve"
        );

        mock.increment(ttl.duration() + Duration::from_secs(1));
        assert!(
            directory.read(written.node).await?.is_none(),
            "a registration past its lease must not resolve"
        );
        Ok(())
    })
}
