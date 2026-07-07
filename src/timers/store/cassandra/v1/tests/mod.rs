//! Tests for V1 schema operations (Cassandra-only).

#[cfg(test)]
use crate::cassandra::CassandraConfiguration;

#[cfg(test)]
mod prop_high_level;
#[cfg(test)]
mod prop_key_triggers;
#[cfg(test)]
mod prop_migration;
#[cfg(test)]
mod prop_slab_metadata;
#[cfg(test)]
mod prop_slab_triggers;

/// V1-suite Cassandra configuration. Unlike
/// [`crate::test_util::test_cassandra_config`], this takes the keyspace as a
/// parameter and pins the datacenter plus a 24h retention.
#[cfg(test)]
fn test_cassandra_config(keyspace: &str) -> CassandraConfiguration {
    use std::time::Duration;

    CassandraConfiguration {
        nodes: vec!["127.0.0.1:9042".to_owned()],
        keyspace: keyspace.to_owned(),
        datacenter: Some("datacenter1".to_owned()),
        rack: None,
        user: None,
        password: None,
        retention: Duration::from_hours(24),
    }
}

#[cfg(test)]
mod test_runner {
    use super::super::V1Operations;
    use super::test_cassandra_config;
    use crate::test_util::{TEST_KEYSPACE, TEST_RUNTIME, integration_test_count};
    use crate::timers::store::cassandra::queries::Queries;
    use crate::tracing::init_test_logging;
    use quickcheck::{QuickCheck, TestResult};
    use std::sync::Arc;
    use tracing::Instrument;
    use tracing::span::EnteredSpan;

    async fn create_v1_operations() -> color_eyre::Result<V1Operations> {
        use crate::cassandra::CassandraStore;

        let config = test_cassandra_config(TEST_KEYSPACE);
        let store = CassandraStore::new(&config).await?;
        let queries = Arc::new(Queries::new(store.session(), &config.keyspace).await?);

        Ok(V1Operations::new(store, queries))
    }

    /// Initialize test tracing with OpenTelemetry layer and return an active
    /// span guard
    fn init_test_tracing() -> EnteredSpan {
        use tracing::info_span;

        init_test_logging();

        // Return an active span guard to ensure Span::current() works during test
        info_span!("test").entered()
    }

    /// Runs one `QuickCheck` trial: fresh `V1Operations` (a session per trial
    /// is deliberate), then `prop` against it on the shared test runtime.
    fn run_v1_property<I>(
        input: I,
        prop: impl AsyncFnOnce(&V1Operations, I) -> color_eyre::Result<()>,
    ) -> TestResult {
        let span = tracing::Span::current();
        let runtime = &*TEST_RUNTIME;

        let operations = match runtime.block_on(create_v1_operations().instrument(span.clone())) {
            Ok(ops) => ops,
            Err(e) => {
                return TestResult::error(format!("Failed to create V1Operations: {e:?}"));
            }
        };

        match runtime.block_on(async { prop(&operations, input).await }.instrument(span)) {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(format!("{e:?}")),
        }
    }

    #[test]
    fn prop_v1_slab_metadata_model_equivalence() {
        use super::prop_slab_metadata::{
            V1SlabMetadataTestInput, prop_v1_slab_metadata_model_equivalence,
        };

        fn test_wrapper(input: V1SlabMetadataTestInput) -> TestResult {
            run_v1_property(input, prop_v1_slab_metadata_model_equivalence)
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(integration_test_count(50))
            .quickcheck(test_wrapper as fn(V1SlabMetadataTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_slab_trigger_model_equivalence() {
        use super::prop_slab_triggers::{
            V1SlabTriggerTestInput, prop_v1_slab_trigger_model_equivalence,
        };

        fn test_wrapper(input: V1SlabTriggerTestInput) -> TestResult {
            run_v1_property(input, prop_v1_slab_trigger_model_equivalence)
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(integration_test_count(50))
            .quickcheck(test_wrapper as fn(V1SlabTriggerTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_key_trigger_model_equivalence() {
        use super::prop_key_triggers::{
            V1KeyTriggerTestInput, prop_v1_key_trigger_model_equivalence,
        };

        fn test_wrapper(input: V1KeyTriggerTestInput) -> TestResult {
            run_v1_property(input, prop_v1_key_trigger_model_equivalence)
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(integration_test_count(50))
            .quickcheck(test_wrapper as fn(V1KeyTriggerTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_high_level_dual_index_consistency() {
        use super::prop_high_level::{
            V1HighLevelTestInput, prop_v1_high_level_dual_index_consistency,
        };

        fn test_wrapper(input: V1HighLevelTestInput) -> TestResult {
            run_v1_property(input, prop_v1_high_level_dual_index_consistency)
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(integration_test_count(25))
            .quickcheck(test_wrapper as fn(V1HighLevelTestInput) -> TestResult);
    }

    #[test]
    fn prop_migration_invariants() {
        use super::prop_migration::{MigrationTestInput, prop_migration_invariants};

        // Boxed so the large migration future doesn't sit on the stack; a fn
        // item (not a closure) so the higher-ranked `AsyncFnOnce` bound infers.
        async fn boxed_migration_prop(
            operations: &V1Operations,
            input: MigrationTestInput,
        ) -> color_eyre::Result<()> {
            Box::pin(prop_migration_invariants(operations, input)).await
        }

        fn test_wrapper(input: MigrationTestInput) -> TestResult {
            run_v1_property(input, boxed_migration_prop)
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(integration_test_count(10))
            .quickcheck(test_wrapper as fn(MigrationTestInput) -> TestResult);
    }
}
