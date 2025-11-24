//! Tests for V1 schema operations (Cassandra-only).

#[cfg(test)]
pub mod prop_high_level;
#[cfg(test)]
pub mod prop_key_triggers;
#[cfg(test)]
pub mod prop_migration;
#[cfg(test)]
pub mod prop_slab_metadata;
#[cfg(test)]
pub mod prop_slab_triggers;

#[cfg(test)]
mod test_runner {
    use super::super::V1Operations;
    use crate::cassandra::CassandraConfiguration;
    use crate::timers::store::cassandra::queries::Queries;
    use crate::tracing::init_test_logging;
    use quickcheck::{QuickCheck, TestResult};
    use std::env;
    use std::sync::Arc;
    use tokio::runtime::Builder;

    fn test_cassandra_config(keyspace: &str) -> CassandraConfiguration {
        use std::time::Duration;

        CassandraConfiguration {
            nodes: vec!["127.0.0.1:9042".to_owned()],
            keyspace: keyspace.to_owned(),
            datacenter: Some("datacenter1".to_owned()),
            rack: None,
            user: None,
            password: None,
            retention: Duration::from_secs(86400),
        }
    }

    async fn create_v1_operations() -> color_eyre::Result<V1Operations> {
        use crate::cassandra::CassandraStore;

        let config = test_cassandra_config("prosody_test");
        let store = CassandraStore::new(&config).await?;
        let queries = Arc::new(Queries::new(store.session(), &config.keyspace).await?);

        Ok(V1Operations::new(store, queries))
    }

    /// Determine the number of tests to run from an environment variable.
    /// Returns the appropriate default based on test complexity if not set.
    /// Uses `INTEGRATION_TESTS` since these tests hit a real Cassandra
    /// database.
    fn get_test_count(default: u64) -> u64 {
        env::var("INTEGRATION_TESTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(default)
    }

    use tracing::span::EnteredSpan;

    /// Initialize test tracing with OpenTelemetry layer and return an active
    /// span guard
    fn init_test_tracing() -> EnteredSpan {
        use tracing::info_span;

        init_test_logging();

        // Return an active span guard to ensure Span::current() works during test
        info_span!("test").entered()
    }

    #[test]
    fn prop_v1_slab_metadata_model_equivalence() {
        use super::prop_slab_metadata::{
            V1SlabMetadataTestInput, prop_v1_slab_metadata_model_equivalence,
        };

        fn test_wrapper(input: V1SlabMetadataTestInput) -> TestResult {
            use tracing::Instrument;

            let span = tracing::Span::current();

            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            let operations = match runtime
                .block_on(async { create_v1_operations().await }.instrument(span.clone()))
            {
                Ok(ops) => ops,
                Err(e) => {
                    return TestResult::error(format!("Failed to create V1Operations: {e:?}"));
                }
            };

            match runtime.block_on(
                async { prop_v1_slab_metadata_model_equivalence(&operations, input).await }
                    .instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(get_test_count(50))
            .quickcheck(test_wrapper as fn(V1SlabMetadataTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_slab_trigger_model_equivalence() {
        use super::prop_slab_triggers::{
            V1SlabTriggerTestInput, prop_v1_slab_trigger_model_equivalence,
        };

        fn test_wrapper(input: V1SlabTriggerTestInput) -> TestResult {
            use tracing::Instrument;

            let span = tracing::Span::current();

            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            let operations = match runtime
                .block_on(async { create_v1_operations().await }.instrument(span.clone()))
            {
                Ok(ops) => ops,
                Err(e) => {
                    return TestResult::error(format!("Failed to create V1Operations: {e:?}"));
                }
            };

            match runtime.block_on(
                async { prop_v1_slab_trigger_model_equivalence(&operations, input).await }
                    .instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(get_test_count(50))
            .quickcheck(test_wrapper as fn(V1SlabTriggerTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_key_trigger_model_equivalence() {
        use super::prop_key_triggers::{
            V1KeyTriggerTestInput, prop_v1_key_trigger_model_equivalence,
        };

        fn test_wrapper(input: V1KeyTriggerTestInput) -> TestResult {
            use tracing::Instrument;

            let span = tracing::Span::current();

            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            let operations = match runtime
                .block_on(async { create_v1_operations().await }.instrument(span.clone()))
            {
                Ok(ops) => ops,
                Err(e) => {
                    return TestResult::error(format!("Failed to create V1Operations: {e:?}"));
                }
            };

            match runtime.block_on(
                async { prop_v1_key_trigger_model_equivalence(&operations, input).await }
                    .instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(get_test_count(50))
            .quickcheck(test_wrapper as fn(V1KeyTriggerTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_high_level_dual_index_consistency() {
        use super::prop_high_level::{
            V1HighLevelTestInput, prop_v1_high_level_dual_index_consistency,
        };

        fn test_wrapper(input: V1HighLevelTestInput) -> TestResult {
            use tracing::Instrument;

            let span = tracing::Span::current();

            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            let operations = match runtime
                .block_on(async { create_v1_operations().await }.instrument(span.clone()))
            {
                Ok(ops) => ops,
                Err(e) => {
                    return TestResult::error(format!("Failed to create V1Operations: {e:?}"));
                }
            };

            match runtime.block_on(
                async { prop_v1_high_level_dual_index_consistency(&operations, input).await }
                    .instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(get_test_count(25))
            .quickcheck(test_wrapper as fn(V1HighLevelTestInput) -> TestResult);
    }

    #[test]
    fn prop_migration_invariants() {
        use super::prop_migration::{MigrationTestInput, prop_migration_invariants};

        fn test_wrapper(input: MigrationTestInput) -> TestResult {
            use tracing::Instrument;

            let span = tracing::Span::current();

            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            let operations = match runtime
                .block_on(async { create_v1_operations().await }.instrument(span.clone()))
            {
                Ok(ops) => ops,
                Err(e) => {
                    return TestResult::error(format!("Failed to create V1Operations: {e:?}"));
                }
            };

            match runtime.block_on(
                async { prop_migration_invariants(&operations, input).await }.instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        let _span = init_test_tracing();
        QuickCheck::new()
            .tests(get_test_count(10))
            .quickcheck(test_wrapper as fn(MigrationTestInput) -> TestResult);
    }
}
