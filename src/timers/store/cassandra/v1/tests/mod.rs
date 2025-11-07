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
    use quickcheck::{QuickCheck, TestResult};
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

        let config = test_cassandra_config("prosody_test_v1");
        let store = CassandraStore::new(&config).await?;
        let queries = Arc::new(Queries::new(store.session(), &config.keyspace).await?);

        Ok(V1Operations::new(store, queries))
    }

    /// Generic helper to run V1 property tests with runtime and operations
    /// setup. Runtime is kept alive for the duration of the test function.
    fn run_v1_test<I>(
        input: I,
        test_fn: impl FnOnce(&V1Operations, I) -> TestResult,
    ) -> TestResult {
        // Initialize tracing subscriber to create valid spans in tests
        // try_init() returns Err if already initialized, which is fine
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        // Create runtime for this test invocation
        let runtime = match Builder::new_multi_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
        };

        // Create V1Operations instance
        let operations = match runtime.block_on(async { create_v1_operations().await }) {
            Ok(ops) => ops,
            Err(e) => return TestResult::error(format!("Failed to create V1Operations: {e:?}")),
        };

        test_fn(&operations, input)
    }

    #[test]
    fn prop_v1_slab_metadata_model_equivalence() {
        use super::prop_slab_metadata::{
            V1SlabMetadataTestInput, test_prop_v1_slab_metadata_model_equivalence,
        };

        fn test_wrapper(input: V1SlabMetadataTestInput) -> TestResult {
            run_v1_test(input, test_prop_v1_slab_metadata_model_equivalence)
        }

        QuickCheck::new().quickcheck(test_wrapper as fn(V1SlabMetadataTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_slab_trigger_model_equivalence() {
        use super::prop_slab_triggers::{
            V1SlabTriggerTestInput, test_prop_v1_slab_trigger_model_equivalence,
        };

        fn test_wrapper(input: V1SlabTriggerTestInput) -> TestResult {
            run_v1_test(input, test_prop_v1_slab_trigger_model_equivalence)
        }

        QuickCheck::new().quickcheck(test_wrapper as fn(V1SlabTriggerTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_key_trigger_model_equivalence() {
        use super::prop_key_triggers::{
            V1KeyTriggerTestInput, test_prop_v1_key_trigger_model_equivalence,
        };

        fn test_wrapper(input: V1KeyTriggerTestInput) -> TestResult {
            run_v1_test(input, test_prop_v1_key_trigger_model_equivalence)
        }

        QuickCheck::new().quickcheck(test_wrapper as fn(V1KeyTriggerTestInput) -> TestResult);
    }

    #[test]
    fn prop_v1_high_level_dual_index_consistency() {
        use super::prop_high_level::{
            V1HighLevelTestInput, test_prop_v1_high_level_dual_index_consistency,
        };

        fn test_wrapper(input: V1HighLevelTestInput) -> TestResult {
            run_v1_test(input, test_prop_v1_high_level_dual_index_consistency)
        }

        QuickCheck::new().quickcheck(test_wrapper as fn(V1HighLevelTestInput) -> TestResult);
    }

    #[test]
    fn prop_migration_invariants() {
        use super::prop_migration::{MigrationTestInput, test_prop_migration_invariants};

        fn test_wrapper(input: MigrationTestInput) -> TestResult {
            run_v1_test(input, test_prop_migration_invariants)
        }

        QuickCheck::new().quickcheck(test_wrapper as fn(MigrationTestInput) -> TestResult);
    }
}
