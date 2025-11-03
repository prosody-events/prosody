//! Tests for V1 schema operations (Cassandra-only).

// High-level tests disabled - require coordinated V1 operations (add_trigger_v1,
// remove_trigger_v1, clear_triggers_for_key_v1) that haven't been implemented yet.
// TODO: Implement coordinated V1 operations and re-enable these tests.
// #[cfg(test)]
// pub mod prop_high_level;
#[cfg(test)]
pub mod prop_key_triggers;
#[cfg(test)]
pub mod prop_slab_metadata;
#[cfg(test)]
pub mod prop_slab_triggers;

#[cfg(test)]
mod test_runner {
    use super::super::V1Operations;
    use crate::cassandra::CassandraConfiguration;
    use crate::timers::store::cassandra::queries::Queries;
    use quickcheck::QuickCheck;
    use std::sync::{Arc, LazyLock};

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

    // Shared runtime instance, created lazily
    static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        use tokio::runtime::Builder;
        Builder::new_multi_thread().enable_all().build().unwrap()
    });

    // Shared V1Operations instance (for V1 property tests)
    static OPERATIONS: LazyLock<V1Operations> = LazyLock::new(|| {
        RUNTIME.block_on(async {
            create_v1_operations()
                .await
                .expect("Failed to create shared V1Operations instance")
        })
    });

    // Helper function to get the shared V1Operations
    fn get_operations() -> &'static V1Operations {
        &OPERATIONS
    }

    #[test]
    fn prop_v1_slab_metadata_model_equivalence() {
        use super::prop_slab_metadata::{
            V1SlabMetadataTestInput, test_prop_v1_slab_metadata_model_equivalence,
        };

        fn test_wrapper(input: V1SlabMetadataTestInput) -> quickcheck::TestResult {
            let operations = get_operations();
            test_prop_v1_slab_metadata_model_equivalence(operations, input)
        }

        QuickCheck::new()
            .quickcheck(test_wrapper as fn(V1SlabMetadataTestInput) -> quickcheck::TestResult);
    }

    #[test]
    fn prop_v1_slab_trigger_model_equivalence() {
        use super::prop_slab_triggers::{
            V1SlabTriggerTestInput, test_prop_v1_slab_trigger_model_equivalence,
        };

        fn test_wrapper(input: V1SlabTriggerTestInput) -> quickcheck::TestResult {
            let operations = get_operations();
            test_prop_v1_slab_trigger_model_equivalence(operations, input)
        }

        QuickCheck::new()
            .quickcheck(test_wrapper as fn(V1SlabTriggerTestInput) -> quickcheck::TestResult);
    }

    #[test]
    fn prop_v1_key_trigger_model_equivalence() {
        use super::prop_key_triggers::{
            V1KeyTriggerTestInput, test_prop_v1_key_trigger_model_equivalence,
        };

        fn test_wrapper(input: V1KeyTriggerTestInput) -> quickcheck::TestResult {
            let operations = get_operations();
            test_prop_v1_key_trigger_model_equivalence(operations, input)
        }

        QuickCheck::new()
            .quickcheck(test_wrapper as fn(V1KeyTriggerTestInput) -> quickcheck::TestResult);
    }

    // TODO: Implement coordinated V1 operations (add_trigger_v1,
    // remove_trigger_v1, clear_triggers_for_key_v1, delete_slab_v1) that
    // maintain dual-index consistency. Then re-enable prop_high_level
    // module and add test runner here.
    //
    // #[test]
    // fn prop_v1_high_level_dual_index_consistency() {
    //     use super::prop_high_level::{...};
    //     ...
    // }
}
