//! The client's one shared infrastructure bundle: building it, memoizing it
//! into the consumer state, and handing it to a consumer being built.

use crate::Codec;
use crate::consumer::{CommonConfiguration, ConsumerConfiguration, ConsumerSetup};
use crate::high_level::config::{ModeConfiguration, TriggerStoreConfiguration};
use crate::high_level::error::HighLevelClientError;
use crate::high_level::state::ConsumerState;
use crate::loader::MemoryLoader;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::{DEFAULT_READER_CACHE_SIZE_BYTES, SharedDeps};

/// The bundle for `state`, building and memoizing it into a `Configured` state
/// on first need. The caller holds the `consumer` lock, so this exclusive
/// `&mut` access is race-free. `Running` already carries its bundle;
/// `Unconfigured`/`ConfigurationFailed` have no config to build from and error.
pub(super) async fn get_or_build<T, C: Codec>(
    state: &mut ConsumerState<T, C>,
) -> Result<SharedDeps<C>, HighLevelClientError<C::Error>>
where
    C::Payload: Clone,
{
    match state {
        ConsumerState::Running { deps, .. } => Ok(deps.clone()),
        ConsumerState::Configured { config, deps } => {
            if let Some(existing) = deps.as_ref() {
                return Ok(existing.clone());
            }
            let built = build(config).await?;
            *deps = Some(built.clone());
            Ok(built)
        }
        ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => {
            Err(HighLevelClientError::UnconfiguredConsumer)
        }
    }
}

/// Builds a bundle from a mode configuration.
///
/// The bundle depends only on the trigger-store backend, group id, and cache
/// budget, all mode-independent, so one build serves any mode. This mirrors the
/// Cassandra-session reuse in `StorePair::new`. `InMemory` selects the memory
/// arm whether or not the client is in mock mode; its bundle carries the shared
/// in-memory stores, so a reader built from it gets read-your-writes against
/// the running consumer.
pub(super) async fn build<C: Codec>(
    mode: &ModeConfiguration,
) -> Result<SharedDeps<C>, HighLevelClientError<C::Error>>
where
    C::Payload: Clone,
{
    let (consumer, keyed_state, trigger_store) = match mode {
        ModeConfiguration::Pipeline {
            consumer,
            common,
            trigger_store,
            ..
        }
        | ModeConfiguration::LowLatency {
            consumer,
            common,
            trigger_store,
            ..
        }
        | ModeConfiguration::BestEffort {
            consumer,
            common,
            trigger_store,
        } => (consumer, &common.keyed_state, trigger_store),
    };
    // One knob: the reader cache follows `read_cache_size_bytes`, then
    // `cache_size_bytes`, then the built-in default.
    let budget = keyed_state
        .read_cache_size_bytes
        .or(keyed_state.cache_size_bytes)
        .unwrap_or(DEFAULT_READER_CACHE_SIZE_BYTES);
    let deps = match trigger_store {
        TriggerStoreConfiguration::InMemory => SharedDeps::memory(
            consumer.group_id.clone(),
            consumer.stall_threshold,
            MemoryCells::new(),
            MemoryPublicationStore::new(),
            MemoryDescriptorIdentityStore::new(),
            MemoryLoader::new(),
            budget.get(),
        ),
        TriggerStoreConfiguration::Cassandra(cassandra) => {
            SharedDeps::connect(consumer, cassandra, budget).await?
        }
    };
    Ok(deps.with_default_read_cache_ttl(keyed_state.read_cache_ttl))
}

/// Pairs a mode's configuration sections with `deps`, so every mode's consumer
/// reuses the one Cassandra session and loader the client already opened.
pub(super) fn consumer_setup<'a, C: Codec>(
    consumer: &'a ConsumerConfiguration,
    trigger_store: &'a TriggerStoreConfiguration,
    common: &'a CommonConfiguration,
    deps: &SharedDeps<C>,
) -> ConsumerSetup<'a, C> {
    ConsumerSetup {
        consumer,
        trigger_store,
        common,
        deps: Some(deps.clone()),
    }
}
