//! A real consumer construction run: the no-op handler and the memory-arm
//! wiring that drive `initialize_consumer` end to end.

use super::super::KafkaObserver;
use crate::JsonCodec;
use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::ConsumerError;
use crate::consumer::kafka_context::PartitionProviders;
use crate::consumer::middleware::CloneProvider;
use crate::consumer::middleware::deduplication::DEFAULT_IDEMPOTENCE_VERSION;
use crate::consumer::middleware::deduplication::MemoryDeduplicationStoreProvider;
use crate::consumer::middleware::tests::test_support::SilentHandler;
use crate::consumer::wiring::peer::NoPeer;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::state::{KeyedStateInputs, memory_state_provider};
use crate::consumer::{Managers, ProsodyConsumer};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MemoryLoader;
use crate::state::config::KeyedStateConfiguration;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::telemetry::Telemetry;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use color_eyre::Result;
use serde_json::Value;
use std::sync::Arc;

/// A no-op handler. The startup tests build a real consumer but deliver nothing
/// to it.
/// Runs the real `initialize_consumer` with `observer`, mirroring the memory
/// arm of the direct mode. The outer result is test setup; the inner one is
/// what construction returned.
pub(super) async fn initialize_with(
    config: &ConsumerConfiguration,
    observer: KafkaObserver,
) -> Result<Result<ProsodyConsumer<JsonCodec>, ConsumerError>> {
    let telemetry = Telemetry::new();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
    let keyed_state = KeyedStateInputs::new(
        KeyedStateConfiguration::builder().build()?,
        config,
        DEFAULT_IDEMPOTENCE_VERSION,
    )?;
    let state_provider = memory_state_provider::<JsonCodec>(
        &keyed_state,
        MemoryDeduplicationStoreProvider::new(),
        MemoryCells::new(),
        MemoryDescriptorIdentityStore::new(),
        MemoryLoader::<Value>::new(),
        None,
    );
    let managers: Arc<Managers<Value>> = Arc::default();
    Ok(Box::pin(initialize_consumer::<_, _, _, JsonCodec, _>(
        config,
        CloneProvider::new(SilentHandler),
        PartitionProviders {
            triggers: InMemoryTriggerStoreProvider::new(),
            state: state_provider,
        },
        StartupServices {
            version: keyed_state.version.clone(),
            telemetry: &telemetry,
            heartbeats,
            observer,
            managers,
        },
        NoPeer,
    ))
    .await)
}
