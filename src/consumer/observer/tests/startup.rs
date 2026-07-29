//! A real consumer construction run: the no-op handler and the memory-arm
//! wiring that drive `initialize_consumer` end to end.

use super::super::KafkaObserver;
use crate::JsonCodec;
use crate::consumer::ProsodyConsumer;
use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::ConsumerError;
use crate::consumer::event_context::EventContext;
use crate::consumer::handler::{DemandType, EventHandler, Uncommitted};
use crate::consumer::message::UncommittedMessage;
use crate::consumer::middleware::CloneProvider;
use crate::consumer::middleware::deduplication::DEFAULT_IDEMPOTENCE_VERSION;
use crate::consumer::storage::StorePair;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::state::{KeyedStateInputs, memory_state_provider};
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::MemoryLoader;
use crate::state::config::KeyedStateConfiguration;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use color_eyre::Result;
use color_eyre::eyre::bail;
use serde_json::Value;
use std::num::NonZeroUsize;
use std::time::Duration;

/// A no-op handler. The startup tests build a real consumer but deliver nothing
/// to it.
#[derive(Clone)]
struct SilentHandler;

impl EventHandler for SilentHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let (_, uncommitted) = message.into_inner();
        uncommitted.commit().await;
    }

    async fn on_timer<C, T>(&self, _context: C, timer: T, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        T: UncommittedTimer,
    {
        timer.commit().await;
    }

    async fn shutdown(self) {}
}

/// Runs the real `initialize_consumer` with `observer`, mirroring the memory
/// arm of the direct mode. The outer result is test setup; the inner one is
/// what construction returned.
pub(super) async fn initialize_with(
    config: &ConsumerConfiguration,
    observer: KafkaObserver,
) -> Result<Result<ProsodyConsumer<JsonCodec>, ConsumerError>> {
    let telemetry = Telemetry::new();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
    let stores = StorePair::new(
        &TriggerStoreConfiguration::InMemory,
        config.mock,
        Duration::default(),
        NonZeroUsize::MIN,
        config.timer_spans,
    )
    .await?;
    let StorePair::Memory {
        trigger_provider,
        dedup_provider,
        ..
    } = stores
    else {
        bail!("the in-memory trigger store configuration must yield the memory arm");
    };
    let keyed_state = KeyedStateInputs::new(
        KeyedStateConfiguration::builder().build()?,
        config,
        DEFAULT_IDEMPOTENCE_VERSION,
    )?;
    let state_provider = memory_state_provider::<JsonCodec>(
        &keyed_state,
        dedup_provider,
        MemoryCells::new(),
        MemoryDescriptorIdentityStore::new(),
        MemoryLoader::<Value>::new(),
        None,
    );
    Ok(initialize_consumer::<_, _, _, JsonCodec>(
        config,
        CloneProvider::new(SilentHandler),
        trigger_provider,
        state_provider,
        StartupServices {
            version: keyed_state.version.clone(),
            telemetry: &telemetry,
            heartbeats,
            observer,
        },
    )
    .await)
}
