//! Lazy construction of the client's shared reader family.

use crate::Codec;
use crate::consumer::TypedConsumerSetup;
use crate::consumer::{CommonConfiguration, ConsumerConfiguration};
use crate::high_level::ClientBackend;
use crate::high_level::config::ModeConfiguration;
use crate::high_level::error::HighLevelClientError;
use crate::high_level::state::ConsumerState;
use crate::state_reader::SharedDeps;

pub(super) async fn get_or_build<T, C, B>(
    state: &mut ConsumerState<T, C, B>,
    backend: &B,
) -> Result<SharedDeps<C, B::Reader>, HighLevelClientError<C::Error>>
where
    C: Codec,
    C::Payload: Clone,
    B: ClientBackend<C>,
{
    match state {
        ConsumerState::Running { deps, .. } => Ok(deps.clone()),
        ConsumerState::Configured { config, deps } => {
            if let Some(existing) = deps {
                return Ok(existing.clone());
            }
            let built = build(config, backend).await?;
            *deps = Some(built.clone());
            Ok(built)
        }
        ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => {
            Err(HighLevelClientError::UnconfiguredConsumer)
        }
    }
}

pub(super) async fn build<C, B>(
    mode: &ModeConfiguration,
    backend: &B,
) -> Result<SharedDeps<C, B::Reader>, HighLevelClientError<C::Error>>
where
    C: Codec,
    C::Payload: Clone,
    B: ClientBackend<C>,
{
    let (consumer, keyed_state) = match mode {
        ModeConfiguration::Pipeline {
            consumer, common, ..
        }
        | ModeConfiguration::LowLatency {
            consumer, common, ..
        }
        | ModeConfiguration::BestEffort {
            consumer, common, ..
        } => (consumer, &common.keyed_state),
    };
    backend
        .build_reader(consumer, keyed_state)
        .await
        .map_err(HighLevelClientError::StateReader)
}

pub(super) fn consumer_setup<'a, C, B>(
    consumer: &'a ConsumerConfiguration,
    common: &'a CommonConfiguration,
    deps: &SharedDeps<C, B::Reader>,
) -> TypedConsumerSetup<'a, C, B::Reader>
where
    C: Codec,
    B: ClientBackend<C>,
{
    TypedConsumerSetup {
        consumer,
        common,
        deps: deps.clone(),
    }
}
