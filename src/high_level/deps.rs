//! Shared reader components for the high-level client.

use crate::Codec;
use crate::consumer::TypedConsumerSetup;
use crate::consumer::{CommonConfiguration, ConsumerConfiguration};
use crate::high_level::ClientBackend;
use crate::high_level::config::ModeConfiguration;
use crate::loader::LoaderConfiguration;
use crate::state_reader::StateReaderDependencies;
use std::num::NonZeroU64;
use std::time::Duration;

/// The reader-only subset passed through the sealed backend contract.
///
/// This type is public only because Rust requires every type named by the
/// public [`ClientBackend`] signature to be public. Its fields and construction
/// remain internal.
pub struct ReaderConfiguration {
    pub(super) loader: LoaderConfiguration,
    pub(super) group_id: String,
    pub(super) stall_threshold: Duration,
    pub(super) cache_size: NonZeroU64,
    pub(super) cache_ttl: Option<Duration>,
}

impl ReaderConfiguration {
    pub(super) fn from_mode(mode: &ModeConfiguration) -> Self {
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
        Self {
            loader: LoaderConfiguration::for_consumer(consumer, keyed_state.subsystem.as_ref()),
            group_id: consumer.group_id.clone(),
            stall_threshold: consumer.stall_threshold,
            cache_size: keyed_state.reader_cache_size(),
            cache_ttl: keyed_state.read_cache_ttl,
        }
    }
}

pub(super) fn consumer_setup<'a, C, B>(
    consumer: &'a ConsumerConfiguration,
    common: &'a CommonConfiguration,
    deps: &StateReaderDependencies<C, B::Reader>,
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
