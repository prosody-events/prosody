//! High-level consumer subscription state.

use crate::Codec;
use crate::consumer::ProsodyConsumer;
use crate::high_level::config::ModeConfiguration;
use educe::Educe;
use std::ops::Deref;
use tokio::sync::MutexGuard;

/// A wrapper around a mutex guard for `ConsumerState`.
///
/// This type provides a view into the current state of the consumer,
/// allowing read-only access to the underlying `ConsumerState`.
pub struct ConsumerStateView<'a, T, C: Codec>(pub(crate) MutexGuard<'a, ConsumerState<T, C>>);

impl<T, C: Codec> Deref for ConsumerStateView<'_, T, C> {
    type Target = ConsumerState<T, C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Represents the current state of the consumer.
///
/// Reader infrastructure belongs to the high-level client's reader component,
/// not this subscription state machine.
#[derive(Educe)]
#[educe(Debug)]
pub enum ConsumerState<T, C: Codec> {
    /// The consumer is configured but not running.
    Configured {
        /// The configuration to run when subscribed.
        config: ModeConfiguration,
    },
    /// The consumer is actively running.
    Running {
        /// The active Prosody consumer instance.
        consumer: ProsodyConsumer<C>,
        /// The configuration used for this consumer.
        config: ModeConfiguration,
        /// The handler for processing messages.
        handler: T,
    },
}
