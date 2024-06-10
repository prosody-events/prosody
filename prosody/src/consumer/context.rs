use std::collections::hash_map::Entry;
use std::future::ready;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::ClientContext;
use tokio::runtime::Handle;
use tracing::{error, warn};

use crate::consumer::partition::PartitionManager;
use crate::consumer::{ConsumerConfiguration, Managers, MessageHandler, WatermarkVersion};
use crate::Topic;

pub struct Context<T> {
    buffer_size: usize,
    max_uncommitted: usize,
    max_enqueued_per_key: usize,
    shutdown_timeout: Option<Duration>,
    message_handler: T,
    watermark_version: Arc<WatermarkVersion>,
    managers: Arc<Managers>,
}

impl<T> Context<T> {
    pub fn new(
        config: &ConsumerConfiguration,
        message_handler: T,
        watermark_version: Arc<WatermarkVersion>,
        managers: Arc<Managers>,
    ) -> Self {
        Self {
            buffer_size: config.max_uncommitted,
            max_uncommitted: config.max_uncommitted,
            max_enqueued_per_key: config.max_enqueued_per_key,
            shutdown_timeout: config.partition_shutdown_timeout,
            message_handler,
            watermark_version,
            managers,
        }
    }
}

impl<T> ClientContext for Context<T> where T: Send + Sync {}

impl<T> ConsumerContext for Context<T>
where
    T: MessageHandler + Clone + Send + Sync + 'static,
{
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        let Rebalance::Assign(partitions) = rebalance else {
            return;
        };

        // see: https://github.com/fede1024/rust-rdkafka/issues/681
        if partitions.count() == 0 {
            return;
        }

        for element in partitions.elements() {
            let topic = Topic::from(element.topic());
            let partition = element.partition();
            let mut managers = self.managers.lock();

            let Entry::Vacant(vacant) = managers.entry((topic, partition)) else {
                warn!("topic {topic} partition {partition} was already assigned");
                continue;
            };

            let manager = PartitionManager::new(
                element.partition(),
                self.message_handler.clone(),
                self.buffer_size,
                self.max_uncommitted,
                self.max_enqueued_per_key,
                self.shutdown_timeout,
                self.watermark_version.clone(),
            );

            vacant.insert(manager);
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        let Rebalance::Revoke(partitions) = rebalance else {
            return;
        };

        // see: https://github.com/fede1024/rust-rdkafka/issues/681
        if partitions.count() == 0 {
            return;
        }

        let shutdown_futures = FuturesUnordered::new();
        for element in partitions.elements() {
            let topic = Topic::from(element.topic());
            let partition = element.partition();
            let Some(manager) = self.managers.lock().remove(&(topic, partition)) else {
                error!("cannot revoke topic {topic} partition {partition}; not assigned");
                continue;
            };
            shutdown_futures.push(manager.shutdown());
        }

        Handle::current().block_on(shutdown_futures.for_each(|_| ready(())));
    }
}
