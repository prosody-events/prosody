use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use crossbeam_utils::CachePadded;
use rdkafka::ClientContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use scc::HashMap;
use tokio::runtime::Handle;
use tracing::error;

use crate::{Partition, Topic};
use crate::consumer::MessageHandler;
use crate::consumer::partition::PartitionManager;

pub struct Context<T> {
    buffer_size: usize,
    max_uncommitted: usize,
    max_enqueued: usize,
    shutdown_timeout: Option<Duration>,
    message_handler: T,
    watermark_version: Arc<CachePadded<AtomicUsize>>,
    managers: Arc<HashMap<(Topic, Partition), PartitionManager>>,
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

        for element in partitions.elements() {
            let topic = Topic::from(element.topic());
            let partition = element.partition();
            let manager = PartitionManager::new(
                element.partition(),
                self.message_handler.clone(),
                self.buffer_size,
                self.max_uncommitted,
                self.max_enqueued,
                self.shutdown_timeout,
                self.watermark_version.clone(),
            );

            if self.managers.insert((topic, partition), manager).is_err() {
                error!("cannot assign topic {topic} partition {partition}; already assigned");
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        let Rebalance::Revoke(partitions) = rebalance else {
            return;
        };

        for element in partitions.elements() {
            let topic = Topic::from(element.topic());
            let partition = element.partition();
            let Some((_, manager)) = self.managers.remove(&(topic, partition)) else {
                error!("cannot revoke topic {topic} partition {partition}; not assigned");
                continue;
            };
            Handle::current().block_on(manager.shutdown());
        }
    }
}
