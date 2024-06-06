use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use ahash::HashMap;
use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use rdkafka::ClientContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use tokio::runtime::Handle;
use tracing::{error, warn};

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
    managers: Arc<Mutex<HashMap<(Topic, Partition), PartitionManager>>>,
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
                self.max_enqueued,
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

        for element in partitions.elements() {
            let topic = Topic::from(element.topic());
            let partition = element.partition();
            let Some(manager) = self.managers.lock().remove(&(topic, partition)) else {
                error!("cannot revoke topic {topic} partition {partition}; not assigned");
                continue;
            };
            Handle::current().block_on(manager.shutdown());
        }
    }
}
