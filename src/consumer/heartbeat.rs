use crossbeam_utils::CachePadded;
use educe::Educe;
use humantime::format_duration;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info};

const HEARTBEAT_MARGIN: u32 = 5;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct Heartbeat {
    #[educe(Debug(ignore))]
    inner: Arc<Inner>,
}

#[derive(Educe)]
#[educe(Debug)]
struct Inner {
    /// Heartbeat name
    name: Cow<'static, str>,

    /// Duration of inactivity allowed before considering a partition stalled
    stall_threshold: Duration,

    /// The reference instant when the partition started, used for liveness
    /// checks
    #[educe(Debug(ignore))]
    epoch: Instant,

    /// The duration in milliseconds between the epoch and the last event loop
    /// iteration, used for liveness checks.
    #[educe(Debug(ignore))]
    last_heartbeat: CachePadded<AtomicU64>,

    /// Last stall state used to prevent spamming the logs with stall errors
    last_state: CachePadded<AtomicBool>,
}

impl Heartbeat {
    pub fn new<T>(name: T, stall_threshold: Duration) -> Self
    where
        T: Into<Cow<'static, str>>,
    {
        Self {
            inner: Arc::new(Inner {
                name: name.into(),
                stall_threshold,
                epoch: Instant::now(),
                last_heartbeat: CachePadded::default(),
                last_state: CachePadded::default(),
            }),
        }
    }

    pub fn beat(&self) {
        self.inner.last_heartbeat.store(
            Instant::now().duration_since(self.inner.epoch).as_millis() as u64,
            Ordering::Release,
        );
    }

    pub async fn next(&self) {
        sleep(self.inner.stall_threshold / HEARTBEAT_MARGIN).await;
    }

    pub fn is_stalled(&self) -> bool {
        let heartbeat_millis = self.inner.last_heartbeat.load(Ordering::Acquire);
        let heartbeat_duration = Duration::from_millis(heartbeat_millis);
        let last_heartbeat = self.inner.epoch + heartbeat_duration;
        let inactivity = last_heartbeat.elapsed();
        let is_stalled = inactivity > self.inner.stall_threshold;
        let last_state = self.inner.last_state.load(Ordering::Acquire);

        match (last_state, is_stalled) {
            (false, true) => {
                self.inner.last_state.store(is_stalled, Ordering::Release);
                error!(
                    "{} has been inactive for {}, which exceeds the stall threshold of {}",
                    self.inner.name,
                    format_duration(inactivity),
                    format_duration(self.inner.stall_threshold)
                );
            }
            (true, false) => {
                self.inner.last_state.store(is_stalled, Ordering::Release);
                info!("{} is now active", self.inner.name);
            }
            _ => {}
        }

        is_stalled
    }
}
