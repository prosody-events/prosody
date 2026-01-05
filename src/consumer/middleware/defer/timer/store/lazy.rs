//! Lazy-initialized store wrapper for deferred timer storage.

use super::{TimerDeferStore, TimerRetryCompletionResult};
use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Factory for creating [`TimerDeferStore`] instances.
///
/// Implementations provide the async initialization logic for
/// [`LazyTimerDeferStore`].
pub trait TimerDeferStoreFactory: Clone + Send + Sync + 'static {
    /// The store type this factory creates.
    type Store: TimerDeferStore;

    /// Creates the store asynchronously.
    fn create(
        self,
    ) -> impl Future<Output = Result<Self::Store, <Self::Store as TimerDeferStore>::Error>> + Send;
}

/// Lazy-initialized timer defer store wrapper.
///
/// Defers store creation until first use. The factory is called at most once;
/// subsequent accesses reuse the initialized store. Thread-safe via
/// [`tokio::sync::OnceCell`].
pub struct LazyTimerDeferStore<F: TimerDeferStoreFactory> {
    /// Lazy cell for the store.
    cell: Arc<OnceCell<F::Store>>,
    /// Factory for initialization.
    factory: F,
}

impl<F: TimerDeferStoreFactory> Clone for LazyTimerDeferStore<F> {
    fn clone(&self) -> Self {
        Self {
            cell: Arc::clone(&self.cell),
            factory: self.factory.clone(),
        }
    }
}

impl<F: TimerDeferStoreFactory> LazyTimerDeferStore<F> {
    /// Creates a new lazy store with the given factory.
    ///
    /// The factory will be called at most once, on the first access to
    /// any store method. If multiple callers race, only one factory
    /// invocation runs; others wait for the result.
    #[must_use]
    pub fn new(factory: F) -> Self {
        Self {
            cell: Arc::new(OnceCell::new()),
            factory,
        }
    }

    /// Returns whether the store has been initialized.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.cell.initialized()
    }

    /// Gets or initializes the underlying store.
    ///
    /// # Errors
    ///
    /// Returns the factory's error if store creation fails.
    async fn get_store(&self) -> Result<&F::Store, <F::Store as TimerDeferStore>::Error> {
        let factory = self.factory.clone();
        self.cell.get_or_try_init(|| factory.create()).await
    }
}

/// Implements lazy delegation for [`TimerDeferStore`] methods.
macro_rules! delegate_timer_defer_store {
    ($($method:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)*) => {
        $(
            async fn $method(&self, $($arg: $ty),*) -> $ret {
                self.get_store().await?.$method($($arg),*).await
            }
        )*
    };
}

impl<F: TimerDeferStoreFactory> TimerDeferStore for LazyTimerDeferStore<F> {
    type Error = <F::Store as TimerDeferStore>::Error;

    delegate_timer_defer_store! {
        defer_first_timer(trigger: &Trigger) -> Result<(), Self::Error>;
        defer_additional_timer(trigger: &Trigger) -> Result<(), Self::Error>;
        complete_retry_success(key: &Key, time: CompactDateTime) -> Result<TimerRetryCompletionResult, Self::Error>;
        increment_retry_count(key: &Key, current_retry_count: u32) -> Result<u32, Self::Error>;
        get_next_deferred_timer(key: &Key) -> Result<Option<(Trigger, u32)>, Self::Error>;
        is_deferred(key: &Key) -> Result<Option<u32>, Self::Error>;
        append_deferred_timer(trigger: &Trigger) -> Result<(), Self::Error>;
        remove_deferred_timer(key: &Key, time: CompactDateTime) -> Result<(), Self::Error>;
        set_retry_count(key: &Key, retry_count: u32) -> Result<(), Self::Error>;
        delete_key(key: &Key) -> Result<(), Self::Error>;
    }

    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        let this = self.clone();
        let key = key.clone();

        try_stream! {
            let store = this.get_store().await?;
            let stream = store.deferred_times(&key);

            pin_mut!(stream);
            while let Some(time) = stream.try_next().await? {
                yield time;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::middleware::defer::segment::Segment;
    use crate::consumer::middleware::defer::timer::store::TimerDeferStoreProvider;
    use crate::consumer::middleware::defer::timer::store::memory::{
        MemoryTimerDeferStore, MemoryTimerDeferStoreProvider,
    };
    use crate::timers::TimerType;
    use crate::{ConsumerGroup, Partition, Topic};
    use tracing::Span;

    /// Test factory that creates memory-backed stores.
    #[derive(Clone)]
    struct TestFactory {
        provider: MemoryTimerDeferStoreProvider,
        segment: Segment,
    }

    impl TimerDeferStoreFactory for TestFactory {
        type Store = MemoryTimerDeferStore;

        async fn create(self) -> Result<Self::Store, <Self::Store as TimerDeferStore>::Error> {
            self.provider.create_store(&self.segment).await
        }
    }

    fn test_factory() -> TestFactory {
        let segment = Segment::new(
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-group") as ConsumerGroup,
        );
        TestFactory {
            provider: MemoryTimerDeferStoreProvider::new(),
            segment,
        }
    }

    fn test_trigger(key: &str, time_secs: u32) -> Trigger {
        let key: Key = Arc::from(key);
        let time = CompactDateTime::from(time_secs);
        Trigger::new(key, time, TimerType::Application, Span::current())
    }

    #[tokio::test]
    async fn test_lazy_initialization() -> color_eyre::Result<()> {
        let lazy_store = LazyTimerDeferStore::new(test_factory());

        // Store should not be initialized yet
        assert!(!lazy_store.is_initialized());

        // First access triggers initialization
        let key: Key = Arc::from("test-key");
        let result = lazy_store.get_next_deferred_timer(&key).await?;
        assert!(result.is_none());

        // Store should now be initialized
        assert!(lazy_store.is_initialized());

        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_store_operations() -> color_eyre::Result<()> {
        let lazy_store = LazyTimerDeferStore::new(test_factory());
        let trigger = test_trigger("test-key", 1000);

        // Defer a timer
        lazy_store.defer_first_timer(&trigger).await?;

        // Verify it's stored
        let result = lazy_store.get_next_deferred_timer(&trigger.key).await?;
        assert!(result.is_some());
        let (retrieved, retry_count) =
            result.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(retrieved.time, trigger.time);
        assert_eq!(retry_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_clone_shares_initialization() -> color_eyre::Result<()> {
        let lazy_store = LazyTimerDeferStore::new(test_factory());
        let clone = lazy_store.clone();

        // Initialize via original
        let trigger = test_trigger("test-key", 1000);
        lazy_store.defer_first_timer(&trigger).await?;

        // Clone shares the same OnceCell, so sees the same data
        let result = clone.get_next_deferred_timer(&trigger.key).await?;
        assert!(result.is_some());
        let (retrieved, _) = result.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(retrieved.time, trigger.time);

        Ok(())
    }
}
