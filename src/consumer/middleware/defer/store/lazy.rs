//! Lazy-initialized store wrapper for deferred message storage.

use super::{MessageDeferStore, MessageRetryCompletionResult};
use crate::{Key, Offset};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Factory for creating [`MessageDeferStore`] instances.
///
/// Implementations provide the async initialization logic for [`LazyStore`].
pub trait StoreFactory: Clone + Send + Sync + 'static {
    /// The store type this factory creates.
    type Store: MessageDeferStore;

    /// Creates the store asynchronously.
    fn create(
        self,
    ) -> impl Future<Output = Result<Self::Store, <Self::Store as MessageDeferStore>::Error>> + Send;
}

/// Lazy-initialized store wrapper.
///
/// Defers store creation until first use. The factory is called at most once;
/// subsequent accesses reuse the initialized store. Thread-safe via
/// [`tokio::sync::OnceCell`].
pub struct LazyStore<F: StoreFactory> {
    /// Lazy cell for the store.
    cell: Arc<OnceCell<F::Store>>,
    /// Factory for initialization.
    factory: F,
}

impl<F: StoreFactory> Clone for LazyStore<F> {
    fn clone(&self) -> Self {
        Self {
            cell: Arc::clone(&self.cell),
            factory: self.factory.clone(),
        }
    }
}

impl<F: StoreFactory> LazyStore<F> {
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
    async fn get_store(&self) -> Result<&F::Store, <F::Store as MessageDeferStore>::Error> {
        let factory = self.factory.clone();
        self.cell.get_or_try_init(|| factory.create()).await
    }
}

/// Implements lazy delegation for [`MessageDeferStore`] methods.
macro_rules! delegate_defer_store {
    ($($method:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)*) => {
        $(
            async fn $method(&self, $($arg: $ty),*) -> $ret {
                self.get_store().await?.$method($($arg),*).await
            }
        )*
    };
}

impl<F: StoreFactory> MessageDeferStore for LazyStore<F> {
    type Error = <F::Store as MessageDeferStore>::Error;

    delegate_defer_store! {
        defer_first_message(key: &Key, offset: Offset) -> Result<(), Self::Error>;
        defer_additional_message(key: &Key, offset: Offset) -> Result<(), Self::Error>;
        complete_retry_success(key: &Key, offset: Offset) -> Result<MessageRetryCompletionResult, Self::Error>;
        increment_retry_count(key: &Key, current_retry_count: u32) -> Result<u32, Self::Error>;
        get_next_deferred_message(key: &Key) -> Result<Option<(Offset, u32)>, Self::Error>;
        is_deferred(key: &Key) -> Result<Option<u32>, Self::Error>;
        append_deferred_message(key: &Key, offset: Offset) -> Result<(), Self::Error>;
        remove_deferred_message(key: &Key, offset: Offset) -> Result<(), Self::Error>;
        set_retry_count(key: &Key, retry_count: u32) -> Result<(), Self::Error>;
        delete_key(key: &Key) -> Result<(), Self::Error>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::middleware::defer::segment::Segment;
    use crate::consumer::middleware::defer::store::MessageDeferStoreProvider;
    use crate::consumer::middleware::defer::store::memory::{
        MemoryDeferStore, MemoryDeferStoreProvider,
    };
    use crate::{ConsumerGroup, Partition, Topic};

    /// Test factory that creates memory-backed stores.
    #[derive(Clone)]
    struct TestFactory {
        provider: MemoryDeferStoreProvider,
        segment: Segment,
    }

    impl StoreFactory for TestFactory {
        type Store = MemoryDeferStore;

        async fn create(self) -> Result<Self::Store, <Self::Store as MessageDeferStore>::Error> {
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
            provider: MemoryDeferStoreProvider::new(),
            segment,
        }
    }

    #[tokio::test]
    async fn test_lazy_initialization() -> color_eyre::Result<()> {
        let lazy_store = LazyStore::new(test_factory());

        // Store should not be initialized yet
        assert!(!lazy_store.is_initialized());

        // First access triggers initialization
        let key: Key = Arc::from("test-key");
        let result = lazy_store.get_next_deferred_message(&key).await?;
        assert!(result.is_none());

        // Store should now be initialized
        assert!(lazy_store.is_initialized());

        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_store_operations() -> color_eyre::Result<()> {
        let lazy_store = LazyStore::new(test_factory());

        let key: Key = Arc::from("test-key");
        let offset = Offset::from(42_i64);

        // Defer a message
        lazy_store.defer_first_message(&key, offset).await?;

        // Verify it's stored
        let result = lazy_store.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((offset, 0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_clone_shares_initialization() -> color_eyre::Result<()> {
        let lazy_store = LazyStore::new(test_factory());
        let clone = lazy_store.clone();

        // Initialize via original
        let key: Key = Arc::from("test-key");
        let offset = Offset::from(42_i64);
        lazy_store.defer_first_message(&key, offset).await?;

        // Clone shares the same OnceCell, so sees the same data
        let result = clone.get_next_deferred_message(&key).await?;
        assert_eq!(result, Some((offset, 0)));

        Ok(())
    }
}
