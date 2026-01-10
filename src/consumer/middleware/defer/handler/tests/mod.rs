use std::sync::LazyLock;
use tokio::runtime::{Builder, Runtime};

// Re-export from the shared test support module.
pub use crate::consumer::middleware::test_support::{MockEventContext, TimerOperation};

mod context;
mod generator;
mod handler;
mod harness;
mod integration;
mod loader;
mod properties;
mod types;

pub use loader::{FailableLoader, FailableLoaderError, LoaderFailureType};

/// Base backoff delay in seconds for test handler config.
pub const TEST_BASE_BACKOFF_SECS: u32 = 1;

/// Maximum backoff delay in seconds for test handler config.
pub const TEST_MAX_BACKOFF_SECS: u32 = 3600;

/// Shared multi-threaded runtime for all defer property tests.
///
/// Using a static runtime avoids the overhead of creating a new runtime for
/// each `QuickCheck` iteration. Multi-threaded to support concurrent test
/// execution.
#[allow(clippy::expect_used)]
pub(crate) static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("Failed to create tokio runtime")
});
