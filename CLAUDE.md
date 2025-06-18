# CLAUDE.md

Development patterns and practices for Prosody.

## Project Overview

Prosody is a high-level Kafka client library for Rust providing:

- **Kafka Consumer/Producer**: Partition-level parallelism with per-key ordering
- **Timer System**: Distributed scheduling with persistent storage backends (Cassandra, memory)
- **Language Bindings**: FFI interfaces for JavaScript, Python, and Ruby
- **Observability**: Native OpenTelemetry integration for distributed tracing

**Core Principles:**

- Partition-level parallelism while maintaining per-key ordering
- Backpressure management through bounded queues and partition pausing
- Generic abstractions over storage backends
- Zero-allocation patterns for high-throughput processing

## Memories

- Don't add documentation examples unless they're critical to understanding the code
- If a method doesn't have `self`, consider refactoring it to be a function unless the method pattern is idiomatic (for
  example, `new` should be a method even though it does not contain self).
- Run `cargo clippy`, `cargo clippy --tests`, and `cargo doc` after each change and fix any lints.
- Don't use clippy to ignore lints unless told to do so by a human.
- Prefer `use` statements over fully qualified prefixes in code.
- Ask the human before making large, structural changes to code.
- Modules should be documented within the module file.

## Error Handling

**Never use `expect`, `unwrap`, or `panic` (forbidden by lints).**

Use structured error types with `thiserror`:

```rust
#[derive(Debug, Error)]
pub enum TimerManagerError<T>
where
    T: Error + Debug,
{
    #[error("Timer store error: {0:#}")]
    Store(T),

    #[error("Failed to schedule timer: {0:#}")]
    Scheduler(#[from] TimerSchedulerError),
}
```

**Error Classification for Fallible Handlers:**

```rust
#[derive(Debug, Clone, Copy)]
pub enum ErrorType {
    Permanent, // Don't retry - business logic errors
    Transient, // Retry with backoff - network/timeout errors
}

trait ClassifyError {
    fn classify_error(&self) -> ErrorType;
}
```

**Only box errors when Clippy warns about size:**

```rust
pub struct CassandraTriggerStoreError(Box<InnerError>);
```

## Testing

**Use `assert` or `color_eyre::Result` - never `expect`/`unwrap` in tests.**

### Property-Based Testing

Use QuickCheck extensively with custom `Arbitrary` implementations:

```rust
impl Arbitrary for CompactDuration {
    fn arbitrary(g: &mut Gen) -> Self {
        let seconds = u32::arbitrary(g) % (24 * 60 * 60) + 1;
        CompactDuration::new(seconds)
    }
}

#[quickcheck]
fn prop_segment_operations(input: SegmentTestInput) -> TestResult {
    // Property test implementation
}
```

### Test Structure

- **Integration tests**: `tests/` directory with real Kafka brokers
- **Unit tests**: `#[cfg(test)]` modules within source files
- **Property tests**: `src/timers/store/tests/` with macro-generated test suites

### Synchronization vs Sleep

**Never use `sleep` in tests except for backpressure simulation.**

Use proper synchronization:

```rust
// Channel-based waiting
let timer_event = env.expect_timer(5).await?;

// Notification-based coordination with timeouts
let deadline = tokio::time::Instant::now() + timeout;
tokio::select! {
    () = notify.notified() => {},
    () = tokio::time::sleep_until(deadline) => {
        return Err("Timeout waiting for completion".into());
    }
}
```

## API Design

### Trait Design

Keep traits generic - avoid type erasure unless absolutely necessary:

```rust
// Preferred: Generic traits
pub trait EventHandler: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    async fn on_message(&self, context: &impl EventContext, message: &Message) -> Result<(), Self::Error>;
}

// Only create type-erased versions when directly exposed to language bindings
#[async_trait]
pub trait DynEventHandler: Send + Sync {
    // Only needed if trait gets mapped to JS/Python/Ruby objects
}
```

**Key Principle:** Use `#[async_trait]` and type erasure only when directly exposed to language bindings (JS, Python,
Ruby).

### Configuration Builder Pattern

```rust
#[derive(Builder, Clone, Debug, Validate)]
pub struct Configuration {
    # [builder(default = "from_env_with_fallback(\"PROSODY_BOOTSTRAP_SERVERS\", vec![])?\")] 
    bootstrap_servers: Vec<String>,
    
    #[validate(range(min = 1, max = 10000))]
    max_concurrency: usize,
}

impl Configuration {
    #[must_use]
    pub fn builder() -> ConfigurationBuilder {
        ConfigurationBuilder::default()
    }
}
```

## Architecture Patterns

### Consumer Architecture

**Hierarchical Message Processing:**

- **Partition-level parallelism**: Each Kafka partition runs independently
- **Per-key ordering**: Messages with same key processed sequentially
- **Cross-key concurrency**: Different keys processed simultaneously
- **Capacity-based backpressure**: Queues have bounded capacity

```rust
struct ProsodyConsumer {
    partition_managers: HashMap<TopicPartition, PartitionManager>,
    polling_task: JoinHandle<()>,
}

struct PartitionManager {
    keyed_managers: HashMap<Key, KeyedManager>,
    offset_tracker: OffsetTracker,
    capacity_monitor: CapacityMonitor,
}
```

### Timer System Architecture

**Slab-Based Time Partitioning:**

```rust
struct TimerManager<T> {
    store: T,                    // Persistent storage
    scheduler: TimerScheduler,   // In-memory scheduler
    slab_loader: SlabLoader,     // Background preloading
}

trait TriggerStore {
    async fn get_active_slabs(&self, segment_id: SegmentId) -> Result<Vec<SlabId>>;
    async fn get_slab_triggers(&self, slab_id: SlabId) -> Result<Vec<Trigger>>;
}
```

## Storage Patterns

### Storage Abstraction

Generic storage trait with dual indexing:

```rust
#[async_trait]
pub trait TriggerStore: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    // Dual indexing: by segment and by slab
    async fn get_active_slabs(&self, segment_id: SegmentId) -> Result<impl Stream<Item=Result<SlabId, Self::Error>> + Send, Self::Error>;
    async fn get_slab_triggers(&self, slab_id: SlabId) -> Result<impl Stream<Item=Result<Trigger, Self::Error>> + Send, Self::Error>;
}
```

### Cassandra Integration

**Configuration with Environment Fallbacks:**

```rust
#[derive(Builder, Clone, Educe, Validate)]
pub struct CassandraConfiguration {
    # [builder(default = "from_env_with_fallback(\"CASSANDRA_HOSTS\", vec![\"localhost:9042\".to_string()])?\")] 
    pub hosts: Vec<String>,
    
    #[educe(Debug(ignore))]  // Never log passwords
    pub password: Option<String>,
}
```

**TTL Management with Overflow Protection:**

```rust
fn calculate_ttl(&self, time: CompactDateTime) -> Option<i32> {
    const MAX_TTL: i32 = 630_720_000;  // Cassandra's maximum TTL

    Some(
        time.compact_duration_from_now()
            .unwrap_or(CompactDuration::MIN)
            .checked_add(self.base_ttl())
            .unwrap_or(CompactDuration::MAX)
            .seconds()
            .try_into()
            .unwrap_or(MAX_TTL),
    )
        .filter(|&ttl| ttl < MAX_TTL)
}
```

### Memory Storage

**Lock-Free Concurrent Implementation:**

```rust
use scc::{HashMap, hash_map};

#[derive(Clone, Debug)]
pub struct MemoryTriggerStore {
    // Lock-free concurrent HashMap
    segment_to_slabs: HashMap<SegmentId, BTreeSet<SlabId>>,
    slab_to_triggers: HashMap<SlabId, BTreeSet<Trigger>>,
}
```

## Performance Considerations

### Memory Optimization

**Efficient String Types:**

```rust
// Stack-allocated small strings
pub type Key = Flexstr<UUID_STR_LEN>;     // 36 bytes on stack for UUIDs
pub type Topic = Intern<str>;             // String interning for repeated topics

let key: Key = "user-12345".into();  // Stack allocation if <= 36 bytes
let topic: Topic = "events".into();  // Interned, shared across instances
```

### CPU Optimization

**Conditional SIMD JSON Processing:**

```rust
#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_reader_with_buffers;

#[cfg(target_arch = "arm")]
use serde_json::from_reader;
```

**Performance Dependencies:**

```toml
ahash = "0.8"  # ~2x faster than std::HashMap
parking_lot = { features = ["hardware-lock-elision"] }  # Intel TSX support
simd-json = { optional = true }  # SIMD JSON parsing
```

### Build Optimizations

```toml
[profile.release]
codegen-units = 1      # Better optimization
lto = "fat"            # Link-time optimization

# Architecture-specific features
[target.'cfg(not(target_arch = "arm"))'.dependencies]
simd-json = { features = ["serde_impl"] }
```

## Security Considerations

### Input Validation

```rust
#[derive(Builder, Clone, Debug, Validate)]
pub struct ConsumerConfiguration {
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,

    #[validate(range(min = 1_usize))]
    pub max_concurrency: usize,
}

// Always validate at construction
let config = ConsumerConfiguration::builder().build() ?;
config.validate() ?;
```

### Secrets Management

```rust
#[derive(Builder, Clone, Educe, Validate)]
pub struct CassandraConfiguration {
    #[educe(Debug(ignore))]  // Never log passwords
    password: Option<String>,
}

// Safe environment variable parsing - no value leakage in errors
pub fn from_env<T>(env_var: &str) -> Result<T, String>
where
    T: FromStr<Err: Display>
{
    let value_str = get_env_value(env_var)?;
    parse_with_error(env_var, &value_str)
}
```

### Security Principles

- **Validate all inputs** with `#[derive(Validate)]` and field constraints
- **Mark sensitive fields** with `#[educe(Debug(ignore))]` to prevent logging
- **Use environment variables** for all credentials and sensitive config
- **Enable TLS by default** for external connections (OTLP, Cassandra)
- **Forbid unsafe code** through `unsafe_code = "forbid"` lint

## Common Pitfalls

### Async/Await Mistakes

**❌ Blocking operations in async context:**

```rust
async fn bad_processing() {
    std::thread::sleep(Duration::from_secs(1)); // Blocks executor
}
```

**✅ Correct approach:**

```rust
async fn good_processing() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}
```

### Configuration Mistakes

**❌ Missing validation:**

```rust
struct BadConfig {
    max_enqueued_per_key: usize, // Could be 0!
}
```

**✅ Correct approach:**

```rust
#[derive(Builder, Clone, Debug, Validate)]
pub struct Configuration {
    #[validate(range(min = 1, max = 10000))]
    max_enqueued_per_key: usize,
}
```

### Concurrency Mistakes

**❌ Using std::sync in async contexts:**

```rust
use std::sync::Mutex;
async fn bad_async_mutex() {
    let _guard = mutex.lock().unwrap(); // Blocks executor
}
```

**✅ Correct approach:**

```rust
use tokio::sync::RwLock;
async fn good_async_locking() {
    let _guard = lock.read().await; // Non-blocking
}
```

## Documentation Standards

### Function Documentation Template

```rust
/// Brief description of what the function does.
///
/// # Arguments
///
/// * `arg1` - Description of the first argument
/// * `arg2` - Description of the second argument
///
/// # Errors
///
/// This function returns an error if:
/// - Specific condition that causes ErrorType1
/// - Another condition that causes ErrorType2
///
/// # Examples
///
/// ```rust
/// let result = function_name(arg1, arg2)?;
/// assert_eq!(result.field, expected_value);
/// ```
pub fn function_name(arg1: Type1, arg2: Type2) -> Result<ReturnType, ErrorType> {
    // Implementation with inline comments explaining critical operations
}
```

### Documentation Guidelines

- **Include Arguments section** for all functions with parameters
- **Include Errors section** if the function returns `Result`
- **Include Panics section** only if the function can clearly panic
- **Quote types** properly with backticks: [`TypeName`]
- **Place comments above code**, never beside it
- **Avoid passive voice** and empty marketing claims
- **Provide objective, evidence-based descriptions**

## Active Lints

Clippy is configured with aggressive linting:

```toml
[lints.rust]
unsafe_code = "forbid"

[lints.clippy]
expect_used = "deny"
unwrap_used = "deny"
panic = "deny"
pedantic = "warn"
cargo = "warn"
```

**Clippy must pass for both code and tests.**

## Commands

Development workflow:

```bash
make build      # Build project
make check      # Type check without building
make lint       # Run clippy (must pass)
make test       # Run all tests with Kafka
make coverage   # Generate coverage report
make format     # Format code and TOML files
```

Integration testing requires Kafka:

```bash
make up         # Start Kafka services
make test       # Run tests against real Kafka
make reset      # Clean up containers/volumes
```

## Code Style

- Follow existing patterns in the codebase
- Use `thiserror` for structured error types
- Prefer `Result<T, E>` over panicking
- Use `parking_lot` mutexes over `std::sync`
- Implement `Arbitrary` for QuickCheck property testing
- Use `color_eyre::Result` in test functions for rich error context
- Leverage tokio's synchronization primitives (`Notify`, channels, `select!`)
- Box complex error types only when Clippy warns about size
- Use `#[must_use]` for builder methods and important return values
- Use `LazyLock` for expensive static initialization
- Environment variable parsing with typed fallbacks in builders

## Development Best Practices

- Always run `cargo clippy --tests` and fix the lints after changes.