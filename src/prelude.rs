//! Convenience re-exports for the common handler and client setup.
//!
//! `use prosody::prelude::*;` brings everything needed to implement a
//! [`FallibleHandler`] and wire it up with a [`HighLevelClient`] into scope.
//!
//! The only items not included here are those that are already short to import
//! from the crate root (`Topic`, `Key`, `Payload`, etc.) and lower-level items
//! that are only needed for advanced use-cases (custom middleware, the raw
//! `EventHandler` API, etc.).

// ── Implement a handler ──────────────────────────────────────────────────────

/// Demand type received in both `on_message` and `on_timer`.
pub use crate::consumer::DemandType;
/// Trait that provides `.key()` on messages and triggers.
pub use crate::consumer::Keyed;
/// The context bound used in `where C: EventContext` clauses.
pub use crate::consumer::event_context::EventContext;
/// Message type received in `on_message`.
pub use crate::consumer::message::ConsumerMessage;
/// The primary handler trait to implement.
pub use crate::consumer::middleware::FallibleHandler;
/// Timer classification passed to `context.schedule()`.
pub use crate::timers::TimerType;
/// Timer event received in `on_timer`.
pub use crate::timers::Trigger;
/// Timestamp type used as the time argument in `context.schedule()`.
pub use crate::timers::datetime::CompactDateTime;

// ── Error classification ─────────────────────────────────────────────────────

/// Trait to implement on custom error types so retry middleware can classify
/// them.
pub use crate::error::ClassifyError;
/// The three error categories: `Transient`, `Permanent`, `Terminal`.
pub use crate::error::ErrorCategory;

// ── Wire up the high-level client ────────────────────────────────────────────

/// Cassandra connection configuration (required for production timer storage).
pub use crate::cassandra::config::CassandraConfigurationBuilder;
/// Consumer bootstrap and subscription configuration.
pub use crate::consumer::ConsumerConfiguration;
/// Bundled consumer configuration builders passed to `HighLevelClient::new`.
pub use crate::high_level::ConsumerBuilders;
/// The combined producer + consumer client.
pub use crate::high_level::HighLevelClient;
/// Operational mode: `Pipeline`, `LowLatency`, or `BestEffort`.
pub use crate::high_level::mode::Mode;
/// Producer bootstrap and delivery configuration.
pub use crate::producer::ProducerConfiguration;
