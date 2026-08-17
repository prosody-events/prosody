//! Composable middleware framework for message processing.
//!
//! This module provides a middleware architecture for building message
//! processing pipelines from reusable components. Each middleware handles a
//! specific cross-cutting concern (retries, concurrency limiting, logging)
//! independently.
//!
//! # Architecture Overview
//!
//! The middleware system transforms your business logic through three layers:
//!
//! ```text
//! Handler → Provider → Middleware Stack → Consumer
//!   │         │           │                │
//!   │         │           │                └─ Kafka partition management
//!   │         │           └─ Cross-cutting concerns
//!   │         └─ Factory pattern for per-partition instances
//!   └─ Your business logic
//! ```
//!
//! ## Components
//!
//! - **Handler**: Your business logic implementing
//!   [`crate::consumer::EventHandler`] or [`FallibleHandler`]
//! - **Provider**: Factory creating handler instances per partition
//!   ([`crate::consumer::HandlerProvider`], [`FallibleHandlerProvider`])
//! - **Middleware**: Composable layers implementing [`HandlerMiddleware`]
//!
//! ## Why Middleware?
//!
//! Message processing requires many cross-cutting concerns: retries,
//! concurrency limits, error logging, dead letter queues, graceful shutdown,
//! and telemetry.
//!
//! Middleware provides:
//! - **Separation of concerns** - Each middleware has one responsibility
//! - **Composability** - Mix and match as needed
//! - **Reusability** - Same middleware works with any handler
//! - **Testability** - Test business logic and infrastructure separately
//!
//! # Available Middleware
//!
//! | Middleware | Purpose |
//! |------------|---------|
//! | [`scheduler`] | Fair work-conserving scheduler with global concurrency limits |
//! | [`retry`] | Exponential backoff for transient failures |
//! | [`cancellation`] | Early exit when already cancelled |
//! | [`telemetry`] | Handler lifecycle observability |
//! | [`topic`] | Dead letter queue routing |
//! | [`log`] | Error categorization and logging |
//! | [`monopolization`] | Detects and prevents key-level execution monopolies |
//!
//! # Usage
//!
//! Compose middleware with [`HandlerMiddleware::layer`] — each call adds a new
//! **outermost** layer — and finalize with
//! [`HandlerMiddleware::into_provider`], which terminates the chain with the
//! handler as the **innermost** component. Retry belongs outermost, since it
//! re-drives the whole stack:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::{RetryMiddleware, RetryConfiguration};
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Payload = serde_json::Value;
//! #     type Error = Infallible;
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_excise<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let handler = MyHandler;
//!
//! // Stack (outer → inner): retry → cancellation → handler.
//! let provider = CancellationMiddleware
//!     .layer(RetryMiddleware::new(retry_config).unwrap())
//!     .into_provider(handler);
//! ```
//!
//! ## Retrying around a rescue layer
//!
//! [`topic::FailureTopicMiddleware`] rescues a failed message to a dead-letter
//! topic. Wrapping it in its own outer [`retry::RetryMiddleware`] retries the
//! DLQ write itself, while the inner retry re-drives the handler:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::topic::*;
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
//! # use prosody::producer::{ProsodyProducer, ProducerConfiguration};
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::telemetry::Telemetry;
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Payload = serde_json::Value;
//! #     type Error = Infallible;
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_excise<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let config = SchedulerConfigurationBuilder::default().build().unwrap();
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let topic_config = FailureTopicConfiguration::builder().failure_topic("dlq").build().unwrap();
//! # let producer_config = ProducerConfiguration::builder().bootstrap_servers(vec!["kafka:9092".to_string()]).build().unwrap();
//! # let producer: ProsodyProducer = ProsodyProducer::new(&producer_config, Telemetry::new().sender()).unwrap();
//! # let telemetry = Telemetry::default();
//! # let my_business_handler = MyHandler;
//!
//! let provider = SchedulerMiddleware::new(&config, &telemetry).unwrap()
//!     .layer(CancellationMiddleware)
//!     .layer(RetryMiddleware::new(retry_config.clone()).unwrap())
//!     .layer(FailureTopicMiddleware::new(topic_config, "consumer-group".to_string(), producer).unwrap())
//!     .layer(RetryMiddleware::new(retry_config).unwrap())
//!     .into_provider(my_business_handler);
//! ```
//!
//! ## Execution Flow
//!
//! Middleware creates an "onion" pattern with bidirectional execution:
//!
//! ```text
//! Request:  Outer → Middle → Inner → Handler
//! Response: Handler → Inner → Middle → Outer
//! ```
//!
//! Each layer can transform requests, handle responses, short-circuit
//! execution, add side effects, or manage resources.
//!
//! ## Error Classification
//!
//! Middleware uses [`ErrorCategory`] for structured error handling:
//!
//! - [`ErrorCategory::Transient`] - Retry with backoff
//! - [`ErrorCategory::Permanent`] - Don't retry, may route to dead letter queue
//! - [`ErrorCategory::Terminal`] - System failure, abort processing

use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::{consumer::DemandType, consumer::event_context::EventContext};

mod boundary;
pub mod cancellation;
mod composition;
/// Message retry mechanism that loads failed messages from specific Kafka
/// offsets.
pub mod deduplication;
pub mod defer;
mod handler;
pub mod log;
pub mod monopolization;
pub mod providers;
pub(crate) mod respond;
pub mod retry;
pub mod scheduler;
mod settle;
pub mod telemetry;
pub mod timeout;
pub mod topic;

// Re-export providers for backwards compatibility and convenience
pub use boundary::FallibleEventHandler;
pub use composition::{ComposedMiddleware, FallibleHandlerProvider, HandlerMiddleware};
pub use handler::FallibleHandler;
pub use providers::{CloneProvider, FallibleCloneProvider, LeafHandler};
pub(crate) use settle::{MarkerWrite, NextAttempt, Settlement, SettlementHandler, abandon, settle};
// `RepinProof` is named by the public `EventContext::redispatch` signature, so
// it must be publicly reachable — its constructor stays module-private, so the
// re-export exposes only an unconstructable token (the `MarkerWrite` idiom).
pub use settle::RepinProof;

#[cfg(test)]
pub(crate) mod tests;
