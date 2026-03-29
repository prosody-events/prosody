//! OpenTelemetry span relationship types and utilities.
//!
//! Shared across both the consumer and timer subsystems so neither has to
//! depend on the other's module for a common configuration type.

use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;

/// Controls how a new span relates to a propagated OpenTelemetry context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpanRelation {
    /// The propagated span becomes this span's `OTel` parent (child-of
    /// relationship).
    #[default]
    Child,
    /// The propagated span is added as an `OTel` link; this span starts a new
    /// trace root.
    Link,
}

impl FromStr for SpanRelation {
    type Err = ParseSpanRelationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "child" => Ok(Self::Child),
            "link" => Ok(Self::Link),
            _ => Err(ParseSpanRelationError(s.to_owned())),
        }
    }
}

/// Creates a span and connects it to a propagated `OTel` context per the
/// [`SpanRelation`].
///
/// - [`SpanRelation::Child`]: inherits the ambient tracing parent; `OTel`
///   parent is overridden to `context` via `set_parent` (child-of
///   relationship).
/// - [`SpanRelation::Link`]: root span in both tracing and `OTel` (`parent:
///   None`); `context`'s span is added as an `OTel` link (follows-from
///   relationship).
///
/// Span name and fields are macro-expanded at the call site, preserving source
/// location.
///
/// # Example
/// ```rust
/// use prosody::related_span;
/// let span = related_span!(self.timer_relation, context, "fetch_trigger", key = %key);
/// ```
#[macro_export]
macro_rules! related_span {
    ($relation:expr, $context:expr, $name:literal $(, $($fields:tt)*)?) => {{
        let __context: ::opentelemetry::Context = $context;
        match $relation {
            $crate::otel::SpanRelation::Child => {
                let __span = ::tracing::info_span!($name $(, $($fields)*)?);
                if let ::core::result::Result::Err(__e) =
                    ::tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(
                        &__span,
                        __context,
                    )
                {
                    ::tracing::debug!("failed to set parent span: {__e:#}");
                }
                __span
            }
            $crate::otel::SpanRelation::Link => {
                let __span =
                    ::tracing::info_span!(parent: None, $name, otel.kind = "consumer" $(, $($fields)*)?);
                let __span_ctx =
                    ::opentelemetry::trace::TraceContextExt::span(&__context)
                        .span_context()
                        .clone();
                if __span_ctx.is_valid() {
                    ::tracing_opentelemetry::OpenTelemetrySpanExt::add_link_with_attributes(
                        &__span,
                        __span_ctx,
                        vec![::opentelemetry::KeyValue::new(
                            "opentracing.ref_type",
                            "follows_from",
                        )],
                    );
                }
                __span
            }
        }
    }};
}

/// Error returned when parsing a [`SpanRelation`] from a string fails.
#[derive(Debug, Error)]
#[error("unknown span relation value '{0}'; expected 'child' or 'link'")]
pub struct ParseSpanRelationError(String);
