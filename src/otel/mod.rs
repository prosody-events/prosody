//! OpenTelemetry span relationship types and utilities.
//!
//! Shared across both the consumer and timer subsystems so neither has to
//! depend on the other's module for a common configuration type.

use opentelemetry::Context;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;
use tracing::{Span, debug};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Controls how a new span relates to a propagated OpenTelemetry context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanRelation {
    /// The propagated span becomes this span's `OTel` parent (child-of
    /// relationship).
    #[default]
    Child,
    /// The propagated span is added as an `OTel` link; this span starts a new
    /// trace root (follows-from relationship).
    FollowsFrom,
}

impl FromStr for SpanRelation {
    type Err = ParseSpanRelationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "child" => Ok(Self::Child),
            "follows_from" => Ok(Self::FollowsFrom),
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
/// - [`SpanRelation::FollowsFrom`]: root span in both tracing and `OTel`
///   (`parent: None`); `context`'s span is added as an `OTel` link
///   (follows-from relationship).
///
/// Both arms mark the span `otel.kind = "consumer"` — every `related_span!`
/// site is a consumer-side continuation of a propagated context, and the kind
/// must not vary with the configured relation.
///
/// The span defaults to INFO — related spans are application-facing message
/// and timer continuations. The `level:` form exists for the trigger dispatch
/// span, whose level follows the fired timer's type (see
/// [`TimerType::is_application`](crate::timers::TimerType::is_application));
/// the level must be a constant expression, since a tracing callsite's level
/// is static.
///
/// Span name and fields are macro-expanded at the call site, preserving source
/// location.
///
/// # Example
/// ```rust,no_run
/// use opentelemetry::Context;
/// use prosody::otel::SpanRelation;
/// use prosody::related_span;
/// let timer_spans = SpanRelation::Child;
/// let context = Context::current();
/// let key = "abc";
/// let span = related_span!(timer_spans, context, "timer_defer.load", key = %key);
/// ```
#[macro_export]
macro_rules! related_span {
    ($relation:expr, $context:expr, $name:literal $(, $($fields:tt)*)?) => {
        $crate::related_span!(level: ::tracing::Level::INFO, $relation, $context, $name $(, $($fields)*)?)
    };
    (level: $level:expr, $relation:expr, $context:expr, $name:literal $(, $($fields:tt)*)?) => {{
        let __context: ::opentelemetry::Context = $context;
        match $relation {
            $crate::otel::SpanRelation::Child => {
                let __span = ::tracing::span!($level, $name, otel.kind = "consumer" $(, $($fields)*)?);
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
            $crate::otel::SpanRelation::FollowsFrom => {
                let __span =
                    ::tracing::span!(parent: None, $level, $name, otel.kind = "consumer" $(, $($fields)*)?);
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

/// Makes `span` the child of a carried `OTel` context.
///
/// The sibling of [`related_span!`] for a span that is not a consumer-side
/// continuation: the call site writes the span and names its kind, and this
/// carries the parent. A function rather than a second exported macro, because
/// the kind differs per call site — an outbound peer call is `client` and the
/// listener that receives it is `server` — and no call site needs the macro's
/// source location.
///
/// A context that cannot be attached is logged rather than propagated. A broken
/// trace never fails the work it describes; the span simply appears at the root
/// of its own trace, which is what makes missing propagation visible.
pub(crate) fn carry_parent(span: &Span, context: Context) {
    if let Err(error) = span.set_parent(context) {
        debug!(%error, "a carried trace context could not be attached");
    }
}

/// Error returned when parsing a [`SpanRelation`] from a string fails.
#[derive(Debug, Error)]
#[error("unknown span relation value '{0}'; expected 'child' or 'follows_from'")]
pub struct ParseSpanRelationError(String);

#[cfg(test)]
mod tests;
