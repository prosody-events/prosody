use crate::test_util::captured_spans_filtered;
use crate::timers::{TimerType, timer_span};
use strum::VariantArray;
use tracing_subscriber::filter::LevelFilter;

/// `timer_span!` levels a span by its [`TimerType::is_application`] axis:
/// an INFO-filtered subscriber exports only application timer spans, while
/// every variant exports at DEBUG. Exhaustive over all variants.
#[test]
fn timer_span_level_follows_timer_type() {
    for &timer_type in TimerType::VARIANTS {
        let info = captured_spans_filtered(LevelFilter::INFO, || {
            drop(timer_span!(timer_type, "level_probe"));
        });
        assert_eq!(
            info.len(),
            usize::from(timer_type.is_application()),
            "{timer_type:?} at INFO"
        );

        let debug = captured_spans_filtered(LevelFilter::DEBUG, || {
            drop(timer_span!(timer_type, "level_probe"));
        });
        assert_eq!(debug.len(), 1, "{timer_type:?} at DEBUG");
    }
}
