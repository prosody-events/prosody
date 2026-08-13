use thiserror::Error;

use super::{
    PlotError, Series, Values, label_positions, next_finite_segment, panel, render, series,
};
use crate::{MetricPoint, MetricTrace, PlantError};

#[test]
fn svg_output_is_deterministic_and_escapes_text() -> Result<(), TestError> {
    let mut trace = MetricTrace::new(3)?;
    for (at_micros, arrivals) in [(0_u64, 1_u32), (1_000_000, 2), (2_000_000, 3)] {
        let mut point = MetricPoint::zero(at_micros);
        point.arrivals = u64::from(arrivals);
        point.useful_throughput_per_second = f64::from(arrivals);
        trace.push(point)?;
    }

    let first = render("load < capacity & stable", &trace)?;
    let replay = render("load < capacity & stable", &trace)?;

    assert_eq!(first, replay);
    assert!(first.contains("load &lt; capacity &amp; stable"));
    assert!(!first.contains("load < capacity & stable"));
    Ok(())
}

#[test]
fn missing_values_split_lines_into_visible_segments() {
    let values = [1.0_f64, 2.0_f64, f64::NAN, 3.0_f64, 4.0_f64];
    let series = Series {
        name: "throughput",
        values: Values::F64(&values),
    };
    let mut cursor = 0_usize;
    assert_eq!(
        next_finite_segment(&series, values.len(), &mut cursor),
        Some(0..2)
    );
    assert_eq!(
        next_finite_segment(&series, values.len(), &mut cursor),
        Some(3..5)
    );
    assert_eq!(
        next_finite_segment(&series, values.len(), &mut cursor),
        None
    );
}

#[test]
fn equal_final_values_get_distinct_direct_labels() {
    let values = [1.0_f64];
    let panel = panel(
        "equal",
        "count",
        [
            series("one", Values::F64(&values)),
            series("two", Values::F64(&values)),
            series("three", Values::F64(&values)),
            series("four", Values::F64(&values)),
        ],
    );
    let mut positions = label_positions(&panel, 1, 1.0_f64)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    positions.sort_by(f64::total_cmp);
    assert!(
        positions
            .windows(2)
            .all(|pair| pair[1] - pair[0] >= 0.12_f64)
    );
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    Plant(#[from] PlantError),
    #[error(transparent)]
    Plot(#[from] PlotError),
}
