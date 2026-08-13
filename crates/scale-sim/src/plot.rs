use std::error::Error as StdError;
use std::fmt;
use std::fs;
use std::io;
use std::iter::once;
use std::ops::Range;
use std::path::Path;

use plotters::coord::Shift;
use plotters::coord::types::{RangedCoordf64, RangedCoordu64};
use plotters::prelude::*;
use thiserror::Error;

use crate::{MetricTrace, PLOT_FONT_FAMILY};

const WIDTH: u32 = 1_200;
const PANEL_HEIGHT: u32 = 220;
const TITLE_HEIGHT: u32 = 44;
const LABEL_TIME_FRACTION: u64 = 7;
const LABEL_GAP_FRACTION: f64 = 0.12_f64;
const MUTED_COLORS: [RGBColor; 4] = [
    RGBColor(35, 35, 35),
    RGBColor(70, 110, 145),
    RGBColor(110, 110, 110),
    RGBColor(135, 115, 80),
];
const STRONG_RED: RGBColor = RGBColor(180, 45, 35);

/// Writes one deterministic Tufte-style metric plot.
///
/// # Errors
///
/// Returns an error when drawing or file output fails.
pub fn write_metric_svg(path: &Path, title: &str, trace: &MetricTrace) -> Result<(), PlotError> {
    fs::write(path, render(title, trace)?)?;
    Ok(())
}

fn render(title: &str, trace: &MetricTrace) -> Result<String, PlotError> {
    let mut svg = String::new();
    render_into(&mut svg, title, trace)?;
    Ok(svg)
}

fn render_into(svg: &mut String, title: &str, trace: &MetricTrace) -> Result<(), PlotError> {
    let height = TITLE_HEIGHT + PANEL_HEIGHT * 13;
    let root = SVGBackend::with_string(svg, (WIDTH, height)).into_drawing_area();
    draw(&root, title, trace).map_err(|error| drawing_error(&error))?;
    root.present().map_err(|error| drawing_error(&error))?;
    Ok(())
}

fn draw<Backend: DrawingBackend>(
    root: &DrawingArea<Backend, Shift>,
    title: &str,
    trace: &MetricTrace,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    root.fill(&WHITE)?;
    let (title_area, panels_area) = root.split_vertically(TITLE_HEIGHT);
    title_area.draw(&Text::new(
        title,
        (88_i32, 28_i32),
        (PLOT_FONT_FAMILY, 22_i32).into_font().color(&BLACK),
    ))?;
    let panel_areas = panels_area.split_evenly((13, 1));
    for (area, panel) in panel_areas.into_iter().zip(panels(trace)) {
        draw_panel(&area, &panel, &trace.at_micros)?;
    }
    Ok(())
}

fn draw_panel<Backend: DrawingBackend>(
    area: &DrawingArea<Backend, Shift>,
    panel: &Panel<'_>,
    at_micros: &[u64],
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    if !panel.available {
        area.draw(&Text::new(
            panel.name,
            (88_i32, 54_i32),
            (PLOT_FONT_FAMILY, 16_i32).into_font().color(&BLACK),
        ))?;
        area.draw(&Text::new(
            "not collected by this trace",
            (88_i32, 94_i32),
            (PLOT_FONT_FAMILY, 13_i32)
                .into_font()
                .color(&RGBColor(100, 100, 100)),
        ))?;
        return Ok(());
    }
    let maximum = panel_max(panel).max(f64::EPSILON);
    let final_micros = at_micros.last().copied().unwrap_or(1).max(1);
    let label_span_micros = final_micros.div_ceil(LABEL_TIME_FRACTION);
    let x_end_micros = final_micros.saturating_add(label_span_micros);
    let mut chart = ChartBuilder::on(area)
        .margin_left(14_u32)
        .margin_right(16_u32)
        .margin_top(8_u32)
        .margin_bottom(4_u32)
        .caption(panel.name, (PLOT_FONT_FAMILY, 16_i32).into_font())
        .x_label_area_size(28_u32)
        .y_label_area_size(70_u32)
        .build_cartesian_2d(0_u64..x_end_micros, 0.0_f64..maximum * 1.08_f64)?;
    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(4)
        .y_labels(3)
        .x_desc("virtual time")
        .x_label_formatter(&|micros| format_time(*micros))
        .y_desc(panel.unit)
        .axis_style(RGBColor(175, 175, 175))
        .label_style(
            (PLOT_FONT_FAMILY, 11_i32)
                .into_font()
                .color(&RGBColor(70, 70, 70)),
        )
        .draw()?;

    for (series_index, series) in panel.series.iter().enumerate() {
        let color = series_color(series.name, series_index);
        draw_line_segments(&mut chart, series, at_micros, color)?;
    }
    draw_direct_labels(
        &mut chart,
        panel,
        at_micros,
        final_micros.saturating_add(label_span_micros / 3),
        maximum,
    )?;
    Ok(())
}

fn draw_line_segments<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordu64, RangedCoordf64>>,
    series: &Series<'_>,
    at_micros: &[u64],
    color: RGBColor,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let mut cursor = 0_usize;
    while let Some(range) = next_finite_segment(series, at_micros.len(), &mut cursor) {
        chart.draw_series(LineSeries::new(
            range.map(|index| (at_micros[index], series.values.at(index))),
            color.stroke_width(2),
        ))?;
    }
    Ok(())
}

fn next_finite_segment(
    series: &Series<'_>,
    point_count: usize,
    cursor: &mut usize,
) -> Option<Range<usize>> {
    while *cursor < point_count {
        while *cursor < point_count && !series.values.at(*cursor).is_finite() {
            *cursor += 1;
        }
        let start = *cursor;
        while *cursor < point_count && series.values.at(*cursor).is_finite() {
            *cursor += 1;
        }
        if (*cursor).saturating_sub(start) >= 2 {
            return Some(start..*cursor);
        }
    }
    None
}

fn draw_direct_labels<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordu64, RangedCoordf64>>,
    panel: &Panel<'_>,
    at_micros: &[u64],
    label_x_micros: u64,
    maximum: f64,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    if at_micros.is_empty() {
        return Ok(());
    }
    let point_count = at_micros.len();
    let positions = label_positions(panel, point_count, maximum);
    let final_index = point_count - 1;
    let final_x_micros = at_micros[final_index];
    for (series_index, series) in panel.series.iter().enumerate() {
        let value = series.values.at(final_index);
        let Some(label_y) = positions[series_index] else {
            continue;
        };
        let color = series_color(series.name, series_index);
        chart.draw_series(once(PathElement::new(
            vec![(final_x_micros, value), (label_x_micros, label_y)],
            color.stroke_width(1),
        )))?;
        chart.draw_series(once(Text::new(
            format!("{} {value:.3}", series.name),
            (label_x_micros, label_y),
            (PLOT_FONT_FAMILY, 12_i32).into_font().color(&color),
        )))?;
    }
    Ok(())
}

fn format_time(micros: u64) -> String {
    let seconds = micros / 1_000_000;
    let tenths = micros % 1_000_000 / 100_000;
    format!("{seconds}.{tenths} s")
}

fn label_positions(panel: &Panel<'_>, point_count: usize, maximum: f64) -> [Option<f64>; 4] {
    let mut desired = [f64::INFINITY; 4];
    for (series_index, series) in panel.series.iter().enumerate() {
        let value = series.values.at(point_count - 1);
        if value.is_finite() {
            desired[series_index] = value;
        }
    }
    let mut order = [0_usize, 1, 2, 3];
    order.sort_by(|left, right| desired[*left].total_cmp(&desired[*right]));
    let gap = maximum * LABEL_GAP_FRACTION;
    let mut positions = [None; 4];
    let mut previous = -gap;
    let mut final_finite = None;
    for series_index in order {
        if !desired[series_index].is_finite() {
            continue;
        }
        let position = desired[series_index].max(previous + gap);
        positions[series_index] = Some(position);
        previous = position;
        final_finite = Some(series_index);
    }
    if let Some(final_index) = final_finite {
        let overflow = positions[final_index].map_or(0.0_f64, |value| value - maximum);
        if overflow > 0.0_f64 {
            for position in positions.iter_mut().flatten() {
                *position -= overflow;
            }
        }
    }
    positions
}

fn series_color(name: &str, series_index: usize) -> RGBColor {
    if matches!(name, "miss fraction" | "timeouts" | "saturation" | "Hold") {
        STRONG_RED
    } else {
        MUTED_COLORS[series_index % MUTED_COLORS.len()]
    }
}

fn panel_max(panel: &Panel<'_>) -> f64 {
    panel
        .series
        .iter()
        .flat_map(|series| (0..series.values.len()).map(|index| series.values.at(index)))
        .filter(|value| value.is_finite())
        .fold(0.0_f64, f64::max)
}

fn plant_panels(trace: &MetricTrace) -> [Panel<'_>; 6] {
    [
        panel(
            "traffic",
            "events",
            [
                series("arrivals", Values::U64(&trace.arrivals)),
                series("backlog", Values::U64(&trace.backlog)),
                series("timers", Values::U64(&trace.timers)),
                series("useful", Values::U64(&trace.useful_completions)),
            ],
        ),
        panel(
            "outcomes",
            "events or fraction",
            [
                series("transient", Values::U64(&trace.transient_failures)),
                series("rejected", Values::U64(&trace.permanent_rejections)),
                series("timeouts", Values::U64(&trace.timeouts)),
                series("miss fraction", Values::F64(&trace.miss_fraction)),
            ],
        ),
        panel(
            "sojourn",
            "microseconds",
            [
                series("p50", Values::U64(&trace.latency_p50_micros)),
                series("p90", Values::U64(&trace.latency_p90_micros)),
                series("p99", Values::U64(&trace.latency_p99_micros)),
                series("p99.9", Values::U64(&trace.latency_p999_micros)),
            ],
        ),
        panel(
            "latency components",
            "microseconds",
            [
                series("permit p99", Values::U64(&trace.permit_wait_p99_micros)),
                series(
                    "handler elapsed p99",
                    Values::U64(&trace.handler_elapsed_p99_micros),
                ),
                series("settle p99", Values::U64(&trace.settle_p99_micros)),
                series("recovery", Values::U64(&trace.recovery_micros)),
            ],
        ),
        panel(
            "queues",
            "events",
            [
                series("queue mean", Values::F64(&trace.queue_mean)),
                series("queue max", Values::U64(&trace.queue_max)),
                series("RIF p50", Values::U64(&trace.requests_in_flight_p50)),
                series("RIF p99", Values::U64(&trace.requests_in_flight_p99)),
            ],
        ),
        panel(
            "utilization",
            "fraction",
            [
                series("handler mean", Values::F64(&trace.handler_utilization_mean)),
                series("handler max", Values::F64(&trace.handler_utilization_max)),
                series("handler CV", Values::F64(&trace.handler_utilization_cv)),
                series("dependency", Values::F64(&trace.dependency_utilization)),
            ],
        ),
    ]
}

fn model_panels(trace: &MetricTrace) -> [Panel<'_>; 7] {
    [
        panel(
            "resource capacity",
            "operations per second",
            [
                series(
                    "completed attempts",
                    Values::F64(&trace.attempt_throughput_per_second),
                ),
                series("capacity low", Values::F64(&trace.capacity_low_per_second)),
                series(
                    "capacity median",
                    Values::F64(&trace.capacity_median_per_second),
                ),
                series(
                    "capacity high",
                    Values::F64(&trace.capacity_high_per_second),
                ),
            ],
        ),
        panel(
            "resource inference",
            "ratio",
            [
                series("concurrency", Values::F64(&trace.resource_concurrency)),
                series("saturation", Values::F64(&trace.saturation_probability)),
                series("no knee", Values::F64(&trace.no_knee_probability)),
                series("expected loss", Values::F64(&trace.expected_loss)),
            ],
        ),
        panel(
            "prediction",
            "events",
            [
                series("observed", Values::U64(&trace.arrivals)),
                series("low", Values::F64(&trace.prediction_low)),
                series("median", Values::F64(&trace.prediction_median)),
                series("high", Values::F64(&trace.prediction_high)),
            ],
        ),
        panel(
            "control",
            "replicas",
            [
                series("replicas", Values::U32(&trace.replicas)),
                series("target", Values::U32(&trace.target)),
                series("cap", Values::U32(&trace.cap)),
                series("Hold", Values::Bool(&trace.hold)),
            ],
        ),
        panel(
            "actuation lead time",
            "seconds",
            [
                series("scale up", Values::F64(&trace.lead_time_up_seconds)),
                series("scale down", Values::F64(&trace.lead_time_down_seconds)),
                series("active change", Values::F64(&trace.lead_time_seconds)),
                missing_series(trace.at_micros.len()),
            ],
        ),
        panel(
            "transport and cost",
            "count or seconds",
            [
                series("snapshot age", Values::U64(&trace.snapshot_age_micros)),
                series("missing", Values::U32(&trace.missing_reporters)),
                series("scale actions", Values::U32(&trace.scale_actions)),
                series("replica-seconds", Values::F64(&trace.replica_seconds)),
            ],
        ),
        panel(
            "controller cost",
            "count, bytes, or nanoseconds",
            [
                series("step ns", Values::U64(&trace.step_nanos)),
                series("allocations", Values::U64(&trace.step_allocations)),
                series("retained bytes", Values::U64(&trace.retained_bytes)),
                series("model time", Values::U64(&trace.at_micros)),
            ],
        ),
    ]
}

fn panels(trace: &MetricTrace) -> [Panel<'_>; 13] {
    let [traffic, outcomes, sojourn, latency, queues, utilization] = plant_panels(trace);
    let [
        resource,
        resource_inference,
        prediction,
        control,
        actuation,
        transport,
        controller,
    ] = model_panels(trace);
    let mut panels = [
        traffic,
        outcomes,
        sojourn,
        latency,
        queues,
        utilization,
        resource,
        resource_inference,
        prediction,
        control,
        actuation,
        transport,
        controller,
    ];
    if !trace.complete_metrics {
        for panel_index in [6_usize, 7, 8, 9, 10, 11, 12] {
            panels[panel_index].available = false;
        }
        panels[6].available = trace.resource_metrics;
        panels[7].available = trace.resource_metrics;
        panels[9].available = trace.controller_metrics;
        panels[10].available = trace.controller_metrics;
    }
    panels
}

fn panel<'a>(name: &'static str, unit: &'static str, series: [Series<'a>; 4]) -> Panel<'a> {
    Panel {
        name,
        unit,
        series,
        available: true,
    }
}

fn series<'a>(name: &'static str, values: Values<'a>) -> Series<'a> {
    Series { name, values }
}

fn missing_series(length: usize) -> Series<'static> {
    series("", Values::Missing(length))
}

struct Panel<'a> {
    name: &'static str,
    unit: &'static str,
    series: [Series<'a>; 4],
    available: bool,
}

struct Series<'a> {
    name: &'static str,
    values: Values<'a>,
}

enum Values<'a> {
    U64(&'a [u64]),
    U32(&'a [u32]),
    F64(&'a [f64]),
    Bool(&'a [bool]),
    Missing(usize),
}

impl Values<'_> {
    fn len(&self) -> usize {
        match self {
            Self::U64(values) => values.len(),
            Self::U32(values) => values.len(),
            Self::F64(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::Missing(length) => *length,
        }
    }

    fn at(&self, index: usize) -> f64 {
        match self {
            Self::U64(values) => u64_f64(values[index]),
            Self::U32(values) => f64::from(values[index]),
            Self::F64(values) => values[index],
            Self::Bool(values) => f64::from(u8::from(values[index])),
            Self::Missing(_) => f64::NAN,
        }
    }
}

fn u64_f64(value: u64) -> f64 {
    let high = (value >> 32_u32) as u32;
    let low = value as u32;
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn drawing_error<Error>(error: &DrawingAreaErrorKind<Error>) -> PlotError
where
    Error: StdError + Send + Sync + fmt::Debug,
{
    PlotError::Drawing(format!("{error:?}"))
}

/// Plot generation failure.
#[derive(Debug, Error)]
pub enum PlotError {
    /// The requested plot has no samples.
    #[error("the plot trace must not be empty")]
    EmptyTrace,
    /// Plotters could not draw the requested plot.
    #[error("plot drawing failed: {0}")]
    Drawing(String),
    /// File output failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[cfg(test)]
#[path = "plot_tests.rs"]
mod tests;
