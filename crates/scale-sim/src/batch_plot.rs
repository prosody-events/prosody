use std::error::Error as StdError;
use std::fmt;
use std::iter::once;
use std::path::Path;

use plotters::coord::Shift;
use plotters::coord::types::{RangedCoordf64, RangedCoordusize};
use plotters::prelude::*;

use crate::{BatchSloSummary, PLOT_FONT_FAMILY, PlotError};

const WIDTH: u32 = 1_200;
const PANEL_HEIGHT: u32 = 220;
const TITLE_HEIGHT: u32 = 44;
const BLUE: RGBColor = RGBColor(70, 110, 145);
const BLACK: RGBColor = RGBColor(35, 35, 35);
const RED: RGBColor = RGBColor(180, 45, 35);

/// Writes the batch SLO sweep as a deterministic SVG plot.
///
/// # Errors
///
/// Returns an error when the sweep is empty or drawing fails.
pub fn write_batch_slo_svg(path: &Path, summaries: &[BatchSloSummary]) -> Result<(), PlotError> {
    if summaries.is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    let root = SVGBackend::new(path, (WIDTH, TITLE_HEIGHT + PANEL_HEIGHT * 4)).into_drawing_area();
    draw(&root, summaries).map_err(|error| drawing_error(&error))?;
    root.present().map_err(|error| drawing_error(&error))?;
    Ok(())
}

/// Writes desired and actual batch replica trajectories.
///
/// # Errors
///
/// Returns an error when the sweep is empty or drawing fails.
pub fn write_batch_actuation_svg(
    path: &Path,
    summaries: &[BatchSloSummary],
) -> Result<(), PlotError> {
    if summaries.is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    let root = SVGBackend::new(path, (WIDTH, 360)).into_drawing_area();
    root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
    let (title, panels) = root.split_vertically(TITLE_HEIGHT);
    title
        .draw(&Text::new(
            "desired and actual replicas during pod readiness",
            (88_i32, 28_i32),
            (PLOT_FONT_FAMILY, 22_i32).into_font().color(&BLACK),
        ))
        .map_err(|error| drawing_error(&error))?;
    let areas = panels.split_evenly((1, summaries.len()));
    for (area, summary) in areas.iter().zip(summaries) {
        draw_actuation_panel(area, summary).map_err(|error| drawing_error(&error))?;
    }
    root.present().map_err(|error| drawing_error(&error))?;
    Ok(())
}

fn draw_actuation_panel<Backend: DrawingBackend>(
    area: &DrawingArea<Backend, Shift>,
    summary: &BatchSloSummary,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let ready_seconds = micros_seconds(summary.actuation_micros);
    let x_max = 95.0_f64;
    let y_max = f64::from(summary.cap.max(summary.target)).max(1.0_f64) * 1.08_f64;
    let mut chart = ChartBuilder::on(area)
        .margin_left(8_u32)
        .margin_right(8_u32)
        .margin_top(8_u32)
        .margin_bottom(8_u32)
        .caption(
            format!("{:.0} h budget", budget_hours(summary)),
            (PLOT_FONT_FAMILY, 15_i32).into_font(),
        )
        .x_label_area_size(34_u32)
        .y_label_area_size(46_u32)
        .build_cartesian_2d(0.0_f64..x_max, 0.0_f64..y_max)?;
    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(4)
        .y_labels(4)
        .x_desc("seconds after decision")
        .y_desc("replicas")
        .axis_style(RGBColor(175, 175, 175))
        .label_style(
            (PLOT_FONT_FAMILY, 10_i32)
                .into_font()
                .color(&RGBColor(70, 70, 70)),
        )
        .draw()?;
    let desired = f64::from(summary.target);
    let initial = f64::from(summary.initial_replicas);
    chart.draw_series(LineSeries::new(
        [(0.0_f64, desired), (x_max, desired)],
        BLUE.stroke_width(2),
    ))?;
    chart.draw_series(LineSeries::new(
        [
            (0.0_f64, initial),
            (ready_seconds, initial),
            (ready_seconds, desired),
            (x_max, desired),
        ],
        BLACK.stroke_width(2),
    ))?;
    chart.draw_series(once(Text::new(
        format!("desired {}", summary.target),
        (2.0_f64, desired),
        (PLOT_FONT_FAMILY, 11_i32).into_font().color(&BLUE),
    )))?;
    chart.draw_series(once(Text::new(
        format!("actual ready at {ready_seconds:.0} s"),
        (ready_seconds, initial),
        (PLOT_FONT_FAMILY, 11_i32).into_font().color(&BLACK),
    )))?;
    Ok(())
}

fn draw<Backend: DrawingBackend>(
    root: &DrawingArea<Backend, Shift>,
    summaries: &[BatchSloSummary],
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    root.fill(&WHITE)?;
    let (title, panels) = root.split_vertically(TITLE_HEIGHT);
    title.draw(&Text::new(
        "50,000-job batch objective sweep",
        (88_i32, 28_i32),
        (PLOT_FONT_FAMILY, 22_i32).into_font().color(&BLACK),
    ))?;
    let areas = panels.split_evenly((4, 1));
    draw_panel(
        &areas[0],
        summaries,
        "controller decision",
        "replicas",
        ("target", target, BLUE),
        Some(("cap", cap, BLACK)),
    )?;
    draw_panel(
        &areas[1],
        summaries,
        "realized objective",
        "miss fraction",
        ("miss fraction", miss_fraction, RED),
        Some(("epsilon", epsilon, BLACK)),
    )?;
    draw_panel(
        &areas[2],
        summaries,
        "cost through final settlement",
        "replica-hours",
        ("replica-hours", replica_hours, BLUE),
        None,
    )?;
    draw_panel(
        &areas[3],
        summaries,
        "batch completion",
        "hours",
        ("completion", completion_hours, BLACK),
        None,
    )?;
    Ok(())
}

type Value = fn(&BatchSloSummary) -> f64;
type Line = (&'static str, Value, RGBColor);

fn draw_panel<Backend: DrawingBackend>(
    area: &DrawingArea<Backend, Shift>,
    summaries: &[BatchSloSummary],
    title: &'static str,
    unit: &'static str,
    first: Line,
    second: Option<Line>,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let maximum = summaries
        .iter()
        .flat_map(|summary| {
            [
                first.1(summary),
                second.map_or(0.0_f64, |line| line.1(summary)),
            ]
        })
        .fold(f64::EPSILON, f64::max);
    let x_end = summaries.len() * 2 + 1;
    let no_axis_label = |_: &usize| String::new();
    let mut chart = ChartBuilder::on(area)
        .margin_left(14_u32)
        .margin_right(16_u32)
        .margin_top(8_u32)
        .margin_bottom(4_u32)
        .caption(title, (PLOT_FONT_FAMILY, 16_i32).into_font())
        .x_label_area_size(32_u32)
        .y_label_area_size(70_u32)
        .build_cartesian_2d(0_usize..x_end, 0.0_f64..maximum * 1.08_f64)?;
    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(1)
        .x_label_formatter(&no_axis_label)
        .x_desc("latency budget (hours)")
        .y_desc(unit)
        .axis_style(RGBColor(175, 175, 175))
        .label_style(
            (PLOT_FONT_FAMILY, 11_i32)
                .into_font()
                .color(&RGBColor(70, 70, 70)),
        )
        .draw()?;
    draw_line(&mut chart, summaries, first, 0.0_f64)?;
    draw_budget_labels(&mut chart, summaries, first.1)?;
    if let Some(line) = second {
        draw_line(&mut chart, summaries, line, maximum * 0.07_f64)?;
    }
    Ok(())
}

fn draw_budget_labels<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordusize, RangedCoordf64>>,
    summaries: &[BatchSloSummary],
    value: Value,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    chart.draw_series(summaries.iter().enumerate().map(|(index, summary)| {
        EmptyElement::at((index * 2, value(summary)))
            + Text::new(
                format!("{:.0} h", budget_hours(summary)),
                (-8_i32, -9_i32),
                (PLOT_FONT_FAMILY, 10_i32)
                    .into_font()
                    .color(&RGBColor(90, 90, 90)),
            )
    }))?;
    Ok(())
}

fn draw_line<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordusize, RangedCoordf64>>,
    summaries: &[BatchSloSummary],
    line: Line,
    label_offset: f64,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    chart.draw_series(LineSeries::new(
        summaries
            .iter()
            .enumerate()
            .map(|(index, summary)| (index * 2, line.1(summary))),
        line.2.stroke_width(2),
    ))?;
    let final_index = summaries.len() - 1;
    let final_value = line.1(&summaries[final_index]);
    chart.draw_series(once(Text::new(
        format!("{} {final_value:.3}", line.0),
        (summaries.len() * 2, final_value + label_offset),
        (PLOT_FONT_FAMILY, 12_i32).into_font().color(&line.2),
    )))?;
    Ok(())
}

fn budget_hours(summary: &BatchSloSummary) -> f64 {
    DurationValue::hours(summary.budget_micros)
}

fn target(summary: &BatchSloSummary) -> f64 {
    f64::from(summary.target)
}

fn cap(summary: &BatchSloSummary) -> f64 {
    f64::from(summary.cap)
}

fn miss_fraction(summary: &BatchSloSummary) -> f64 {
    summary.miss_fraction
}

fn epsilon(summary: &BatchSloSummary) -> f64 {
    summary.epsilon
}

fn replica_hours(summary: &BatchSloSummary) -> f64 {
    summary.replica_seconds / 3_600.0_f64
}

fn completion_hours(summary: &BatchSloSummary) -> f64 {
    DurationValue::hours(summary.completion_micros)
}

fn micros_seconds(micros: u64) -> f64 {
    DurationValue::micros(micros) / 1_000_000.0_f64
}

struct DurationValue;

impl DurationValue {
    fn hours(micros: u64) -> f64 {
        Self::micros(micros) / 3_600_000_000.0_f64
    }

    fn micros(micros: u64) -> f64 {
        let high = (micros >> 32_u32) as u32;
        let low = micros as u32;
        f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
    }
}

fn drawing_error<Error>(error: &DrawingAreaErrorKind<Error>) -> PlotError
where
    Error: StdError + Send + Sync + fmt::Debug,
{
    PlotError::Drawing(format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{write_batch_actuation_svg, write_batch_slo_svg};
    use crate::PlotError;

    #[test]
    fn empty_batch_sweep_is_rejected_before_file_creation() {
        let result = write_batch_slo_svg(Path::new("unused.svg"), &[]);
        assert!(matches!(result, Err(PlotError::EmptyTrace)));
        let result = write_batch_actuation_svg(Path::new("unused.svg"), &[]);
        assert!(matches!(result, Err(PlotError::EmptyTrace)));
    }
}
