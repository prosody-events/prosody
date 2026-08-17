use std::error::Error as StdError;
use std::fmt;
use std::fs;
use std::iter;
use std::path::Path;

use plotters::coord::Shift;
use plotters::coord::types::{RangedCoordf64, RangedCoordu64};
use plotters::prelude::*;
use prosody_scale_core::{PosteriorQuery, TransitionDirection};

use crate::{ControllerTrace, PLOT_FONT_FAMILY, PlotError};

const WIDTH: u32 = 1_440;
const HEATMAP_HEIGHT: u32 = 440;
const SNAPSHOT_HEIGHT: u32 = 480;

pub(crate) struct PosteriorHeatmap {
    pub(crate) at_micros: Vec<u64>,
    pub(crate) values: Vec<f64>,
    pub(crate) probabilities: Vec<f64>,
}

struct PosteriorPanel {
    file: &'static str,
    unit: &'static str,
    heatmap: PosteriorHeatmap,
    prior: Vec<f64>,
    y_label: fn(f64) -> String,
}

struct SnapshotSelection<'a> {
    important_title: String,
    important: &'a [f64],
    final_mass: &'a [f64],
}

/// Writes each retained model posterior as one SVG figure.
///
/// # Errors
///
/// Returns an error when the trace is empty or output fails.
pub fn write_model_belief_figures(
    directory: &Path,
    controller: &ControllerTrace,
) -> Result<(), PlotError> {
    if controller.is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    let panels = model_panels(controller);
    fs::create_dir_all(directory)?;
    for panel in &panels {
        let mut svg = String::new();
        {
            let root =
                SVGBackend::with_string(&mut svg, (WIDTH, HEATMAP_HEIGHT)).into_drawing_area();
            root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
            draw_panel(&root, panel).map_err(|error| drawing_error(&error))?;
            root.present().map_err(|error| drawing_error(&error))?;
        };
        fs::write(
            directory.join(format!("{}.svg", panel.file)),
            svg.replace("<rect ", "<rect shape-rendering=\"crispEdges\" "),
        )?;
    }
    Ok(())
}

/// Writes prior, important-update, and final distributions for each factor.
///
/// # Errors
///
/// Returns an error when the trace is empty or output fails.
pub fn write_model_belief_snapshot_figures(
    directory: &Path,
    controller: &ControllerTrace,
) -> Result<(), PlotError> {
    if controller.is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    let panels = model_panels(controller);
    fs::create_dir_all(directory)?;
    for panel in &panels {
        let mut svg = String::new();
        {
            let root =
                SVGBackend::with_string(&mut svg, (WIDTH, SNAPSHOT_HEIGHT)).into_drawing_area();
            root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
            draw_snapshot_row(&root, panel).map_err(|error| drawing_error(&error))?;
            root.present().map_err(|error| drawing_error(&error))?;
        };
        fs::write(directory.join(format!("{}.svg", panel.file)), svg)?;
    }
    Ok(())
}

pub(crate) fn draw_posterior_heatmap<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordu64, RangedCoordf64>>,
    heatmap: &PosteriorHeatmap,
    final_micros: u64,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let width = heatmap.values.len();
    if width == 0 {
        return Ok(());
    }
    let (pixel_width, pixel_height) = chart.plotting_area().dim_in_pixel();
    let horizontal_overlap = final_micros.div_ceil(u64::from(pixel_width.max(1)));
    let vertical_overlap =
        heatmap.values.last().copied().unwrap_or(0.0_f64) / f64::from(pixel_height.max(1));
    let color_maximum = heatmap
        .probabilities
        .iter()
        .copied()
        .fold(0.0_f64, f64::max)
        .max(f64::MIN_POSITIVE);
    for time_index in 0..heatmap.at_micros.len() {
        let x_start = heatmap.at_micros[time_index];
        let x_end = heatmap
            .at_micros
            .get(time_index + 1)
            .copied()
            .unwrap_or(final_micros)
            .saturating_add(horizontal_overlap)
            .min(final_micros);
        let row_start = time_index * width;
        for value_index in 0..width {
            let Some(&probability) = heatmap.probabilities.get(row_start + value_index) else {
                continue;
            };
            if !probability.is_finite() {
                continue;
            }
            let value = heatmap.values[value_index];
            let lower = if value_index == 0 {
                let upper = heatmap.values.get(1).copied().unwrap_or(value);
                (value - (upper - value) / 2.0_f64).max(0.0_f64)
            } else {
                heatmap.values[value_index - 1].midpoint(value)
            };
            let upper = heatmap.values.get(value_index + 1).map_or_else(
                || {
                    let lower_value = heatmap
                        .values
                        .get(value_index.saturating_sub(1))
                        .copied()
                        .unwrap_or(value);
                    value + (value - lower_value) / 2.0_f64
                },
                |next| value.midpoint(*next),
            ) + vertical_overlap;
            chart.draw_series(iter::once(Rectangle::new(
                [(x_start, lower), (x_end, upper)],
                posterior_color(probability / color_maximum).filled(),
            )))?;
        }
    }
    Ok(())
}

fn draw_snapshot_row<Backend: DrawingBackend>(
    area: &DrawingArea<Backend, Shift>,
    panel: &PosteriorPanel,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let selection = select_snapshots(panel);
    let titles = [
        "prior".to_owned(),
        selection.important_title,
        "final posterior".to_owned(),
    ];
    let masses = [
        panel.prior.as_slice(),
        selection.important,
        selection.final_mass,
    ];
    let y_max = masses
        .iter()
        .flat_map(|mass| mass.iter().copied())
        .fold(0.0_f64, f64::max)
        .max(f64::MIN_POSITIVE)
        * 1.12_f64;
    for ((plot_area, title), mass) in area
        .split_evenly((1, 3))
        .into_iter()
        .zip(titles)
        .zip(masses)
    {
        draw_snapshot(&plot_area, panel, &title, mass, y_max)?;
    }
    Ok(())
}

fn draw_snapshot<Backend: DrawingBackend>(
    area: &DrawingArea<Backend, Shift>,
    panel: &PosteriorPanel,
    title: &str,
    mass: &[f64],
    y_max: f64,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let (x_min, x_max) = value_bounds(&panel.heatmap.values);
    let mut chart = ChartBuilder::on(area)
        .margin_left(10_u32)
        .margin_right(12_u32)
        .margin_top(8_u32)
        .margin_bottom(8_u32)
        .caption(title, (PLOT_FONT_FAMILY, 21_i32).into_font())
        .x_label_area_size(48_u32)
        .y_label_area_size(92_u32)
        .build_cartesian_2d(x_min..x_max, 0.0_f64..y_max)?;
    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(4)
        .y_labels(3)
        .x_desc(panel.unit)
        .y_desc("mass")
        .x_label_formatter(&|value| (panel.y_label)(*value))
        .axis_style(RGBColor(180, 180, 180))
        .label_style(
            (PLOT_FONT_FAMILY, 18_i32)
                .into_font()
                .color(&RGBColor(65, 65, 65)),
        )
        .draw()?;
    chart.draw_series(
        panel
            .heatmap
            .values
            .iter()
            .copied()
            .zip(mass.iter().copied())
            .map(|(value, probability)| {
                PathElement::new(
                    vec![(value, 0.0_f64), (value, probability)],
                    RGBColor(126, 3, 168).stroke_width(2),
                )
            }),
    )?;
    let [low, median, high] = quantiles(&panel.heatmap.values, mass);
    for (value, color, width) in [
        (low, RGBColor(135, 135, 135), 1_u32),
        (high, RGBColor(135, 135, 135), 1_u32),
        (median, RGBColor(240, 125, 35), 2_u32),
    ] {
        chart.draw_series(iter::once(PathElement::new(
            vec![(value, 0.0_f64), (value, y_max * 0.96_f64)],
            color.stroke_width(width),
        )))?;
    }
    Ok(())
}

fn draw_panel<Backend: DrawingBackend>(
    area: &DrawingArea<Backend, Shift>,
    panel: &PosteriorPanel,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let final_micros = panel.heatmap.at_micros.last().copied().unwrap_or(1).max(1);
    let (minimum, maximum) = value_bounds(&panel.heatmap.values);
    let mut chart = ChartBuilder::on(area)
        .margin_left(8_u32)
        .margin_right(12_u32)
        .margin_top(8_u32)
        .margin_bottom(8_u32)
        .x_label_area_size(48_u32)
        .y_label_area_size(96_u32)
        .build_cartesian_2d(0_u64..final_micros, minimum..maximum)?;
    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(3)
        .y_labels(3)
        .x_desc("virtual time")
        .y_desc(panel.unit)
        .x_label_formatter(&|micros| format_time(*micros))
        .y_label_formatter(&|value| (panel.y_label)(*value))
        .axis_style(RGBColor(180, 180, 180))
        .label_style(
            (PLOT_FONT_FAMILY, 20_i32)
                .into_font()
                .color(&RGBColor(65, 65, 65)),
        )
        .draw()?;
    draw_posterior_heatmap(&mut chart, &panel.heatmap, final_micros)
}

fn model_panels(controller: &ControllerTrace) -> Vec<PosteriorPanel> {
    let queries = [
        (
            "partition-share",
            "partition",
            PosteriorQuery::PartitionShare,
        ),
        ("peak-capacity", "operations/s", PosteriorQuery::Capacity),
        ("service-time", "seconds", PosteriorQuery::ServiceTime),
        ("collapse-strength", "fraction", PosteriorQuery::Collapse),
        ("knee-concurrency", "operations", PosteriorQuery::Knee),
        (
            "normal-retry-probability",
            "probability",
            PosteriorQuery::NormalRetryProbability,
        ),
        (
            "failure-retry-probability",
            "probability",
            PosteriorQuery::FailureRetryProbability,
        ),
        (
            "scale-up-lead-time",
            "seconds",
            PosteriorQuery::LeadTime {
                direction: TransitionDirection::Up,
                replica_delta: 1,
            },
        ),
        (
            "scale-down-lead-time",
            "seconds",
            PosteriorQuery::LeadTime {
                direction: TransitionDirection::Down,
                replica_delta: 1,
            },
        ),
        (
            "scale-up-rebalance-time",
            "seconds",
            PosteriorQuery::RebalanceTime {
                direction: TransitionDirection::Up,
                replica_delta: 1,
            },
        ),
        (
            "scale-down-rebalance-time",
            "seconds",
            PosteriorQuery::RebalanceTime {
                direction: TransitionDirection::Down,
                replica_delta: 1,
            },
        ),
    ];
    let mut panels = Vec::with_capacity(12);
    panels.push(PosteriorPanel {
        file: "arrival-rate",
        unit: "events/s",
        heatmap: arrival_heatmap(controller),
        prior: arrival_prior_mass(controller),
        y_label: format_log_rate,
    });
    for (file, unit, query) in queries {
        panels.push(PosteriorPanel {
            file,
            unit,
            heatmap: discrete_heatmap(controller, query),
            prior: controller
                .posterior_prior(query)
                .map_or_else(Vec::new, <[f64]>::to_vec),
            y_label: format_value,
        });
    }
    panels
}

fn discrete_heatmap(controller: &ControllerTrace, query: PosteriorQuery) -> PosteriorHeatmap {
    let values = controller
        .posterior_values(query)
        .map_or_else(Vec::new, <[f64]>::to_vec);
    let width = values.len();
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut probabilities = Vec::with_capacity(controller.len().saturating_mul(width));
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let Some(posterior) = controller.posterior(query, index) else {
            continue;
        };
        at_micros.push(sample.at_micros);
        probabilities.extend_from_slice(posterior);
    }
    PosteriorHeatmap {
        at_micros,
        values,
        probabilities,
    }
}

fn arrival_heatmap(controller: &ControllerTrace) -> PosteriorHeatmap {
    let values = controller
        .arrival_posterior_values()
        .iter()
        .map(|rate| rate.log2())
        .collect::<Vec<_>>();
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut probabilities = Vec::with_capacity(controller.len().saturating_mul(values.len()));
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let Some(posterior) = controller.arrival_posterior(index) else {
            continue;
        };
        probabilities.extend_from_slice(posterior);
        at_micros.push(sample.at_micros);
    }
    PosteriorHeatmap {
        at_micros,
        values,
        probabilities,
    }
}

fn arrival_prior_mass(controller: &ControllerTrace) -> Vec<f64> {
    controller.arrival_prior().to_vec()
}

fn select_snapshots(panel: &PosteriorPanel) -> SnapshotSelection<'_> {
    let width = panel.heatmap.values.len();
    let final_start = panel.heatmap.probabilities.len().saturating_sub(width);
    let final_mass = &panel.heatmap.probabilities[final_start..];
    let mut previous = panel.prior.as_slice();
    let mut important_index = 0_usize;
    let mut maximum_change = 0.0_f64;
    for index in 0..panel.heatmap.at_micros.len() {
        let start = index * width;
        let end = start + width;
        let posterior = &panel.heatmap.probabilities[start..end];
        let change = total_variation(previous, posterior);
        if change > maximum_change {
            maximum_change = change;
            important_index = index;
        }
        previous = posterior;
    }
    let start = important_index * width;
    let end = start + width;
    let important = &panel.heatmap.probabilities[start..end];
    let important_title = if maximum_change == 0.0_f64 {
        "no posterior update".to_owned()
    } else {
        format!(
            "largest update · {}",
            format_time(panel.heatmap.at_micros[important_index])
        )
    };
    SnapshotSelection {
        important_title,
        important,
        final_mass,
    }
}

fn total_variation(before: &[f64], after: &[f64]) -> f64 {
    before
        .iter()
        .zip(after)
        .map(|(left, right)| (left - right).abs())
        .sum::<f64>()
        / 2.0_f64
}

fn quantiles(values: &[f64], mass: &[f64]) -> [f64; 3] {
    let fallback = values.last().copied().unwrap_or(0.0_f64);
    let mut result = [fallback; 3];
    let thresholds = [0.1_f64, 0.5_f64, 0.9_f64];
    let mut threshold = 0_usize;
    let mut cumulative = 0.0_f64;
    for (&value, &probability) in values.iter().zip(mass) {
        cumulative += probability;
        while threshold < thresholds.len() && cumulative >= thresholds[threshold] {
            result[threshold] = value;
            threshold += 1;
        }
    }
    result
}

fn posterior_color(probability: f64) -> RGBColor {
    const STOPS: [[u8; 3]; 5] = [
        [13, 8, 135],
        [126, 3, 168],
        [204, 71, 120],
        [248, 149, 64],
        [240, 249, 33],
    ];
    let scaled = probability.clamp(0.0_f64, 1.0_f64);
    let position = scaled * f64::from((STOPS.len() - 1) as u32);
    let low = (position.floor() as usize).min(STOPS.len() - 1);
    let high = (low + 1).min(STOPS.len() - 1);
    let fraction = position - f64::from(low as u32);
    RGBColor(
        interpolate_channel(STOPS[low][0], STOPS[high][0], fraction),
        interpolate_channel(STOPS[low][1], STOPS[high][1], fraction),
        interpolate_channel(STOPS[low][2], STOPS[high][2], fraction),
    )
}

fn interpolate_channel(low: u8, high: u8, fraction: f64) -> u8 {
    let value = f64::from(low) + (f64::from(high) - f64::from(low)) * fraction;
    value.round().clamp(0.0_f64, 255.0_f64) as u8
}

fn format_time(micros: u64) -> String {
    if micros >= 120_000_000 {
        return format!("{:.1} min", crate::u64_to_f64(micros) / 60_000_000.0_f64);
    }
    format!("{:.1} s", crate::u64_to_f64(micros) / 1_000_000.0_f64)
}

fn value_bounds(values: &[f64]) -> (f64, f64) {
    let Some(&first) = values.first() else {
        return (0.0_f64, 1.0_f64);
    };
    let Some(&last) = values.last() else {
        return (0.0_f64, 1.0_f64);
    };
    if values.len() == 1 {
        let padding = first.abs().max(1.0_f64) * 0.5_f64;
        return (first - padding, first + padding);
    }
    let lower = first - (values[1] - first) / 2.0_f64;
    let upper = last + (last - values[values.len() - 2]) / 2.0_f64;
    (lower, upper)
}

fn format_value(value: f64) -> String {
    if value.abs() < 0.01_f64 {
        return format!("{value:.4}");
    }
    if value.abs() < 1.0_f64 {
        return format!("{value:.2}");
    }
    format!("{value:.1}")
}

fn format_log_rate(value: f64) -> String {
    let rate = 2.0_f64.powf(value);
    if rate < 0.1_f64 {
        return format!("{rate:.3}");
    }
    if rate < 100.0_f64 {
        return format!("{rate:.1}");
    }
    format!("{rate:.0}")
}

fn drawing_error<Error>(error: &DrawingAreaErrorKind<Error>) -> PlotError
where
    Error: StdError + Send + Sync + fmt::Debug,
{
    PlotError::Drawing(format!("{error:?}"))
}

#[cfg(test)]
#[path = "posterior_plot_tests.rs"]
mod tests;
